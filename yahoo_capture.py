#!/usr/bin/env python3
"""Capture Yahoo BTC-USD, QQQ, and SPY histories for the private model.

This standalone publisher contains no score, signal, action, rate-weighting, or
portfolio logic.  It is intended for a repository that exposes public market
data only.  The private model independently reparses and revalidates every
published source response before scoring.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import exchange_calendars as xc

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
ASSETS = ("BTC", "QQQ", "SPY")
SYMBOL = {"BTC": "BTC-USD", "QQQ": "QQQ", "SPY": "SPY"}
START = {"BTC": date(2016, 1, 1), "QQQ": date(1999, 3, 10), "SPY": date(1999, 3, 10)}
BASIS = {"BTC": "SPOT_CLOSE", "QQQ": "TOTAL_RETURN_ADJUSTED_CLOSE", "SPY": "TOTAL_RETURN_ADJUSTED_CLOSE"}
SOURCE = {"BTC": "Yahoo chart BTC-USD close", "QQQ": "Yahoo chart QQQ adjusted close", "SPY": "Yahoo chart SPY adjusted close"}


def numeric(value) -> float:
    if isinstance(value, bool):
        raise ValueError("Boolean is not a price or timestamp")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Nonfinite vendor value")
    return result


def utc_period_end(day: date) -> datetime:
    return datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC)


def expected_latest(asset: str, now: datetime) -> date:
    if asset == "BTC":
        return now.date() - timedelta(days=1)
    start = now.date() - timedelta(days=14)
    calendar = xc.get_calendar("XNYS", start=str(start - timedelta(days=7)), end=str(now.date() + timedelta(days=7)))
    closed = [session.date() for session in calendar.sessions_in_range(str(start), str(now.date()))
              if calendar.session_close(session).to_pydatetime() <= now]
    if not closed:
        raise ValueError("Cannot identify the latest completed exchange session")
    return closed[-1]


def parse_yahoo(payload: dict, asset: str, asof: datetime) -> list[dict]:
    chart = payload["chart"]
    if chart.get("error"):
        raise ValueError(str(chart["error"]))
    if not isinstance(chart.get("result"), list) or len(chart["result"]) != 1:
        raise ValueError("Exactly one Yahoo instrument response is required")
    result = chart["result"][0]
    meta = result.get("meta", {})
    if meta.get("symbol") != SYMBOL[asset] or meta.get("currency") != "USD" or meta.get("dataGranularity") != "1d":
        raise ValueError("Yahoo symbol/currency/daily-interval metadata mismatch")
    times = result["timestamp"]
    close = result["indicators"]["quote"][0]["close"]
    adj_block = result.get("indicators", {}).get("adjclose")
    adjusted = adj_block[0].get("adjclose") if adj_block else close
    if len(times) != len(close) or len(times) != len(adjusted):
        raise ValueError("Yahoo array lengths differ")
    calendar = None if asset == "BTC" else xc.get_calendar(
        "XNYS", start=str(START[asset]), end=str(asof.date() + timedelta(days=3)))
    rows = []
    for epoch, adj, raw_close in zip(times, adjusted, close):
        if adj is None or raw_close is None:
            raise ValueError("Yahoo missing daily close; no filling permitted")
        stamp = numeric(epoch)
        if stamp != int(stamp):
            raise ValueError("Fractional Yahoo epoch")
        if asset == "BTC":
            day = datetime.fromtimestamp(stamp, UTC).date()
            if day < START[asset] or day >= asof.date():
                continue
            close_value = numeric(raw_close)
            if close_value <= 0:
                raise ValueError("Invalid BTC close")
            rows.append({"date": str(day), "close": close_value,
                         "bar_end_utc": utc_period_end(day).isoformat(),
                         "source": SOURCE[asset], "basis": BASIS[asset]})
        else:
            day = datetime.fromtimestamp(stamp, UTC).astimezone(NY).date()
            if day < START[asset]:
                continue
            if day > asof.date() or not calendar.is_session(str(day)):
                raise ValueError("Future or non-session Yahoo equity observation")
            end = calendar.session_close(str(day)).to_pydatetime()
            if end > asof:
                continue
            adj_value, raw_value = numeric(adj), numeric(raw_close)
            if min(adj_value, raw_value) <= 0:
                raise ValueError("Invalid equity close")
            rows.append({"date": str(day), "close": adj_value, "raw_close": raw_value,
                         "bar_end_utc": end.isoformat(), "source": SOURCE[asset],
                         "basis": BASIS[asset]})
    if not rows or rows[0]["date"] != str(START[asset]):
        raise ValueError("Canonical history has the wrong start")
    if rows[-1]["date"] != str(expected_latest(asset, asof)):
        raise ValueError("Canonical history is stale or missing the latest completed bar")
    actual_days = [date.fromisoformat(row["date"]) for row in rows]
    if asset == "BTC":
        expected_days = [START[asset] + timedelta(days=i)
                         for i in range((expected_latest(asset, asof) - START[asset]).days + 1)]
    else:
        expected_days = [session.date() for session in calendar.sessions_in_range(
            str(START[asset]), str(expected_latest(asset, asof)))]
    if actual_days != expected_days:
        raise ValueError(f"{asset}: missing, duplicate, or non-session daily observations")
    return rows


def fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "BTC-QQQ-public-capture/1.0 (public price data; no orders)"})
    with urlopen(request, timeout=30) as response:
        if getattr(response, "status", 200) != 200:
            raise ValueError(f"Yahoo returned HTTP {response.status}")
        return response.read()


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def capture(out: Path, asof: datetime) -> Path:
    out.mkdir(parents=True, exist_ok=False)
    (out / "responses").mkdir()
    manifest = {"schema_version": "2.0.0", "as_of_utc": asof.isoformat(), "assets": {},
                "macro_status": "NOT SUPPLIED — SHADOW LAYER ONLY"}
    for asset in ASSETS:
        start_epoch = int(datetime.combine(START[asset], datetime.min.time(), UTC).timestamp())
        url = "https://query1.finance.yahoo.com/v8/finance/chart/" + SYMBOL[asset] + "?" + urlencode({
            "period1": start_epoch, "period2": int(asof.timestamp()), "interval": "1d", "events": "div,splits"})
        raw = fetch(url)
        digest = hashlib.sha256(raw).hexdigest()
        response_name = f"responses/{digest}.json"
        (out / response_name).write_bytes(raw)
        rows = parse_yahoo(json.loads(raw), asset, asof)
        csv_path = out / f"{asset}_daily.csv"
        write_csv(csv_path, rows)
        manifest["assets"][asset] = {
            "quality": "VERIFIED_SINGLE_BASIS",
            "daily": {"file": csv_path.name, "basis": BASIS[asset],
                      "sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest()},
            "source_receipts": [{"url": url, "retrieved_at_utc": datetime.now(UTC).isoformat(),
                                 "response_sha256": digest, "response_file": response_name}],
        }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return out / "manifest.json"


def make_bundle(source: Path, bundle: Path) -> str:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix="capture-", suffix=".zip", dir=bundle.parent, delete=False) as temp:
        temporary = Path(temp.name)
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for path in sorted(source.rglob("*")):
                if path.is_file():
                    archive.write(path, f"verified_prices/{path.relative_to(source).as_posix()}")
        os.replace(temporary, bundle)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(bundle.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--asof")
    args = parser.parse_args()
    asof = datetime.fromisoformat(args.asof.replace("Z", "+00:00")).astimezone(UTC) if args.asof else datetime.now(UTC)
    with tempfile.TemporaryDirectory(prefix="yahoo-capture-") as temp_name:
        root = Path(temp_name) / "verified_prices"
        try:
            capture(root, asof)
            digest = make_bundle(root, args.bundle.resolve())
        except Exception as exc:
            print(json.dumps({"status": "FAILED", "reason": f"{type(exc).__name__}: {exc}"}, indent=2))
            return 2
    print(json.dumps({"status": "PASSED", "bundle": str(args.bundle.resolve()),
                      "sha256": digest, "as_of_utc": asof.isoformat(), "assets": list(ASSETS)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
