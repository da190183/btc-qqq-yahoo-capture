#!/usr/bin/env python3
"""Capture official FRED/Federal Reserve rate histories for the shadow model.

This standalone publisher contains no score, signal, action, fitted-weight,
portfolio, or order logic.  It publishes only timestamped source observations.
The private model independently verifies the archived source responses and
reconstructs the canonical observation file before running any shadow fit.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import tempfile
import zipfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

UTC = timezone.utc
SCHEMA_VERSION = "1.0.0"
ROOT_NAME = "verified_rates"
H15_IDS = ("DGS2", "DGS10", "DFII10")
POLICY_IDS = ("DFEDTAR", "DFEDTARL", "DFEDTARU")
SOURCE_IDS = H15_IDS + POLICY_IDS
URL = {series_id: f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
       for series_id in SOURCE_IDS}
START = date(1999, 3, 10)
RANGE_START = date(2008, 12, 16)
REAL_START = date(2003, 1, 2)
MODEL_FEATURES = (
    "policy_change_1_bps",
    "policy_change_long_bps",
    "nominal_2y_change_1_bps",
    "nominal_10y_change_1_bps",
    "real_10y_change_1_bps",
    "real_10y_change_long_bps",
)
OUTPUT_SERIES = {
    "DGS2": "NOMINAL_2Y",
    "DGS10": "NOMINAL_10Y",
    "DFII10": "REAL_10Y",
}
OBSERVATION_FIELDS = (
    "series", "value", "unit", "observed_at_utc", "available_at_utc", "source",
)
POLICY_EVENT_FIELDS = (
    "event_id", "announced_at_utc", "available_at_utc", "actual_change_bps",
    "expected_change_bps", "expectation_asof_utc", "expectation_available_at_utc",
    "expectation_event_id", "expectation_source", "path_before_pct", "path_after_pct",
    "path_before_at_utc", "path_after_at_utc", "path_available_at_utc", "path_source",
    "path_instrument", "source",
)


def numeric(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Nonfinite FRED observation")
    return result


def parse_fred(raw: bytes, series_id: str) -> list[tuple[date, float]]:
    """Strictly parse one FRED graph CSV response."""
    if series_id not in SOURCE_IDS:
        raise ValueError(f"Unsupported FRED series: {series_id}")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("FRED response is not UTF-8 CSV") from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if not reader.fieldnames:
        raise ValueError("FRED response has no CSV header")
    date_field = "observation_date" if "observation_date" in reader.fieldnames else "DATE"
    if date_field not in reader.fieldnames or series_id not in reader.fieldnames:
        raise ValueError(f"FRED response header does not identify {series_id}")
    rows: list[tuple[date, float]] = []
    seen: set[date] = set()
    for record in reader:
        raw_date = (record.get(date_field) or "").strip()
        raw_value = (record.get(series_id) or "").strip()
        if not raw_date:
            raise ValueError("FRED response contains a blank observation date")
        day = date.fromisoformat(raw_date)
        if day in seen:
            raise ValueError(f"Duplicate FRED observation date for {series_id}: {day}")
        seen.add(day)
        if raw_value in ("", "."):
            continue
        value = numeric(raw_value)
        if not -10.0 <= value <= 30.0:
            raise ValueError(f"Implausible percent-per-annum value for {series_id}: {value}")
        if rows and day <= rows[-1][0]:
            raise ValueError(f"FRED observations are not strictly ordered for {series_id}")
        rows.append((day, value))
    if not rows:
        raise ValueError(f"FRED response contains no usable observations for {series_id}")
    return rows


def next_weekday(day: date) -> date:
    candidate = day + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def available_at(day: date, h15: bool) -> datetime:
    """Use conservative availability rather than treating an observation as instant.

    H.15 values are not admitted until 23:59:59 UTC on the following weekday.
    Policy targets are admitted at midnight UTC following their effective date.
    """
    available_day = next_weekday(day) if h15 else day + timedelta(days=1)
    available_time = time(23, 59, 59) if h15 else time.min
    return datetime.combine(available_day, available_time, UTC)


def observation(series: str, day: date, value: float, source: str, h15: bool) -> dict:
    return {
        "series": series,
        "value": format(value, ".12g"),
        "unit": "percent_per_annum",
        "observed_at_utc": datetime.combine(day, time.min, UTC).isoformat(),
        "available_at_utc": available_at(day, h15).isoformat(),
        "source": source,
    }


def _bounded(rows: list[tuple[date, float]], start: date, stop: date) -> list[tuple[date, float]]:
    return [(day, value) for day, value in rows if start <= day <= stop]


def build_observations(series: dict[str, list[tuple[date, float]]], asof: datetime) -> list[dict]:
    """Build the exact four-series input contract from six official source series."""
    if asof.tzinfo is None:
        raise ValueError("Capture time must be timezone-aware")
    asof = asof.astimezone(UTC)
    if set(series) != set(SOURCE_IDS):
        raise ValueError("Exactly the six declared FRED source series are required")
    stop = asof.date()
    rows: list[dict] = []
    starts = {"DGS2": START, "DGS10": START, "DFII10": REAL_START}
    for series_id in H15_IDS:
        selected = _bounded(series[series_id], starts[series_id], stop)
        if not selected or selected[0][0] != starts[series_id]:
            raise ValueError(f"{series_id} history does not begin at the declared research boundary")
        if (stop - selected[-1][0]).days > 7:
            raise ValueError(f"{series_id} source is stale by more than seven calendar days")
        source = (f"FRED {series_id}; Board of Governors of the Federal Reserve System, "
                  "H.15 Selected Interest Rates")
        rows.extend(observation(OUTPUT_SERIES[series_id], day, value, source, True)
                    for day, value in selected)

    legacy = {day: value for day, value in _bounded(series["DFEDTAR"], START, RANGE_START - timedelta(days=1))}
    lower = {day: value for day, value in _bounded(series["DFEDTARL"], RANGE_START, stop)}
    upper = {day: value for day, value in _bounded(series["DFEDTARU"], RANGE_START, stop)}
    if set(lower) != set(upper):
        raise ValueError("FRED lower and upper target-range dates do not match")
    combined: list[tuple[date, float, str]] = [
        (day, value, "FRED DFEDTAR; Federal Open Market Committee target rate")
        for day, value in sorted(legacy.items())
    ]
    combined.extend(
        (day, (lower[day] + upper[day]) / 2.0,
         "FRED DFEDTARL/DFEDTARU midpoint; Federal Open Market Committee target range")
        for day in sorted(lower)
    )
    if not combined or combined[0][0] != START or (stop - combined[-1][0]).days > 7:
        raise ValueError("Policy target history has the wrong boundary or is stale")
    expected = [START + timedelta(days=i) for i in range((combined[-1][0] - START).days + 1)]
    if [day for day, _, _ in combined] != expected:
        raise ValueError("Policy target history is missing or duplicating calendar days")
    rows.extend(observation("POLICY_MIDPOINT", day, value, source, False)
                for day, value, source in combined)
    return sorted(rows, key=lambda item: (item["series"], item["observed_at_utc"]))


def canonical_csv_bytes(rows: list[dict], fieldnames: tuple[str, ...]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="raise", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def fetch(url: str) -> bytes:
    request = Request(url, headers={
        "User-Agent": "BTC-QQQ-public-rate-capture/1.0 (official public data; no orders)"
    })
    with urlopen(request, timeout=45) as response:
        if getattr(response, "status", 200) != 200:
            raise ValueError(f"FRED returned HTTP {response.status}")
        body = response.read()
    if not body:
        raise ValueError("FRED returned an empty response")
    return body


def capture(out: Path, asof: datetime) -> Path:
    out.mkdir(parents=True, exist_ok=False)
    (out / "responses").mkdir()
    parsed: dict[str, list[tuple[date, float]]] = {}
    receipts = []
    for series_id in SOURCE_IDS:
        raw = fetch(URL[series_id])
        digest = hashlib.sha256(raw).hexdigest()
        response_name = f"responses/{digest}.csv"
        (out / response_name).write_bytes(raw)
        parsed[series_id] = parse_fred(raw, series_id)
        receipts.append({
            "series_id": series_id,
            "url": URL[series_id],
            "retrieved_at_utc": datetime.now(UTC).isoformat(),
            "response_sha256": digest,
            "response_file": response_name,
        })
    rows = build_observations(parsed, asof)
    observations = out / "observations.csv"
    observations.write_bytes(canonical_csv_bytes(rows, OBSERVATION_FIELDS))
    events = out / "policy_events.csv"
    events.write_bytes(canonical_csv_bytes([], POLICY_EVENT_FIELDS))
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "as_of_utc": asof.astimezone(UTC).isoformat(),
        "model_features": list(MODEL_FEATURES),
        "observations": {
            "file": observations.name,
            "sha256": hashlib.sha256(observations.read_bytes()).hexdigest(),
        },
        "policy_events": {
            "file": events.name,
            "sha256": hashlib.sha256(events.read_bytes()).hexdigest(),
        },
        "source_receipts": receipts,
        "policy_event_scope": "NOT SUPPLIED; no surprise or path-repricing feature is declared",
        "live_score_points": 0,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return out / "manifest.json"


def make_bundle(source: Path, bundle: Path) -> str:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix="rate-capture-", suffix=".zip", dir=bundle.parent,
                                     delete=False) as temp:
        temporary = Path(temp.name)
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for path in sorted(source.rglob("*")):
                if path.is_file():
                    archive.write(path, f"{ROOT_NAME}/{path.relative_to(source).as_posix()}")
        os.replace(temporary, bundle)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(bundle.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--asof")
    args = parser.parse_args()
    asof = (datetime.fromisoformat(args.asof.replace("Z", "+00:00")).astimezone(UTC)
            if args.asof else datetime.now(UTC))
    with tempfile.TemporaryDirectory(prefix="rate-capture-") as temp_name:
        root = Path(temp_name) / ROOT_NAME
        try:
            capture(root, asof)
            digest = make_bundle(root, args.bundle.resolve())
        except Exception as exc:
            print(json.dumps({"status": "FAILED", "reason": f"{type(exc).__name__}: {exc}"}, indent=2))
            return 2
    print(json.dumps({
        "status": "PASSED",
        "bundle": str(args.bundle.resolve()),
        "sha256": digest,
        "as_of_utc": asof.isoformat(),
        "source_series": list(SOURCE_IDS),
        "model_features": list(MODEL_FEATURES),
        "live_score_points": 0,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
