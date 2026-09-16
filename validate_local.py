"""Local validation for the weather pipeline -- no GCP credentials required.

Exercises everything in main.py except the two GCP calls, and simulates
transform.sql in pandas so the dedup and unit-conversion logic can be checked
before deployment.

    python validate_local.py
"""

import datetime
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests

API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=40.7128&longitude=-74.0060"
    "&hourly=temperature_2m,relative_humidity_2m"
)

# Parquet logical type -> the BigQuery type a PARQUET load job produces.
BQ_TYPE_FROM_PARQUET = {
    "timestamp[us, tz=UTC]": "TIMESTAMP",
    "timestamp[ns, tz=UTC]": "TIMESTAMP",
    "timestamp[us]": "DATETIME",
    "timestamp[ns]": "DATETIME",
    "double": "FLOAT64",
    "int64": "INT64",
    "string": "STRING",
    "large_string": "STRING",
}


def build_frame(hourly_data, extracted_at):
    """Identical to the DataFrame construction in main.py."""
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(hourly_data["time"], utc=True),
            "temperature_2m": hourly_data["temperature_2m"],
            "humidity": hourly_data["relative_humidity_2m"],
            "extracted_at": extracted_at,
        }
    )


def simulate_view(staging: pd.DataFrame) -> pd.DataFrame:
    """pandas equivalent of transform.sql's ranked_records / rn = 1 dedup."""
    ranked = staging.copy()
    ranked["metric_time"] = ranked["timestamp"]
    ranked["temperature_celsius"] = ranked["temperature_2m"]
    ranked["temperature_fahrenheit"] = (
        (ranked["temperature_2m"] * 9 / 5) + 32
    ).round(2)
    ranked["rn"] = (
        ranked.sort_values("extracted_at", ascending=False)
        .groupby("timestamp")
        .cumcount()
        + 1
    )
    return (
        ranked[ranked["rn"] == 1]
        .sort_values("metric_time")
        .loc[
            :,
            [
                "metric_time",
                "temperature_celsius",
                "temperature_fahrenheit",
                "humidity",
                "extracted_at",
            ],
        ]
        .reset_index(drop=True)
    )


def main():
    print("=" * 72)
    print("1. EXTRACT")
    print("=" * 72)
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    hourly_data = data["hourly"]
    print(f"  HTTP {response.status_code}")
    print(f"  timezone       : {data['timezone']} (utc_offset={data['utc_offset_seconds']}s)")
    print(f"  units          : {data['hourly_units']['temperature_2m']} / "
          f"{data['hourly_units']['relative_humidity_2m']}")
    print(f"  rows returned  : {len(hourly_data['time'])}")
    print(f"  range          : {hourly_data['time'][0]} -> {hourly_data['time'][-1]}")

    nulls = sum(1 for t in hourly_data["temperature_2m"] if t is None)
    print(f"  null temps     : {nulls}")

    run_1_at = datetime.datetime.now(datetime.timezone.utc)
    df = build_frame(hourly_data, run_1_at)

    print()
    print("=" * 72)
    print("2. DATAFRAME DTYPES")
    print("=" * 72)
    for col, dtype in df.dtypes.items():
        print(f"  {col:<16} {dtype}")

    print()
    print("=" * 72)
    print("3. PARQUET SCHEMA -> BIGQUERY TYPE MAPPING")
    print("=" * 72)
    tmp = Path(tempfile.gettempdir()) / "weather_validate.parquet"
    df.to_parquet(tmp, index=False)
    arrow_schema = pq.read_schema(tmp)
    ok = True
    for field in arrow_schema:
        arrow_type = str(field.type)
        bq_type = BQ_TYPE_FROM_PARQUET.get(arrow_type, "?? UNKNOWN")
        flag = ""
        if field.name in ("timestamp", "extracted_at") and bq_type != "TIMESTAMP":
            flag = "  <-- transform.sql CAST would behave differently!"
            ok = False
        print(f"  {field.name:<16} {arrow_type:<24} -> {bq_type}{flag}")
    print(f"  file size      : {tmp.stat().st_size:,} bytes")
    tmp.unlink()

    print()
    print("=" * 72)
    print("4. SIMULATED transform.sql (two runs, overlapping forecasts)")
    print("=" * 72)
    # Second run an hour later: same timestamps, newer extracted_at, and a
    # revised temperature so we can prove the newest value wins.
    run_2_at = run_1_at + datetime.timedelta(hours=1)
    df2 = build_frame(hourly_data, run_2_at)
    df2["temperature_2m"] = df2["temperature_2m"] + 1.0

    staging = pd.concat([df, df2], ignore_index=True)
    view = simulate_view(staging)

    print(f"  staging rows           : {len(staging)}")
    print(f"  distinct timestamps    : {staging['timestamp'].nunique()}")
    print(f"  rows surfaced by view  : {len(view)}")
    print(f"  duplicate metric_time  : {view['metric_time'].duplicated().sum()}")

    took_newest = (view["extracted_at"] == run_2_at).all()
    print(f"  all rows from newest extract : {took_newest}")

    print()
    print("  sample (first 3 rows):")
    print(view.head(3).to_string(index=False))

    print()
    print("=" * 72)
    print("5. UNIT CONVERSION CHECK")
    print("=" * 72)
    for c, expected_f in [(0.0, 32.0), (100.0, 212.0), (-40.0, -40.0), (37.0, 98.6)]:
        got = round((c * 9 / 5) + 32, 2)
        status = "OK " if abs(got - expected_f) < 0.01 else "BAD"
        print(f"  {status} {c:>7.1f} C -> {got:>7.2f} F  (expected {expected_f})")

    print()
    print("=" * 72)
    dedup_ok = len(view) == staging["timestamp"].nunique() and took_newest
    if ok and dedup_ok:
        print("RESULT: local validation PASSED")
        print("  Parquet types map to TIMESTAMP/FLOAT64/INT64 as transform.sql expects.")
        print("  Dedup keeps exactly one row per hour, from the newest extract.")
    else:
        print("RESULT: local validation FAILED -- see flags above")
    print("=" * 72)


if __name__ == "__main__":
    main()
