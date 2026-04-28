#!/usr/bin/env python
"""
validate_pDash_cache.py

Validates whether pDash analytics cache represents the raw parquet data correctly.

Checks:
1. Raw parquet metadata row count.
2. Raw DuckDB COUNT(*) row count.
3. Cached base SUM(requests).
4. Date-level raw vs cache requests.
5. File inventory consistency, if available.
6. Possible duplicate amplification warning.

Run:
python validate_pDash_cache.py --input "D:\Vs - Code Work\cleaned_output" --db "D:\Vs - Code Work\pDash_analytics_cache.duckdb" --time-col reqTimeSec --threads 6
"""

import argparse
from pathlib import Path
import sys

import duckdb
import pyarrow.parquet as pq
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Folder containing parquet files")
    p.add_argument("--db", required=True, help="DuckDB cache file")
    p.add_argument("--time-col", default="reqTimeSec", help="Timestamp column name")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--sample-dates", type=int, default=20, help="How many date rows to print")
    return p.parse_args()


def main():
    args = parse_args()
    input_dir = Path(args.input)
    db_path = Path(args.db)

    files = sorted(input_dir.glob("*.parquet"))
    if not files:
        print(f"ERROR: No parquet files found in {input_dir}")
        sys.exit(1)

    if not db_path.exists():
        print(f"ERROR: Cache DB not found: {db_path}")
        sys.exit(1)

    print("=" * 90)
    print("pDash cache validator")
    print("=" * 90)
    print(f"Parquet folder : {input_dir}")
    print(f"Cache DB       : {db_path}")
    print(f"Files          : {len(files)}")
    print()

    print("Step 1/5: Reading parquet metadata row counts...")
    metadata_rows = 0
    file_rows = []
    for f in files:
        meta = pq.read_metadata(f)
        rows = int(meta.num_rows or 0)
        metadata_rows += rows
        file_rows.append({"file": str(f), "file_name": f.name, "metadata_rows": rows, "size_bytes": f.stat().st_size})

    print(f"Raw metadata rows: {metadata_rows:,}")
    print()

    print("Step 2/5: Connecting to DuckDB...")
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(args.threads)}")

    parquet_list = [str(f) for f in files]

    print("Step 3/5: Counting raw parquet rows through DuckDB...")
    raw_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({parquet_list!r}, union_by_name=true)"
    ).fetchone()[0]
    print(f"Raw DuckDB COUNT(*): {int(raw_count):,}")
    print()

    print("Step 4/5: Reading cache totals...")
    tables = con.execute("SHOW TABLES").df()["name"].astype(str).tolist()
    print("Tables in DB:", ", ".join(tables))
    print()

    if "behavior_device_day" not in tables:
        print("ERROR: behavior_device_day table not found in cache DB.")
        sys.exit(1)

    cache_summary = con.execute("""
        SELECT
            COUNT(*) AS cached_rows,
            SUM(requests) AS cached_requests,
            COUNT(DISTINCT event_date) AS active_days,
            MIN(event_date) AS min_date,
            MAX(event_date) AS max_date
        FROM behavior_device_day
    """).df()
    print(cache_summary.to_string(index=False))
    print()

    cached_requests = int(cache_summary.loc[0, "cached_requests"] or 0)

    print("Step 5/5: Comparing by date...")
    # Use DOUBLE->BIGINT-safe conversion. If reqTimeSec has decimal seconds as string,
    # BIGINT direct cast may round/fail depending on values, so TRY_CAST to DOUBLE first.
    raw_by_date = con.execute(f"""
        SELECT
            to_timestamp(TRY_CAST({_dq(args.time_col)} AS DOUBLE))::DATE AS event_date,
            COUNT(*) AS raw_requests
        FROM read_parquet({parquet_list!r}, union_by_name=true)
        WHERE TRY_CAST({_dq(args.time_col)} AS DOUBLE) IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).df()

    cache_by_date = con.execute("""
        SELECT
            event_date,
            SUM(requests) AS cached_requests
        FROM behavior_device_day
        GROUP BY 1
        ORDER BY 1
    """).df()

    cmp = raw_by_date.merge(cache_by_date, on="event_date", how="outer").fillna(0)
    cmp["raw_requests"] = cmp["raw_requests"].astype("int64")
    cmp["cached_requests"] = cmp["cached_requests"].astype("int64")
    cmp["diff"] = cmp["cached_requests"] - cmp["raw_requests"]
    cmp["diff_pct"] = cmp.apply(
        lambda r: round((r["diff"] / r["raw_requests"] * 100), 4) if r["raw_requests"] else None,
        axis=1,
    )

    print("Date comparison sample:")
    print(cmp.head(args.sample_dates).to_string(index=False))
    print()

    bad = cmp[cmp["diff"] != 0].copy()
    total_diff = cached_requests - int(raw_count)

    print("=" * 90)
    print("RESULT")
    print("=" * 90)
    print(f"Metadata rows        : {metadata_rows:,}")
    print(f"Raw DuckDB rows      : {int(raw_count):,}")
    print(f"Cached SUM(requests) : {cached_requests:,}")
    print(f"Cache - raw diff     : {total_diff:,}")

    if metadata_rows != int(raw_count):
        print()
        print("WARNING: Metadata row count and DuckDB raw COUNT(*) differ.")
        print("This is unusual. It may indicate unreadable files, schema issues, or a DuckDB/parquet scan issue.")

    if total_diff == 0:
        print()
        print("PASS: Cached requests exactly match raw parquet row count.")
    else:
        print()
        print("FAIL/WARNING: Cached requests do not match raw parquet row count.")
        print(f"Dates with mismatch: {len(bad):,}")
        if not bad.empty:
            print()
            print("Largest mismatches:")
            print(
                bad.reindex(bad["diff"].abs().sort_values(ascending=False).index)
                   .head(20)
                   .to_string(index=False)
            )
        print()
        print("Most likely causes:")
        print("1. The cache builder joined raw rows to a non-unique channel mapping table.")
        print("2. The cache builder included duplicate parquet file paths.")
        print("3. Timestamp conversion differs between raw validation and cache build.")
        print("4. Cache DB was built from a different folder/version than the validator input.")

    print("=" * 90)


def _dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


if __name__ == "__main__":
    main()
