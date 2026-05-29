#!/usr/bin/env python3
"""
check_timezone.py
Read a sample of rows from a Parquet file and show reqTimeSec as UTC vs IST.
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta

# IST offset: UTC + 5:30
IST = timezone(timedelta(hours=5, minutes=30))

def main():
    # Ask for lake folder path
    lake_input = input("\n📁 Lake folder path (e.g., Z:\\05 Veto Logs\\lake): ").strip()
    lake_path = Path(lake_input).resolve()
    if not lake_path.exists():
        print(f"❌ Folder not found: {lake_path}")
        return

    # Optional: filter to a specific day
    day_input = input("\n📅 Optional: specific day (e.g., 2026-05-01) or press Enter for all: ").strip()
    
    if day_input:
        parts = day_input.split("-")
        if len(parts) == 3:
            year, month, day = parts
            day_dir = lake_path / f"year={year}" / f"month={month}" / f"day={day}"
            if not day_dir.exists():
                print(f"❌ Day partition not found: {day_dir}")
                return
            reader = f"read_parquet('{day_dir.as_posix()}/*.parquet', union_by_name=true)"
        else:
            print("❌ Invalid date format. Use YYYY-MM-DD")
            return
    else:
        # Sample from the first Parquet file found
        files = list(lake_path.rglob("*.parquet"))
        if not files:
            print("❌ No Parquet files found.")
            return
        first_file = files[0]
        print(f"\nSampling from: {first_file}")
        reader = f"read_parquet('{first_file.as_posix()}')"

    con = duckdb.connect()
    
    # Cast reqTimeSec to DOUBLE because it's stored as string
    sql = f"""
        SELECT 
            CAST(reqTimeSec AS DOUBLE) AS reqTimeSec_float,
            year, month, day
        FROM {reader}
        WHERE reqTimeSec IS NOT NULL
        LIMIT 10
    """
    try:
        df = con.execute(sql).df()
        if df.empty:
            print("No rows with reqTimeSec found.")
            return
        
        print("\n" + "=" * 90)
        print(f"{'raw reqTimeSec (float)':<20} {'UTC datetime':<27} {'IST datetime (UTC+5:30)':<27} {'Partition date'}")
        print("=" * 90)
        
        for _, row in df.iterrows():
            raw = row['reqTimeSec_float']
            # Convert to UTC datetime (timestamp is in seconds)
            try:
                utc_dt = datetime.fromtimestamp(raw, tz=timezone.utc)
                ist_dt = utc_dt.astimezone(IST)
                part_date = f"{int(row['year'])}-{int(row['month']):02d}-{int(row['day']):02d}"
                print(f"{raw:<20.3f} {utc_dt.strftime('%Y-%m-%d %H:%M:%S UTC'):<27} "
                      f"{ist_dt.strftime('%Y-%m-%d %H:%M:%S IST'):<27} {part_date}")
            except Exception as e:
                print(f"{raw:<20} Error converting: {e}")
    except Exception as e:
        print(f"Query error: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    main()