#!/usr/bin/env python3
"""
detailed_analysis.py
Generates a flat table (date, state, channel, platform, device, requests, unique devices, unique sessions)
from the Parquet lake. Can be used for pivot tables with slicers in Excel.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import duckdb
import pandas as pd
import xlsxwriter

# ----------------------------------------------------------------------
# CONFIGURATION (edit as needed)
# ----------------------------------------------------------------------
LAKE_FOLDER = Path(r"Z:\05 Veto Logs\lake")   # or pass as argument
OUTPUT_FILE = Path(r"Z:\05 Veto Logs\detailed_analysis.xlsx")
YEAR_FILTER = "2026"     # set to None for all years
MONTH_FILTER = "05"      # set to None for all months

# IST offset
IST_OFFSET = timedelta(hours=5, minutes=30)

# ----------------------------------------------------------------------
# DuckDB connection and helpers
# ----------------------------------------------------------------------
def get_conn():
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='16GB'")
    con.execute("SET preserve_insertion_order=false")
    return con

def lake_reader(lake_root, year=None, month=None):
    if year and month:
        path = lake_root / f"year={year}" / f"month={month}"
        if path.exists():
            return f"read_parquet('{path.as_posix()}/**/*.parquet', union_by_name=true)"
    return f"read_parquet('{lake_root.as_posix()}/**/*.parquet', hive_partitioning=true, union_by_name=true)"

def qs_extract(qs_col, param):
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"

# Platform suffix stripping
_PLATFORM_SUFFIX_PATTERN = (
    "_(firetv|firestick|fireos|androidtv|android|webos|web|"
    "ios|iphone|ipad|appletv|apple|samsung|samsungtv|tizen|"
    "roku|mi|mitv|xiaomi|sony|bravia|lg|lgtv|"
    "mobile|phone|tablet|tv)$"
)

def channel_clean_expr(qs_col):
    raw = f"COALESCE({qs_extract(qs_col, 'channel')}, {qs_extract(qs_col, 'channel_name')}, 'Unknown')"
    return f"TRIM(regexp_replace(url_decode({raw}), '{_PLATFORM_SUFFIX_PATTERN}', '', 'i'))"

# ----------------------------------------------------------------------
# Main query
# ----------------------------------------------------------------------
def main():
    lake_root = Path(sys.argv[1]) if len(sys.argv) > 1 else LAKE_FOLDER
    if not lake_root.exists():
        print(f"❌ Lake folder not found: {lake_root}")
        sys.exit(1)

    print(f"Lake: {lake_root}")
    print(f"Filter: year={YEAR_FILTER}, month={MONTH_FILTER} (if set)")

    con = get_conn()
    reader = lake_reader(lake_root, YEAR_FILTER, MONTH_FILTER)
    qs_col = "queryStr"

    # Build SQL: daily counts per state, channel, platform, device
    sql = f"""
        SELECT
            make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS utc_date,
            state,
            {channel_clean_expr(qs_col)} AS channel,
            COALESCE({qs_extract(qs_col, 'platform')}, 'Unknown') AS platform,
            COALESCE({qs_extract(qs_col, 'device')}, 'Unknown') AS device,
            COUNT(*) AS requests,
            COUNT(DISTINCT {qs_extract(qs_col, 'device_id')}) AS unique_devices,
            COUNT(DISTINCT {qs_extract(qs_col, 'session_id')}) AS unique_sessions
        FROM {reader}
        WHERE state IS NOT NULL
          AND state != ''
          AND {qs_col} IS NOT NULL
          AND {qs_col} LIKE '%channel=%'
        GROUP BY year, month, day, state, channel, platform, device
        ORDER BY utc_date, state, channel, platform, device
    """

    print("Executing query – this may take a while...")
    df = con.execute(sql).df()
    con.close()

    if df.empty:
        print("No data found (missing state, channel, etc.).")
        return

    # Convert UTC date to IST date
    df['ist_date'] = pd.to_datetime(df['utc_date']) + IST_OFFSET
    df['date_ist_str'] = df['ist_date'].dt.strftime("%Y-%m-%d")

    # Compute % of requests per day (global percentage for each row)
    # First calculate total requests per day
    total_requests_per_day = df.groupby('date_ist_str')['requests'].transform('sum')
    df['pct_of_requests'] = (df['requests'] / total_requests_per_day * 100).round(2)

    # Select and reorder columns
    output_df = df[[
        'date_ist_str', 'state', 'channel', 'platform', 'device',
        'requests', 'unique_devices', 'unique_sessions', 'pct_of_requests'
    ]].copy()
    output_df.columns = [
        'Date (IST)', 'State', 'Channel', 'Platform', 'Device',
        'Requests', 'Unique Devices', 'Unique Sessions', '% of Requests'
    ]

    # Write to Excel
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
        output_df.to_excel(writer, sheet_name='Detailed_Analysis', index=False)
        # Auto-adjust column widths
        worksheet = writer.sheets['Detailed_Analysis']
        for i, col in enumerate(output_df.columns):
            max_len = max(output_df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))

    print(f"\n✅ Output saved to: {OUTPUT_FILE}")
    print("Open in Excel → Insert Pivot Table → add slicers on State, Channel, Platform, Device.")
    print("The '% of Requests' column is the percentage of that combination within its day.\n")

if __name__ == "__main__":
    main()