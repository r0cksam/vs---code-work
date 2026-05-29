#!/usr/bin/env python3
"""
generate_detailed_analysis.py - Generate the detailed flat table
(per date, state, channel, platform, device, user agent) with counts.

Interactive script: asks for lake folder and output folder.
Output file name includes timestamp to avoid overwrites.
"""

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from shared_utils import (
    IST_OFFSET,
    get_conn, lake_reader, qs_extract, channel_clean_expr,
    decode_device, clean_percent_encoding, write_to_excel
)


def prompt_for_path(question: str, is_file: bool = False) -> Path:
    """Ask user for a path, loop until a valid existing directory (or file if is_file)."""
    while True:
        raw = input(question).strip()
        if not raw:
            print("  Path cannot be empty.")
            continue
        p = Path(raw).expanduser().resolve()
        if is_file:
            if p.exists() and p.is_file():
                return p
            else:
                print(f"  ❌ File not found: {p}")
        else:
            if p.exists() and p.is_dir():
                return p
            else:
                print(f"  ❌ Directory not found: {p}")


def prompt_for_optional(question: str, default: str = "") -> str:
    """Ask for optional input; return None if empty, otherwise stripped value."""
    ans = input(question).strip()
    if not ans:
        return default if default else None
    return ans


def main():
    print("\n" + "═" * 58)
    print("  Detailed Analysis Report Generator (interactive)")
    print("═" * 58 + "\n")

    # 1. Input lake folder
    print("📁 Input (lake) folder – must contain year=* partitions")
    lake_root = prompt_for_path("  Lake folder path: ", is_file=False)
    print(f"  ✅ Lake: {lake_root}\n")

    # 2. Output folder
    print("💾 Output folder – Excel file will be saved here")
    out_dir = prompt_for_path("  Output folder path: ", is_file=False)
    print(f"  ✅ Output dir: {out_dir}\n")

    # 3. Optional year/month filters
    print("📅 Optional date filters (press Enter to skip)")
    year_filter = prompt_for_optional("  Year (e.g., 2026): ")
    month_filter = None
    if year_filter:
        month_filter = prompt_for_optional("  Month (1-12): ")
        if month_filter:
            # Validate and zero-pad
            try:
                m_int = int(month_filter)
                if not (1 <= m_int <= 12):
                    print("  ❌ Month must be between 1 and 12 – ignoring month filter.")
                    month_filter = None
                else:
                    month_filter = f"{m_int:02d}"
            except ValueError:
                print("  ❌ Invalid month – ignoring.")
                month_filter = None

    print("\n" + "─" * 58)
    print(f"Lake        : {lake_root}")
    print(f"Output dir  : {out_dir}")
    if year_filter:
        print(f"Filter      : year={year_filter}" + (f", month={month_filter}" if month_filter else ""))
    else:
        print("Filter      : none (full lake)")
    print("─" * 58)

    proceed = input("\n  Proceed? (y/n): ").strip().lower()
    if proceed != "y":
        print("  Cancelled.")
        sys.exit(0)

    # Generate timestamped output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = out_dir / f"detailed_analysis_{timestamp}.xlsx"
    print(f"\nOutput file: {output_file}")

    # Connect to DuckDB
    con = get_conn()
    reader = lake_reader(lake_root, year_filter, month_filter)
    qs_col = "queryStr"
    ua_column = "UA"

    sql = f"""
        SELECT
            make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS utc_date,
            state,
            {ua_column},
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
        GROUP BY year, month, day, state, {ua_column}, channel, platform, device
        ORDER BY utc_date, state, {ua_column}, channel, platform, device
    """

    print("Executing query – this may take a while...")
    try:
        df = con.execute(sql).df()
    except Exception as e:
        print(f"❌ SQL Error. Check column names. Details: {e}")
        sys.exit(1)
    finally:
        con.close()

    if df.empty:
        print("No data found for the selected filters.")
        return

    # Convert UTC date to IST
    df['ist_date'] = pd.to_datetime(df['utc_date']) + IST_OFFSET
    df['date_ist_str'] = df['ist_date'].dt.strftime("%Y-%m-%d")

    # Compute % of requests per day
    total_requests_per_day = df.groupby('date_ist_str')['requests'].transform('sum')
    df['pct_of_requests'] = (df['requests'] / total_requests_per_day * 100).round(2)

    # Prepare final DataFrame
    output_df = df[[
        'date_ist_str', 'state', ua_column, 'channel', 'platform', 'device',
        'requests', 'unique_devices', 'unique_sessions', 'pct_of_requests'
    ]].copy()
    output_df.columns = [
        'Date (IST)', 'State', 'User Agent', 'Channel', 'Platform', 'Device',
        'Requests', 'Unique Devices', 'Unique Sessions', '% of Requests'
    ]

    # Clean %20 from all text columns
    output_df = clean_percent_encoding(output_df)

    # Decode device names
    output_df['Device'] = output_df['Device'].apply(decode_device)

    # Write to Excel
    write_to_excel(output_file, output_df, sheet_name="Detailed_Analysis")
    print(f"\n✅ Output saved to: {output_file}")
    print("Open in Excel → Insert Pivot Table → add slicers on State, Channel, Platform, Device, User Agent.")


if __name__ == "__main__":
    main()