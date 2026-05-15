import sys
from pathlib import Path

import pandas as pd
from shared_utils import (
    LAKE_FOLDER, OUTPUT_DIR, YEAR_FILTER, MONTH_FILTER, IST_OFFSET,
    get_conn, lake_reader, qs_extract, channel_clean_expr,
    decode_device, clean_percent_encoding, write_to_excel
)

def main():
    # Allow override from command line
    lake_root = Path(sys.argv[1]) if len(sys.argv) > 1 else LAKE_FOLDER
    if not lake_root.exists():
        print(f"❌ Lake folder not found: {lake_root}")
        sys.exit(1)

    output_file = OUTPUT_DIR / "detailed_analysis.xlsx"
    print(f"Lake: {lake_root}")
    print(f"Filter: year={YEAR_FILTER}, month={MONTH_FILTER} (if set)")

    con = get_conn()
    reader = lake_reader(lake_root, YEAR_FILTER, MONTH_FILTER)
    qs_col = "queryStr"

    # UA column name as found in Parquet
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
        print("No data found.")
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