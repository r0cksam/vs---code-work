#!/usr/bin/env python3
"""
detailed_analysis.py - Generate a flat table from Parquet lake,
replace %20 with spaces, decode device names, and produce an Excel file.
"""

import sys
from pathlib import Path
from datetime import timedelta

import duckdb
import pandas as pd

# ----------------------------------------------------------------------
# CONFIGURATION (edit as needed)
# ----------------------------------------------------------------------
LAKE_FOLDER = Path(r"Z:\05 Veto Logs\lake")   # default, can be overridden by CLI
OUTPUT_FILE = Path(r"Z:\05 Veto Logs\detailed_analysis.xlsx")
YEAR_FILTER = "2026"     # set to None for all years
MONTH_FILTER = "05"      # set to None for all months

# IST offset
IST_OFFSET = timedelta(hours=5, minutes=30)

# ----------------------------------------------------------------------
# Device name decoding dictionary (expand as needed)
# ----------------------------------------------------------------------
DEVICE_MAP = {
    # Fire TV / Amazon
    "AFTSS": "Amazon Fire TV Stick (2nd Gen / Lite)",
    "AFTSSS": "Amazon Fire TV Stick 4K",
    "AFTMM": "Amazon Fire TV Cube",
    "AFTKA": "Amazon Fire TV Stick 4K Max",
    "AFTKM": "Amazon Fire TV Stick (3rd Gen)",
    "AFTBU001": "Amazon Fire TV Stick 4K Max",
    "AFTBTX4": "Amazon Fire TV Stick 4K Max (2023)",
    "AFTDE70DFC": "Amazon Fire TV device (unidentified model)",
    "AFTMA08C15": "Amazon Fire TV device",
    "AFTMAN001": "Amazon Fire TV device",
    "AFTMV00": "Amazon Fire TV device",
    "AFTNE4E43F": "Amazon Fire TV device",
    "AFTPR001": "Amazon Fire TV device",
    "AFTLE": "Amazon Fire TV Stick Lite",
    "AFTGRO001": "Amazon Fire TV device",
    "AFTCA002": "Amazon Fire TV device",
    "AFTGAZL": "Amazon Fire TV device",
    "AFTBOXE1": "Amazon Fire TV Stick (box edition)",
    "AFTKRT": "Amazon Fire TV Stick (Krypton)",
    "AFTMA475B1": "Amazon Fire TV device",
    "AFTANNA0": "Amazon Fire TV device",
    "AFTJMST12": "Amazon Fire TV device",
    "AFTEAMR311": "Amazon Fire TV device",
    "AFTCL001": "Amazon Fire TV device",
    "AFTHA004": "Amazon Fire TV device",
    "AFTA": "Amazon Fire TV (original / 1st gen)",
    "AFTDEC012E": "Amazon Fire TV device",
    "AFTEU011": "Amazon Fire TV (European)",
    "AFTKADE001": "Amazon Fire TV device",

    # Google / Chromecast
    "Chromecast": "Google Chromecast",
    "Chromecast HD": "Google Chromecast with Google TV (HD)",
    "Chromecast with Google TV": "Google Chromecast with Google TV (4K)",

    # Sony Bravia
    "BRAVIA": "Sony Bravia TV",
    "BRAVIA 4K VH2": "Sony Bravia 4K VH2 series",
    "BRAVIA 4K VH21": "Sony Bravia 4K VH21 series",
    "BRAVIA 4K VH22": "Sony Bravia 4K VH22 series",
    "BRAVIA 4K AE2": "Sony Bravia 4K AE2 series",
    "BRAVIA 4K GB": "Sony Bravia 4K GB series",
    "BRAVIA 4K UR2": "Sony Bravia 4K UR2 series",
    "BRAVIA 4K UR3": "Sony Bravia 4K UR3 series",
    "BRAVIA 2015": "Sony Bravia (2015 model)",
    "BRAVIA VU1": "Sony Bravia VU1 series",
    "BRAVIA VU2": "Sony Bravia VU2 series",
    "BRAVIA VU3": "Sony Bravia VU3 series",
    "BRAVIA VU31": "Sony Bravia VU31 series",
    "BRAVIA BF1": "Sony Bravia BF1 series",
    "BRAVIA VH1": "Sony Bravia VH1 series",

    # Xiaomi Mi TV / Mi Box
    "MiTV-AXSO0": "Xiaomi Mi TV Stick (4K)",
    "MiTV-AXSO1": "Xiaomi Mi TV Stick (1080p)",
    "MiTV-AXSO2": "Xiaomi Mi TV Stick (4K)",
    "MiTV-MOOQ0": "Xiaomi Mi Box S",
    "MiTV-MOOQ1": "Xiaomi Mi Box S (2nd Gen)",
    "MiTV-MOOQ3": "Xiaomi Mi TV Stick (2023)",
    "MiTV-AXFR2": "Xiaomi Mi TV Stick (4K)",
    "MiTV-AESP0": "Xiaomi Mi TV P1",
    "MiTV-MOSR1": "Xiaomi Mi TV Stick (4K)",
    "MiTV-MZTU0": "Xiaomi Mi TV Stick (unidentified)",
    "MIBOX4": "Xiaomi Mi Box 4",

    # Realme
    "realme Smart TV": "Realme Smart TV",
    "realme Smart TV 4K": "Realme Smart TV 4K",

    # OnePlus
    "Oneplus Dosa IN": "OnePlus TV (Dosa series India)",

    # VU
    "VU 4K": "VU 4K TV",
    "VU 4K Google TV": "VU 4K Google TV",
    "VU TV FFM": "VU TV (FFM series)",

    # Haier
    "haierATV": "Haier Android TV",
    "haierATVnippori": "Haier Android TV (Nippori series)",

    # Tata Sky
    "Tata Sky Binge Plus": "Tata Sky Binge+ set‑top box",

    # Airtel Xstream
    "XStream Smart Box 001": "Airtel Xstream Smart Box",
    "Xstream4 AR": "Airtel Xstream 4K",
    "XstreamIPTV1-VT": "Airtel Xstream IPTV (VT)",
    "XstreamIPTV2-SM": "Airtel Xstream IPTV (SM)",

    # Nokia
    "NSTV": "Nokia Streaming Box",
    "NSTV FF": "Nokia Streaming Box (FF series)",

    # Cloud TV
    "CloudTV518": "Cloud TV (generic Android TV box)",
    "CloudTV521": "Cloud TV (generic Android TV box)",
    "Cloud TV": "Cloud TV (generic)",

    # Generic / fallback
    "AndroidTV": "Generic Android TV device",
    "Smart TV": "Generic Smart TV",
    "Smart TV Pro": "Generic Smart TV Pro",
    "SmartTV 4K": "Generic 4K Smart TV",
    "SmartTV 4K FFM": "Generic 4K Smart TV (FFM series)",
    "Toshiba TV": "Toshiba Android TV",
    "Toshiba TV with MIC": "Toshiba Android TV (with microphone)",
    "Philips Smart 4K TV": "Philips 4K Android TV",
    "Philips UHD Android TV": "Philips UHD Android TV",
    "Panasonic TV": "Panasonic Android TV",
    "HiSmartTV A4": "Hisense Smart TV (A4 series)",
    "HiSmart TV": "Hisense Smart TV",
    "LLOYD 4K SMART TV": "Lloyd 4K Android TV",
    "LLOYD 2K SMART TV": "Lloyd 2K Android TV",
}

def decode_device(device: str) -> str:
    """Replace %20 and try to decode a device name."""
    if not isinstance(device, str):
        return device
    cleaned = device.replace("%20", " ")
    if cleaned in DEVICE_MAP:
        return DEVICE_MAP[cleaned]
    if cleaned.startswith("AFT"):
        return f"Amazon Fire TV device (code: {cleaned})"
    if cleaned.startswith("BRAVIA"):
        return f"Sony Bravia TV ({cleaned})"
    if cleaned.startswith("MiTV"):
        return f"Xiaomi Mi TV ({cleaned})"
    return cleaned

# ----------------------------------------------------------------------
# DuckDB helpers
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
# Main
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

    # Compute % of requests per day
    total_requests_per_day = df.groupby('date_ist_str')['requests'].transform('sum')
    df['pct_of_requests'] = (df['requests'] / total_requests_per_day * 100).round(2)

    # Prepare final DataFrame
    output_df = df[[
        'date_ist_str', 'state', 'channel', 'platform', 'device',
        'requests', 'unique_devices', 'unique_sessions', 'pct_of_requests'
    ]].copy()
    output_df.columns = [
        'Date (IST)', 'State', 'Channel', 'Platform', 'Device',
        'Requests', 'Unique Devices', 'Unique Sessions', '% of Requests'
    ]

    # ------------------------------------------------------------------
    # CLEAN %20 FROM ALL TEXT COLUMNS (robust method)
    # ------------------------------------------------------------------
    # Convert all object/string columns to string and replace %20
    for col in output_df.columns:
        # Check if column is string-like
        if pd.api.types.is_string_dtype(output_df[col]) or pd.api.types.is_object_dtype(output_df[col]):
            # Convert to string, replace %20 with space, also handle double encoding %2520
            output_df[col] = output_df[col].astype(str).str.replace('%20', ' ', regex=False)
            # Optional: also replace %2520 (double encoded) just in case
            output_df[col] = output_df[col].str.replace('%2520', ' ', regex=False)

    # Decode device names (also handles any remaining %20)
    output_df['Device'] = output_df['Device'].apply(decode_device)

    # ------------------------------------------------------------------
    # Save to Excel with a README sheet
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
        output_df.to_excel(writer, sheet_name='Detailed_Analysis', index=False)

        # Add README sheet with decoding note
        note = [
            "Device names decoded using a custom lookup table based on community knowledge.",
            "No third‑party API was used."
        ]
        pd.DataFrame({'Info': note}).to_excel(writer, sheet_name='README', index=False)

        # Auto-adjust column widths for the data sheet
        worksheet = writer.sheets['Detailed_Analysis']
        for i, col in enumerate(output_df.columns):
            max_len = max(output_df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))

    print(f"\n✅ Output saved to: {OUTPUT_FILE}")
    print("Open in Excel → Insert Pivot Table → add slicers on State, Channel, Platform, Device.")
    print("The '% of Requests' column is the percentage of that combination within its day.\n")

if __name__ == "__main__":
    main()