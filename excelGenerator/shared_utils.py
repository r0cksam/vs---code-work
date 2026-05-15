#!/usr/bin/env python3
"""
shared_utils.py - Common functions and constants for all analysis scripts.
"""

from pathlib import Path
from datetime import timedelta
import duckdb
import pandas as pd

# ----------------------------------------------------------------------
# CONFIGURATION (shared across scripts)
# ----------------------------------------------------------------------
LAKE_FOLDER = Path(r"Z:\05 Veto Logs\lake")   # default, can be overridden
OUTPUT_DIR = Path(r"Z:\05 Veto Logs\Reports")         # directory for all output files
YEAR_FILTER = "2026"     # set to None for all years
MONTH_FILTER = "05"      # set to None for all months

# IST offset
IST_OFFSET = timedelta(hours=5, minutes=30)

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
    """Return a DuckDB table expression for reading Parquet files."""
    if year and month:
        path = lake_root / f"year={year}" / f"month={month}"
        if path.exists():
            return f"read_parquet('{path.as_posix()}/**/*.parquet', union_by_name=true)"
    return f"read_parquet('{lake_root.as_posix()}/**/*.parquet', hive_partitioning=true, union_by_name=true)"

def qs_extract(qs_col, param):
    """Extract a query string parameter."""
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"

# Platform suffix removal pattern
_PLATFORM_SUFFIX_PATTERN = (
    "_(firetv|firestick|fireos|androidtv|android|webos|web|"
    "ios|iphone|ipad|appletv|apple|samsung|samsungtv|tizen|"
    "roku|mi|mitv|xiaomi|sony|bravia|lg|lgtv|"
    "mobile|phone|tablet|tv)$"
)

def channel_clean_expr(qs_col):
    """Return SQL expression to clean channel name from queryStr."""
    raw = f"COALESCE({qs_extract(qs_col, 'channel')}, {qs_extract(qs_col, 'channel_name')}, 'Unknown')"
    return f"TRIM(regexp_replace(url_decode({raw}), '{_PLATFORM_SUFFIX_PATTERN}', '', 'i'))"

# ----------------------------------------------------------------------
# Device name decoding
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

    # Realme, OnePlus, VU, Haier
    "realme Smart TV": "Realme Smart TV",
    "realme Smart TV 4K": "Realme Smart TV 4K",
    "Oneplus Dosa IN": "OnePlus TV (Dosa series India)",
    "VU 4K": "VU 4K TV",
    "VU 4K Google TV": "VU 4K Google TV",
    "VU TV FFM": "VU TV (FFM series)",
    "haierATV": "Haier Android TV",
    "haierATVnippori": "Haier Android TV (Nippori series)",

    # Set-top boxes
    "Tata Sky Binge Plus": "Tata Sky Binge+ set‑top box",
    "XStream Smart Box 001": "Airtel Xstream Smart Box",
    "Xstream4 AR": "Airtel Xstream 4K",
    "XstreamIPTV1-VT": "Airtel Xstream IPTV (VT)",
    "XstreamIPTV2-SM": "Airtel Xstream IPTV (SM)",
    "NSTV": "Nokia Streaming Box",
    "NSTV FF": "Nokia Streaming Box (FF series)",

    # Cloud TV
    "CloudTV518": "Cloud TV (generic Android TV box)",
    "CloudTV521": "Cloud TV (generic Android TV box)",
    "Cloud TV": "Cloud TV (generic)",

    # Generic fallbacks
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
    """Replace %20 and decode device name using lookup table."""
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
# Common data cleaning
# ----------------------------------------------------------------------
def clean_percent_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """Replace %20 and %2520 with spaces in all string columns (in-place)."""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).str.replace('%20', ' ', regex=False)
            df[col] = df[col].str.replace('%2520', ' ', regex=False)
    return df

# ----------------------------------------------------------------------
# Excel writer with README sheet
# ----------------------------------------------------------------------
def write_to_excel(output_path: Path, data_df: pd.DataFrame, sheet_name: str = "Data"):
    """Write DataFrame to Excel with an auto-sized column width and a README sheet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        data_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Add README sheet
        note = [
            "Device names decoded using a custom lookup table based on community knowledge.",
            "No third‑party API was used."
        ]
        pd.DataFrame({'Info': note}).to_excel(writer, sheet_name='README', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets[sheet_name]
        for i, col in enumerate(data_df.columns):
            max_len = max(data_df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))
            
def utc_date_to_ist_str(utc_date) -> str:
    """Convert a UTC date (datetime.date or datetime) to IST date string dd/mm/yy."""
    from datetime import datetime, timezone
    if isinstance(utc_date, pd.Timestamp):
        dt = utc_date.to_pydatetime()
    elif isinstance(utc_date, datetime):
        dt = utc_date
    else:
        dt = datetime.combine(utc_date, datetime.min.time())
    ist_dt = dt + IST_OFFSET
    return ist_dt.strftime("%d/%m/%y")

def utc_date_str_to_ist_str(utc_str: str) -> str:
    """Convert UTC date string 'YYYY-MM-DD' to IST date string 'DD/MM/YY'."""
    y, m, d = map(int, utc_str.split('-'))
    from datetime import datetime, timezone
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    ist_dt = dt + IST_OFFSET
    return ist_dt.strftime("%d/%m/%y")