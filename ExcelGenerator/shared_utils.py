#!/usr/bin/env python3
"""
shared_utils.py - Common functions, constants, and Excel helpers for all report scripts.

Changes from original:
  - Added get_existing_dates()  → reads which IST dates are already in an Excel file
  - Added load_existing_data()  → loads a full sheet as a DataFrame for merge-rewrite
  - write_to_excel() is unchanged; it is used for both first-time creation and full rewrites
"""

from pathlib import Path
from datetime import timedelta, datetime, timezone
import duckdb
import pandas as pd

# ──────────────────────────────────────────────────────────────
# CONFIGURATION (defaults; overridden at runtime by prompts)
# ──────────────────────────────────────────────────────────────
LAKE_FOLDER  = Path(r"Z:\05 Veto Logs\lake")
OUTPUT_DIR   = Path(r"Z:\05 Veto Logs\lake")
YEAR_FILTER  = "2026"   # None → all years
MONTH_FILTER = "05"     # None → all months

# IST = UTC + 05:30
IST_OFFSET = timedelta(hours=5, minutes=30)

# Fixed output filename for the detailed analysis (append-aware)
DETAILED_ANALYSIS_FILENAME = "detailed_analysis.xlsx"
DETAILED_SHEET_NAME        = "Detailed_Analysis"


# ──────────────────────────────────────────────────────────────
# DuckDB helpers
# ──────────────────────────────────────────────────────────────

def get_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='16GB'")
    con.execute("SET preserve_insertion_order=false")
    return con


def lake_reader(lake_root: Path, year: str | None = None, month: str | None = None) -> str:
    """Return a DuckDB table expression for reading Parquet files from the lake."""
    if year and month:
        path = lake_root / f"year={year}" / f"month={month}"
        if path.exists():
            return f"read_parquet('{path.as_posix()}/**/*.parquet', union_by_name=true)"
    return (
        f"read_parquet('{lake_root.as_posix()}/**/*.parquet', "
        f"hive_partitioning=true, union_by_name=true)"
    )


def qs_extract(qs_col: str, param: str) -> str:
    """SQL expression: extract a single query-string parameter value, or NULL."""
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"


# Platform suffix removal – strips "_android", "_ios", etc. from channel names
_PLATFORM_SUFFIX_PATTERN = (
    "_(firetv|firestick|fireos|androidtv|android|webos|web|"
    "ios|iphone|ipad|appletv|apple|samsung|samsungtv|tizen|"
    "roku|mi|mitv|xiaomi|sony|bravia|lg|lgtv|"
    "mobile|phone|tablet|tv)$"
)

def channel_clean_expr(qs_col: str) -> str:
    """SQL expression: extract and clean the channel name from a queryStr column."""
    raw = (
        f"COALESCE({qs_extract(qs_col, 'channel')}, "
        f"{qs_extract(qs_col, 'channel_name')}, 'Unknown')"
    )
    return f"TRIM(regexp_replace(url_decode({raw}), '{_PLATFORM_SUFFIX_PATTERN}', '', 'i'))"


# ──────────────────────────────────────────────────────────────
# Device name decoding
# ──────────────────────────────────────────────────────────────

DEVICE_MAP: dict[str, str] = {
    # Fire TV / Amazon
    "AFTSS":        "Amazon Fire TV Stick (2nd Gen / Lite)",
    "AFTSSS":       "Amazon Fire TV Stick 4K",
    "AFTMM":        "Amazon Fire TV Cube",
    "AFTKA":        "Amazon Fire TV Stick 4K Max",
    "AFTKM":        "Amazon Fire TV Stick (3rd Gen)",
    "AFTBU001":     "Amazon Fire TV Stick 4K Max",
    "AFTBTX4":      "Amazon Fire TV Stick 4K Max (2023)",
    "AFTDE70DFC":   "Amazon Fire TV device (unidentified model)",
    "AFTMA08C15":   "Amazon Fire TV device",
    "AFTMAN001":    "Amazon Fire TV device",
    "AFTMV00":      "Amazon Fire TV device",
    "AFTNE4E43F":   "Amazon Fire TV device",
    "AFTPR001":     "Amazon Fire TV device",
    "AFTLE":        "Amazon Fire TV Stick Lite",
    "AFTGRO001":    "Amazon Fire TV device",
    "AFTCA002":     "Amazon Fire TV device",
    "AFTGAZL":      "Amazon Fire TV device",
    "AFTBOXE1":     "Amazon Fire TV Stick (box edition)",
    "AFTKRT":       "Amazon Fire TV Stick (Krypton)",
    "AFTMA475B1":   "Amazon Fire TV device",
    "AFTANNA0":     "Amazon Fire TV device",
    "AFTJMST12":    "Amazon Fire TV device",
    "AFTEAMR311":   "Amazon Fire TV device",
    "AFTCL001":     "Amazon Fire TV device",
    "AFTHA004":     "Amazon Fire TV device",
    "AFTA":         "Amazon Fire TV (original / 1st gen)",
    "AFTDEC012E":   "Amazon Fire TV device",
    "AFTEU011":     "Amazon Fire TV (European)",
    "AFTKADE001":   "Amazon Fire TV device",
    # Google / Chromecast
    "Chromecast":                   "Google Chromecast",
    "Chromecast HD":                "Google Chromecast with Google TV (HD)",
    "Chromecast with Google TV":    "Google Chromecast with Google TV (4K)",
    # Sony Bravia
    "BRAVIA":           "Sony Bravia TV",
    "BRAVIA 4K VH2":    "Sony Bravia 4K VH2 series",
    "BRAVIA 4K VH21":   "Sony Bravia 4K VH21 series",
    "BRAVIA 4K VH22":   "Sony Bravia 4K VH22 series",
    "BRAVIA 4K AE2":    "Sony Bravia 4K AE2 series",
    "BRAVIA 4K GB":     "Sony Bravia 4K GB series",
    "BRAVIA 4K UR2":    "Sony Bravia 4K UR2 series",
    "BRAVIA 4K UR3":    "Sony Bravia 4K UR3 series",
    "BRAVIA 2015":      "Sony Bravia (2015 model)",
    "BRAVIA VU1":       "Sony Bravia VU1 series",
    "BRAVIA VU2":       "Sony Bravia VU2 series",
    "BRAVIA VU3":       "Sony Bravia VU3 series",
    "BRAVIA VU31":      "Sony Bravia VU31 series",
    "BRAVIA BF1":       "Sony Bravia BF1 series",
    "BRAVIA VH1":       "Sony Bravia VH1 series",
    # Xiaomi
    "MiTV-AXSO0":   "Xiaomi Mi TV Stick (4K)",
    "MiTV-AXSO1":   "Xiaomi Mi TV Stick (1080p)",
    "MiTV-AXSO2":   "Xiaomi Mi TV Stick (4K)",
    "MiTV-MOOQ0":   "Xiaomi Mi Box S",
    "MiTV-MOOQ1":   "Xiaomi Mi Box S (2nd Gen)",
    "MiTV-MOOQ3":   "Xiaomi Mi TV Stick (2023)",
    "MiTV-AXFR2":   "Xiaomi Mi TV Stick (4K)",
    "MiTV-AESP0":   "Xiaomi Mi TV P1",
    "MiTV-MOSR1":   "Xiaomi Mi TV Stick (4K)",
    "MiTV-MZTU0":   "Xiaomi Mi TV Stick (unidentified)",
    "MIBOX4":        "Xiaomi Mi Box 4",
    # Others
    "realme Smart TV":          "Realme Smart TV",
    "realme Smart TV 4K":       "Realme Smart TV 4K",
    "Oneplus Dosa IN":          "OnePlus TV (Dosa series India)",
    "VU 4K":                    "VU 4K TV",
    "VU 4K Google TV":          "VU 4K Google TV",
    "VU TV FFM":                "VU TV (FFM series)",
    "haierATV":                 "Haier Android TV",
    "haierATVnippori":          "Haier Android TV (Nippori series)",
    "Tata Sky Binge Plus":      "Tata Sky Binge+ set-top box",
    "XStream Smart Box 001":    "Airtel Xstream Smart Box",
    "Xstream4 AR":              "Airtel Xstream 4K",
    "XstreamIPTV1-VT":          "Airtel Xstream IPTV (VT)",
    "XstreamIPTV2-SM":          "Airtel Xstream IPTV (SM)",
    "NSTV":                     "Nokia Streaming Box",
    "NSTV FF":                  "Nokia Streaming Box (FF series)",
    "CloudTV518":               "Cloud TV (generic Android TV box)",
    "CloudTV521":               "Cloud TV (generic Android TV box)",
    "Cloud TV":                 "Cloud TV (generic)",
    "AndroidTV":                "Generic Android TV device",
    "Smart TV":                 "Generic Smart TV",
    "Smart TV Pro":             "Generic Smart TV Pro",
    "SmartTV 4K":               "Generic 4K Smart TV",
    "SmartTV 4K FFM":           "Generic 4K Smart TV (FFM series)",
    "Toshiba TV":               "Toshiba Android TV",
    "Toshiba TV with MIC":      "Toshiba Android TV (with microphone)",
    "Philips Smart 4K TV":      "Philips 4K Android TV",
    "Philips UHD Android TV":   "Philips UHD Android TV",
    "Panasonic TV":             "Panasonic Android TV",
    "HiSmartTV A4":             "Hisense Smart TV (A4 series)",
    "HiSmart TV":               "Hisense Smart TV",
    "LLOYD 4K SMART TV":        "Lloyd 4K Android TV",
    "LLOYD 2K SMART TV":        "Lloyd 2K Android TV",
}


def decode_device(device: str) -> str:
    """Decode a raw device code into a human-readable name."""
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


# ──────────────────────────────────────────────────────────────
# Data cleaning
# ──────────────────────────────────────────────────────────────

def clean_percent_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """Replace %20 / %2520 with spaces in all string/object columns."""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).str.replace('%20',   ' ', regex=False)
            df[col] = df[col].str.replace('%2520', ' ', regex=False)
    return df


# ──────────────────────────────────────────────────────────────
# Date conversion helpers
# ──────────────────────────────────────────────────────────────

def utc_date_to_ist_str(utc_date) -> str:
    """Convert a UTC date (date / datetime / Timestamp) to IST date string DD/MM/YY."""
    if isinstance(utc_date, pd.Timestamp):
        dt = utc_date.to_pydatetime()
    elif isinstance(utc_date, datetime):
        dt = utc_date
    else:
        dt = datetime.combine(utc_date, datetime.min.time())
    return (dt + IST_OFFSET).strftime("%d/%m/%y")


def utc_date_str_to_ist_str(utc_str: str) -> str:
    """Convert 'YYYY-MM-DD' UTC string to 'DD/MM/YY' IST string."""
    y, m, d = map(int, utc_str.split('-'))
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    return (dt + IST_OFFSET).strftime("%d/%m/%y")


# ──────────────────────────────────────────────────────────────
# Excel – first-time creation  (xlsxwriter, formatted)
# ──────────────────────────────────────────────────────────────

def write_to_excel(
    output_path: Path,
    data_df:     pd.DataFrame,
    sheet_name:  str = "Data",
) -> None:
    """
    Write *data_df* to *output_path* (xlsxwriter).
    Creates the file from scratch – used on first run and on full rewrites.
    Adds a README sheet and auto-sizes columns.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        data_df.to_excel(writer, sheet_name=sheet_name, index=False)

        # README sheet
        notes = [
            "Device names decoded using a custom lookup table (community knowledge).",
            "No third-party API was used.",
            "File is append-aware: re-running the generator adds only new dates.",
        ]
        pd.DataFrame({'Info': notes}).to_excel(writer, sheet_name='README', index=False)

        # Auto-size columns (capped at 50 chars wide)
        ws = writer.sheets[sheet_name]
        for i, col in enumerate(data_df.columns):
            max_len = max(data_df[col].astype(str).map(len).max(), len(col)) + 2
            ws.set_column(i, i, min(max_len, 50))


# ──────────────────────────────────────────────────────────────
# Excel – append helpers  (pandas + xlsxwriter for merge-rewrite)
# ──────────────────────────────────────────────────────────────

def get_existing_dates(
    output_path: Path,
    sheet_name:  str = DETAILED_SHEET_NAME,
    date_column: str = "Date (IST)",
) -> set[str]:
    """
    Return the set of IST date strings already present in *output_path*.
    Returns an empty set if the file does not exist or cannot be read.

    Example return value: {'2026-05-01', '2026-05-02', ...}
    """
    if not output_path.exists():
        return set()
    try:
        df = pd.read_excel(output_path, sheet_name=sheet_name, usecols=[date_column])
        return set(df[date_column].astype(str).str.strip().unique())
    except Exception as exc:
        print(f"  ⚠️  Could not read existing dates from {output_path.name}: {exc}")
        return set()


def load_existing_data(
    output_path: Path,
    sheet_name:  str = DETAILED_SHEET_NAME,
) -> pd.DataFrame:
    """
    Load the full existing data sheet into a DataFrame.
    Returns an empty DataFrame if the file does not exist or cannot be read.
    """
    if not output_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(output_path, sheet_name=sheet_name, dtype=str)
    except Exception as exc:
        print(f"  ⚠️  Could not load existing data from {output_path.name}: {exc}")
        return pd.DataFrame()


def merge_and_rewrite(
    output_path:  Path,
    new_df:       pd.DataFrame,
    sheet_name:   str = DETAILED_SHEET_NAME,
    date_column:  str = "Date (IST)",
) -> None:
    """
    Load existing data, append *new_df* (only truly new dates), sort by date,
    then rewrite the whole file cleanly with write_to_excel().

    This keeps formatting consistent across all runs.
    """
    existing_df = load_existing_data(output_path, sheet_name)

    if existing_df.empty:
        combined = new_df
    else:
        # Restore numeric types that read_excel turns into strings
        numeric_cols = ["Requests", "Unique Devices", "Unique Sessions", "% of Requests"]
        for col in numeric_cols:
            if col in existing_df.columns:
                existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce')

        # Drop rows from existing that overlap with new dates (safety: avoid dupes)
        new_dates = set(new_df[date_column].astype(str))
        existing_df = existing_df[~existing_df[date_column].astype(str).isin(new_dates)]

        combined = pd.concat([existing_df, new_df], ignore_index=True)

    # Sort chronologically
    combined = combined.sort_values(date_column, ignore_index=True)

    write_to_excel(output_path, combined, sheet_name=sheet_name)