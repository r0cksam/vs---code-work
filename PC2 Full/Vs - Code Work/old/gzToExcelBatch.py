import gzip
import json
from pathlib import Path
from urllib.parse import unquote
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from tqdm import tqdm

# ---------------- CONFIG ----------------

INPUT_FOLDER = r"D:\VETO Logs\06"
OUTPUT_FOLDER = r"D:\VETO Logs"

# Optional analyzer CSV usage
PROFILE_CSV = r"D:\Vs - Code Work\060.csv"   # Example: r"D:\VETO Logs\log_analysis.csv"
USE_PROFILE_CSV = True
DROP_100_PERCENT_BLANK_COLUMNS = True

# Cleaning options
CONVERT_PLACEHOLDERS_TO_NULL = True
CONVERT_REQTIMESEC_TO_IST = True
DECODE_URL_TEXT = True

# Excel hard limit
EXCEL_MAX_ROWS = 1_048_576
RESERVED_EMPTY_ROWS = 1_000
HARD_USABLE_ROWS_LIMIT = EXCEL_MAX_ROWS - RESERVED_EMPTY_ROWS  # 1,047,576

# Practical batching limit for performance
# Keep this much smaller than Excel max so writing stays responsive
TARGET_ROWS_PER_BATCH = 100_000

# Also flush after this many .gz files even if row limit is not reached
MAX_FILES_PER_BATCH = 5_000

PLACEHOLDERS = {"-", "^"}
IST_TZ = ZoneInfo("Asia/Kolkata")


# ---------------- HELPERS ----------------

def parse_epoch_seconds(value):
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(str(value).strip()), tz=timezone.utc)
    except Exception:
        return None


def safe_none_if_placeholder(value):
    if not CONVERT_PLACEHOLDERS_TO_NULL:
        return value
    if value is None:
        return None
    if str(value) in PLACEHOLDERS:
        return None
    return value


def auto_decode_url_if_needed(value):
    if not DECODE_URL_TEXT:
        return value
    if value is None:
        return value
    if not isinstance(value, str):
        return value
    if "%" not in value and "+" not in value:
        return value
    try:
        return unquote(value)
    except Exception:
        return value


def clean_value(column_name, value):
    value = safe_none_if_placeholder(value)

    if value is None:
        return None

    if CONVERT_REQTIMESEC_TO_IST and column_name == "reqTimeSec":
        dt_utc = parse_epoch_seconds(value)
        return dt_utc.astimezone(IST_TZ).isoformat() if dt_utc else None

    value = auto_decode_url_if_needed(value)
    return value


def read_profile_csv(profile_csv_path):
    df = pd.read_csv(profile_csv_path)
    required_cols = {"Column Name", "Status"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Profile CSV missing columns: {sorted(missing)}")
    return df


def get_dropped_columns_from_profile(profile_csv_path):
    if not USE_PROFILE_CSV or not profile_csv_path:
        return set()

    df = read_profile_csv(profile_csv_path)
    if DROP_100_PERCENT_BLANK_COLUMNS:
        status_series = df["Status"].astype(str).str.upper().str.strip()
        col_series = df["Column Name"].astype(str)
        return set(col_series[status_series == "BLANK"].tolist())

    return set()


def read_single_gz_rows(gz_file, dropped_columns):
    rows = []

    with gzip.open(gz_file, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except Exception:
                continue

            if not isinstance(record, dict):
                continue

            cleaned_record = {}
            for key, value in record.items():
                if key in dropped_columns:
                    continue
                cleaned_record[key] = clean_value(key, value)

            rows.append(cleaned_record)

    return rows


def flush_batch_to_excel(batch_rows, output_folder, batch_number, batch_file_count):
    if not batch_rows:
        return None

    print(f"\nWriting batch {batch_number:05d} "
          f"with {len(batch_rows):,} rows from {batch_file_count:,} .gz files...")

    df = pd.DataFrame(batch_rows)
    output_file = output_folder / f"batch_{batch_number:05d}.xlsx"
    df.to_excel(output_file, index=False, engine="openpyxl")

    print(f"Finished writing {output_file.name}")
    return output_file


# ---------------- MAIN ----------------

if __name__ == "__main__":
    input_folder = Path(INPUT_FOLDER)
    output_folder = Path(OUTPUT_FOLDER)
    output_folder.mkdir(parents=True, exist_ok=True)

    if not input_folder.exists():
        print(f"Input folder not found: {INPUT_FOLDER}")
        raise SystemExit(1)

    gz_files = sorted(input_folder.glob("*.gz"))
    if not gz_files:
        print("No .gz files found.")
        raise SystemExit(1)

    dropped_columns = set()
    if USE_PROFILE_CSV:
        try:
            dropped_columns = get_dropped_columns_from_profile(PROFILE_CSV)
            print(f"Loaded profile CSV. Dropping {len(dropped_columns)} blank columns.")
        except Exception as e:
            print(f"Failed to read profile CSV: {e}")
            raise SystemExit(1)

    print(f"Found {len(gz_files)} .gz files.")
    print(f"Excel max rows:             {EXCEL_MAX_ROWS:,}")
    print(f"Reserved empty rows:        {RESERVED_EMPTY_ROWS:,}")
    print(f"Hard usable Excel limit:    {HARD_USABLE_ROWS_LIMIT:,}")
    print(f"Target rows per batch:      {TARGET_ROWS_PER_BATCH:,}")
    print(f"Max .gz files per batch:    {MAX_FILES_PER_BATCH:,}")
    print(f"Output folder:              {OUTPUT_FOLDER}")

    batch_rows = []
    batch_row_count = 0
    batch_file_count = 0
    batch_number = 1

    success_count = 0
    fail_count = 0
    failures = []
    written_files = []

    for gz_file in tqdm(gz_files, desc="Processing .gz files", unit="file"):
        try:
            file_rows = read_single_gz_rows(gz_file, dropped_columns)
            valid_row_count = len(file_rows)

            if valid_row_count == 0:
                fail_count += 1
                failures.append(f"{gz_file.name}: no valid rows")
                continue

            # If one single file is too large even for Excel hard limit, exit
            if valid_row_count > HARD_USABLE_ROWS_LIMIT:
                print("\nERROR:")
                print(
                    f"File '{gz_file.name}' has {valid_row_count:,} valid rows, "
                    f"which exceeds the hard Excel usable limit of {HARD_USABLE_ROWS_LIMIT:,} rows."
                )
                print("Exiting without continuing further.")
                raise SystemExit(1)

            # If adding this file would exceed target batch size or file-count cap, flush first
            if (
                batch_row_count + valid_row_count > TARGET_ROWS_PER_BATCH
                or batch_file_count >= MAX_FILES_PER_BATCH
            ):
                output_file = flush_batch_to_excel(
                    batch_rows=batch_rows,
                    output_folder=output_folder,
                    batch_number=batch_number,
                    batch_file_count=batch_file_count
                )
                if output_file:
                    written_files.append(str(output_file))
                    batch_number += 1

                batch_rows = []
                batch_row_count = 0
                batch_file_count = 0

            # Add current file to active batch
            batch_rows.extend(file_rows)
            batch_row_count += valid_row_count
            batch_file_count += 1
            success_count += 1

        except SystemExit:
            raise
        except Exception as e:
            fail_count += 1
            failures.append(f"{gz_file.name}: {e}")

    # Flush final batch
    if batch_rows:
        output_file = flush_batch_to_excel(
            batch_rows=batch_rows,
            output_folder=output_folder,
            batch_number=batch_number,
            batch_file_count=batch_file_count
        )
        if output_file:
            written_files.append(str(output_file))

    print("\n" + "=" * 80)
    print("CONVERSION COMPLETE")
    print("=" * 80)
    print(f"Successful .gz files: {success_count:,}")
    print(f"Failed .gz files:     {fail_count:,}")
    print(f"Excel files written:  {len(written_files):,}")
    print(f"Output folder:        {output_folder}")

    if written_files:
        print("\nSample output files:")
        for item in written_files[:10]:
            print(f" - {item}")

    if failures:
        print("\nSample failures:")
        for item in failures[:20]:
            print(f" - {item}")