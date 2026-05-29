import os
import gzip
import json
from pathlib import Path
from urllib.parse import unquote
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import orjson
from tqdm import tqdm


# ---------------- CONFIG DEFAULTS ----------------

DEFAULT_PLACEHOLDERS = {"-", "^"}
IST_TZ = ZoneInfo("Asia/Kolkata")

PARQUET_COMPRESSION = "snappy"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_ROW_GROUP_SIZE = 128_000
DEFAULT_MAX_WORKERS = max(1, (os.cpu_count() or 4) - 2)


# ---------------- HELPER: MENU ----------------

def ask_yes_no(prompt, default="y"):
    default = default.lower().strip()
    while True:
        raw = input(f"{prompt} ({'Y/n' if default == 'y' else 'y/N'}): ").strip().lower()

        if not raw:
            return default == "y"
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False

        print("Please enter y or n.")


def ask_text(prompt, default=""):
    raw = input(f"{prompt}: ").strip()
    return raw if raw else default


def ask_int(prompt, default):
    raw = input(f"{prompt} [{default}]: ").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        print(f"Invalid number. Using default = {default}")
        return default


def parse_column_list(raw_text):
    if not raw_text.strip():
        return []
    return [x.strip() for x in raw_text.split(",") if x.strip()]


def print_column_block(title, columns, max_show=200):
    columns = sorted(columns)
    print(f"\n{title} ({len(columns)}):")
    if not columns:
        print("  [none]")
        return

    for col in columns[:max_show]:
        print(f"  - {col}")

    if len(columns) > max_show:
        print(f"  ... and {len(columns) - max_show} more")


# ---------------- HELPER: PROFILE CSV ----------------

def read_profile_csv(csv_path):
    df = pd.read_csv(csv_path)

    required_cols = {
        "Column Name",
        "Status",
        "Filled Rows",
        "Present Rows",
        "Total Rows",
        "% Filled (All Rows)",
        "% Filled (When Present)",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {sorted(missing)}\n"
            f"Make sure you are using the CSV produced by your analyzer script."
        )

    return df


def collect_columns_from_profile(df):
    return set(df["Column Name"].astype(str).tolist())


def collect_blank_columns_from_profile(df):
    status_series = df["Status"].astype(str).str.upper().str.strip()
    col_series = df["Column Name"].astype(str)
    return set(col_series[status_series == "BLANK"].tolist())


# ---------------- HELPER: VALUE TRANSFORMS ----------------

def safe_none_if_placeholder(value, placeholders, convert_placeholders):
    if not convert_placeholders:
        return value

    if value is None:
        return None

    if value in placeholders:
        return None

    if isinstance(value, str) and value in placeholders:
        return None

    if str(value) in placeholders:
        return None

    return value


def parse_epoch_seconds(value):
    if value is None:
        return None

    try:
        num = float(str(value).strip())
        return datetime.fromtimestamp(num, tz=timezone.utc)
    except Exception:
        return None


def auto_decode_url_if_needed(value):
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


def build_output_record(
    record,
    kept_columns,
    placeholders,
    convert_placeholders,
    convert_reqtimesec_to_ist=False,
    decode_url_text=False,
    add_source_file=False,
    source_file_name=None,
):
    output = {}

    for col in kept_columns:
        value = record.get(col, None)

        # Step 1: placeholder cleanup
        value = safe_none_if_placeholder(value, placeholders, convert_placeholders)

        # Step 2: reqTimeSec in-place conversion to IST datetime
        if convert_reqtimesec_to_ist and col == "reqTimeSec":
            dt_utc = parse_epoch_seconds(value)
            value = dt_utc.astimezone(IST_TZ).replace(tzinfo=None) if dt_utc else None

        # Step 3: URL decode text
        if decode_url_text:
            value = auto_decode_url_if_needed(value)

        output[col] = value

    if add_source_file:
        output["_source_file"] = source_file_name

    return output


# ---------------- HELPER: FIXED SCHEMA ----------------

def build_fixed_arrow_schema(kept_columns, convert_reqtimesec_to_ist=False, add_source_file=False):
    fields = []

    for col in kept_columns:
        if col == "reqTimeSec" and convert_reqtimesec_to_ist:
            fields.append(pa.field(col, pa.timestamp("us")))
        else:
            fields.append(pa.field(col, pa.string()))

    if add_source_file:
        fields.append(pa.field("_source_file", pa.string()))

    return pa.schema(fields)


def normalize_record_for_schema(
    record,
    kept_columns,
    convert_reqtimesec_to_ist=False,
    add_source_file=False
):
    normalized = {}

    for col in kept_columns:
        value = record.get(col, None)

        if col == "reqTimeSec" and convert_reqtimesec_to_ist:
            normalized[col] = value
        else:
            normalized[col] = None if value is None else str(value)

    if add_source_file:
        source_value = record.get("_source_file", None)
        normalized["_source_file"] = None if source_value is None else str(source_value)

    return normalized


# ---------------- HELPER: BATCH / META ----------------

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def load_processed_batches(meta_file: Path):
    done = set()
    if not meta_file.exists():
        return done

    with meta_file.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
            except Exception:
                continue

            if entry.get("status") == "success":
                done.add(entry["batch_id"])

    return done


def append_result(meta_file: Path, result: dict):
    with meta_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")


# ---------------- CORE: PROCESS ONE BATCH ----------------

def process_batch(task: dict) -> dict:
    batch_id = task["batch_id"]
    file_paths = task["file_paths"]
    output_path = Path(task["output_path"])
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")

    kept_columns = task["kept_columns"]
    placeholders = set(task["placeholders"])
    convert_placeholders = task["convert_placeholders"]
    convert_reqtimesec_to_ist = task["convert_reqtimesec_to_ist"]
    decode_url_text = task["decode_url_text"]
    add_source_file = task["add_source_file"]
    row_group_size = task["row_group_size"]

    arrow_schema = build_fixed_arrow_schema(
        kept_columns=kept_columns,
        convert_reqtimesec_to_ist=convert_reqtimesec_to_ist,
        add_source_file=add_source_file,
    )

    writer = None
    failed_files = []
    rows_written = 0

    try:
        for gz_path in file_paths:
            batch_rows = []

            try:
                with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            record = orjson.loads(line)
                        except Exception:
                            continue

                        if not isinstance(record, dict):
                            continue

                        output_record = build_output_record(
                            record=record,
                            kept_columns=kept_columns,
                            placeholders=placeholders,
                            convert_placeholders=convert_placeholders,
                            convert_reqtimesec_to_ist=convert_reqtimesec_to_ist,
                            decode_url_text=decode_url_text,
                            add_source_file=add_source_file,
                            source_file_name=os.path.basename(gz_path),
                        )

                        output_record = normalize_record_for_schema(
                            record=output_record,
                            kept_columns=kept_columns,
                            convert_reqtimesec_to_ist=convert_reqtimesec_to_ist,
                            add_source_file=add_source_file,
                        )

                        batch_rows.append(output_record)

                if batch_rows:
                    table = pa.Table.from_pylist(batch_rows, schema=arrow_schema)

                    if writer is None:
                        writer = pq.ParquetWriter(
                            where=str(temp_path),
                            schema=arrow_schema,
                            compression=PARQUET_COMPRESSION,
                        )

                    writer.write_table(table, row_group_size=row_group_size)
                    rows_written += len(batch_rows)

            except Exception:
                failed_files.append(os.path.basename(gz_path))

        if writer is None:
            return {
                "status": "failed",
                "batch_id": batch_id,
                "error": "no valid rows",
                "failed_files": failed_files,
            }

        writer.close()
        temp_path.replace(output_path)

        return {
            "status": "success",
            "batch_id": batch_id,
            "output_file": output_path.name,
            "files_in_batch": len(file_paths),
            "rows_written": rows_written,
            "failed_files": failed_files,
        }

    except Exception as e:
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass

        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass

        return {
            "status": "failed",
            "batch_id": batch_id,
            "error": str(e),
            "failed_files": failed_files,
        }


# ---------------- MAIN ----------------

if __name__ == "__main__":
    print("=" * 80)
    print("FAST MENU-DRIVEN GZ TO PARQUET CONVERTER")
    print("=" * 80)

    # Step 1: profile csv
    profile_csv_path = ask_text("Enter analyzer CSV path")
    profile_csv = Path(profile_csv_path)

    if not profile_csv.exists():
        print("CSV file not found.")
        raise SystemExit(1)

    try:
        profile_df = read_profile_csv(profile_csv)
    except Exception as e:
        print(f"Failed to read profile CSV: {e}")
        raise SystemExit(1)

    all_columns = collect_columns_from_profile(profile_df)
    blank_columns = collect_blank_columns_from_profile(profile_df)

    print(f"\nProfile CSV loaded successfully.")
    print(f"Total columns in profile: {len(all_columns)}")
    print(f"Columns marked BLANK:     {len(blank_columns)}")

    # Step 2: menu decisions
    drop_blank_fields = ask_yes_no("Should I drop 100% blank fields", default="y")
    convert_placeholders = ask_yes_no("Should I convert '-' and '^' to NULL", default="y")

    dropped_columns = set()
    if drop_blank_fields:
        dropped_columns.update(blank_columns)

    remaining_columns = sorted(all_columns - dropped_columns)

    print_column_block("Automatically dropped columns", dropped_columns)
    print_column_block("Columns remaining after automatic rules", remaining_columns)

    if ask_yes_no("Do you want to manually remove more columns", default="n"):
        print_column_block("Remaining columns available for manual removal", remaining_columns)
        raw_remove = ask_text("Enter column names to remove, comma separated", default="")
        manual_remove = set(parse_column_list(raw_remove))

        invalid_remove = manual_remove - set(remaining_columns)
        valid_remove = manual_remove & set(remaining_columns)

        if invalid_remove:
            print_column_block("These columns were not found and were ignored", invalid_remove)

        dropped_columns.update(valid_remove)
        remaining_columns = sorted(all_columns - dropped_columns)

    if ask_yes_no("Do you want to explicitly choose columns to keep from remaining columns", default="n"):
        print_column_block("Remaining columns available to keep", remaining_columns)
        raw_keep = ask_text("Enter column names to keep, comma separated", default="")
        manual_keep = set(parse_column_list(raw_keep))

        invalid_keep = manual_keep - set(remaining_columns)
        valid_keep = manual_keep & set(remaining_columns)

        if invalid_keep:
            print_column_block("These columns were not found and were ignored", invalid_keep)

        remaining_columns = sorted(valid_keep)
        dropped_columns = all_columns - set(remaining_columns)

    kept_columns = sorted(remaining_columns)

    print_column_block("Final dropped columns", dropped_columns)
    print_column_block("Final kept columns", kept_columns)

    if not kept_columns:
        print("No columns left to convert. Exiting.")
        raise SystemExit(1)

    convert_reqtimesec_to_ist = False
    if "reqTimeSec" in kept_columns:
        print("\nDetected column: reqTimeSec")
        convert_reqtimesec_to_ist = ask_yes_no(
            "Convert reqTimeSec in place to IST",
            default="y"
        )

    decode_url_text = ask_yes_no(
        "Decode URL-encoded text in kept string columns (%20 -> space, etc.)",
        default="y"
    )

    add_source_file = ask_yes_no(
        "Add _source_file column to trace original gz file",
        default="y"
    )

    # Step 3: runtime config
    input_folder_path = ask_text("Enter input folder containing .gz files")
    output_folder_path = ask_text("Enter output folder for .parquet files")

    input_folder = Path(input_folder_path)
    output_folder = Path(output_folder_path)

    if not input_folder.exists():
        print("Input folder not found.")
        raise SystemExit(1)

    output_folder.mkdir(parents=True, exist_ok=True)

    batch_size = ask_int("How many .gz files per parquet batch", DEFAULT_BATCH_SIZE)
    max_workers = ask_int("How many worker processes", DEFAULT_MAX_WORKERS)
    row_group_size = ask_int("Parquet row group size", DEFAULT_ROW_GROUP_SIZE)

    meta_file = output_folder / "batch_results.jsonl"

    # Final summary
    print("\n" + "=" * 80)
    print("FINAL CONVERSION PLAN")
    print("=" * 80)
    print(f"Drop 100% blank fields:           {'Yes' if drop_blank_fields else 'No'}")
    print(f"Convert '-' and '^' to NULL:      {'Yes' if convert_placeholders else 'No'}")
    print(f"Convert reqTimeSec to IST:        {'Yes' if convert_reqtimesec_to_ist else 'No'}")
    print(f"Decode URL-encoded text:          {'Yes' if decode_url_text else 'No'}")
    print(f"Add _source_file:                 {'Yes' if add_source_file else 'No'}")
    print(f"Total dropped columns:            {len(dropped_columns)}")
    print(f"Total kept columns:               {len(kept_columns)}")
    print(f"Batch size (.gz per parquet):     {batch_size}")
    print(f"Worker processes:                 {max_workers}")
    print(f"Row group size:                   {row_group_size}")
    print(f"Meta file:                        {meta_file}")

    print_column_block("Columns that will be written to parquet", kept_columns)

    if not ask_yes_no("Proceed with conversion", default="y"):
        print("Conversion cancelled.")
        raise SystemExit(0)

    # Step 4: discover files
    print("\nScanning input folder...")
    gz_files = [str(p) for p in input_folder.glob("*.gz")]

    if not gz_files:
        print("No .gz files found in input folder.")
        raise SystemExit(1)

    print(f"Found {len(gz_files)} .gz files.")

    # Step 5: build batches
    batches = []
    for i, chunk in enumerate(chunked(gz_files, batch_size)):
        batch_id = f"batch_{i:06d}"
        output_path = output_folder / f"{batch_id}.parquet"

        batches.append({
            "batch_id": batch_id,
            "file_paths": chunk,
            "output_path": str(output_path),
            "kept_columns": kept_columns,
            "placeholders": list(DEFAULT_PLACEHOLDERS),
            "convert_placeholders": convert_placeholders,
            "convert_reqtimesec_to_ist": convert_reqtimesec_to_ist,
            "decode_url_text": decode_url_text,
            "add_source_file": add_source_file,
            "row_group_size": row_group_size,
        })

    total_batches = len(batches)
    done_batches = load_processed_batches(meta_file)
    pending_batches = [b for b in batches if b["batch_id"] not in done_batches]

    print(f"Total batches:                    {total_batches}")
    print(f"Already completed:                {total_batches - len(pending_batches)}")
    print(f"Pending batches:                  {len(pending_batches)}")

    if not pending_batches:
        print("All batches already processed. Nothing to do.")
        raise SystemExit(0)

    # Step 6: execute
    success_count = 0
    fail_count = 0
    total_rows = 0
    partial_failures = []

    print("\nStarting parallel conversion...")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(process_batch, batch): batch
            for batch in pending_batches
        }

        with tqdm(total=len(pending_batches), desc="Batches", unit="batch") as pbar:
            for future in as_completed(future_to_batch):
                result = future.result()
                append_result(meta_file, result)

                if result["status"] == "success":
                    success_count += 1
                    total_rows += result.get("rows_written", 0)

                    if result.get("failed_files"):
                        partial_failures.extend(result["failed_files"])
                else:
                    fail_count += 1
                    partial_failures.append(
                        f"{result['batch_id']}: {result.get('error', 'unknown error')}"
                    )

                pbar.update(1)
                pbar.set_postfix({
                    "Success": success_count,
                    "Fail": fail_count,
                    "Rows": total_rows,
                })

    # Step 7: summary
    print("\n" + "=" * 80)
    print("CONVERSION COMPLETE")
    print("=" * 80)
    print(f"Successful batches:               {success_count}")
    print(f"Failed batches:                   {fail_count}")
    print(f"Total rows written:               {total_rows}")
    print(f"Output folder:                    {output_folder}")
    print(f"Meta file:                        {meta_file}")

    if partial_failures:
        print("\nSample failures:")
        for item in partial_failures[:20]:
            print(f"  - {item}")