import os
import csv
import gzip
import math
import time
import json
import hashlib
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# Production-style GZ JSONL -> Manifest -> Profile -> Parquet
# ============================================================
# What this script does:
# 1. Recursively discovers .gz files across one or more root folders
# 2. Builds a manifest so work can resume safely
# 3. Samples files to infer columns / schema profile
# 4. Converts raw .gz JSONL logs into compact Parquet batches
# 5. Writes summary CSV + JSON reports
#
# Why this is better than per-folder ad hoc scripts:
# - handles many folders
# - supports resume
# - avoids repeated raw scanning for analysis
# - compacts millions of small gzip files into fewer Parquet files
#
# Assumptions:
# - each .gz contains JSON Lines
# - each valid line decodes to a dict-like JSON object
# - schema may drift across files
# ============================================================


PLACEHOLDERS = {"-", "^"}
DEFAULT_BATCH_FILE_COUNT = 1000
DEFAULT_ROW_GROUP_SIZE = 100_000
DEFAULT_SAMPLE_FILES = 5000
DEFAULT_MAX_WORKERS = max(1, min(8, (os.cpu_count() or 4) - 1))
DEFAULT_OUTPUT_COMPRESSION = "snappy"


# -----------------------------
# Utility helpers
# -----------------------------

def now_ts() -> float:
    return time.time()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def safe_relpath(path: Path, roots: list[Path]) -> str:
    for root in roots:
        try:
            return str(path.relative_to(root))
        except Exception:
            pass
    return str(path)


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def normalize_scalar(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    try:
        return orjson.dumps(value).decode("utf-8")
    except Exception:
        return str(value)


# -----------------------------
# Manifest discovery
# -----------------------------

def discover_gz_files(input_roots: list[Path]) -> list[dict]:
    records = []
    for root in input_roots:
        if not root.exists():
            print(f"[WARN] Input root not found: {root}")
            continue

        for path in root.rglob("*.gz"):
            try:
                stat = path.stat()
                records.append({
                    "file_id": sha1_text(str(path.resolve())),
                    "root": str(root.resolve()),
                    "folder_name": root.name,
                    "file_path": str(path.resolve()),
                    "relative_path": safe_relpath(path, input_roots),
                    "file_name": path.name,
                    "size_bytes": stat.st_size,
                    "modified_ts": stat.st_mtime,
                })
            except Exception as e:
                records.append({
                    "file_id": sha1_text(str(path)),
                    "root": str(root),
                    "folder_name": root.name,
                    "file_path": str(path),
                    "relative_path": safe_relpath(path, input_roots),
                    "file_name": path.name,
                    "size_bytes": -1,
                    "modified_ts": 0,
                    "discovery_error": str(e),
                })
    return records


def write_manifest_csv(records: list[dict], path: Path) -> None:
    ensure_dir(path.parent)
    fieldnames = [
        "file_id", "root", "folder_name", "file_path", "relative_path", "file_name",
        "size_bytes", "modified_ts", "discovery_error"
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


# -----------------------------
# Sampling / profiling
# -----------------------------

def profile_single_gz(file_path: str) -> dict:
    total_rows = 0
    valid_rows = 0
    invalid_json_rows = 0
    nondict_rows = 0
    present_counts = defaultdict(int)
    filled_counts = defaultdict(int)
    type_counter = defaultdict(Counter)
    error = None

    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue

                total_rows += 1
                try:
                    obj = orjson.loads(line)
                except Exception:
                    invalid_json_rows += 1
                    continue

                if not isinstance(obj, dict):
                    nondict_rows += 1
                    continue

                valid_rows += 1
                for k, v in obj.items():
                    present_counts[k] += 1
                    v_norm = normalize_scalar(v)
                    if v_norm is not None and str(v_norm) not in PLACEHOLDERS:
                        filled_counts[k] += 1
                    type_counter[k][type(v).__name__] += 1
    except Exception as e:
        error = str(e)

    return {
        "file_path": file_path,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_json_rows": invalid_json_rows,
        "nondict_rows": nondict_rows,
        "present_counts": dict(present_counts),
        "filled_counts": dict(filled_counts),
        "type_counter": {k: dict(v) for k, v in type_counter.items()},
        "error": error,
    }


def merge_profile_results(results: list[dict]) -> dict:
    grand_total_rows = 0
    grand_valid_rows = 0
    grand_invalid_json_rows = 0
    grand_nondict_rows = 0
    file_errors = []

    present_counts = defaultdict(int)
    filled_counts = defaultdict(int)
    type_counter = defaultdict(Counter)

    for r in results:
        grand_total_rows += r["total_rows"]
        grand_valid_rows += r["valid_rows"]
        grand_invalid_json_rows += r["invalid_json_rows"]
        grand_nondict_rows += r["nondict_rows"]
        if r.get("error"):
            file_errors.append({"file_path": r["file_path"], "error": r["error"]})

        for k, v in r["present_counts"].items():
            present_counts[k] += v
        for k, v in r["filled_counts"].items():
            filled_counts[k] += v
        for k, tc in r["type_counter"].items():
            type_counter[k].update(tc)

    columns = sorted(set(present_counts) | set(filled_counts) | set(type_counter))
    column_rows = []

    for col in columns:
        present = present_counts.get(col, 0)
        filled = filled_counts.get(col, 0)
        pct_all_valid = (filled / grand_valid_rows * 100.0) if grand_valid_rows else 0.0
        pct_when_present = (filled / present * 100.0) if present else 0.0
        dominant_type = ""
        if type_counter.get(col):
            dominant_type = type_counter[col].most_common(1)[0][0]

        column_rows.append({
            "column_name": col,
            "status": "BLANK" if filled == 0 else "HAS_DATA",
            "filled_rows": filled,
            "present_rows": present,
            "valid_rows": grand_valid_rows,
            "pct_filled_all_valid_rows": round(pct_all_valid, 4),
            "pct_filled_when_present": round(pct_when_present, 4),
            "dominant_type": dominant_type,
            "type_breakdown": dict(type_counter[col]),
        })

    return {
        "summary": {
            "total_rows_seen": grand_total_rows,
            "valid_rows": grand_valid_rows,
            "invalid_json_rows": grand_invalid_json_rows,
            "nondict_rows": grand_nondict_rows,
            "unique_columns": len(columns),
            "blank_columns": sum(1 for x in column_rows if x["status"] == "BLANK"),
            "columns_with_data": sum(1 for x in column_rows if x["status"] == "HAS_DATA"),
            "files_with_errors": len(file_errors),
        },
        "columns": column_rows,
        "file_errors": file_errors,
    }


def write_profile_outputs(profile: dict, out_dir: Path) -> None:
    ensure_dir(out_dir)

    with (out_dir / "profile_summary.json").open("w", encoding="utf-8") as f:
        json.dump(profile["summary"], f, ensure_ascii=False, indent=2)

    with (out_dir / "profile_columns.json").open("w", encoding="utf-8") as f:
        json.dump(profile["columns"], f, ensure_ascii=False, indent=2)

    with (out_dir / "profile_errors.json").open("w", encoding="utf-8") as f:
        json.dump(profile["file_errors"], f, ensure_ascii=False, indent=2)

    csv_path = out_dir / "profile_columns.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Column Name", "Status", "Filled Rows", "Present Rows", "Valid Rows",
            "% Filled (All Valid Rows)", "% Filled (When Present)", "Dominant Type", "Type Breakdown"
        ])
        for row in profile["columns"]:
            writer.writerow([
                row["column_name"],
                row["status"],
                row["filled_rows"],
                row["present_rows"],
                row["valid_rows"],
                row["pct_filled_all_valid_rows"],
                row["pct_filled_when_present"],
                row["dominant_type"],
                json.dumps(row["type_breakdown"], ensure_ascii=False),
            ])


# -----------------------------
# Column selection
# -----------------------------

def choose_kept_columns(profile: dict, drop_blank_columns: bool, explicit_keep: list[str] | None) -> list[str]:
    all_columns = [row["column_name"] for row in profile["columns"]]
    blank_columns = {row["column_name"] for row in profile["columns"] if row["status"] == "BLANK"}

    if explicit_keep:
        chosen = [c for c in explicit_keep if c in set(all_columns)]
        return sorted(chosen)

    if drop_blank_columns:
        return sorted([c for c in all_columns if c not in blank_columns])

    return sorted(all_columns)


# -----------------------------
# Parquet conversion helpers
# -----------------------------

def build_arrow_schema(columns: list[str], add_source_meta: bool = True) -> pa.Schema:
    fields = [pa.field(c, pa.string()) for c in columns]
    if add_source_meta:
        fields.extend([
            pa.field("_source_file", pa.string()),
            pa.field("_source_folder", pa.string()),
            pa.field("_source_root", pa.string()),
        ])
    return pa.schema(fields)


def normalize_record_for_output(record: dict, columns: list[str], convert_placeholders: bool,
                                add_source_meta: bool, source_file: str, source_folder: str, source_root: str) -> dict:
    out = {}
    for c in columns:
        value = record.get(c)
        value = normalize_scalar(value)
        if convert_placeholders and value is not None and str(value) in PLACEHOLDERS:
            value = None
        out[c] = None if value is None else str(value)

    if add_source_meta:
        out["_source_file"] = source_file
        out["_source_folder"] = source_folder
        out["_source_root"] = source_root

    return out


def process_parquet_batch(task: dict) -> dict:
    batch_id = task["batch_id"]
    output_path = Path(task["output_path"])
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    schema = build_arrow_schema(task["columns"], add_source_meta=task["add_source_meta"])

    rows_written = 0
    files_processed = 0
    files_failed = []
    writer = None

    try:
        for file_info in task["files"]:
            file_path = file_info["file_path"]
            source_file = file_info["file_name"]
            source_folder = file_info["folder_name"]
            source_root = file_info["root"]

            buffer_rows = []
            try:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            obj = orjson.loads(line)
                        except Exception:
                            continue
                        if not isinstance(obj, dict):
                            continue

                        out_row = normalize_record_for_output(
                            record=obj,
                            columns=task["columns"],
                            convert_placeholders=task["convert_placeholders"],
                            add_source_meta=task["add_source_meta"],
                            source_file=source_file,
                            source_folder=source_folder,
                            source_root=source_root,
                        )
                        buffer_rows.append(out_row)

                        if len(buffer_rows) >= task["row_buffer_size"]:
                            table = pa.Table.from_pylist(buffer_rows, schema=schema)
                            if writer is None:
                                writer = pq.ParquetWriter(
                                    str(temp_path),
                                    schema=schema,
                                    compression=task["compression"],
                                )
                            writer.write_table(table, row_group_size=task["row_group_size"])
                            rows_written += len(buffer_rows)
                            buffer_rows.clear()

                if buffer_rows:
                    table = pa.Table.from_pylist(buffer_rows, schema=schema)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(temp_path),
                            schema=schema,
                            compression=task["compression"],
                        )
                    writer.write_table(table, row_group_size=task["row_group_size"])
                    rows_written += len(buffer_rows)
                    buffer_rows.clear()

                files_processed += 1
            except Exception as e:
                files_failed.append({"file_path": file_path, "error": str(e)})

        if writer is None:
            return {
                "status": "failed",
                "batch_id": batch_id,
                "rows_written": 0,
                "files_processed": files_processed,
                "files_failed": files_failed,
                "error": "No valid rows written in batch",
            }

        writer.close()
        temp_path.replace(output_path)

        return {
            "status": "success",
            "batch_id": batch_id,
            "rows_written": rows_written,
            "files_processed": files_processed,
            "files_failed": files_failed,
            "output_file": output_path.name,
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
            "rows_written": rows_written,
            "files_processed": files_processed,
            "files_failed": files_failed,
            "error": str(e),
        }


# -----------------------------
# Status / resume tracking
# -----------------------------

def append_jsonl(path: Path, row: dict) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_successful_batch_ids(path: Path) -> set[str]:
    done = set()
    if not path.exists():
        return done

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("status") == "success":
                done.add(obj.get("batch_id"))
    return done


# -----------------------------
# Main pipeline
# -----------------------------

def run_pipeline(args):
    input_roots = [Path(p).resolve() for p in args.input_roots]
    output_root = Path(args.output_root).resolve()
    ensure_dir(output_root)

    manifest_dir = output_root / "manifest"
    profile_dir = output_root / "profile"
    parquet_dir = output_root / "parquet"
    status_dir = output_root / "status"

    ensure_dir(manifest_dir)
    ensure_dir(profile_dir)
    ensure_dir(parquet_dir)
    ensure_dir(status_dir)

    # 1) Discover files
    print("\n[1/5] Discovering gzip files...")
    records = discover_gz_files(input_roots)
    if not records:
        raise RuntimeError("No .gz files found under the provided roots.")

    manifest_csv = manifest_dir / "all_gz_files.csv"
    write_manifest_csv(records, manifest_csv)
    print(f"Discovered {len(records):,} .gz files")
    print(f"Manifest written: {manifest_csv}")

    # 2) Profile sample
    print("\n[2/5] Profiling sample files...")
    sample_n = min(args.sample_files, len(records))
    sample_records = records[:sample_n]
    sample_paths = [r["file_path"] for r in sample_records]

    profile_results = []
    with ProcessPoolExecutor(max_workers=args.max_workers) as ex:
        futures = [ex.submit(profile_single_gz, p) for p in sample_paths]
        for idx, fut in enumerate(as_completed(futures), 1):
            profile_results.append(fut.result())
            if idx % 100 == 0 or idx == len(futures):
                print(f"  Profiled {idx:,}/{len(futures):,} sample files")

    profile = merge_profile_results(profile_results)
    write_profile_outputs(profile, profile_dir)
    print(f"Profile summary written to: {profile_dir}")
    print(json.dumps(profile["summary"], indent=2))

    # 3) Choose columns
    print("\n[3/5] Choosing output columns...")
    explicit_keep = [x.strip() for x in args.keep_columns.split(",") if x.strip()] if args.keep_columns else None
    kept_columns = choose_kept_columns(
        profile=profile,
        drop_blank_columns=not args.keep_blank_columns,
        explicit_keep=explicit_keep,
    )

    if not kept_columns:
        raise RuntimeError("No columns selected for output.")

    with (profile_dir / "selected_columns.json").open("w", encoding="utf-8") as f:
        json.dump(kept_columns, f, ensure_ascii=False, indent=2)

    print(f"Selected {len(kept_columns):,} columns for Parquet output")

    # 4) Build conversion batches
    print("\n[4/5] Building conversion batches...")
    all_batches = []
    for idx, chunk in enumerate(chunked(records, args.batch_file_count)):
        batch_id = f"batch_{idx:07d}"
        all_batches.append({
            "batch_id": batch_id,
            "files": chunk,
            "output_path": str(parquet_dir / f"{batch_id}.parquet"),
            "columns": kept_columns,
            "convert_placeholders": args.convert_placeholders,
            "add_source_meta": True,
            "compression": args.compression,
            "row_group_size": args.row_group_size,
            "row_buffer_size": args.row_buffer_size,
        })

    batch_status_jsonl = status_dir / "batch_status.jsonl"
    done_batch_ids = load_successful_batch_ids(batch_status_jsonl)
    pending_batches = [b for b in all_batches if b["batch_id"] not in done_batch_ids]

    print(f"Total batches: {len(all_batches):,}")
    print(f"Already done: {len(done_batch_ids):,}")
    print(f"Pending: {len(pending_batches):,}")

    if not pending_batches:
        print("Nothing left to process.")
        return

    # 5) Convert to Parquet
    print("\n[5/5] Converting gzip to compact Parquet batches...")
    total_rows_written = 0
    success_batches = 0
    failed_batches = 0
    total_files_failed = 0

    with ProcessPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {ex.submit(process_parquet_batch, batch): batch["batch_id"] for batch in pending_batches}
        for idx, fut in enumerate(as_completed(futures), 1):
            result = fut.result()
            append_jsonl(batch_status_jsonl, result)

            if result["status"] == "success":
                success_batches += 1
                total_rows_written += result.get("rows_written", 0)
                total_files_failed += len(result.get("files_failed", []))
            else:
                failed_batches += 1
                total_files_failed += len(result.get("files_failed", []))

            if idx % 10 == 0 or idx == len(futures):
                print(
                    f"  Done {idx:,}/{len(futures):,} batches | "
                    f"success={success_batches:,} fail={failed_batches:,} rows={total_rows_written:,}"
                )

    final_summary = {
        "discovered_gz_files": len(records),
        "sampled_files": sample_n,
        "selected_columns": len(kept_columns),
        "total_batches": len(all_batches),
        "successful_batches": success_batches,
        "failed_batches": failed_batches,
        "rows_written": total_rows_written,
        "files_failed_inside_batches": total_files_failed,
        "output_root": str(output_root),
        "manifest_csv": str(manifest_csv),
        "profile_dir": str(profile_dir),
        "parquet_dir": str(parquet_dir),
        "batch_status": str(batch_status_jsonl),
    }

    with (output_root / "final_summary.json").open("w", encoding="utf-8") as f:
        json.dump(final_summary, f, ensure_ascii=False, indent=2)

    print("\nPipeline complete.")
    print(json.dumps(final_summary, indent=2))


# -----------------------------
# CLI
# -----------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        description="Recursively profile and convert large collections of .gz JSONL logs into compact Parquet batches."
    )
    parser.add_argument(
        "--input-roots",
        nargs="+",
        required=True,
        help="One or more root folders that contain .gz files in nested folders.",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Folder where manifest, profile, parquet, and status outputs will be written.",
    )
    parser.add_argument(
        "--sample-files",
        type=int,
        default=DEFAULT_SAMPLE_FILES,
        help=f"How many files to sample for schema profiling. Default={DEFAULT_SAMPLE_FILES}",
    )
    parser.add_argument(
        "--batch-file-count",
        type=int,
        default=DEFAULT_BATCH_FILE_COUNT,
        help=f"How many gzip files to compact into one Parquet batch. Default={DEFAULT_BATCH_FILE_COUNT}",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Parallel worker processes. Default={DEFAULT_MAX_WORKERS}",
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=DEFAULT_ROW_GROUP_SIZE,
        help=f"Parquet row group size. Default={DEFAULT_ROW_GROUP_SIZE}",
    )
    parser.add_argument(
        "--row-buffer-size",
        type=int,
        default=50000,
        help="How many parsed rows to hold before flushing to Parquet.",
    )
    parser.add_argument(
        "--compression",
        default=DEFAULT_OUTPUT_COMPRESSION,
        choices=["snappy", "gzip", "brotli", "lz4", "zstd", "none"],
        help="Parquet compression codec.",
    )
    parser.add_argument(
        "--keep-blank-columns",
        action="store_true",
        help="Keep columns that are 100%% blank in the profile sample. Default is to drop them.",
    )
    parser.add_argument(
        "--keep-columns",
        default="",
        help="Optional comma-separated explicit list of columns to keep. Overrides automatic selection.",
    )
    parser.add_argument(
        "--convert-placeholders",
        action="store_true",
        help="Convert '-' and '^' to null during Parquet output.",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    run_pipeline(args)
