"""
Akamai GZ → Parquet Converter
==============================
Converts Akamai DataStream 2 NDJSON .gz log files to Parquet format.
Handles placeholders, URL decoding, type coercion, deduplication,
parallel processing, and incremental reruns via hash tracking.

Usage:
    python akamai_to_parquet.py

Dependencies:
    pip install pandas pyarrow tqdm
"""

import os
import gzip
import json
import hashlib
import urllib.parse
import logging
from multiprocessing import Pool, cpu_count
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


# ─── CONFIGURATION ────────────────────────────────────────────────────────────

INPUT_DIR  = r"D:\VETO Logs\01"
OUTPUT_DIR = r"D:\VETO Logs\01 parquet"
META_FILE  = os.path.join(OUTPUT_DIR, "file_hashes.jsonl")

# Akamai DataStream 2 uses "-" for missing values and "^" for empty strings.
# Both are treated as null — tracked separately so you can change behaviour.
AKAMAI_NULL_VALUES = {"-", "^"}

# Columns that arrive as strings but should be numeric for analysis.
# Add/remove based on your property's log format version.
NUMERIC_COLS = [
    "bytes", "objSize", "throughput", "statusCode", "asn",
    "reqTimeSec", "transferTimeMSec", "totalBytes", "rspContentLen",
    "timeToFirstByte", "turnAroundTimeMSec", "tlsOverheadTimeMSec",
    "downloadTime", "reqEndTimeMSec", "maxAgeSec",
    "edgeAttempts", "billingRegion", "deliveryPolicyId",
    "deliveryPolicyReqStatus", "deliveryFormat", "deliveryType",
]

# Columns that may contain percent-encoded characters (e.g. %20 → space).
URL_ENCODED_COLS = ["UA", "state", "city", "reqPath", "queryStr", "referer"]

# reqTimeSec is a Unix float — parse it into a real datetime column.
TIMESTAMP_COL = "reqTimeSec"

# Workers for multiprocessing. Leave one core free for the OS.
MAX_WORKERS = max(1, cpu_count() - 1)


# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def md5_of_file(path: str) -> str:
    """
    Returns the MD5 hash of a file's raw bytes.
    Used to detect whether a source file has changed since last run,
    so we can skip re-processing unchanged files (incremental mode).
    """
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_processed_hashes(meta_file: str) -> dict[str, str]:
    """
    Reads the JSONL sidecar file that stores {filename → hash} of every
    successfully processed file. Returns a dict so lookups are O(1).
    If the file doesn't exist yet, returns an empty dict (first run).
    """
    seen: dict[str, str] = {}
    if not os.path.exists(meta_file):
        return seen
    with open(meta_file, "r", encoding="utf-8") as f:
        for line in f:
            entry = json.loads(line.strip())
            # Only keep "success" records — failed files will be retried.
            if entry.get("status") == "success":
                seen[entry["file_name"]] = entry["file_hash"]
    return seen


def append_result(meta_file: str, result: dict) -> None:
    """
    Appends one processing result to the JSONL sidecar atomically (line-by-line).
    Using append mode means we never need to rewrite the whole file.
    """
    with open(meta_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(result) + "\n")


# ─── CORE PROCESSING ──────────────────────────────────────────────────────────

def process_akamai_file(task: tuple[str, str]) -> dict:
    """
    Converts a single Akamai .gz NDJSON file to a Snappy-compressed Parquet file.

    Steps:
        1. Stream-read the gzipped NDJSON line by line (memory efficient).
        2. Replace Akamai null placeholders ("-", "^") with real NaN/None.
        3. URL-decode string columns that may contain percent-encoding.
        4. Coerce numeric columns from string → float/int.
        5. Parse the Unix timestamp into a proper datetime column.
        6. Drop exact duplicate rows (Akamai can occasionally repeat events).
        7. Write to Parquet using a .tmp file, then atomically rename.
           This prevents corrupt partial files if the process is killed mid-write.

    Returns a result dict that gets written to the META_FILE sidecar.
    """
    file_name, file_hash = task
    input_path  = os.path.join(INPUT_DIR, file_name)
    output_name = os.path.splitext(file_name)[0] + ".parquet"
    # Handle double extension: file.gz → file (not file.)
    if output_name.endswith("."):
        output_name = output_name[:-1]
    output_path = os.path.join(OUTPUT_DIR, output_name)
    temp_path   = output_path + ".tmp"

    try:
        # ── 1. Read NDJSON from gzip ──────────────────────────────────────────
        # pd.read_json(lines=True) is fine for moderate files.
        # For very large files (>500 MB uncompressed), switch to chunked reading:
        #   chunks = pd.read_json(f, lines=True, chunksize=50_000)
        #   df = pd.concat(chunks, ignore_index=True)
        with gzip.open(input_path, "rt", encoding="utf-8") as f:
            df = pd.read_json(f, lines=True, dtype=False)
            # dtype=False keeps everything as strings initially — safer than
            # letting pandas guess types, which can silently miscast fields.

        if df.empty:
            return {
                "status": "skipped", "file_name": file_name,
                "reason": "empty file",
            }

        # ── 2. Replace Akamai placeholders with None ──────────────────────────
        # Akamai uses "-" for "field not applicable" and "^" for "empty string".
        # Both become None so downstream aggregations ignore them correctly.
        # We use .replace() with regex=False for performance (exact match only).
        df.replace(list(AKAMAI_NULL_VALUES), None, inplace=True)

        # ── 3. URL-decode string columns ──────────────────────────────────────
        # Akamai percent-encodes UA strings, city names, paths, etc.
        # unquote() is idempotent — safe to call even if a value isn't encoded.
        for col in URL_ENCODED_COLS:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: urllib.parse.unquote(x) if isinstance(x, str) else x
                )

        # ── 4. Coerce to numeric ──────────────────────────────────────────────
        # errors='coerce' turns anything unparseable into NaN rather than raising.
        # We use Int64 (nullable int) instead of float where the column is always
        # whole-number (e.g. statusCode, asn) to save space and keep semantics.
        INTEGER_COLS = {"statusCode", "asn", "bytes", "objSize", "totalBytes",
                        "rspContentLen", "maxAgeSec", "edgeAttempts",
                        "billingRegion", "deliveryPolicyId",
                        "deliveryPolicyReqStatus", "deliveryFormat", "deliveryType"}
        for col in NUMERIC_COLS:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if col in INTEGER_COLS:
                # Pandas nullable integer keeps NaN support (regular int64 doesn't).
                df[col] = df[col].astype("Int64")

        # ── 5. Parse Unix timestamp ───────────────────────────────────────────
        # reqTimeSec is a float like 1775031224.205 (seconds since epoch).
        # Converting it to datetime(UTC) makes time-series analysis much easier.
        if TIMESTAMP_COL in df.columns:
            df["reqTime"] = pd.to_datetime(
                df[TIMESTAMP_COL], unit="s", utc=True, errors="coerce"
            )

        # ── 6. Drop exact duplicate rows ──────────────────────────────────────
        # Akamai's delivery guarantees can occasionally produce duplicate log
        # lines within the same file. Drop them before writing.
        before = len(df)
        df.drop_duplicates(inplace=True)
        dupes = before - len(df)
        if dupes:
            log.debug("%s: dropped %d duplicate rows", file_name, dupes)

        # ── 7. Write Parquet (atomic via .tmp rename) ─────────────────────────
        # Snappy is the best default: fast compression/decompression, good ratio.
        # Use "zstd" if you prefer higher compression at slightly more CPU cost.
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, temp_path, compression="snappy")
        os.replace(temp_path, output_path)  # atomic on same filesystem

        return {
            "status": "success",
            "file_name": file_name,
            "file_hash": file_hash,
            "output_file": output_name,
            "rows": len(df),
            "dupes_dropped": dupes,
        }

    except Exception as e:
        # Clean up any partial .tmp file so next run starts fresh.
        if os.path.exists(temp_path):
            os.remove(temp_path)
        log.error("FAILED %s: %s", file_name, e)
        return {"status": "failed", "file_name": file_name, "error": str(e)}


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Build list of .gz files in INPUT_DIR
    all_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".gz")]
    if not all_files:
        log.warning("No .gz files found in %s", INPUT_DIR)
        return

    # Load previously processed hashes for incremental mode.
    # Files already converted with the same hash are skipped.
    processed = load_processed_hashes(META_FILE)

    tasks: list[tuple[str, str]] = []
    skipped = 0
    for file_name in sorted(all_files):
        file_path = os.path.join(INPUT_DIR, file_name)
        file_hash = md5_of_file(file_path)
        if processed.get(file_name) == file_hash:
            skipped += 1
            continue
        tasks.append((file_name, file_hash))

    log.info(
        "Files: %d total | %d already processed (skipping) | %d to convert",
        len(all_files), skipped, len(tasks),
    )

    if not tasks:
        log.info("Nothing to do. All files are up to date.")
        return

    # ── Parallel processing ───────────────────────────────────────────────────
    # Pool.imap_unordered streams results back as soon as each worker finishes,
    # so tqdm can show live progress without waiting for the full batch.
    success = failed = 0
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.imap_unordered(process_akamai_file, tasks)
        for result in tqdm(results, total=len(tasks), desc="Converting", unit="file"):
            append_result(META_FILE, result)
            if result["status"] == "success":
                success += 1
            else:
                failed += 1

    log.info("Done. ✓ %d succeeded | ✗ %d failed", success, failed)
    if failed:
        log.warning(
            "Re-run the script to retry failed files, or check %s for errors.", META_FILE
        )


if __name__ == "__main__":
    main()