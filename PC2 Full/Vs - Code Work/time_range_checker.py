#!/usr/bin/env python3
"""
time_range_checker.py
Scan a folder containing .parquet or .gz (JSON lines) files,
report total row count and min/max reqTimeSec in UTC and IST.
"""

import gzip
import json
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pyarrow.parquet as pq

# IST offset
IST_OFFSET = timedelta(hours=5, minutes=30)

def parse_timestamp(ts):
    """Convert to float, accept int or string."""
    if ts is None:
        return None
    try:
        return float(ts)
    except (ValueError, TypeError):
        return None

def scan_parquet_file(filepath):
    """Return (row_count, min_ts, max_ts) from a Parquet file's metadata."""
    try:
        pf = pq.ParquetFile(filepath)
        meta = pf.metadata
        row_count = meta.num_rows
        # Try to get column statistics for reqTimeSec
        schema = pf.schema
        if 'reqTimeSec' not in schema.names:
            return row_count, None, None
        col_idx = schema.names.index('reqTimeSec')
        min_ts = None
        max_ts = None
        for rg in range(meta.num_row_groups):
            col_stats = meta.row_group(rg).column(col_idx).statistics
            if col_stats is not None:
                if col_stats.min is not None:
                    val = parse_timestamp(col_stats.min)
                    if min_ts is None or val < min_ts:
                        min_ts = val
                if col_stats.max is not None:
                    val = parse_timestamp(col_stats.max)
                    if max_ts is None or val > max_ts:
                        max_ts = val
        return row_count, min_ts, max_ts
    except Exception:
        return 0, None, None

def scan_json_gz_file(filepath):
    """Return (row_count, min_ts, max_ts) from a .gz file containing JSON lines."""
    row_count = 0
    min_ts = None
    max_ts = None
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    ts = parse_timestamp(data.get('reqTimeSec'))
                    if ts is not None:
                        row_count += 1
                        if min_ts is None or ts < min_ts:
                            min_ts = ts
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                except json.JSONDecodeError:
                    pass
    except Exception:
        pass
    return row_count, min_ts, max_ts

def scan_folder(root_dir):
    """Recursively scan all files and accumulate statistics."""
    total_rows = 0
    global_min = None
    global_max = None
    processed_files = 0

    for filepath in Path(root_dir).rglob('*'):
        if not filepath.is_file():
            continue
        ext = filepath.suffix.lower()
        rows = 0
        mn = mx = None
        if ext == '.parquet':
            rows, mn, mx = scan_parquet_file(filepath)
        elif ext == '.gz':
            rows, mn, mx = scan_json_gz_file(filepath)
        else:
            # Skip other extensions
            continue

        if rows:
            total_rows += rows
            if mn is not None:
                if global_min is None or mn < global_min:
                    global_min = mn
            if mx is not None:
                if global_max is None or mx > global_max:
                    global_max = mx

        processed_files += 1
        if processed_files % 1000 == 0:
            print(f"  Processed {processed_files} files...")

    return total_rows, global_min, global_max

def main():
    print("\n🔍 Scan folder for reqTimeSec range and row count\n")
    folder = input("📁 Folder path: ").strip()
    if not folder:
        print("No folder given.")
        return
    path = Path(folder)
    if not path.exists():
        print(f"❌ Folder not found: {path}")
        return

    print(f"\nScanning {path} ...")
    total_rows, min_ts, max_ts = scan_folder(path)

    if total_rows == 0:
        print("No valid rows found.")
        return

    print("\n" + "=" * 60)
    print(f"Total rows: {total_rows:,}")
    if min_ts is not None:
        min_utc = datetime.fromtimestamp(min_ts, tz=timezone.utc)
        min_ist = min_utc + IST_OFFSET
        print(f"\nMinimum reqTimeSec: {min_ts:.3f}")
        print(f"  → UTC: {min_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  → IST: {min_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    if max_ts is not None:
        max_utc = datetime.fromtimestamp(max_ts, tz=timezone.utc)
        max_ist = max_utc + IST_OFFSET
        print(f"\nMaximum reqTimeSec: {max_ts:.3f}")
        print(f"  → UTC: {max_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  → IST: {max_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

if __name__ == "__main__":
    main()