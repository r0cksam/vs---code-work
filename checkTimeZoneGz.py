#!/usr/bin/env python3
"""
check_downloaded_folder.py
Efficiently find min/max reqTimeSec from many .gz compressed Parquet files.
Uses PyArrow to read only the footer metadata (row group statistics).
"""

import gzip
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pyarrow.parquet as pq

def main():
    print("\n🔍 Check downloaded folder for data time range (using Parquet metadata)\n")
    folder = input("📁 Folder path (e.g., Z:\\05 Veto Logs\\12): ").strip()
    if not folder:
        print("No folder given.")
        return
    path = Path(folder)
    if not path.exists():
        print(f"❌ Folder not found: {path}")
        return

    gz_files = list(path.glob("*.gz"))
    if not gz_files:
        print("No .gz files found.")
        return
    print(f"Found {len(gz_files)} .gz files. Reading metadata (no data scan)...")

    min_ts = float('inf')
    max_ts = float('-inf')
    processed = 0

    for gz_path in gz_files:
        try:
            with gzip.open(gz_path, 'rb') as f:
                # Read the entire gzipped file into memory? That could be large.
                # Better: use pyarrow's ParquetFile with a seekable stream.
                # But gzip.open returns a non-seekable stream. We'll read into BytesIO.
                # For large files, this may use memory. An alternative: decompress to temp file?
                # Given typical Parquet file sizes (a few MB), this is acceptable.
                buf = io.BytesIO(f.read())
                pf = pq.ParquetFile(buf)
                # Iterate over row groups
                for rg in range(pf.num_row_groups):
                    rg_meta = pf.metadata.row_group(rg)
                    # Find index of 'reqTimeSec' column
                    col_names = pf.schema.names
                    if 'reqTimeSec' not in col_names:
                        continue
                    col_idx = col_names.index('reqTimeSec')
                    col_meta = rg_meta.column(col_idx)
                    if col_meta.statistics is not None:
                        if col_meta.statistics.min is not None:
                            min_ts = min(min_ts, col_meta.statistics.min)
                        if col_meta.statistics.max is not None:
                            max_ts = max(max_ts, col_meta.statistics.max)
        except Exception as e:
            # Silently skip files that cannot be read (e.g., not Parquet)
            pass

        processed += 1
        if processed % 50000 == 0:
            print(f"  Processed {processed} files...")

    if min_ts == float('inf') or max_ts == float('-inf'):
        print("No valid timestamps found.")
        return

    min_utc = datetime.fromtimestamp(min_ts, tz=timezone.utc)
    max_utc = datetime.fromtimestamp(max_ts, tz=timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    min_ist = min_utc + ist_offset
    max_ist = max_utc + ist_offset

    print("\n" + "=" * 60)
    print("  Data time range in the downloaded folder")
    print("=" * 60)
    print(f"  Minimum reqTimeSec : {min_ts:.3f}")
    print(f"    → UTC: {min_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"    → IST: {min_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print()
    print(f"  Maximum reqTimeSec : {max_ts:.3f}")
    print(f"    → UTC: {max_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"    → IST: {max_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

if __name__ == "__main__":
    main()