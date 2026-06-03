#!/usr/bin/env python3
"""
inspect_gz_file.py
Examine a single .gz file to determine its format and extract sample timestamps.
"""

import gzip
import io
import sys
from pathlib import Path

import pyarrow.parquet as pq

def main():
    folder = input("\n📁 Folder path (e.g., Z:\\05 Veto Logs\\12): ").strip()
    path = Path(folder)
    if not path.exists():
        print(f"❌ Folder not found: {path}")
        return

    gz_files = list(path.glob("*.gz"))
    if not gz_files:
        print("No .gz files found.")
        return

    first_file = gz_files[0]
    print(f"\nInspecting: {first_file.name}")

    # Step 1: Read first 100 bytes of decompressed data
    with gzip.open(first_file, 'rb') as f:
        raw = f.read(100)
    print(f"\nFirst 100 bytes (decompressed):\n{raw}")
    print(f"ASCII view: {raw[:50]}\n")

    # Step 2: Try to interpret as Parquet
    try:
        with gzip.open(first_file, 'rb') as f:
            # Read entire decompressed content into memory (small file)
            buf = io.BytesIO(f.read())
            pf = pq.ParquetFile(buf)
            print("✅ File is a valid Parquet file!")
            print(f"Schema columns: {pf.schema.names}")
            if 'reqTimeSec' in pf.schema.names:
                # Read a few rows to show timestamps
                table = pf.read(columns=['reqTimeSec'], use_threads=False)
                df = table.to_pandas()
                print("\nSample reqTimeSec values (first 5):")
                for ts in df['reqTimeSec'].head(5):
                    print(f"  {ts}")
            else:
                print("\n⚠️ Column 'reqTimeSec' not found in schema.")
                print("   Available columns:", pf.schema.names)
    except Exception as e:
        print(f"❌ Not a Parquet file (or unreadable): {e}")
        # Step 3: Try as CSV/JSON
        with gzip.open(first_file, 'rb') as f:
            lines = f.read(2000).decode('utf-8', errors='ignore').splitlines()
        print("\nFirst few lines as text (trying CSV/JSON):")
        for i, line in enumerate(lines[:5]):
            print(f"  Line {i+1}: {line[:200]}")

if __name__ == "__main__":
    main()