#!/usr/bin/env python3
"""
scan_json_gz_timestamps.py
Stream through many .gz files containing JSON lines, extract min/max reqTimeSec.
"""

import gzip
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

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
    print(f"Found {len(gz_files)} .gz files. Streaming JSON lines (low memory)...")
    
    # Optional: limit for testing
    limit = input("Scan all files? (y/n): ").strip().lower()
    if limit != 'y':
        try:
            max_files = int(input("How many files to scan? (e.g., 100): "))
            gz_files = gz_files[:max_files]
            print(f"Scanning first {len(gz_files)} files...")
        except ValueError:
            print("Invalid number, scanning all.")
    
    min_ts = float('inf')
    max_ts = float('-inf')
    processed = 0
    sample_timestamps = []  # store first few timestamps from first file

    for gz_path in gz_files:
        try:
            with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        ts = data.get('reqTimeSec')
                        if ts is not None:
                            ts = float(ts)
                            if ts < min_ts:
                                min_ts = ts
                            if ts > max_ts:
                                max_ts = ts
                            if len(sample_timestamps) < 5:
                                sample_timestamps.append(ts)
                    except json.JSONDecodeError:
                        pass  # skip malformed line
        except Exception as e:
            print(f"  Error reading {gz_path.name}: {e}")
        
        processed += 1
        if processed % 10000 == 0:
            print(f"  Processed {processed} files...")
    
    if min_ts == float('inf'):
        print("No valid timestamps found.")
        return

    # Convert to datetime
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
    print("\nSample timestamps (first 5 from first file):")
    for ts in sample_timestamps:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        ist_dt = dt + ist_offset
        print(f"  {ts:.3f} → UTC {dt.strftime('%Y-%m-%d %H:%M:%S')}  IST {ist_dt.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()