import gzip
import os
import json
from datetime import datetime, timezone, timedelta
from tqdm import tqdm

def get_absolute_range(folder_path):
    ist_offset = timedelta(hours=5, minutes=30)
    gz_files = [f for f in os.listdir(folder_path) if f.endswith(".gz")]
    
    if not gz_files:
        print("No .gz files found.")
        return

    absolute_min_ts = float('inf')
    absolute_max_ts = float('-inf')

    print(f"Deep scanning {len(gz_files)} files in folder: {os.path.basename(folder_path)}")

    for file_name in tqdm(gz_files, desc="Reading Timestamps", unit="file"):
        file_path = os.path.join(folder_path, file_name)
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                lines = f.readlines()
                if not lines: continue
                
                # Check first line of this file
                first_line = lines[0].split('] ')[-1] if ']' in lines[0] else lines[0]
                ts_start = float(json.loads(first_line).get("reqTimeSec", 0))
                
                # Check last line of this file
                last_line = lines[-1].split('] ')[-1] if ']' in lines[-1] else lines[-1]
                ts_end = float(json.loads(last_line).get("reqTimeSec", 0))
                
                # Update global min/max
                if ts_start < absolute_min_ts: absolute_min_ts = ts_start
                if ts_end > absolute_max_ts: absolute_max_ts = ts_end
        except:
            continue

    # Convert Results
    start_gmt = datetime.fromtimestamp(absolute_min_ts, tz=timezone.utc)
    end_gmt = datetime.fromtimestamp(absolute_max_ts, tz=timezone.utc)
    
    print(f"\n{'='*60}")
    print(f"FINAL VERIFIED RANGE FOR FOLDER: {os.path.basename(folder_path)}")
    print(f"{'='*60}")
    print(f"EARLIEST RECORD:")
    print(f"  GMT: {start_gmt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  IST: {(start_gmt + ist_offset).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'-'*60}")
    print(f"LATEST RECORD:")
    print(f"  GMT: {end_gmt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  IST: {(end_gmt + ist_offset).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

# --- Run ---
get_absolute_range(r"D:\VETO Logs\03")