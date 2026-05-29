import gzip
import os
import json
from datetime import datetime, timezone, timedelta
from tqdm import tqdm  # Requires: pip install tqdm

def find_logs_with_progress(root_dir, target_date_str):
    target_date = target_date_str 
    ist_offset = timedelta(hours=5, minutes=30)
    
    # Counters to track matches
    gmt_match_count = 0
    ist_match_count = 0
    max_matches = 5
    
    # 1. Collect all .gz files first to set the bar length
    all_files = []
    for root, _, files in os.walk(root_dir):
        for f in files:
            if f.endswith(".gz"):
                all_files.append(os.path.join(root, f))

    print(f"Scanning {len(all_files)} files for date: {target_date} (GMT/IST)")
    print(f"Target: Find {max_matches} GMT matches and {max_matches} IST matches.\n")

    # 2. Use tqdm for the loading bar
    for file_path in tqdm(all_files, desc="Searching Archive", unit="file"):
        # Stop if we have found 5 of both
        if gmt_match_count >= max_matches and ist_match_count >= max_matches:
            tqdm.write("\n[SUCCESS] Found 5 matches for both timezones. Stopping search.")
            break
            
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                line = f.readline()
                if not line: continue
                
                # Handle the prefix found in your logs 
                clean_line = line.split('] ')[-1] if ']' in line else line
                data = json.loads(clean_line)
                
                # Convert the raw reqTimeSec (e.g., 1775056767.993) 
                ts_val = float(data.get("reqTimeSec", 0))
                dt_gmt = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                dt_ist = dt_gmt + ist_offset
                
                date_gmt = dt_gmt.strftime('%Y-%m-%d')
                date_ist = dt_ist.strftime('%Y-%m-%d')
                
                matched_gmt = (date_gmt == target_date and gmt_match_count < max_matches)
                matched_ist = (date_ist == target_date and ist_match_count < max_matches)

                if matched_gmt or matched_ist:
                    tqdm.write(f"\n[!] MATCH FOUND: {os.path.basename(file_path)}")
                    tqdm.write(f"    Folder: {os.path.dirname(file_path)}")
                    
                    if matched_gmt:
                        gmt_match_count += 1
                        tqdm.write(f"    -> Added to GMT count ({gmt_match_count}/{max_matches})")
                    
                    if matched_ist:
                        ist_match_count += 1
                        tqdm.write(f"    -> Added to IST count ({ist_match_count}/{max_matches})")
                    
                    tqdm.write(f"    Times: GMT({date_gmt}) | IST({date_ist})")
                            
        except Exception:
            continue

    print(f"\nFinal Results: Found {gmt_match_count} GMT matches and {ist_match_count} IST matches.")

# --- Run ---
archive_path = r"D:\VETO Logs\archive"
find_logs_with_progress(archive_path, "2026-04-02")