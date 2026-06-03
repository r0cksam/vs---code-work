import gzip
import json
import os
import glob
from tqdm import tqdm  # This is the loading bar library

def count_unique_arlid_from_gz(folder_path):
    # Find all .gz files
    files = glob.glob(os.path.join(folder_path, "*.gz"))
    
    if not files:
        print(f"No .gz files found in {folder_path}")
        return
    
    unique_arlids = set()
    
    # Initialize the loading bar
    # 'unit="file"' makes the bar count files
    with tqdm(total=len(files), desc="Reading GZ Logs", unit="file") as pbar:
        for file_path in files:
            try:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            # Parse each line as JSON
                            log_entry = json.loads(line)
                            # Add arlid to our set
                            if 'arlid' in log_entry:
                                unique_arlids.add(log_entry['arlid'])
                        except json.JSONDecodeError:
                            continue # Skip broken lines
                
                # Update the loading bar by 1 after finishing a file
                pbar.update(1)
                
            except Exception as e:
                # Use pbar.write to keep the loading bar at the bottom
                pbar.write(f"Error reading {os.path.basename(file_path)}: {e}")

    print("\n" + "="*30)
    print(f"Total Unique arlids in GZ: {len(unique_arlids):,}")
    print("="*30)

# --- RUN THE SCRIPT ---
# Change this to your actual .gz folder path
gz_folder = r'D:\VETO Logs\01' 
count_unique_arlid_from_gz(gz_folder)