import gzip
import json
import os
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Updated to your specific path
# Using 'r' before the string to handle Windows backslashes correctly
FOLDER_PATH = r'D:\VETO Logs\08'

def process_single_file(file_path):
    """Worker function: Scans one file to see which columns contain actual data."""
    local_real_data = {}
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                for key, value in data.items():
                    # If we find anything other than the placeholders, mark it as 'True'
                    if str(value) not in ["-", "^"]:
                        local_real_data[key] = True
                    elif key not in local_real_data:
                        local_real_data[key] = False
    except Exception:
        # Skips corrupted or empty files
        pass 
    return local_real_data

if __name__ == '__main__':
    # 1. Gather all files using a generator to save memory
    print(f"Searching for .gz files in: {FOLDER_PATH}...")
    files = list(Path(FOLDER_PATH).glob('*.gz'))
    total_files = len(files)
    
    if total_files == 0:
        print("Error: No .gz files found in that directory. Check the path.")
        exit()

    final_real_data_found = {}
    num_cpus = cpu_count()

    print(f"Detected {total_files} files. Starting analysis on {num_cpus} cores...")

    # 2. Process with Multiprocessing and tqdm loading bar
    with Pool(num_cpus) as pool:
        # 'chunksize=100' makes it much faster for 500,000 small files
        results = pool.imap_unordered(process_single_file, files, chunksize=100)
        
        # tqdm creates the loading bar
        for result in tqdm(results, total=total_files, desc="Scanning Logs", unit="file"):
            for key, has_data in result.items():
                # Once a column is True (has data), it stays True
                if has_data:
                    final_real_data_found[key] = True
                elif key not in final_real_data_found:
                    final_real_data_found[key] = False

    # 3. Calculate and Report
    completely_blank = [k for k, found in final_real_data_found.items() if not found]
    total_col_count = len(final_real_data_found)
    blank_col_count = len(completely_blank)

    print("\n" + "="*50)
    print("ANALYSIS COMPLETE")
    print(f"Total Unique Columns Found:    {total_col_count}")
    print(f"Completely Blank Columns:       {blank_col_count}")
    print(f"Usage Rate:                     {((total_col_count - blank_col_count) / total_col_count * 100):.1f}%")
    print("="*50)
    print("List of columns that are 100% blank/suppressed:")
    for col in sorted(completely_blank):
        print(f" - {col}")