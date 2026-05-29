import gzip
import os
import json
from tqdm import tqdm

def check_entry_overlap(folder1, folder2):
    ids_folder1 = set()
    ids_folder2 = set()

    def get_ids_from_folder(folder_path, id_set):
        gz_files = [f for f in os.listdir(folder_path) if f.endswith(".gz")]
        print(f"Scanning {len(gz_files)} files in {os.path.basename(folder_path)}...")
        
        for file_name in tqdm(gz_files, unit="file"):
            file_path = os.path.join(folder_path, file_name)
            try:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        clean_line = line.split('] ')[-1] if ']' in line else line
                        data = json.loads(clean_line)
                        # Store the unique Request ID
                        if 'reqId' in data:
                            id_set.add(data['reqId'])
            except:
                continue

    # Process both folders
    get_ids_from_folder(folder1, ids_folder1)
    get_ids_from_folder(folder2, ids_folder2)

    # Find the intersection (duplicates)
    duplicate_ids = ids_folder1.intersection(ids_folder2)
    
    total_f1 = len(ids_folder1)
    total_f2 = len(ids_folder2)
    overlap_count = len(duplicate_ids)

    print(f"\n{'='*50}")
    print(f"OVERLAP ANALYSIS RESULTS")
    print(f"{'='*50}")
    print(f"Folder 1 Total Entries: {total_f1}")
    print(f"Folder 2 Total Entries: {total_f2}")
    print(f"Common Entries (Duplicates): {overlap_count}")
    
    if overlap_count > 0:
        percentage = (overlap_count / (total_f1 + total_f2)) * 100
        print(f"Result: {overlap_count} entries are duplicated across these folders ({percentage:.2f}% overlap).")
    else:
        print("Result: No duplicate entries found. The timing overlaps, but the records are individual and unique.")
    print(f"{'='*50}")

# --- Run ---
folder_a = r"D:\VETO Logs\03"
folder_b = r"D:\VETO Logs\02"
check_entry_overlap(folder_a, folder_b)