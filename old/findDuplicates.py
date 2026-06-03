import os
import hashlib
from collections import defaultdict
from tqdm import tqdm

def get_file_hash(file_path):
    """Generates a SHA-256 hash for a file's content."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        return None

def find_duplicate_gz(folders):
    hash_map = defaultdict(list)
    all_files = []

    # 1. Collect all .gz files from both folders
    for folder in folders:
        if not os.path.exists(folder):
            print(f"Warning: Folder not found: {folder}")
            continue
        for root, _, files in os.walk(folder):
            for file in files:
                if file.endswith(".gz"):
                    all_files.append(os.path.join(root, file))

    if not all_files:
        print("No .gz files found in the specified folders.")
        return

    print(f"Checking {len(all_files)} files for duplicates...")

    # 2. Hash files and find duplicates
    for path in tqdm(all_files, desc="Hashing files", unit="file"):
        f_hash = get_file_hash(path)
        if f_hash:
            hash_map[f_hash].append(path)

    # 3. Report results
    duplicates = {h: paths for h, paths in hash_map.items() if len(paths) > 1}

    if not duplicates:
        print("\nNice! No duplicate .gz files found.")
    else:
        print(f"\n[!] Found {len(duplicates)} sets of duplicate files:\n")
        for i, (f_hash, paths) in enumerate(duplicates.items(), 1):
            print(f"Group {i} (Hash: {f_hash[:10]}...):")
            for p in paths:
                print(f"  - {p}")
            print("-" * 30)

# --- Configuration ---
# List your two folder paths here
folders_to_check = [
    r"D:\VETO Logs\02",
    r"D:\VETO Logs\03"
]

find_duplicate_gz(folders_to_check)