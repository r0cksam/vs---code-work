# -*- coding: utf-8 -*-
import os
import time
from pathlib import Path
import concurrent.futures

ROOT_PATH = r"Y:\Veto Logs Backup\Veto Stream Logs\04\18"

def human_size(b):
    if b >= 1_073_741_824: return f"{b/1_073_741_824:.2f} GB"
    if b >= 1_048_576:     return f"{b/1_048_576:.2f} MB"
    if b >= 1024:          return f"{b/1024:.2f} KB"
    return f"{b} B"

def scan_folder_fast(path_str):
    """Uses os.scandir to grab file sizes in a single pass without extra stat() calls."""
    start_t = time.time()
    count, size = 0, 0
    dirs_to_scan = [path_str]
    
    while dirs_to_scan:
        current_dir = dirs_to_scan.pop()
        try:
            with os.scandir(current_dir) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        count += 1
                        # st_size is already cached in memory on Windows; no extra OS call!
                        size += entry.stat(follow_symlinks=False).st_size
                    elif entry.is_dir(follow_symlinks=False):
                        dirs_to_scan.append(entry.path)
        except (PermissionError, OSError):
            pass
            
    elapsed = time.time() - start_t
    return count, size, elapsed

def main():
    root = Path(ROOT_PATH)
    if not root.exists():
        print(f"ERROR: Path not found: {root}")
        return

    # Sort entries to keep your original numbering logic
    entries = sorted(root.iterdir(), key=lambda p: p.name)
    subfolders = [e for e in entries if e.is_dir()]
    root_files = [e for e in entries if e.is_file()]

    print(f"\nMaster folder: {root}")
    print(f"Found {len(subfolders)} subfolders\n")
    print(f"{'#':<4}  {'FOLDER':<40}  {'FILES':>8}  {'SIZE':>10}  {'TIME':>6}")
    print("─" * 75)

    total_files, total_size = 0, 0
    overall_start = time.time()

    # Scan folders simultaneously using up to 16 threads (ideal for network/HDD I/O)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        # Submit all folders to the thread pool
        future_to_folder = {
            executor.submit(scan_folder_fast, str(folder)): (i, folder)
            for i, folder in enumerate(subfolders, 1)
        }
        
        # as_completed yields results exactly as they finish, matching your "print as soon as done" rule
        for future in concurrent.futures.as_completed(future_to_folder):
            i, folder = future_to_folder[future]
            try:
                count, size, elapsed = future.result()
                total_files += count
                total_size  += size
                print(f"{i:<4}  {folder.name:<40}  {count:>8,}  {human_size(size):>10}  {elapsed:>5.1f}s")
            except Exception as e:
                print(f"{i:<4}  {folder.name:<40}  ERROR: {e}")

    # Process any loose files sitting directly in the master folder
    if root_files:
        print()
        loose_size = sum(f.stat().st_size for f in root_files if f.exists())
        print(f"{'--':<4}  {'(loose files in master)':<40}  {len(root_files):>8,}  {human_size(loose_size):>10}")
        total_files += len(root_files)
        total_size  += loose_size

    print("─" * 75)
    print(f"{'TOT':<4}  {'TOTAL':<40}  {total_files:>8,}  {human_size(total_size):>10}  {time.time()-overall_start:>5.1f}s")
    print()
    input("Press Enter to exit...")

if __name__ == '__main__':
    main()