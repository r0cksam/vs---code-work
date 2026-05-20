# -*- coding: utf-8 -*-
import os, time
from pathlib import Path

ROOT_PATH = r"D:\Veto Logs Backup\04 Veto Logs"

def human_size(b):
    if b >= 1_073_741_824: return f"{b/1_073_741_824:.2f} GB"
    if b >= 1_048_576:     return f"{b/1_048_576:.2f} MB"
    if b >= 1024:          return f"{b/1024:.2f} KB"
    return f"{b} B"

def scan_folder(path):
    count, size = 0, 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            try: size += (Path(dirpath) / f).stat().st_size
            except: pass
            count += 1
    return count, size

root = Path(ROOT_PATH)
if not root.exists():
    print(f"ERROR: Path not found: {root}")
    exit()

entries   = sorted(root.iterdir(), key=lambda p: p.name)
subfolders = [e for e in entries if e.is_dir()]
root_files = [e for e in entries if e.is_file()]

print(f"\nMaster folder: {root}")
print(f"Found {len(subfolders)} subfolders\n")
print(f"{'#':<4}  {'FOLDER':<40}  {'FILES':>8}  {'SIZE':>10}  {'TIME':>6}")
print("─" * 75)

total_files, total_size = 0, 0
overall_start = time.time()

# scan each subfolder one by one, print as soon as done
for i, folder in enumerate(subfolders, 1):
    t = time.time()
    count, size = scan_folder(folder)
    elapsed = time.time() - t
    total_files += count
    total_size  += size
    print(f"{i:<4}  {folder.name:<40}  {count:>8,}  {human_size(size):>10}  {elapsed:>5.1f}s")

# loose files sitting directly in master folder
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