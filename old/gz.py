import polars as pl
import glob
import os
from tqdm import tqdm
from urllib.parse import unquote
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
INPUT_DIRS = [
    r"D:\VETO Logs\01",
    r"D:\VETO Logs\02"
]

OUTPUT_DIR = r"D:\VETO Logs\parquet_output"          # Temporary batches
FINAL_DIR  = r"D:\VETO Logs\parquet_final_clean"     # Optimized for Dashboard

BATCH_SIZE = 4000
SKIP_EXISTING = True

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_DIR, exist_ok=True)

URL_COLS = ["UA", "state", "reqPath", "queryStr"]

# ============================================================
# HELPER: Ask before clearing folder
# ============================================================
def ask_to_clear_folder(folder_path: str, folder_name: str) -> bool:
    if not os.path.exists(folder_path):
        return True
    
    files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
    if not files:
        return True

    print(f"\n⚠️  {folder_name} already contains {len(files)} Parquet file(s).")
    choice = input(f"Do you want to DELETE all existing files in {folder_name}? (yes/no): ").strip().lower()
    
    if choice in ['yes', 'y']:
        for f in files:
            os.remove(os.path.join(folder_path, f))
        print(f"✅ Cleared {folder_name}\n")
        return True
    else:
        print(f"⏭️  Keeping existing files in {folder_name}\n")
        return False


# ============================================================
# PROCESS ONE BATCH
# ============================================================
def process_batch(file_list, batch_index):
    output_path = os.path.join(OUTPUT_DIR, f"akamai_batch_{batch_index:05d}.parquet")
    
    if SKIP_EXISTING and os.path.exists(output_path):
        print(f"⏭️  Skipping batch {batch_index:05d} (already exists)")
        return True

    try:
        df = pl.read_ndjson(file_list)

        # Cleanup: Replace Akamai nulls
        df = df.with_columns(pl.col(pl.Utf8).replace({"-": None, "^": None}))

        # URL Decoding
        for col in URL_COLS:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).map_elements(
                        lambda x: unquote(x) if x else x, 
                        return_dtype=pl.Utf8
                    )
                )

        # Data Type Fixes
        if "reqTimeSec" in df.columns:
            df = df.with_columns([
                pl.col("reqTimeSec").cast(pl.Float64).alias("reqTimeFloat")
            ]).with_columns([
                pl.from_epoch("reqTimeFloat", time_unit="s").alias("datetime")
            ]).drop("reqTimeFloat")

        numeric_cols = ["bytes", "objSize", "throughput", "statusCode", "asn", 
                       "totalBytes", "timeToFirstByte", "downloadTime"]
        df = df.with_columns([
            pl.col(col).cast(pl.Float64, strict=False) 
            for col in numeric_cols if col in df.columns
        ])

        # Save batch
        df.write_parquet(output_path, compression="snappy")
        return True

    except Exception as e:
        print(f"❌ Error in batch {batch_index:05d}: {e}")
        return False


# ============================================================
# CONVERT ALL .gz FILES
# ============================================================
def convert_all_inputs():
    print("🔄 Scanning and collecting .gz files...\n")
    
    all_files = []
    for input_dir in INPUT_DIRS:
        files = sorted(glob.glob(os.path.join(input_dir, "*.gz")))
        all_files.extend(files)
        print(f"Found {len(files)} .gz files in → {input_dir}")

    if not all_files:
        print("❌ No .gz files found!")
        return 0

    batches = [all_files[i:i + BATCH_SIZE] for i in range(0, len(all_files), BATCH_SIZE)]
    
    print(f"\nStarting conversion of {len(all_files)} files in {len(batches)} batches...\n")

    success = 0
    for i, batch in enumerate(tqdm(batches, desc="Converting .gz → Parquet", unit="batch")):
        if process_batch(batch, i):
            success += 1

    print(f"\n✅ Conversion Phase Completed! {success}/{len(batches)} batches successful.\n")
    return success


# ============================================================
# OPTIMIZE FOR DASHBOARD (with progress bar)
# ============================================================
def optimize_for_dashboard():
    print("🚀 Starting Optimization for DuckDB Dashboard...\n")
    
    parquet_files = sorted(glob.glob(os.path.join(OUTPUT_DIR, "*.parquet")))
    if not parquet_files:
        print("❌ No parquet files found to optimize!")
        return

    print(f"Found {len(parquet_files)} parquet files. Loading and optimizing...\n")

    try:
        # Use tqdm with manual progress for better visibility
        with tqdm(total=3, desc="Optimization Steps", unit="step") as pbar:

            # Step 1: Scan + Deduplicate
            pbar.set_description("Step 1/3: Scanning & Removing Duplicates")
            lf = pl.scan_parquet(parquet_files).unique(keep="first")
            pbar.update(1)

            # Step 2: Sort by datetime
            pbar.set_description("Step 2/3: Sorting by datetime")
            if "datetime" in lf.collect_schema().names():
                lf = lf.sort("datetime")
            pbar.update(1)

            # Step 3: Write optimized Parquet
            pbar.set_description("Step 3/3: Writing Optimized Parquet (ZSTD)")
            final_file = os.path.join(FINAL_DIR, "akamai_logs_optimized.parquet")

            lf.collect(streaming=True).write_parquet(
                final_file,
                compression="zstd",
                compression_level=5,
                row_group_size=500_000,
                statistics=True,
            )
            pbar.update(1)

        print(f"\n✅ OPTIMIZATION SUCCESSFUL!")
        print(f"📁 Final optimized file saved at:")
        print(f"   {final_file}")
        print(f"   → ZSTD | Sorted by time | Row groups: 500,000")

    except Exception as e:
        print(f"❌ Optimization failed: {e}")


# ============================================================
# MAIN EXECUTION
# ============================================================
if __name__ == "__main__":
    start_time = datetime.now()
    print("="*70)
    print("🚀 AKAMAI LOGS CONVERSION & OPTIMIZATION SCRIPT")
    print("="*70)
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Ask user before clearing old files
    ask_to_clear_folder(OUTPUT_DIR, "OUTPUT_DIR (intermediate batches)")

    # Phase 1: Convert .gz files
    convert_all_inputs()

    # Phase 2: Optimize for Dashboard
    optimize_for_dashboard()

    duration = datetime.now() - start_time
    print("\n" + "="*70)
    print(f"🎉 ALL DONE in {str(duration).split('.')[0]}")
    print("="*70)