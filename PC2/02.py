import duckdb
from pathlib import Path
import time
import re

base_folder = Path(r"Y:\Veto Logs Backup\Veto Stream Logs\03")
output_folder = base_folder  # final files will save directly inside Veto Logs folder

con = duckdb.connect()

con.execute("SET threads=12;")
con.execute("SET memory_limit='28GB';")
con.execute("SET preserve_insertion_order=false;")
con.execute("SET enable_progress_bar = true;")
con.execute("SET enable_progress_bar_print = true;")

# Find folders like 01_parquet, 02_parquet, 29_parquet
parquet_folders = sorted(
    [
        f for f in base_folder.iterdir()
        if f.is_dir() and re.fullmatch(r"\d+_parquet", f.name)
    ],
    key=lambda x: int(x.name.split("_")[0])
)

print(f"Found {len(parquet_folders)} parquet folders.")

for folder in parquet_folders:
    folder_num = folder.name.split("_")[0]
    output_file = output_folder / f"{folder_num}_final_clean.parquet"

    if output_file.exists():
        print(f"\n⏩ Skipping {folder.name} — already exists:")
        print(output_file)
        continue

    print(f"\n🚀 Processing {folder.name}")
    print(f"Saving to: {output_file}")

    start_time = time.time()

    try:
        con.execute(f"""
        COPY (
            SELECT DISTINCT *
            FROM read_parquet('{folder.as_posix()}/*.parquet')
        )
        TO '{output_file.as_posix()}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 22
        );
        """)

        elapsed = time.time() - start_time
        print(f"✅ Done {folder.name} in {elapsed / 60:.1f} minutes.")

    except Exception as e:
        print(f"❌ Error while processing {folder.name}: {e}")

con.close()

print("\n🎯 All parquet folders processed.")