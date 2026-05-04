import duckdb
from pathlib import Path
import time

input_folder = Path(r"Z:\Veto Logs\03_parquet")
output_file = Path(r"Z:\Clean Veto Logs\03.parquet")

output_file.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

con.execute("SET threads=12;")
con.execute("SET memory_limit='28GB';")
con.execute("SET preserve_insertion_order=false;")

con.execute("SET enable_progress_bar = true;")
con.execute("SET enable_progress_bar_print = true;")

print("Starting merge + DISTINCT operation...")
print("Progress bar should appear below:\n")

start_time = time.time()

try:
    con.execute(f"""
    COPY (
        SELECT DISTINCT *
        FROM read_parquet('{input_folder.as_posix()}/*.parquet')
    )
    TO '{output_file.as_posix()}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        COMPRESSION_LEVEL 22
    );
    """)

    elapsed = time.time() - start_time
    print(f"\n✅ Done! Completed in {elapsed/60:.1f} minutes.")
    print(f"Output file: {output_file}")

except Exception as e:
    print(f"\n❌ Error: {e}")

finally:
    con.close()