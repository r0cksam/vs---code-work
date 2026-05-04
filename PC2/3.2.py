import duckdb
from pathlib import Path

folder_path = r"D:\Veto Logs Backup\Veto Logs Parquet"

con = duckdb.connect()

files = sorted(Path(folder_path).glob("*.parquet"))

if len(files) < 2:
    print("Need at least 2 parquet files in the folder.")
    raise SystemExit

print(f"Found {len(files)} parquet files.\n")

for i in range(len(files)):
    for j in range(i + 1, len(files)):
        file1 = str(files[i])
        file2 = str(files[j])

        print(f"Checking:")
        print(f"  File 1: {files[i].name}")
        print(f"  File 2: {files[j].name}")

        common_count = con.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT *
                FROM read_parquet(?)
                INTERSECT
                SELECT *
                FROM read_parquet(?)
            )
        """, [file1, file2]).fetchone()[0]

        if common_count > 0:
            print(f"  ❌ Duplicate/common rows found: {common_count:,}\n")
        else:
            print("  ✅ No common rows found\n")