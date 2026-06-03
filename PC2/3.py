import duckdb

file_path = r"D:\Veto Logs Backup\Veto Logs Parquet\29.parquet"

con = duckdb.connect()

duplicates = con.execute("""
    SELECT *, COUNT(*) AS duplicate_count
    FROM read_parquet(?)
    GROUP BY ALL
    HAVING COUNT(*) > 1
""", [file_path]).fetchdf()

print(duplicates)

print(f"\nDuplicate row groups found: {len(duplicates)}")
print(f"Extra duplicate rows: {duplicates['duplicate_count'].sum() - len(duplicates) if len(duplicates) else 0}")