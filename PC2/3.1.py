import duckdb

file1 = r"D:\Veto Logs Backup\Veto Logs Parquet\01.parquet"
file2 = r"D:\Veto Logs Backup\Veto Logs Parquet\02.parquet"

con = duckdb.connect()

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

print(f"\nDuplicate/common row groups between both files: {common_count}")