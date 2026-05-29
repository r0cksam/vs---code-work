import duckdb
from pathlib import Path

INPUT_DIR = Path(r"D:\Veto Logs Backup")
OUTPUT_DIR = Path(r"D:\Data Lake")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

input_path = str(INPUT_DIR / "**" / "*.parquet").replace("\\", "/")
output_path = str(OUTPUT_DIR).replace("\\", "/")

con = duckdb.connect()
con.execute("SET threads=12;")
con.execute("SET memory_limit='28GB';")
con.execute("SET preserve_insertion_order=false;")

print("🚀 Building Data Lake from Parquet...")

con.execute(f"""
COPY (
    SELECT
        *,
        to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE)) AS req_time_utc,
        to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE)) AT TIME ZONE 'Asia/Kolkata' AS req_time_ist,
        CAST(
            to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE)) AT TIME ZONE 'Asia/Kolkata'
            AS DATE
        ) AS log_date
    FROM read_parquet('{input_path}')
    WHERE TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
)
TO '{output_path}'
(
    FORMAT PARQUET,
    PARTITION_BY (log_date),
    COMPRESSION ZSTD,
    COMPRESSION_LEVEL 3
);
""")

con.close()

print(f"✅ Data Lake created at: {OUTPUT_DIR}")