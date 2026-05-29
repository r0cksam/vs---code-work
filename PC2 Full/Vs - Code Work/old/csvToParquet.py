import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_path = r"D:\full_rows_device_id_4cda938608ed98a8.csv"
parquet_path = r"D:\full_rows_device_id_4cda938608ed98a8.parquet"

chunksize = 500_000

writer = None

for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize, low_memory=False)):
    print(f"Processing chunk {i+1}")

    table = pa.Table.from_pandas(chunk)

    if writer is None:
        writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")

    writer.write_table(table)

if writer:
    writer.close()

print("✅ Done (memory safe)")