import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

input_folder = Path(r"Z:\Veto Logs\22_parquet")
output_file = r"Z:\Veto Logs Parquet\22.parquet"

writer = None

for i, file in enumerate(input_folder.glob("*.parquet")):
    print(f"Processing {i+1}: {file}")

    table = pq.read_table(file)

    if writer is None:
        writer = pq.ParquetWriter(output_file, table.schema)

    writer.write_table(table)

if writer:
    writer.close()

print("Merged safely!")