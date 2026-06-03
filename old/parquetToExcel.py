import pandas as pd
import pyarrow.parquet as pq
import os
from tqdm import tqdm

# Configuration
input_path = r"Z:\veto fast logs\14\1110280_final_clean.parquet"
output_path = r"Z:\veto fast logs\14\1110280_final_clean.xlsx"
chunk_size = 50000  
MAX_EXCEL_ROWS = 1000000 # Safety margin (Excel max is 1,048,576)

if not os.path.exists(input_path):
    print(f"Error: Path {input_path} not found.")
else:
    parquet_file = pq.ParquetFile(input_path)
    total_rows = parquet_file.metadata.num_rows
    total_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size > 0 else 0)

    print(f"Starting Multi-Sheet Conversion: {total_rows:,} total rows.")

    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            with tqdm(total=total_chunks, desc="Processing Chunks", unit="chunk") as pbar:
                
                sheet_num = 1
                rows_in_current_sheet = 0
                
                for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
                    df = batch.to_pandas()
                    
                    # Check if this chunk will overflow the current sheet
                    if rows_in_current_sheet + len(df) > MAX_EXCEL_ROWS:
                        sheet_num += 1
                        rows_in_current_sheet = 0 # Reset counter for the new tab
                    
                    current_sheet_name = f"Logs_Part_{sheet_num}"
                    
                    # Write data
                    # header=True only if we just started a fresh sheet
                    df.to_excel(
                        writer, 
                        index=False, 
                        header=(rows_in_current_sheet == 0), 
                        startrow=rows_in_current_sheet,
                        sheet_name=current_sheet_name
                    )
                    
                    rows_in_current_sheet += len(df)
                    pbar.update(1)

        print(f"\nDone! Created {sheet_num} sheets in {output_path}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")