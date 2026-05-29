import pandas as pd
import os
import glob
import urllib.parse

def extract_unique_uas_from_parquet():
    # 1. Configuration
    input_folder = r'D:\VETO Logs\04 parquet'
    output_csv = 'unique_uas_list.csv'
    
    # 2. Find all parquet files
    files = glob.glob(os.path.join(input_folder, "*.parquet"))
    
    if not files:
        print(f"No parquet files found in {input_folder}. Check the path!")
        return

    print(f"Found {len(files)} files. Extracting unique User Agents...")
    
    unique_uas = set()

    for file_path in files:
        try:
            # We only read the UA column to save speed/memory
            # If your parquet already has 'UA_decoded', use that. Otherwise use 'UA'.
            df_temp = pd.read_parquet(file_path, columns=['UA'])
            
            # Get unique values from this file and add to our master set
            for val in df_temp['UA'].dropna().unique():
                # Decode the URL encoding (e.g. %20 -> space)
                decoded = urllib.parse.unquote(str(val))
                unique_uas.add(decoded)
                
            print(f"Processed: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # 3. Save the final list
    df_unique = pd.DataFrame(sorted(list(unique_uas)), columns=['User_Agent_String'])
    df_unique.to_csv(output_csv, index=False)
    
    print(f"\n--- DONE ---")
    print(f"Total Unique UAs found: {len(df_unique)}")
    print(f"List saved to: {os.path.join(os.getcwd(), output_csv)}")

if __name__ == "__main__":
    extract_unique_uas_from_parquet()