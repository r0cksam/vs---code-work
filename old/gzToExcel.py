import pandas as pd
import gzip
import json
import os

def gz_to_excel(input_file, output_file):
    data = []
    
    print(f"Reading {input_file}...")
    
    try:
        # Open the .gz file in text mode
        with gzip.open(input_file, 'rt', encoding='utf-8') as f:
            for line in f:
                # Remove the prefix if it exists in your data
                clean_line = line.split('] ')[-1] if ']' in line else line
                
                # Parse JSON and add to our list
                try:
                    data.append(json.loads(clean_line))
                except json.JSONDecodeError:
                    continue
        
        # Create a DataFrame
        df = pd.DataFrame(data)
        
        # Optional: Convert the timestamp to a readable date format
        if 'reqTimeSec' in df.columns:
            df['reqTimeSec'] = pd.to_datetime(df['reqTimeSec'].astype(float), unit='s')
        
        # Write to Excel
        print(f"Writing to {output_file}...")
        df.to_excel(output_file, index=False)
        print("Success!")

    except Exception as e:
        print(f"An error occurred: {e}")

# --- Run ---
input_gz = r"Z:\Veto Logs\22\ak-783418-1776874909-824045-ds.gz" # Your file path
output_xlsx = r"D:\Vs - Code Work\01.xlsx"

gz_to_excel(input_gz, output_xlsx)