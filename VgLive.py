import pandas as pd

# CONFIGURATION - Set these based on your streaming profile
PARQUET_FILE_INPUT = r"D:\Veto Logs Backup\05 Veto Logs\22_final_clean.parquet"
SEGMENT_DURATION_SEC = 6.0  # Assumed industry-standard 6 seconds based on your 3.3MB average size

def calculate_stream_bitrate(file_path, segment_len):
    print(f"Analyzing Akamai Parquet data from: {file_path}...")
    
    # 1. Load relevant data columns
    columns_to_load = ['reqTimeSec', 'reqPath', 'bytes', 'statusCode']
    
    try:
        df = pd.read_parquet(file_path, columns=columns_to_load)
        
        # 2. Filter for successful, full video chunk downloads (.ts files with HTTP 200)
        df_ts = df[
            (df['reqPath'].str.endswith('.ts', na=False)) & 
            (df['statusCode'] == '200')
        ].copy()
        
        if df_ts.empty:
            print("\n❌ error: No successful '.ts' files found in this log selection.")
            print("Please ensure your Parquet file contains requests for actual video segments.")
            return

        # 🚨 FIX: Convert 'bytes' from text strings to numbers so math operations work
        df_ts['bytes'] = pd.to_numeric(df_ts['bytes'], errors='coerce')
        
        # Drop rows where 'bytes' might have been a blank space or hyphen
        df_ts = df_ts.dropna(subset=['bytes'])

        # 3. Apply the Bitrate Formula
        # (Bytes * 8 bits) / (duration * 1000) = Kilobits per second (Kbps)
        df_ts['bitrate_kbps'] = (df_ts['bytes'] * 8) / (segment_len * 1000)
        
        # Convert to Mbps for easy reading
        df_ts['bitrate_mbps'] = df_ts['bitrate_kbps'] / 1000

        # 4. Generate the streaming report
        print("\n" + "="*50)
        print("          AKAMAI VIDEO BITRATE REPORT          ")
        print("="*50)
        print(f"Assumed Chunk Duration: {segment_len} seconds")
        print(f"Total Segments Sampled: {len(df_ts)}")
        print("-"*50)
        print(f"📈 Maximum Stream Bitrate: {df_ts['bitrate_mbps'].max():.2f} Mbps")
        print(f"📊 Average Stream Bitrate: {df_ts['bitrate_mbps'].mean():.2f} Mbps")
        print(f"📉 Minimum Stream Bitrate: {df_ts['bitrate_mbps'].min():.2f} Mbps")
        print("="*50)
        
        # Preview the data structure
        print("\nPreview of parsed video streams:")
        print(df_ts[['reqPath', 'bytes', 'bitrate_mbps']].head())

    except Exception as e:
        print(f"An unexpected script error occurred: {e}")

if __name__ == "__main__":
    calculate_stream_bitrate(PARQUET_FILE_INPUT, SEGMENT_DURATION_SEC)
