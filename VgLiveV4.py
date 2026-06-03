import pandas as pd

PARQUET_FILE_INPUT = r"D:\Veto Logs Backup\05 Veto Logs\22_final_clean.parquet"

def inspect_user_timeline(file_path):
    print("Loading Parquet data into memory...")
    df = pd.read_parquet(file_path, columns=['reqTimeSec', 'reqPath', 'cliIP'])
    
    # Filter for .ts files
    df_ts = df[df['reqPath'].str.endswith('.ts', na=False)].copy()
    
    # Isolate Viewer #5 (Real user)
    target_ip = "2401:4900:1c5b:4b70:ed3f:33ab:f750:1626"
    user_df = df_ts[df_ts['cliIP'] == target_ip].copy()
    
    # Sort purely by the raw log timeline string to preserve the order Akamai recorded them
    user_df = user_df.sort_values(by='reqTimeSec')
    
    print("\n" + "="*70)
    print(f"       RAW CHRONOLOGICAL TIMELINE FOR USER: {target_ip}       ")
    print("="*70)
    # Print the first 20 rows to see the pattern
    print(user_df[['reqTimeSec', 'reqPath']].head(20).to_string(index=False))
    print("="*70)

if __name__ == "__main__":
    inspect_user_timeline(PARQUET_FILE_INPUT)
