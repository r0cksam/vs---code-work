import pandas as pd

PARQUET_FILE_INPUT = r"D:\Veto Logs Backup\05 Veto Logs\22_final_clean.parquet"

def prove_segment_duration(file_path):
    print("Loading Parquet data into memory...")
    df = pd.read_parquet(file_path, columns=['reqTimeSec', 'reqPath', 'cliIP'])
    
    # 1. Filter for video segment (.ts) requests
    df_ts = df[df['reqPath'].str.endswith('.ts', na=False)].copy()
    df_ts['reqTimeSec'] = pd.to_datetime(df_ts['reqTimeSec'], format='%Y-%m-%d %H.%M.%S', errors='coerce')
    
    # 2. Find the top 5 most active IP addresses
    top_ips = df_ts['cliIP'].value_counts().head(5).index.tolist()
    
    print("\n" + "="*60)
    print("         CONCURRENT USER TIME-GAP ANALYSIS          ")
    print("="*60)
    
    # 3. Analyze the pacing for each of the top users
    for idx, ip in enumerate(top_ips, start=1):
        user_df = df_ts[df_ts['cliIP'] == ip].sort_values(by='reqTimeSec').copy()
        
        # Calculate time gaps between consecutive requests for this user
        user_df['time_delta_seconds'] = user_df['reqTimeSec'].diff().dt.total_seconds()
        
        # Keep gaps greater than 0 to exclude instant multi-file downloads
        valid_gaps = user_df[user_df['time_delta_seconds'] > 0]['time_delta_seconds']
        
        print(f"\n👤 Viewer #{idx} [IP: {ip}]")
        print(f"   Total Chunks Watched: {len(user_df)}")
        
        if not valid_gaps.empty:
            # Display the top 3 most common intervals for this user
            top_intervals = valid_gaps.value_counts().head(3)
            print("   Most common request intervals (Time Gaps):")
            for gap, count in top_intervals.items():
                print(f"   - {gap:.1f} seconds (occurred {count} times)")
        else:
            print("   - No sequential time gaps recorded (Likely a bot/downloader burst).")
            
    print("="*60)

if __name__ == "__main__":
    prove_segment_duration(PARQUET_FILE_INPUT)
