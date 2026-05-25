import pandas as pd
import re

PARQUET_FILE_INPUT = r"D:\Veto Logs Backup\05 Veto Logs\22_final_clean.parquet"

def prove_via_sequence_numbers(file_path):
    print("Loading Parquet data into memory...")
    df = pd.read_parquet(file_path, columns=['reqTimeSec', 'reqPath', 'cliIP'])
    
    # Filter for .ts files and convert dates
    df_ts = df[df['reqPath'].str.endswith('.ts', na=False)].copy()
    df_ts['reqTimeSec'] = pd.to_datetime(df_ts['reqTimeSec'], format='%Y-%m-%d %H.%M.%S', errors='coerce')
    
    # Use Viewer #5 (An Indian ISP IPv6 address - likely a real smart TV viewer)
    target_ip = "2401:4900:1c5b:4b70:ed3f:33ab:f750:1626"
    user_df = df_ts[df_ts['cliIP'] == target_ip].sort_values(by='reqTimeSec').copy()
    
    # Extract the trailing numbers from the filename (e.g., 9129580 from main_1_9129580.ts)
    def extract_seq(path):
        match = re.search(r'_(\d+)\.ts$', path)
        return int(match.group(1)) if match else None
        
    user_df['seq_num'] = user_df['reqPath'].apply(extract_seq)
    user_df = user_df.dropna(subset=['seq_num']).sort_values(by='seq_num')
    
    # Calculate the gaps between sequence numbers and time elapsed
    user_df['seq_gap'] = user_df['seq_num'].diff()
    user_df['time_elapsed_sec'] = user_df['reqTimeSec'].diff().dt.total_seconds()
    
    # Filter for normal chronological playback (next segment requested)
    playback = user_df[(user_df['seq_gap'] == 1) & (user_df['time_elapsed_sec'] > 0)]
    
    print("\n" + "="*60)
    print("         MEDIA SEQUENCE ANALYSIS REPORT         ")
    print("="*60)
    print(f"Analyzing Real Viewer IP: {target_ip}")
    
    if not playback.empty:
        # How many real clock seconds pass between sequence N and sequence N+1?
        implied_duration = playback['time_elapsed_sec'].mode().iloc[0]
        print(f"✨ SUCCESS: Real clock time elapsed between sequential chunks: {implied_duration} seconds")
    else:
        # Alternative: Calculate total time window divided by total segments watched
        total_time = (user_df['reqTimeSec'].max() - user_df['reqTimeSec'].min()).total_seconds()
        total_segments = user_df['seq_num'].max() - user_df['seq_num'].min()
        if total_segments > 0:
            avg_duration = total_time / total_segments
            print(f"✨ SUCCESS: Total timeline average duration per segment: {avg_duration:.2f} seconds")
        else:
            print("❌ Unable to calculate sequence gaps for this IP.")
    print("="*60)

if __name__ == "__main__":
    prove_via_sequence_numbers(PARQUET_FILE_INPUT)
