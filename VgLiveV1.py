import pandas as pd

PARQUET_FILE_INPUT = r"D:\Veto Logs Backup\05 Veto Logs\22_final_clean.parquet"

def prove_segment_duration(file_path):
    df = pd.read_parquet(file_path, columns=['reqTimeSec', 'reqPath', 'cliIP'])
    
    # 1. Filter for .ts files
    df_ts = df[df['reqPath'].str.endswith('.ts', na=False)].copy()
    df_ts['reqTimeSec'] = pd.to_datetime(df_ts['reqTimeSec'], format='%Y-%m-%d %H.%M.%S', errors='coerce')
    
    # 2. Pick the top active viewer (IP with the most requests) to test standard playback behavior
    top_ip = df_ts['cliIP'].value_counts().index[0]
    user_df = df_ts[df_ts['cliIP'] == top_ip].sort_values(by='reqTimeSec').copy()
    
    # 3. Calculate the request time delta for this single user
    user_df['time_delta_seconds'] = user_df['reqTimeSec'].diff().dt.total_seconds()
    
    # Clean out rapid-fire bursts (pre-buffering clicks) and look at the real pacing
    steady_playback_deltas = user_df[user_df['time_delta_seconds'] > 1.0]['time_delta_seconds']
    
    print(f"User Session Analyzed: {top_ip}")
    print(f"Most frequent request interval for this user: {steady_playback_deltas.mode().iloc[0]} seconds")

if __name__ == "__main__":
    prove_segment_duration(PARQUET_FILE_INPUT)
