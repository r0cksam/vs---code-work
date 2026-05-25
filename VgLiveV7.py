import pandas as pd
import numpy as np

# Configuration paths
PARQUET_FILE_INPUT = r"D:\Veto Logs Backup\05 Veto Logs\22_final_clean.parquet"
OUTPUT_CHANNEL_CSV = "channel_watch_hours.csv"
OUTPUT_USER_CSV = "user_watch_hours.csv"

# Channel mapping dictionary
CHANNEL_MAP = {
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
    "vglive-sk-238731": "NDTV Marathi",
    "vglive-sk-639201": "IndiaTV Cricket",
    "vglive-sk-834057": "Ndtv India"
}

# The proven duration configuration (6 seconds per segment)
CHUNK_DURATION_HOURS = 6 / 3600 

def calculate_watch_hours(file_path):
    print("Loading Akamai Parquet data into memory...")
    columns_to_load = ['reqPath', 'cliIP', 'statusCode', 'reqTimeSec']
    
    try:
        df = pd.read_parquet(file_path, columns=columns_to_load)
        
        # 1. Filter for valid, successful segment requests (.ts files with HTTP 200)
        df_ts = df[
            (df['reqPath'].str.endswith('.ts', na=False)) & 
            (df['statusCode'] == '200')
        ].copy()
        
        if df_ts.empty:
            print("❌ Error: No successful video chunks (.ts) found in this log selection.")
            return

        # 🚀 FAST TIMELINE LOGIC: Math runs on raw numbers
        raw_min = pd.to_numeric(df_ts['reqTimeSec'], errors='coerce').min()
        raw_max = pd.to_numeric(df_ts['reqTimeSec'], errors='coerce').max()
        
        # Convert only the 2 boundary numbers to UTC Datetime, then localize and convert to IST (+5:30)
        if not np.isnan(raw_min) and not np.isnan(raw_max):
            log_start_time = pd.to_datetime(raw_min, unit='s', utc=True).tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
            log_end_time = pd.to_datetime(raw_max, unit='s', utc=True).tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
        else:
            log_start_time, log_end_time = "Unknown", "Unknown"

        # 2. Extract Stream ID from reqPath using a fast vectorised split
        print("Extracting Stream IDs and mapping channel names...")
        df_ts['stream_id'] = df_ts['reqPath'].apply(
            lambda x: x.split('/') if x.startswith('v1/') else x.split('/')
        )
        
        # Map to channel names; fill unknown stream IDs with 'Other Streams'
        df_ts['channel_name'] = df_ts['stream_id'].map(CHANNEL_MAP).fillna("Other Streams")

        # 3. Deduplicate rapid retry/re-buffered requests
        print("Deduplicating duplicate segment deliveries...")
        df_unique_segments = df_ts.drop_duplicates(subset=['cliIP', 'channel_name', 'reqPath'])

        # 4. Calculate Global Channel Metrics
        print("Calculating Watch Hours across Network Channels...")
        channel_metrics = df_unique_segments.groupby('channel_name').agg(
            total_chunks=('reqPath', 'count'),
            unique_viewers=('cliIP', 'nunique')
        ).reset_index()
        
        channel_metrics['watch_hours'] = channel_metrics['total_chunks'] * CHUNK_DURATION_HOURS
        channel_metrics = channel_metrics.sort_values(by='watch_hours', ascending=False)

        # 5. Calculate Individual User Metrics (Top Viewers)
        print("Calculating individual user consumption profiles...")
        user_metrics = df_unique_segments.groupby(['cliIP', 'channel_name']).agg(
            chunks_watched=('reqPath', 'count')
        ).reset_index()
        
        user_metrics['watch_hours'] = user_metrics['chunks_watched'] * CHUNK_DURATION_HOURS
        user_metrics = user_metrics.sort_values(by='watch_hours', ascending=False)

        # 6. Output Reports to Terminal
        print("\n" + "="*65)
        print("                 GLOBAL CHANNEL WATCH REPORT                    ")
        print("="*65)
        print(f"📅 LOG METRICS WINDOW (IST Timezone):")
        print(f"   - File Collection Starts : {log_start_time} IST")
        print(f"   - File Collection Ends   : {log_end_time} IST")
        print("-"*65)
        print(channel_metrics.to_string(index=False, formatters={
            'watch_hours': '{:.2f} hrs'.format,
            'total_chunks': '{:,}'.format,
            'unique_viewers': '{:,}'.format
        }))
        print("="*65)
        
        print("\n🏆 Top 5 Heavy Viewers Session Profiles:")
        print(user_metrics.head(5).to_string(index=False, formatters={'watch_hours': '{:.2f} hrs'.format}))

        # Save datasets to local files
        channel_metrics.to_csv(OUTPUT_CHANNEL_CSV, index=False)
        user_metrics.to_csv(OUTPUT_USER_CSV, index=False)
        print(f"\n📊 Complete summary reports saved to '{OUTPUT_CHANNEL_CSV}' and '{OUTPUT_USER_CSV}'")

    except Exception as e:
        print(f"An unexpected analysis error occurred: {e}")

if __name__ == "__main__":
    calculate_watch_hours(PARQUET_FILE_INPUT)
