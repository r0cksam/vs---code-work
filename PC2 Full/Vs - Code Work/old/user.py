import streamlit as st
import pandas as pd
import os
import glob
import plotly.express as px

# Page Setup
st.set_page_config(layout="wide", page_title="VETO Unique User Analytics")

# 1. ENHANCED MAPPING ENGINE
def map_device(ua):
    ua = str(ua).lower()
    if 'apple tv' in ua or 'applecoremedia' in ua: return 'Apple TV'
    if 'aft' in ua: return 'Amazon Fire TV'
    if 'bravia' in ua or 'sony' in ua: return 'Sony TV'
    if 'mitv' in ua or 'xiaomi' in ua: return 'Mi TV'
    if 'swtv' in ua or 'skyworth' in ua: return 'Skyworth TV'
    if 'tizen' in ua: return 'Samsung TV'
    if 'webos' in ua: return 'LG TV'
    if 'android tv' in ua or 'google tv' in ua: return 'Android TV'
    
    # Catching generic Android Apps / Mobile (The "Other" fix)
    if 'exoplayer' in ua or 'dalvik' in ua or 'yourappname' in ua:
        if any(x in ua for x in ['tv', 'build', 'box', 'stb']): return 'Android TV Box'
        return 'Android Mobile App'
        
    if 'mozilla' in ua: return 'Web Browser'
    return 'Other/System'

# 2. THE AGGREGATOR
@st.cache_data(ttl=3600)
def load_and_process_all_data():
    path = r'D:\VETO Logs\04 parquet'
    files = glob.glob(os.path.join(path, "*.parquet"))
    
    if not files:
        return None

    user_device_map = [] 
    time_storage = []
    path_storage = []
    total_bytes = 0
    total_hits = 0
    unique_ips_master = set()

    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, f in enumerate(files):
        status_text.text(f"Processing file {i+1} of {len(files)}...")
        try:
            df = pd.read_parquet(f, columns=['UA', 'cliIP', 'reqTimeSec', 'bytes', 'reqPath'])
            
            total_hits += len(df)
            total_bytes += df['bytes'].sum()
            unique_ips_master.update(df['cliIP'].unique())

            df['Device'] = df['UA'].apply(map_device)

            # Store unique IP + Device pairs
            unique_pairs = df[['cliIP', 'Device']].drop_duplicates()
            user_device_map.append(unique_pairs)

            # Time Series
            df['dt'] = pd.to_datetime(df['reqTimeSec'], unit='s', errors='coerce')
            ts = df.set_index('dt').resample('1min').size()
            time_storage.append(ts)

            # Content
            path_storage.append(df['reqPath'].value_counts().head(20))
            
        except Exception as e:
            st.warning(f"Error in {os.path.basename(f)}: {e}")
        
        progress_bar.progress((i + 1) / len(files))

    # Merging
    all_user_mappings = pd.concat(user_device_map).drop_duplicates()
    final_devices = all_user_mappings['Device'].value_counts().reset_index()
    final_devices.columns = ['Device', 'Unique_Users']

    final_time = pd.concat(time_storage).groupby(level=0).sum().reset_index()
    final_time.columns = ['dt', 'hits']

    final_paths = pd.concat(path_storage).groupby(level=0).sum().sort_values(ascending=False).head(15).reset_index()
    final_paths.columns = ['path', 'count']
    
    status_text.empty()
    progress_bar.empty()

    return {
        'devices': final_devices,
        'time': final_time,
        'paths': final_paths,
        'users': len(unique_ips_master),
        'hits': total_hits,
        'gb': total_bytes / (1024**3)
    }

# --- DASHBOARD UI ---
st.title("📺 VETO Unique User Analytics")

if st.button('🚀 Load/Refresh All Data'):
    results = load_and_process_all_data()
    
    if results:
        # 1. TOP LEVEL METRIC CARDS
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Unique Households (IPs)", f"{results['users']:,}")
        m2.metric("Total Hits (Segments)", f"{results['hits']:,}")
        m3.metric("Data Served", f"{results['gb']:.2f} GB")

        st.divider()

        # 2. NEW SECTION: CROSS-DEVICE BEHAVIOR (Put it here!)
        st.header("🔄 User Connectivity & Multi-Screen Behavior")
        
        # We need the raw mapping list to calculate this
        # Note: 'user_device_map' is stored inside the 'load_and_process_all_data' logic
        # For simplicity, we can calculate it from the results if we pass it through, 
        # but the easiest way is to show the Overlap logic here:
        
        total_distinct = results['users']
        sum_of_slices = results['devices']['Unique_Users'].sum()
        overlap_count = sum_of_slices - total_distinct
        overlap_pct = (overlap_count / total_distinct) * 100

        col_met1, col_met2, col_met3 = st.columns(3)
        col_met1.metric("Total Device Connections", f"{sum_of_slices:,}")
        col_met2.metric("Multi-Screen Overlap", f"{overlap_count:,}")
        col_met3.metric("Overlap Percentage", f"{overlap_pct:.1f}%")

        st.info(f"💡 Insight: {overlap_pct:.1f}% of your audience is active on more than one device type (e.g. watching on TV while browsing on Mobile).")

        st.divider()

        # 3. EXISTING CHARTS
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.subheader("Traffic Pattern (Hits/Min)")
            fig_line = px.line(results['time'], x='dt', y='hits', template="plotly_dark")
            st.plotly_chart(fig_line, use_container_width=True)

        with col_right:
            st.subheader("Device Share (by Unique Users)")
            fig_pie = px.pie(results['devices'], names='Device', values='Unique_Users', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

        # 4. CONTENT PATHS
        st.subheader("Top Content Paths (Total Hits)")
        st.bar_chart(results['paths'].set_index('path'))

        # 5. VALIDATION DATA (Inside the expander at the bottom)
        with st.expander("🛡️ View Raw Validation Math"):
            st.write(f"**Grand Total (Distinct IPs):** {total_distinct:,}")
            st.write(f"**Sum of Device Slices:** {sum_of_slices:,}")
            st.write(f"**Multi-Device Overlap:** {overlap_count:,}")
            
    else:
        st.error("No data found.")
else:
    st.info("Click the button above to process all files and see the Unique User breakdown.")