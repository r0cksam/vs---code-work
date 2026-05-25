import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

# ===== CONFIGURATION =====
LAKE_FOLDER = Path(r"D:\Veto Logs Backup\05 Veto Logs\lake")
CHUNK_DURATION_HOURS = 6 / 3600

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

CARD_COLOURS = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7C6C6"]


def get_available_dates(lake_path):
    con = duckdb.connect()
    query = f"""
        SELECT MIN(reqTimeSec), MAX(reqTimeSec)
        FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
        WHERE statusCode = '200' AND reqPath LIKE '%.ts'
    """
    result = con.execute(query).fetchone()
    con.close()
    if result[0] and result[1]:
        return datetime.fromtimestamp(result[0]), datetime.fromtimestamp(result[1])
    return None, None


def inspect_lake(lake_path):
    con = duckdb.connect()
    query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE reqPath LIKE '%.ts') AS ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS valid_ts_rows
        FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
    """
    try:
        result = con.execute(query).fetchone()
        con.close()
        return {
            "total_rows": result[0],
            "ts_rows": result[1],
            "valid_ts_rows": result[2],
        }
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=3600, show_spinner=False)
def compute_metrics(lake_path, start_date, end_date, progress_callback=None):
    """
    Compute metrics with optional progress callback (function that takes step_name, step_index, total_steps).
    """
    lake_path = Path(lake_path)
    con = duckdb.connect()
    total_steps = 4
    step = 0

    # Step 1: Build whitelist
    if progress_callback:
        progress_callback("🔍 Building whitelist of video segments...", step, total_steps)
    whitelist_query = f"""
        SELECT DISTINCT reqPath
        FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
        WHERE statusCode = '200' AND reqPath LIKE '%.ts'
    """
    con.execute("CREATE TEMP TABLE whitelist AS (" + whitelist_query + ")")
    whitelist_count = con.execute("SELECT COUNT(*) FROM whitelist").fetchone()[0]
    if whitelist_count == 0:
        con.close()
        return pd.DataFrame(), pd.DataFrame(), "No video segments found"
    step += 1

    # Step 2: Date filter & main query (fetch in chunks for progress)
    if progress_callback:
        progress_callback("📥 Streaming chunk requests from lake...", step, total_steps)
    start_epoch = int(start_date.timestamp()) if start_date else 0
    end_epoch = int(end_date.timestamp()) if end_date else 2**63 - 1
    date_filter = f"AND reqTimeSec BETWEEN {start_epoch} AND {end_epoch}"

    main_query = f"""
        SELECT t.reqPath, t.cliIP, t.reqTimeSec
        FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1) t
        JOIN whitelist w ON t.reqPath = w.reqPath
        WHERE t.statusCode = '200'
          {date_filter}
    """
    # Execute and fetch all (no easy way to get incremental progress from DuckDB)
    # We'll just show a spinner for now; user sees step 2 completed.
    df_raw = con.execute(main_query).fetchdf()
    step += 1

    if df_raw.empty:
        con.close()
        return pd.DataFrame(), pd.DataFrame(), "No data in selected date range"

    # Step 3: Extract channel names
    if progress_callback:
        progress_callback("🏷️ Mapping channels...", step, total_steps)
    def extract_stream_id(path):
        for part in path.split('/'):
            if part in CHANNEL_MAP:
                return part
        return "Unknown"
    df_raw['stream_id'] = df_raw['reqPath'].apply(extract_stream_id)
    df_raw['channel_name'] = df_raw['stream_id'].map(CHANNEL_MAP).fillna("Other")
    step += 1

    # Step 4: Aggregate metrics
    if progress_callback:
        progress_callback("📊 Aggregating channel & user metrics...", step, total_steps)
    channel_agg = df_raw.groupby('channel_name').agg(
        unique_viewers=('cliIP', 'nunique'),
        total_chunks=('reqPath', 'count')
    ).reset_index()
    channel_agg['watch_hours'] = channel_agg['total_chunks'] * CHUNK_DURATION_HOURS
    channel_agg = channel_agg.sort_values('watch_hours', ascending=False)

    user_agg = df_raw.groupby(['channel_name', 'cliIP']).agg(
        chunks_watched=('reqPath', 'count')
    ).reset_index()
    user_agg['watch_hours'] = user_agg['chunks_watched'] * CHUNK_DURATION_HOURS
    user_agg = user_agg.sort_values('watch_hours', ascending=False).head(10000)

    con.close()
    step += 1

    # Time range for display
    if not channel_agg.empty:
        min_ts = df_raw['reqTimeSec'].min()
        max_ts = df_raw['reqTimeSec'].max()
        start_dt = pd.to_datetime(min_ts, unit='s', utc=True).tz_convert('Asia/Kolkata')
        end_dt = pd.to_datetime(max_ts, unit='s', utc=True).tz_convert('Asia/Kolkata')
        time_range = f"{start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')} IST"
    else:
        time_range = "No data"

    return channel_agg, user_agg, time_range


# ===== STREAMLIT UI =====
st.set_page_config(page_title="Live TV Dashboard", layout="wide")
st.title("📺 Live TV Channel Watch Dashboard")
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Settings")
    lake_input = st.text_input("Lake Folder Path", value=str(LAKE_FOLDER))

    if st.button("🔍 Inspect Lake"):
        info = inspect_lake(lake_input)
        if "error" in info:
            st.error(f"Error: {info['error']}")
        else:
            st.success(f"Total rows: {info['total_rows']:,}")
            st.info(f"Rows with .ts: {info['ts_rows']:,}")
            st.info(f"Valid .ts (status=200): {info['valid_ts_rows']:,}")
            if info['valid_ts_rows'] == 0:
                st.warning("No valid video segments found. Check that your lake contains logs with reqPath, cliIP, statusCode, reqTimeSec.")

    min_dt, max_dt = get_available_dates(lake_input)
    if min_dt and max_dt:
        min_default = min_dt.date()
        max_default = max_dt.date()
    else:
        min_default = datetime(2026, 3, 1).date()
        max_default = datetime(2026, 5, 1).date()

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=min_default)
    with col2:
        end_date = st.date_input("To", value=max_default)

    run_button = st.button("🔄 Refresh Data", type="primary", use_container_width=True)


if run_button or "channel_df" not in st.session_state:
    # Use a status container to show steps
    with st.status("⏳ Computing metrics...", expanded=True) as status:
        # Define progress callback that updates status
        def update_status(msg, current, total):
            status.update(label=f"{msg} ({current+1}/{total})", state="running")
        
        channel_df, user_df, time_range = compute_metrics(
            lake_input, start_date, end_date,
            progress_callback=lambda msg, step, total: update_status(msg, step, total)
        )
        if channel_df.empty:
            status.update(label="❌ No data found for the selected range", state="error")
            st.warning("Try a different date range or inspect the lake.")
        else:
            status.update(label="✅ Metrics computed successfully!", state="complete")
            st.session_state.channel_df = channel_df
            st.session_state.user_df = user_df
            st.session_state.time_range = time_range

if "channel_df" in st.session_state and not st.session_state.channel_df.empty:
    df = st.session_state.channel_df
    user_df = st.session_state.user_df
    time_range = st.session_state.time_range

    st.markdown(f"**Data period:** {time_range}")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Channels", len(df))
    with col2:
        st.metric("Total Watch Hours", f"{df['watch_hours'].sum():,.0f} hrs")
    with col3:
        st.metric("Unique Viewers (total)", f"{df['unique_viewers'].sum():,}")

    st.markdown("---")
    st.subheader("📡 Channel Performance")
    cols = st.columns(3)
    for idx, row in df.iterrows():
        with cols[idx % 3]:
            colour = CARD_COLOURS[idx % len(CARD_COLOURS)]
            st.markdown(f"""
            <div style="background-color:{colour}; padding:1rem; border-radius:15px; margin-bottom:1rem;">
                <h3>{row['channel_name']}</h3>
                <p>📺 <b>{row['watch_hours']:.1f}</b> hrs</p>
                <p>👥 {row['unique_viewers']:,} viewers</p>
                <p>🎬 {row['total_chunks']:,} chunks</p>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("🏆 Top Viewers by Channel")
    selected_channel = st.selectbox("Select channel", df['channel_name'].tolist())
    if selected_channel:
        top_users = user_df[user_df['channel_name'] == selected_channel].head(10)
        if not top_users.empty:
            st.dataframe(
                top_users[['cliIP', 'watch_hours', 'chunks_watched']].style.format({
                    'watch_hours': '{:.2f} hrs',
                    'chunks_watched': '{:,}'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No viewer data for this channel.")

    csv_channel = df.to_csv(index=False).encode('utf-8')
    csv_users = user_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Channel Report (CSV)", csv_channel, "channel_watch.csv", "text/csv")
    st.download_button("📥 Download User Report (CSV)", csv_users, "user_watch.csv", "text/csv")

elif "channel_df" in st.session_state:
    st.info("No data found. Adjust date range or inspect the lake.")