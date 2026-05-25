import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, time
import re

# ====================== CONFIG ======================
LAKE_FOLDER = Path(r"D:\Veto Logs Backup\05 Veto Logs\lake")
CHUNK_DURATION_SECONDS = 6
CHUNK_DURATION_HOURS = CHUNK_DURATION_SECONDS / 3600.0

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


# ====================== FAST DATE RETRIEVAL (NO FULL SCAN) ======================
@st.cache_data(ttl=86400)  # cache for 1 day
def get_available_dates(lake_path):
    """Get min and max dates from partition folders (fast) – or fallback to parquet metadata."""
    lake_path = Path(lake_path)
    dates = []
    # Look for Hive partition folders: year=*/month=*/day=*
    for year_dir in lake_path.glob("year=*"):
        try:
            year = int(year_dir.name.split('=')[1])
            for month_dir in year_dir.glob("month=*"):
                month = int(month_dir.name.split('=')[1])
                for day_dir in month_dir.glob("day=*"):
                    day = int(day_dir.name.split('=')[1])
                    dates.append(datetime(year, month, day))
        except (ValueError, IndexError):
            continue

    if dates:
        return min(dates), max(dates)

    # Fallback: read only the first Parquet file's metadata (no row scan)
    parquet_files = list(lake_path.glob("**/*.parquet"))
    if not parquet_files:
        return None, None
    con = duckdb.connect()
    try:
        result = con.execute(f"""
            SELECT MIN(reqTimeSec), MAX(reqTimeSec)
            FROM parquet_metadata('{parquet_files[0]}')
        """).fetchone()
        if result and result[0] and result[1]:
            return datetime.fromtimestamp(result[0]), datetime.fromtimestamp(result[1])
    except Exception:
        pass
    finally:
        con.close()
    return None, None


# ====================== FAST LAKE INSPECTION (USES METADATA) ======================
def inspect_lake(lake_path):
    """Inspect lake statistics without full scan where possible."""
    lake_path = Path(lake_path)
    parquet_files = list(lake_path.glob("**/*.parquet"))
    if not parquet_files:
        return {"error": "No parquet files found in the lake folder."}

    con = duckdb.connect()
    try:
        # Total rows from Parquet metadata (fast)
        meta_df = con.execute(f"""
            SELECT SUM(num_rows) as total_rows
            FROM parquet_metadata('{lake_path.as_posix()}/**/*.parquet')
        """).fetchdf()
        total_rows = int(meta_df['total_rows'].iloc[0]) if not meta_df.empty else 0

        # Valid .ts rows – requires a scan (but we limit by showing a warning)
        # We'll do a COUNT(*) with a simple filter – this is still a full scan, but it's a one‑off inspect.
        valid_ts = 0
        try:
            valid_ts = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
                WHERE statusCode = '200' AND reqPath LIKE '%.ts'
            """).fetchone()[0]
        except Exception:
            valid_ts = "unknown (query failed)"

        return {
            "total_rows": total_rows,
            "valid_ts_rows": valid_ts,
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        con.close()


# ====================== MAIN METRICS (SINGLE SCAN + PARTITION PRUNING) ======================
def build_partition_filter(start_date, end_date):
    """
    Build a DuckDB filter on Hive partition columns (year, month, day).
    Assumes folder structure: year=YYYY/month=MM/day=DD.
    Returns a string like "(year=2026 AND month=5 AND day BETWEEN 1 AND 25) OR ..."
    """
    filters = []
    current = start_date
    while current <= end_date:
        year = current.year
        month = current.month
        day = current.day
        filters.append(f"(year = {year} AND month = {month} AND day = {day})")
        # Move to next day
        from datetime import timedelta
        current += timedelta(days=1)
    return " OR ".join(filters)


@st.cache_data(ttl=7200, show_spinner=False)
def compute_metrics(lake_path, start_date, end_date):
    lake_path = Path(lake_path)
    con = duckdb.connect()

    try:
        start_epoch = int(datetime.combine(start_date, time.min).timestamp())
        end_epoch = int(datetime.combine(end_date, time.max).timestamp())

        # Build partition filter (if partitions exist)
        partition_filter = build_partition_filter(start_date, end_date)

        # Step 1: Create a temporary table with the base data (single scan)
        # This materialises only the columns we need.
        con.execute(f"""
            CREATE TEMP TABLE base_tmp AS
            SELECT 
                cliIP,
                regexp_extract(reqPath, '(vglive-sk-[0-9]+)', 1) AS channel_id
            FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
            WHERE statusCode = '200'
              AND reqPath LIKE '%.ts'
              AND CAST(reqTimeSec AS DOUBLE) BETWEEN {start_epoch} AND {end_epoch}
              AND ({partition_filter})
        """)

        # Step 2: Channel aggregation
        channel_df = con.execute("""
            SELECT 
                channel_id,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                COUNT(*) AS total_chunks
            FROM base_tmp
            GROUP BY channel_id
        """).fetchdf()

        # Step 3: User aggregation (top 20000)
        user_df = con.execute("""
            SELECT 
                channel_id,
                cliIP,
                COUNT(*) AS chunks_watched
            FROM base_tmp
            GROUP BY channel_id, cliIP
            ORDER BY chunks_watched DESC
            LIMIT 20000
        """).fetchdf()

        # Clean up temporary table
        con.execute("DROP TABLE base_tmp")

        if channel_df.empty:
            return pd.DataFrame(), pd.DataFrame(), "No data found in selected date range."

        # Map channel IDs to friendly names
        channel_df['channel_name'] = channel_df['channel_id'].map(CHANNEL_MAP).fillna('Other')
        # Aggregate in case multiple IDs map to the same name (e.g., duplicate Yoga)
        channel_df = channel_df.groupby('channel_name', as_index=False).agg({
            'unique_viewers': 'sum',
            'total_chunks': 'sum'
        })

        channel_df['watch_hours'] = channel_df['total_chunks'] * CHUNK_DURATION_HOURS
        channel_df['avg_chunks_per_viewer'] = channel_df['total_chunks'] / channel_df['unique_viewers']
        channel_df['avg_watch_hours_per_viewer'] = channel_df['watch_hours'] / channel_df['unique_viewers']
        channel_df = channel_df.sort_values('watch_hours', ascending=False)

        # Map channel names in user_df
        user_df['channel_name'] = user_df['channel_id'].map(CHANNEL_MAP).fillna('Other')
        user_df['watch_hours'] = user_df['chunks_watched'] * CHUNK_DURATION_HOURS
        user_df = user_df.drop(columns=['channel_id'])

        # Get time range from actual data (optional)
        time_range = f"{start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}"

        return channel_df, user_df, time_range

    finally:
        con.close()


# ====================== STREAMLIT UI ======================
st.set_page_config(page_title="Live TV Dashboard", layout="wide")
st.title("📺 Live TV Channel Watch Dashboard")
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Settings")
    lake_input = st.text_input("Lake Folder Path", value=str(LAKE_FOLDER))

    if st.button("🔍 Inspect Lake"):
        with st.spinner("Inspecting lake (this may take a moment)..."):
            info = inspect_lake(lake_input)
            if "error" in info:
                st.error(f"Error: {info['error']}")
            else:
                st.success(f"Total rows (metadata): {info['total_rows']:,}")
                st.info(f"Valid .ts rows (full scan): {info['valid_ts_rows']:,}")

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

    if start_date > end_date:
        st.error("❌ Start date cannot be after end date")
        st.stop()

    st.info("📁 Assumes Hive partitioning: `year=YYYY/month=MM/day=DD` inside the lake folder.\nIf not present, falls back to slower full scan.")

    run_button = st.button("🔄 Refresh Data", type="primary", use_container_width=True)


# Compute data when button clicked or first load
if run_button or "channel_df" not in st.session_state:
    with st.spinner("Processing data (optimised single scan + partition pruning)..."):
        channel_df, user_df, time_range = compute_metrics(lake_input, start_date, end_date)

        if channel_df.empty:
            st.error("No data found for the selected range.")
        else:
            st.session_state.channel_df = channel_df
            st.session_state.user_df = user_df
            st.session_state.time_range = time_range
            st.success("✅ Analysis completed successfully!")


# Display results
if "channel_df" in st.session_state and not st.session_state.channel_df.empty:
    df = st.session_state.channel_df
    user_df = st.session_state.user_df
    time_range = st.session_state.time_range

    st.markdown(f"**Data Period:** {time_range}")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Channels", len(df))
    with c2:
        st.metric("Total Watch Hours", f"{df['watch_hours'].sum():,.0f}")
    with c3:
        st.metric("Unique Viewers", f"{df['unique_viewers'].sum():,}")
    with c4:
        st.metric("Total Chunks", f"{df['total_chunks'].sum():,}")

    st.markdown("---")
    st.subheader("📡 Channel Performance")

    cols = st.columns(3)
    for idx, row in df.iterrows():
        colour = CARD_COLOURS[idx % len(CARD_COLOURS)]
        with cols[idx % 3]:
            st.markdown(f"""
            <div style="background-color:{colour}; padding:1.2rem; border-radius:15px; margin-bottom:1rem; color:black;">
                <h3>{row['channel_name']}</h3>
                <p><b>{row['watch_hours']:.1f}</b> hrs watched</p>
                <p>👥 {row['unique_viewers']:,} viewers</p>
                <p>🎬 {row['total_chunks']:,} chunks</p>
                <p>Avg: {row['avg_watch_hours_per_viewer']:.2f} hrs/viewer</p>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("🏆 Top Viewers by Channel")
    selected_channel = st.selectbox("Select Channel", df['channel_name'].tolist())
    if selected_channel:
        top_users = user_df[user_df['channel_name'] == selected_channel].head(15)
        if not top_users.empty:
            st.dataframe(
                top_users[['cliIP', 'watch_hours', 'chunks_watched']].style.format({
                    'watch_hours': '{:.2f} hrs',
                    'chunks_watched': '{:,}'
                }),
                use_container_width=True,
                hide_index=True
            )

    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button("📥 Channel Report", df.to_csv(index=False).encode('utf-8'),
                           "channel_report.csv", "text/csv")
    with col_dl2:
        st.download_button("📥 User Report", user_df.to_csv(index=False).encode('utf-8'),
                           "user_report.csv", "text/csv")

else:
    st.info("👆 Click **Refresh Data** to load analytics.")