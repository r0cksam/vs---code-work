#!/usr/bin/env python3
"""
dashboard.py - Live Streamlit dashboard for Linode S3 Parquet lake.
Uses DuckDB with persistent connection and caching for fast interactivity.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path

# Import S3 helpers from your existing shared module
from shared_utils_s3 import (
    get_conn, lake_reader, qs_extract, channel_clean_expr,
    IST_OFFSET, decode_device, clean_percent_encoding, LAKE_S3_ROOT
)

# ------------------------------------------------------------------
# Page config
st.set_page_config(page_title="Veto Logs Dashboard", layout="wide")
st.title("📊 Veto Logs Dashboard – Linode S3")

# ------------------------------------------------------------------
# Cache the DuckDB connection (persistent across reruns)
@st.cache_resource
def get_cached_conn():
    """Return a DuckDB connection with httpfs and S3 secret already loaded."""
    con = get_conn()
    # Optional: create a persistent view of the lake for easier queries
    # But we'll use lake_reader() dynamically to respect filters.
    return con

# ------------------------------------------------------------------
# Cache the schema and a small sample to avoid repeated full scans
@st.cache_data(ttl=3600)  # cache for 1 hour
def get_schema_and_sample():
    """
    Discover columns and a single row of data using the most recent day partition.
    This is fast because it only lists files in one day folder.
    """
    con = get_cached_conn()
    # Find the latest day partition
    try:
        partitions = con.execute(f"""
            SELECT DISTINCT year, month, day
            FROM glob('{LAKE_S3_ROOT}/**/*.parquet')
            WHERE year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
            ORDER BY year DESC, month DESC, day DESC
            LIMIT 1
        """).fetchone()
        if not partitions:
            st.error("No partitions found in the lake.")
            return None, None
        y, m, d = partitions
        sample_path = f"{LAKE_S3_ROOT}/year={y}/month={m}/day={d}"
        reader = f"read_parquet('{sample_path}/*.parquet', union_by_name=true)"
        sample_df = con.execute(f"SELECT * FROM {reader} LIMIT 1").df()
        cols = list(sample_df.columns)
        return cols, sample_df
    except Exception as e:
        st.error(f"Schema detection failed: {e}")
        return None, None

# ------------------------------------------------------------------
# Helper to build a filtered query expression (using lake_reader)
def get_filtered_reader(year=None, month=None, day_start=None, day_end=None,
                        state=None, channel=None):
    """
    Return a DuckDB table expression (string) that reads the lake
    with optional hive partitioning filters.
    """
    # Base reader from lake_reader (handles year/month filtering)
    reader = lake_reader(LAKE_S3_ROOT, year, month)
    # Additional filters on day, state, channel can be added later in the query.
    # We'll return the reader and apply filters in the SQL.
    return reader

# ------------------------------------------------------------------
# Load aggregated data based on sidebar filters
@st.cache_data(ttl=300)  # cache for 5 minutes
def load_summary_data(year, month, state_filter, channel_filter, limit_days=90):
    """
    Load requests, unique devices, unique sessions grouped by date.
    Filters: year, month, state (like), channel (like), and last N days.
    """
    con = get_cached_conn()
    reader = lake_reader(LAKE_S3_ROOT, year, month)
    qs_col = "queryStr"
    # Build WHERE clause
    where_parts = []
    if state_filter and state_filter != "All":
        where_parts.append(f"state ILIKE '%{state_filter}%'")
    if channel_filter and channel_filter != "All":
        where_parts.append(f"{channel_clean_expr(qs_col)} ILIKE '%{channel_filter}%'")
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    # Also add a day limit (using make_date from year/month/day columns)
    # We'll use a subquery to limit to last N days from max date
    # Simpler: use the existing year/month filter if provided, otherwise scan last 90 days via a subquery.
    if not year and not month:
        # Get max date from the lake
        max_date_query = f"""
            SELECT MAX(make_date(year::INT, month::INT, day::INT)) as max_date
            FROM {reader}
        """
        max_date = con.execute(max_date_query).fetchone()[0]
        if max_date:
            cutoff = max_date - timedelta(days=limit_days)
            where_parts.append(f"make_date(year::INT, month::INT, day::INT) >= '{cutoff.date()}'")
            where_clause = " AND ".join(where_parts)

    sql = f"""
        SELECT
            make_date(year::INT, month::INT, day::INT) as date,
            COUNT(*) as requests,
            COUNT(DISTINCT {qs_extract(qs_col, 'device_id')}) as unique_devices,
            COUNT(DISTINCT {qs_extract(qs_col, 'session_id')}) as unique_sessions
        FROM {reader}
        WHERE {where_clause}
        GROUP BY date
        ORDER BY date
    """
    df = con.execute(sql).df()
    # Convert date to IST for display
    if not df.empty:
        df['date_ist'] = pd.to_datetime(df['date']) + IST_OFFSET
    return df

@st.cache_data(ttl=600)
def load_top_items(year, month, state_filter, channel_filter, top_n=20):
    """Load top channels, devices, states, etc."""
    con = get_cached_conn()
    reader = lake_reader(LAKE_S3_ROOT, year, month)
    qs_col = "queryStr"
    where_parts = []
    if state_filter and state_filter != "All":
        where_parts.append(f"state ILIKE '%{state_filter}%'")
    if channel_filter and channel_filter != "All":
        where_parts.append(f"{channel_clean_expr(qs_col)} ILIKE '%{channel_filter}%'")
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    # Top channels
    channels_sql = f"""
        SELECT
            {channel_clean_expr(qs_col)} as channel,
            COUNT(*) as requests
        FROM {reader}
        WHERE {where_clause} AND {qs_col} IS NOT NULL AND {qs_col} LIKE '%channel=%'
        GROUP BY channel
        ORDER BY requests DESC
        LIMIT {top_n}
    """
    top_channels = con.execute(channels_sql).df()

    # Top devices (decoded later)
    devices_sql = f"""
        SELECT
            COALESCE({qs_extract(qs_col, 'device')}, 'Unknown') as device,
            COUNT(*) as requests
        FROM {reader}
        WHERE {where_clause} AND {qs_col} IS NOT NULL
        GROUP BY device
        ORDER BY requests DESC
        LIMIT {top_n}
    """
    top_devices = con.execute(devices_sql).df()
    if not top_devices.empty:
        top_devices['device_name'] = top_devices['device'].apply(decode_device)

    # Top states
    states_sql = f"""
        SELECT
            COALESCE(state, 'Unknown') as state,
            COUNT(*) as requests
        FROM {reader}
        WHERE {where_clause}
        GROUP BY state
        ORDER BY requests DESC
        LIMIT {top_n}
    """
    top_states = con.execute(states_sql).df()

    return top_channels, top_devices, top_states

# ------------------------------------------------------------------
# Main UI
# ------------------------------------------------------------------
# Sidebar filters
st.sidebar.header("🔍 Filters")

# Get schema and available years/months (use a quick query)
con = get_cached_conn()
available_years = con.execute(f"""
    SELECT DISTINCT year FROM glob('{LAKE_S3_ROOT}/**/*.parquet') WHERE year IS NOT NULL ORDER BY year DESC
""").fetchall()
year_options = [str(y[0]) for y in available_years] if available_years else []
selected_year = st.sidebar.selectbox("Year", ["All"] + year_options, index=0)

month_options = []
if selected_year != "All":
    months = con.execute(f"""
        SELECT DISTINCT month FROM glob('{LAKE_S3_ROOT}/year={selected_year}/**/*.parquet') WHERE month IS NOT NULL ORDER BY month
    """).fetchall()
    month_options = [str(m[0]) for m in months]
selected_month = st.sidebar.selectbox("Month", ["All"] + month_options, index=0)

state_filter = st.sidebar.text_input("State (contains)", placeholder="e.g. Maharashtra")
channel_filter = st.sidebar.text_input("Channel (contains)", placeholder="e.g. vod")

# Convert to None if empty
year_param = selected_year if selected_year != "All" else None
month_param = selected_month if selected_month != "All" else None
state_param = state_filter if state_filter else None
channel_param = channel_filter if channel_filter else None

# Load data with caching
summary_df = load_summary_data(year_param, month_param, state_param, channel_param)
top_channels, top_devices, top_states = load_top_items(year_param, month_param, state_param, channel_param)

# ------------------------------------------------------------------
# Dashboard layout
if summary_df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

# Key metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Requests", f"{summary_df['requests'].sum():,}")
col2.metric("Unique Devices", f"{summary_df['unique_devices'].sum():,}")
col3.metric("Unique Sessions", f"{summary_df['unique_sessions'].sum():,}")
col4.metric("Days Range", f"{summary_df['date_ist'].min().date()} → {summary_df['date_ist'].max().date()}")

# Time series chart
st.subheader("📈 Requests Over Time")
fig = px.line(summary_df, x='date_ist', y='requests', title="Daily Requests")
st.plotly_chart(fig, use_container_width=True)

# Two columns for top items
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🏆 Top Channels")
    if not top_channels.empty:
        st.dataframe(top_channels.head(10), use_container_width=True)
    else:
        st.info("No channel data.")

    st.subheader("📍 Top States")
    if not top_states.empty:
        st.dataframe(top_states.head(10), use_container_width=True)
    else:
        st.info("No state data.")

with col_right:
    st.subheader("📱 Top Devices")
    if not top_devices.empty:
        # Show decoded names
        display_devices = top_devices[['device_name', 'requests']].head(10)
        st.dataframe(display_devices, use_container_width=True)
    else:
        st.info("No device data.")

# Optional: download filtered raw data (limited for performance)
st.subheader("📥 Download Filtered Data (CSV)")
if st.button("Prepare CSV (last 100k rows)"):
    with st.spinner("Querying..."):
        con2 = get_cached_conn()
        reader = lake_reader(LAKE_S3_ROOT, year_param, month_param)
        qs_col = "queryStr"
        where = []
        if state_param:
            where.append(f"state ILIKE '%{state_param}%'")
        if channel_param:
            where.append(f"{channel_clean_expr(qs_col)} ILIKE '%{channel_param}%'")
        where_clause = " AND ".join(where) if where else "1=1"
        sql = f"""
            SELECT
                make_date(year::INT, month::INT, day::INT) as date,
                state,
                {channel_clean_expr(qs_col)} as channel,
                COALESCE({qs_extract(qs_col, 'platform')}, 'Unknown') as platform,
                COALESCE({qs_extract(qs_col, 'device')}, 'Unknown') as raw_device,
                {qs_extract(qs_col, 'device_id')} as device_id,
                {qs_extract(qs_col, 'session_id')} as session_id
            FROM {reader}
            WHERE {where_clause}
            LIMIT 100000
        """
        df_download = con2.execute(sql).df()
        df_download['device_decoded'] = df_download['raw_device'].apply(decode_device)
        df_download = clean_percent_encoding(df_download)
        csv = df_download.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "filtered_logs.csv", "text/csv")