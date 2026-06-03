import re
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# CONFIG
# ============================================================
# Use forward slashes for the glob path to prevent escape character issues in SQL
PARQUET_GLOB = "D:/VETO Logs/parquet_output/*.parquet"
LOCAL_TZ = "Asia/Kolkata"

st.set_page_config(
    page_title="Akamai Log Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# DB CONNECTION
# ============================================================
@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA enable_progress_bar=false;")
    return con

# ============================================================
# BASE TABLE / VIEW
# ============================================================
@st.cache_resource
def initialize_db(parquet_glob: str) -> None:
    """
    Creates a VIRTUAL VIEW instead of a physical table.
    This saves RAM because DuckDB reads from disk on-demand.
    """
    con = get_conn()

    query = f"""
    CREATE OR REPLACE VIEW logs AS
    SELECT
        *,
        CAST(reqTime AS TIMESTAMP) AS req_time_utc,

        CASE
            WHEN reqPath ~ '([0-9]{{3,4}}p)' THEN regexp_extract(reqPath, '([0-9]{{3,4}}p)', 1)
            ELSE 'unknown'
        END AS rendition,

        CASE
            WHEN lower(UA) LIKE '%android%' AND lower(UA) LIKE '%aft%' THEN 'Fire TV'
            WHEN lower(UA) LIKE '%android tv%' THEN 'Android TV'
            WHEN lower(UA) LIKE '%android%' THEN 'Android Mobile'
            WHEN lower(UA) LIKE '%iphone%' OR lower(UA) LIKE '%ipad%' OR lower(UA) LIKE '%ios%' THEN 'iOS'
            WHEN lower(UA) LIKE '%bravia%' THEN 'Sony TV'
            WHEN lower(UA) LIKE '%tizen%' OR lower(UA) LIKE '%samsungbrowser%' THEN 'Samsung TV'
            WHEN lower(UA) LIKE '%webos%' OR lower(UA) LIKE '%lg%' THEN 'LG TV'
            WHEN lower(UA) LIKE '%windows%' THEN 'Windows'
            WHEN lower(UA) LIKE '%mac os%' OR lower(UA) LIKE '%macintosh%' THEN 'Mac'
            WHEN lower(UA) LIKE '%linux%' THEN 'Linux'
            ELSE 'Other'
        END AS device_family,

        CASE
            WHEN statusCode BETWEEN 200 AND 299 THEN '2xx'
            WHEN statusCode BETWEEN 300 AND 399 THEN '3xx'
            WHEN statusCode BETWEEN 400 AND 499 THEN '4xx'
            WHEN statusCode BETWEEN 500 AND 599 THEN '5xx'
            ELSE 'other'
        END AS status_class
    FROM read_parquet('{parquet_glob}');
    """
    con.execute(query)

# ============================================================
# HELPERS
# ============================================================
def q(sql: str, params: Optional[list] = None) -> pd.DataFrame:
    con = get_conn()
    if params is None:
        params = []
    return con.execute(sql, params).df()

def build_filters_clause(
    start_ts, end_ts, hosts: List[str], states: List[str], cities: List[str],
    status_classes: List[str], renditions: List[str], device_families: List[str],
    path_contains: str,
) -> tuple[str, list]:
    clauses = ["req_time_utc >= ?", "req_time_utc <= ?"]
    params = [start_ts, end_ts]

    if hosts:
        clauses.append("reqHost IN (" + ",".join(["?"] * len(hosts)) + ")")
        params.extend(hosts)
    if states:
        clauses.append("state IN (" + ",".join(["?"] * len(states)) + ")")
        params.extend(states)
    if cities:
        clauses.append("city IN (" + ",".join(["?"] * len(cities)) + ")")
        params.extend(cities)
    if status_classes:
        clauses.append("status_class IN (" + ",".join(["?"] * len(status_classes)) + ")")
        params.extend(status_classes)
    if renditions:
        clauses.append("rendition IN (" + ",".join(["?"] * len(renditions)) + ")")
        params.extend(renditions)
    if device_families:
        clauses.append("device_family IN (" + ",".join(["?"] * len(device_families)) + ")")
        params.extend(device_families)
    if path_contains.strip():
        clauses.append("reqPath ILIKE ?")
        params.append(f"%{path_contains.strip()}%")

    return " WHERE " + " AND ".join(clauses), params

@st.cache_data(show_spinner=False)
def get_date_bounds() -> tuple[pd.Timestamp, pd.Timestamp]:
    df = q("SELECT min(req_time_utc) AS min_ts, max(req_time_utc) AS max_ts FROM logs")
    min_t = pd.Timestamp(df.loc[0, "min_ts"]).tz_localize("UTC").tz_convert(LOCAL_TZ)
    max_t = pd.Timestamp(df.loc[0, "max_ts"]).tz_localize("UTC").tz_convert(LOCAL_TZ)
    return min_t, max_t

@st.cache_data(show_spinner=False)
def get_distinct_values(column_name: str) -> list:
    df = q(f"SELECT DISTINCT {column_name} FROM logs WHERE {column_name} IS NOT NULL ORDER BY 1")
    return df.iloc[:, 0].dropna().astype(str).tolist()

def to_local_tz(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns and not df.empty:
        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(LOCAL_TZ)
    return df

def format_bytes(num_bytes: float) -> str:
    if pd.isna(num_bytes) or num_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:,.2f} {unit}"
        size /= 1024

@st.cache_data(show_spinner=False)
def get_kpis(where_clause: str, params: list) -> pd.DataFrame:
    sql = f"""
    SELECT
        COUNT(*) AS total_requests,
        COUNT(DISTINCT cliIP) AS unique_ips,
        COUNT(DISTINCT reqId) AS unique_request_ids,
        COUNT(DISTINCT streamId) AS unique_streams,
        SUM(bytes) AS total_bytes,
        AVG(throughput) AS avg_throughput,
        AVG(timeToFirstByte) AS avg_ttfb,
        AVG(downloadTime) AS avg_download_time,
        SUM(CASE WHEN statusCode >= 400 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS error_rate_pct,
        SUM(CASE WHEN statusCode >= 500 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS server_error_rate_pct
    FROM logs
    {where_clause}
    """
    return q(sql, params)

@st.cache_data(show_spinner=False)
def get_timeseries(where_clause: str, params: list) -> pd.DataFrame:
    sql = f"""
    SELECT
        date_trunc('hour', req_time_utc) AS hour_ts,
        COUNT(*) AS requests,
        SUM(bytes) AS bytes_served,
        AVG(throughput) AS avg_throughput,
        AVG(timeToFirstByte) AS avg_ttfb,
        SUM(CASE WHEN statusCode >= 400 THEN 1 ELSE 0 END) AS error_requests
    FROM logs
    {where_clause}
    GROUP BY 1 ORDER BY 1
    """
    return q(sql, params)

def get_breakdown(column: str, where_clause: str, params: list) -> pd.DataFrame:
    sql = f"SELECT {column}, COUNT(*) AS requests FROM logs {where_clause} GROUP BY 1 ORDER BY requests DESC"
    return q(sql, params)


# ============================================================
# INIT
# ============================================================
PARQUET_GLOB = "D:/VETO Logs/parquet/**/*.parquet"
folder_path = Path("D:/VETO Logs/parquet/")

if not list(folder_path.glob("**/*.parquet")):
    st.error(f"No Parquet files found in {folder_path} or its subfolders.")
    st.stop()

initialize_db(PARQUET_GLOB)
min_ts, max_ts = get_date_bounds()

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.title("Filters")

start_date = st.sidebar.date_input("Start date", value=min_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())
end_date = st.sidebar.date_input("End date", value=max_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())

start_hour = st.sidebar.slider("Start hour", 0, 23, 0)
end_hour = st.sidebar.slider("End hour", 0, 23, 23)

start_local = pd.Timestamp(start_date).tz_localize(LOCAL_TZ) + pd.Timedelta(hours=start_hour)
end_local = pd.Timestamp(end_date).tz_localize(LOCAL_TZ) + pd.Timedelta(hours=end_hour, minutes=59, seconds=59)

start_ts_utc = start_local.tz_convert("UTC").replace(tzinfo=None)
end_ts_utc = end_local.tz_convert("UTC").replace(tzinfo=None)

hosts = st.sidebar.multiselect("Host", get_distinct_values("reqHost"))
states = st.sidebar.multiselect("State", get_distinct_values("state"))
cities = st.sidebar.multiselect("City", get_distinct_values("city"))
status_classes = st.sidebar.multiselect("Status class", ["2xx", "3xx", "4xx", "5xx", "other"])
renditions = st.sidebar.multiselect("Rendition", get_distinct_values("rendition"))
device_families = st.sidebar.multiselect("Device family", get_distinct_values("device_family"))
path_contains = st.sidebar.text_input("Path contains")

where_clause, params = build_filters_clause(
    start_ts_utc, end_ts_utc, hosts, states, cities,
    status_classes, renditions, device_families, path_contains
)

if st.sidebar.button("🔄 Refresh Data Cache"):
    st.cache_resource.clear()
    st.cache_data.clear()
    st.rerun()

# ============================================================
# HEADER & KPIs
# ============================================================
st.title("Akamai Log Dashboard")
st.caption("Fast dashboard over parquet logs using DuckDB")

kpi_df = get_kpis(where_clause, params)

if kpi_df.empty or pd.isna(kpi_df['total_requests'].iloc[0]) or kpi_df['total_requests'].iloc[0] == 0:
    st.warning("No data found for the selected filters. Please adjust the date range or filters in the sidebar.")
    st.stop()

kpi = kpi_df.iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Requests", f"{int(kpi['total_requests']):,}")
c2.metric("Unique IPs", f"{int(kpi['unique_ips']):,}")
c3.metric("Unique Streams", f"{int(kpi['unique_streams']):,}")
c4.metric("Data Served", format_bytes(kpi["total_bytes"]))
c5.metric("Error Rate", f"{kpi['error_rate_pct'] or 0:.2f}%")

c6, c7, c8 = st.columns(3)
c6.metric("Avg Throughput", f"{kpi['avg_throughput'] or 0:.2f}")
c7.metric("Avg TTFB (ms)", f"{kpi['avg_ttfb'] or 0:.2f}")
c8.metric("5xx Rate", f"{kpi['server_error_rate_pct'] or 0:.2f}%")

st.divider()

# ============================================================
# TRAFFIC OVER TIME
# ============================================================
ts = get_timeseries(where_clause, params)
if not ts.empty:
    ts = to_local_tz(ts, "hour_ts")
    left, right = st.columns(2)
    with left:
        fig = px.line(ts, x="hour_ts", y="requests", title="Requests Over Time")
        fig.update_layout(xaxis_title="", yaxis_title="Requests", height=380)
        st.plotly_chart(fig, use_container_width=True)
    with right:
        fig = px.line(ts, x="hour_ts", y="error_requests", title="Errors Over Time")
        fig.update_layout(xaxis_title="", yaxis_title="Error Requests", height=380)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# BREAKDOWNS
# ============================================================
b1, b2, b3, b4 = st.columns(4)

with b1:
    status_df = get_breakdown("statusCode", where_clause, params)
    if not status_df.empty:
        fig = px.bar(status_df, x="statusCode", y="requests", title="Status Code Breakdown")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

with b2:
    cache_df = q(f"SELECT COALESCE(cacheStatus, 'unknown') AS cache_status, COUNT(*) AS requests FROM logs {where_clause} GROUP BY 1 ORDER BY requests DESC", params)
    if not cache_df.empty:
        fig = px.pie(cache_df, names="cache_status", values="requests", title="Cache Status Mix")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

with b3:
    rendition_df = get_breakdown("rendition", where_clause, params)
    if not rendition_df.empty:
        fig = px.bar(rendition_df, x="rendition", y="requests", title="Rendition Requests")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

with b4:
    device_df = get_breakdown("device_family", where_clause, params)
    if not device_df.empty:
        fig = px.bar(device_df, x="device_family", y="requests", title="Device Family")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# TOP TABLES & RAW DATA
# ============================================================
t1, t2 = st.columns(2)

with t1:
    st.subheader("Top Paths")
    top_paths = q(f"SELECT reqPath, COUNT(*) AS requests, SUM(bytes) AS total_bytes FROM logs {where_clause} GROUP BY 1 ORDER BY requests DESC LIMIT 25", params)
    if not top_paths.empty:
        top_paths["total_bytes"] = top_paths["total_bytes"].apply(format_bytes)
        st.dataframe(top_paths, use_container_width=True, height=400)

with t2:
    st.subheader("Top Hosts")
    top_hosts = q(f"SELECT reqHost, COUNT(*) AS requests, SUM(bytes) AS total_bytes FROM logs {where_clause} GROUP BY 1 ORDER BY requests DESC LIMIT 25", params)
    if not top_hosts.empty:
        top_hosts["total_bytes"] = top_hosts["total_bytes"].apply(format_bytes)
        st.dataframe(top_hosts, use_container_width=True, height=400)

st.subheader("Raw Sample (200 rows)")
sample_rows = q(f"SELECT * FROM logs {where_clause} ORDER BY req_time_utc DESC LIMIT 200", params)
if not sample_rows.empty:
    sample_rows = to_local_tz(sample_rows, "req_time_utc")
    st.dataframe(sample_rows, use_container_width=True, height=400)