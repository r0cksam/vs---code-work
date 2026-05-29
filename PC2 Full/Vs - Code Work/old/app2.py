from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# CONFIG
# ============================================================
PARQUET_SOURCES = [
    "D:/VETO Logs/01_parquet/date=2026-03-31/00000000.parquet"
]
LOCAL_TZ = "Asia/Kolkata"

st.set_page_config(
    page_title="Veto Log Dashboard",
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
    con.execute("PRAGMA memory_limit='8GB';")
    con.execute("PRAGMA temp_directory='D:/VETO Logs/duckdb_temp';")
    return con

# ============================================================
# BASE VIEW
# ============================================================
@st.cache_resource
def initialize_db(parquet_list: List[str]) -> None:
    con = get_conn()

    paths_joined = ", ".join([f"'{p}'" for p in parquet_list])

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
            WHEN lower(COALESCE(UA, '')) LIKE '%android%' AND lower(COALESCE(UA, '')) LIKE '%aft%' THEN 'Fire TV'
            WHEN lower(COALESCE(UA, '')) LIKE '%android tv%' THEN 'Android TV'
            WHEN lower(COALESCE(UA, '')) LIKE '%android%' THEN 'Android Mobile'
            WHEN lower(COALESCE(UA, '')) LIKE '%iphone%' OR lower(COALESCE(UA, '')) LIKE '%ipad%' OR lower(COALESCE(UA, '')) LIKE '%ios%' THEN 'iOS'
            WHEN lower(COALESCE(UA, '')) LIKE '%bravia%' THEN 'Sony TV'
            WHEN lower(COALESCE(UA, '')) LIKE '%tizen%' OR lower(COALESCE(UA, '')) LIKE '%samsungbrowser%' THEN 'Samsung TV'
            WHEN lower(COALESCE(UA, '')) LIKE '%webos%' OR lower(COALESCE(UA, '')) LIKE '%lg%' THEN 'LG TV'
            WHEN lower(COALESCE(UA, '')) LIKE '%windows%' THEN 'Windows'
            WHEN lower(COALESCE(UA, '')) LIKE '%mac os%' OR lower(COALESCE(UA, '')) LIKE '%macintosh%' THEN 'Mac'
            WHEN lower(COALESCE(UA, '')) LIKE '%linux%' THEN 'Linux'
            ELSE 'Other'
        END AS device_family,

        CASE
            WHEN statusCode BETWEEN 200 AND 299 THEN '2xx'
            WHEN statusCode BETWEEN 300 AND 399 THEN '3xx'
            WHEN statusCode BETWEEN 400 AND 499 THEN '4xx'
            WHEN statusCode BETWEEN 500 AND 599 THEN '5xx'
            ELSE 'other'
        END AS status_class
    FROM read_parquet([{paths_joined}]);
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
    start_ts,
    end_ts,
    hosts: List[str],
    states: List[str],
    cities: List[str],
    status_classes: List[str],
    renditions: List[str],
    device_families: List[str],
    path_contains: str,
    extra_clause: str = "",
    extra_params: Optional[list] = None,
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

    where_sql = " WHERE " + " AND ".join(clauses)

    if extra_clause:
        where_sql += f" {extra_clause}"
        if extra_params:
            params.extend(extra_params)

    return where_sql, params

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

# ============================================================
# INIT
# ============================================================
any_files_found = False
for pattern in PARQUET_SOURCES:
    base_path = Path(pattern.split("*")[0])
    if base_path.exists() and list(base_path.glob("*.parquet")):
        any_files_found = True
        break

if not any_files_found:
    st.error("No Parquet files found in any of the configured folders.")
    st.stop()

Path("D:/VETO Logs/duckdb_temp").mkdir(parents=True, exist_ok=True)

initialize_db(PARQUET_SOURCES)
min_ts, max_ts = get_date_bounds()

from datetime import datetime, time

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.title("Filters")

time_filter_mode = st.sidebar.radio(
    "Time filter mode",
    [
        "All time",
        "Single day",
        "Date range (full days)",
        "Date range (time window each day)",
    ],
)

time_window_clause = ""
time_window_params = []

if time_filter_mode == "All time":
    start_ts_utc = min_ts.tz_convert("UTC").replace(tzinfo=None)
    end_ts_utc = max_ts.tz_convert("UTC").replace(tzinfo=None)

elif time_filter_mode == "Single day":
    selected_date = st.sidebar.date_input(
        "Select date",
        value=min_ts.date(),
        min_value=min_ts.date(),
        max_value=max_ts.date(),
    )

    all_day = st.sidebar.checkbox("All day", value=True)

    if all_day:
        start_local = pd.Timestamp(selected_date).tz_localize(LOCAL_TZ)
        end_local = start_local + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    else:
        start_time = st.sidebar.time_input("Start time", value=time(0, 0))
        end_time = st.sidebar.time_input("End time", value=time(23, 59))

        start_local = pd.Timestamp.combine(selected_date, start_time).tz_localize(LOCAL_TZ)
        end_local = pd.Timestamp.combine(selected_date, end_time).tz_localize(LOCAL_TZ)

        if end_local < start_local:
            st.sidebar.error("End time must be after start time for a single day.")
            st.stop()

    start_ts_utc = start_local.tz_convert("UTC").replace(tzinfo=None)
    end_ts_utc = end_local.tz_convert("UTC").replace(tzinfo=None)

elif time_filter_mode == "Date range (full days)":
    start_date = st.sidebar.date_input(
        "Start date",
        value=min_ts.date(),
        min_value=min_ts.date(),
        max_value=max_ts.date(),
        key="range_full_start",
    )
    end_date = st.sidebar.date_input(
        "End date",
        value=max_ts.date(),
        min_value=min_ts.date(),
        max_value=max_ts.date(),
        key="range_full_end",
    )

    if end_date < start_date:
        st.sidebar.error("End date must be on or after start date.")
        st.stop()

    start_local = pd.Timestamp(start_date).tz_localize(LOCAL_TZ)
    end_local = pd.Timestamp(end_date).tz_localize(LOCAL_TZ) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    start_ts_utc = start_local.tz_convert("UTC").replace(tzinfo=None)
    end_ts_utc = end_local.tz_convert("UTC").replace(tzinfo=None)

elif time_filter_mode == "Date range (time window each day)":
    start_date = st.sidebar.date_input(
        "Start date",
        value=min_ts.date(),
        min_value=min_ts.date(),
        max_value=max_ts.date(),
        key="range_window_start",
    )
    end_date = st.sidebar.date_input(
        "End date",
        value=max_ts.date(),
        min_value=min_ts.date(),
        max_value=max_ts.date(),
        key="range_window_end",
    )

    if end_date < start_date:
        st.sidebar.error("End date must be on or after start date.")
        st.stop()

    window_start = st.sidebar.time_input("Daily start time", value=time(0, 0), key="daily_start")
    window_end = st.sidebar.time_input("Daily end time", value=time(23, 59), key="daily_end")

    # broad UTC bounds for the selected date range
    start_local = pd.Timestamp(start_date).tz_localize(LOCAL_TZ)
    end_local = pd.Timestamp(end_date).tz_localize(LOCAL_TZ) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    start_ts_utc = start_local.tz_convert("UTC").replace(tzinfo=None)
    end_ts_utc = end_local.tz_convert("UTC").replace(tzinfo=None)

    # local time-of-day filter applied inside the SQL WHERE clause
    if window_end >= window_start:
        time_window_clause = """
            AND CAST((req_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS TIME) >= ?
            AND CAST((req_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS TIME) <= ?
        """
        time_window_params = [window_start.strftime("%H:%M:%S"), window_end.strftime("%H:%M:%S")]
    else:
        # handles overnight windows like 22:00 to 06:00
        time_window_clause = """
            AND (
                CAST((req_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS TIME) >= ?
                OR CAST((req_time_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS TIME) <= ?
            )
        """
        time_window_params = [window_start.strftime("%H:%M:%S"), window_end.strftime("%H:%M:%S")]

hosts = st.sidebar.multiselect("Host", get_distinct_values("reqHost"))
states = st.sidebar.multiselect("State", get_distinct_values("state"))
cities = st.sidebar.multiselect("City", get_distinct_values("city"))
status_classes = st.sidebar.multiselect("Status class", ["2xx", "3xx", "4xx", "5xx", "other"])
renditions = st.sidebar.multiselect("Rendition", get_distinct_values("rendition"))
device_families = st.sidebar.multiselect("Device family", get_distinct_values("device_family"))
path_contains = st.sidebar.text_input("Path contains")

where_clause, params = build_filters_clause(
    start_ts_utc,
    end_ts_utc,
    hosts,
    states,
    cities,
    status_classes,
    renditions,
    device_families,
    path_contains,
    extra_clause=time_window_clause,
    extra_params=time_window_params,
)

# ============================================================
# HEADER & KPIs
# ============================================================
st.title("Veto Dashboard")
st.caption("Hello JI")

@st.cache_data(show_spinner=False)
def get_kpis(where_clause: str, params: list) -> pd.DataFrame:
    sql = f"""
    SELECT
        COUNT(*) AS total_requests,
        COUNT(DISTINCT cliIP) AS unique_ips,
        COUNT(DISTINCT reqId) AS unique_request_ids,
        COUNT(DISTINCT streamId) AS unique_streams,
        SUM("bytes") AS total_bytes,
        AVG(throughput) AS avg_throughput,
        AVG(timeToFirstByte) AS avg_ttfb,
        AVG(downloadTime) AS avg_download_time,
        SUM(CASE WHEN statusCode >= 400 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS error_rate_pct,
        SUM(CASE WHEN statusCode >= 500 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS server_error_rate_pct
    FROM logs
    {where_clause}
    """
    return q(sql, params)

kpi_df = get_kpis(where_clause, params)

if kpi_df["total_requests"].iloc[0] == 0:
    st.warning("No data found for the selected filters.")
    st.stop()

kpi = kpi_df.iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Requests", f"{int(kpi['total_requests']):,}")
c2.metric("Unique IPs", f"{int(kpi['unique_ips']):,}")
c3.metric("Unique Streams", f"{int(kpi['unique_streams']):,}")
c4.metric("Data Served", format_bytes(kpi["total_bytes"]))
c5.metric("Error Rate", f"{(kpi['error_rate_pct'] or 0):.2f}%")

c6, c7, c8 = st.columns(3)
c6.metric("Avg Throughput", f"{(kpi['avg_throughput'] or 0):.2f}")
c7.metric("Avg TTFB (ms)", f"{(kpi['avg_ttfb'] or 0):.2f}")
c8.metric("5xx Rate", f"{(kpi['server_error_rate_pct'] or 0):.2f}%")

st.divider()

# ============================================================
# TRAFFIC OVER TIME
# ============================================================
@st.cache_data(show_spinner=False)
def get_timeseries(where_clause: str, params: list) -> pd.DataFrame:
    sql = f"""
    SELECT
        date_trunc('hour', req_time_utc) AS hour_ts,
        COUNT(*) AS requests,
        SUM("bytes") AS bytes_served,
        AVG(throughput) AS avg_throughput,
        AVG(timeToFirstByte) AS avg_ttfb,
        SUM(CASE WHEN statusCode >= 400 THEN 1 ELSE 0 END) AS error_requests
    FROM logs
    {where_clause}
    GROUP BY 1
    ORDER BY 1
    """
    return q(sql, params)

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
def get_breakdown(column: str, where_clause: str, params: list) -> pd.DataFrame:
    sql = f"""
    SELECT {column}, COUNT(*) AS requests
    FROM logs
    {where_clause}
    GROUP BY 1
    ORDER BY requests DESC
    """
    return q(sql, params)

b1, b2, b3, b4 = st.columns(4)

with b1:
    status_df = get_breakdown("statusCode", where_clause, params)
    if not status_df.empty:
        fig = px.bar(status_df, x="statusCode", y="requests", title="Status Code Breakdown")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

with b2:
    cache_df = q(
        f"""
        SELECT COALESCE(cacheStatus, 'unknown') AS cache_status, COUNT(*) AS requests
        FROM logs
        {where_clause}
        GROUP BY 1
        ORDER BY requests DESC
        """,
        params,
    )
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
    top_paths = q(
        f"""
        SELECT reqPath, COUNT(*) AS requests, SUM("bytes") AS total_bytes
        FROM logs
        {where_clause}
        GROUP BY 1
        ORDER BY requests DESC
        LIMIT 25
        """,
        params,
    )
    if not top_paths.empty:
        top_paths["total_bytes"] = top_paths["total_bytes"].apply(format_bytes)
        st.dataframe(top_paths, use_container_width=True, height=400)

with t2:
    st.subheader("Top Hosts")
    top_hosts = q(
        f"""
        SELECT reqHost, COUNT(*) AS requests, SUM("bytes") AS total_bytes
        FROM logs
        {where_clause}
        GROUP BY 1
        ORDER BY requests DESC
        LIMIT 25
        """,
        params,
    )
    if not top_hosts.empty:
        top_hosts["total_bytes"] = top_hosts["total_bytes"].apply(format_bytes)
        st.dataframe(top_hosts, use_container_width=True, height=400)

st.subheader("Raw Sample (200 rows)")
sample_rows = q(
    f"""
    SELECT *
    FROM logs
    {where_clause}
    ORDER BY req_time_utc DESC
    LIMIT 200
    """,
    params,
)
if not sample_rows.empty:
    sample_rows = to_local_tz(sample_rows, "req_time_utc")
    st.dataframe(sample_rows, use_container_width=True, height=400)