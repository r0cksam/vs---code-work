import re
from pathlib import Path
from datetime import datetime, time as dtime

import duckdb
import pandas as pd
import streamlit as st

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
    UTC = pytz.utc
except Exception:
    IST = None
    UTC = None

st.set_page_config(page_title="Watch Hour Diagnostics", page_icon="🧪", layout="wide")
st.title("🧪 Watch Hour Diagnostics")
st.caption("Use this to validate whether device watch-hour numbers are believable before changing the main dashboard.")

CAP_SECONDS_DEFAULT = 60


def dq(col: str) -> str:
    return '"' + str(col).replace('"', '""') + '"'


def date_time_to_epoch(d, t, tz_mode: str) -> int:
    dt = datetime.combine(d, t)
    if IST is None:
        return int(dt.timestamp())
    tz = IST if tz_mode == "IST" else UTC
    return int(tz.localize(dt).timestamp())


@st.cache_data(show_spinner=False, ttl=600)
def list_parquet_files(folder: str):
    p = Path(folder)
    if not p.exists() or not p.is_dir():
        return []
    return sorted([str(x) for x in p.rglob("*.parquet")])


@st.cache_data(show_spinner=False, ttl=600)
def get_columns(files):
    if not files:
        return []
    con = duckdb.connect()
    try:
        # Read zero rows just to infer schema.
        df = con.execute(f"SELECT * FROM read_parquet({files!r}, union_by_name=true) LIMIT 0").df()
        return list(df.columns)
    finally:
        con.close()


def build_base_cte(files, ts_col, device_mode, device_col, query_col, session_mode, session_col, status_col, path_col, start_epoch, end_epoch):
    ts_expr = f"TRY_CAST({dq(ts_col)} AS BIGINT)"

    if device_mode == "Extract device_id from query string":
        device_expr = f"regexp_extract(CAST({dq(query_col)} AS VARCHAR), '(?:^|&)device_id=([^&]+)', 1)"
    else:
        device_expr = f"CAST({dq(device_col)} AS VARCHAR)"

    if session_mode == "Extract session_id from query string":
        session_expr = f"regexp_extract(CAST({dq(query_col)} AS VARCHAR), '(?:^|&)session_id=([^&]+)', 1)"
    elif session_mode == "Use direct session column":
        session_expr = f"CAST({dq(session_col)} AS VARCHAR)"
    else:
        session_expr = "''"

    status_expr = f"CAST({dq(status_col)} AS VARCHAR)" if status_col else "''"
    path_expr = f"CAST({dq(path_col)} AS VARCHAR)" if path_col else "''"

    return f"""
    WITH base AS (
        SELECT
            {ts_expr} AS req_ts,
            {device_expr} AS device_id,
            {session_expr} AS session_id,
            {status_expr} AS status_code,
            {path_expr} AS req_path
        FROM read_parquet({files!r}, union_by_name=true)
        WHERE {ts_expr} BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND {ts_expr} IS NOT NULL
    ), clean AS (
        SELECT *
        FROM base
        WHERE device_id IS NOT NULL
          AND TRIM(device_id) <> ''
          AND LOWER(TRIM(device_id)) NOT IN ('nan', 'none', 'null')
          AND req_ts IS NOT NULL
    )
    """


def run_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


with st.sidebar:
    st.header("Input")
    folder = st.text_input("Folder containing parquet files", placeholder="/path/to/parquet/root")
    files_all = list_parquet_files(folder.strip()) if folder.strip() else []

    if files_all:
        st.success(f"Found {len(files_all):,} parquet files")
        use_all = st.checkbox("Use all files", value=True)
        if use_all:
            files = files_all
        else:
            files = st.multiselect("Select files", options=files_all, default=files_all[: min(20, len(files_all))])
    else:
        files = []
        if folder.strip():
            st.warning("No parquet files found.")

if not files:
    st.info("Enter a folder in the sidebar to begin.")
    st.stop()

columns = get_columns(files)
if not columns:
    st.error("Could not read columns from selected parquet files.")
    st.stop()

st.subheader("1) Column mapping")
col1, col2, col3 = st.columns(3)
with col1:
    ts_col = st.selectbox("Timestamp column", columns, index=columns.index("reqTimeSec") if "reqTimeSec" in columns else 0)
with col2:
    query_col = st.selectbox("Query string column", columns, index=columns.index("queryStr") if "queryStr" in columns else 0)
with col3:
    cap_seconds = st.number_input("Gap cap seconds", min_value=5, max_value=600, value=CAP_SECONDS_DEFAULT, step=5)

col4, col5 = st.columns(2)
with col4:
    device_mode = st.radio("Device_ID source", ["Extract device_id from query string", "Use direct device column"], horizontal=True)
    device_col = ""
    if device_mode == "Use direct device column":
        device_col = st.selectbox("Device column", columns)
with col5:
    session_mode = st.radio("Session source", ["Extract session_id from query string", "Use direct session column", "Do not use session"], horizontal=True)
    session_col = ""
    if session_mode == "Use direct session column":
        session_col = st.selectbox("Session column", columns)

optional1, optional2 = st.columns(2)
with optional1:
    status_col = st.selectbox("Status column optional", [""] + columns, index=([""] + columns).index("statusCode") if "statusCode" in columns else 0)
with optional2:
    path_col = st.selectbox("Path column optional", [""] + columns, index=([""] + columns).index("reqPath") if "reqPath" in columns else 0)

st.subheader("2) Date/time range")
timezone_mode = st.radio("Date/time interpreted as", ["IST", "UTC"], horizontal=True)

# Detect available date range from selected files.
con = duckdb.connect()
try:
    minmax = con.execute(
        f"""
        SELECT MIN(TRY_CAST({dq(ts_col)} AS BIGINT)) AS min_ts,
               MAX(TRY_CAST({dq(ts_col)} AS BIGINT)) AS max_ts
        FROM read_parquet({files!r}, union_by_name=true)
        WHERE TRY_CAST({dq(ts_col)} AS BIGINT) IS NOT NULL
        """
    ).fetchone()
finally:
    con.close()

if not minmax or minmax[0] is None:
    st.error("No valid timestamp values found.")
    st.stop()

min_epoch, max_epoch = int(minmax[0]), int(minmax[1])
if IST is not None:
    tz = IST if timezone_mode == "IST" else UTC
    min_dt = datetime.fromtimestamp(min_epoch, tz=tz)
    max_dt = datetime.fromtimestamp(max_epoch, tz=tz)
else:
    min_dt = datetime.fromtimestamp(min_epoch)
    max_dt = datetime.fromtimestamp(max_epoch)

m1, m2 = st.columns(2)
m1.metric("Available from", str(min_dt).split("+")[0])
m2.metric("Available to", str(max_dt).split("+")[0])

d1, d2, t1, t2 = st.columns(4)
with d1:
    start_date = st.date_input("Start date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
with d2:
    end_date = st.date_input("End date", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
with t1:
    start_time = st.time_input("Start time", value=dtime(0, 0))
with t2:
    end_time = st.time_input("End time", value=dtime(23, 59, 59))

start_epoch = date_time_to_epoch(start_date, start_time, timezone_mode)
end_epoch = date_time_to_epoch(end_date, end_time, timezone_mode)

if end_epoch < start_epoch:
    st.error("End date/time must be after start date/time.")
    st.stop()

base_cte = build_base_cte(
    files=files,
    ts_col=ts_col,
    device_mode=device_mode,
    device_col=device_col,
    query_col=query_col,
    session_mode=session_mode,
    session_col=session_col,
    status_col=status_col,
    path_col=path_col,
    start_epoch=start_epoch,
    end_epoch=end_epoch,
)

st.subheader("3) Run diagnostics")
run = st.button("Run watch-hour diagnostics", type="primary")
if not run:
    st.stop()

with st.spinner("Calculating diagnostics with DuckDB..."):
    overview_sql = base_cte + f"""
    SELECT
        COUNT(*) AS rows_with_device_id,
        COUNT(DISTINCT device_id) AS total_device_id_count,
        MIN(req_ts) AS min_req_ts,
        MAX(req_ts) AS max_req_ts,
        COUNT(DISTINCT req_ts) AS distinct_timestamps,
        ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT device_id), 0), 2) AS rows_per_device_avg
    FROM clean
    """
    overview_df = run_df(overview_sql)

    dup_sql = base_cte + """
    SELECT
        COUNT(*) AS device_second_groups,
        SUM(row_count) AS rows_in_device_second_groups,
        SUM(CASE WHEN row_count > 1 THEN row_count - 1 ELSE 0 END) AS extra_rows_same_device_same_second,
        SUM(CASE WHEN row_count > 1 THEN 1 ELSE 0 END) AS duplicate_device_second_groups
    FROM (
        SELECT device_id, req_ts, COUNT(*) AS row_count
        FROM clean
        GROUP BY 1,2
    ) x
    """
    dup_df = run_df(dup_sql)

    device_gap_sql = base_cte + f"""
    , ordered AS (
        SELECT
            device_id,
            session_id,
            req_ts,
            LEAD(req_ts) OVER (PARTITION BY device_id ORDER BY req_ts) AS next_ts
        FROM clean
    ), gaps AS (
        SELECT
            *,
            next_ts - req_ts AS raw_gap_sec,
            LEAST(GREATEST(next_ts - req_ts, 0), {int(cap_seconds)}) AS watch_sec
        FROM ordered
        WHERE next_ts IS NOT NULL
    )
    SELECT
        COUNT(*) AS rows_with_next_event,
        SUM(CASE WHEN raw_gap_sec = 0 THEN 1 ELSE 0 END) AS zero_gap_rows,
        ROUND(100.0 * SUM(CASE WHEN raw_gap_sec = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS zero_gap_pct,
        ROUND(AVG(raw_gap_sec), 3) AS avg_raw_gap_sec,
        quantile_cont(raw_gap_sec, 0.50) AS p50_raw_gap_sec,
        quantile_cont(raw_gap_sec, 0.90) AS p90_raw_gap_sec,
        quantile_cont(raw_gap_sec, 0.95) AS p95_raw_gap_sec,
        quantile_cont(raw_gap_sec, 0.99) AS p99_raw_gap_sec,
        MAX(raw_gap_sec) AS max_raw_gap_sec,
        ROUND(AVG(watch_sec), 3) AS avg_capped_watch_sec_per_event,
        ROUND(SUM(watch_sec) / 3600.0, 4) AS watch_hours_device_only
    FROM gaps
    """
    device_gap_df = run_df(device_gap_sql)

    session_gap_df = pd.DataFrame()
    if session_mode != "Do not use session":
        session_gap_sql = base_cte + f"""
        , ordered AS (
            SELECT
                device_id,
                session_id,
                req_ts,
                LEAD(req_ts) OVER (PARTITION BY device_id, session_id ORDER BY req_ts) AS next_ts
            FROM clean
            WHERE session_id IS NOT NULL AND TRIM(session_id) <> ''
        ), gaps AS (
            SELECT
                *,
                next_ts - req_ts AS raw_gap_sec,
                LEAST(GREATEST(next_ts - req_ts, 0), {int(cap_seconds)}) AS watch_sec
            FROM ordered
            WHERE next_ts IS NOT NULL
        )
        SELECT
            COUNT(*) AS rows_with_next_event,
            SUM(CASE WHEN raw_gap_sec = 0 THEN 1 ELSE 0 END) AS zero_gap_rows,
            ROUND(100.0 * SUM(CASE WHEN raw_gap_sec = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS zero_gap_pct,
            ROUND(AVG(raw_gap_sec), 3) AS avg_raw_gap_sec,
            quantile_cont(raw_gap_sec, 0.50) AS p50_raw_gap_sec,
            quantile_cont(raw_gap_sec, 0.90) AS p90_raw_gap_sec,
            quantile_cont(raw_gap_sec, 0.95) AS p95_raw_gap_sec,
            quantile_cont(raw_gap_sec, 0.99) AS p99_raw_gap_sec,
            MAX(raw_gap_sec) AS max_raw_gap_sec,
            ROUND(AVG(watch_sec), 3) AS avg_capped_watch_sec_per_event,
            ROUND(SUM(watch_sec) / 3600.0, 4) AS watch_hours_device_session
        FROM gaps
        """
        session_gap_df = run_df(session_gap_sql)

    per_device_sql = base_cte + f"""
    , ordered AS (
        SELECT device_id, req_ts,
               LEAD(req_ts) OVER (PARTITION BY device_id ORDER BY req_ts) AS next_ts
        FROM clean
    ), gaps AS (
        SELECT device_id,
               LEAST(GREATEST(next_ts - req_ts, 0), {int(cap_seconds)}) AS watch_sec
        FROM ordered
        WHERE next_ts IS NOT NULL
    ), per_device AS (
        SELECT device_id, SUM(watch_sec) / 60.0 AS watch_minutes
        FROM gaps
        GROUP BY 1
    )
    SELECT
        COUNT(*) AS devices_with_watch,
        ROUND(AVG(watch_minutes), 4) AS avg_watch_minutes_per_device,
        ROUND(quantile_cont(watch_minutes, 0.50), 4) AS p50_watch_minutes_per_device,
        ROUND(quantile_cont(watch_minutes, 0.90), 4) AS p90_watch_minutes_per_device,
        ROUND(quantile_cont(watch_minutes, 0.95), 4) AS p95_watch_minutes_per_device,
        ROUND(MAX(watch_minutes), 4) AS max_watch_minutes_per_device
    FROM per_device
    """
    per_device_df = run_df(per_device_sql)

    top_devices_sql = base_cte + f"""
    , ordered AS (
        SELECT device_id, req_ts,
               LEAD(req_ts) OVER (PARTITION BY device_id ORDER BY req_ts) AS next_ts
        FROM clean
    ), gaps AS (
        SELECT device_id,
               LEAST(GREATEST(next_ts - req_ts, 0), {int(cap_seconds)}) AS watch_sec
        FROM ordered
        WHERE next_ts IS NOT NULL
    )
    SELECT device_id, ROUND(SUM(watch_sec) / 3600.0, 4) AS watch_hours
    FROM gaps
    GROUP BY 1
    ORDER BY watch_hours DESC
    LIMIT 25
    """
    top_devices_df = run_df(top_devices_sql)

    tz_expr = "CAST(to_timestamp(req_ts) AT TIME ZONE 'Asia/Kolkata' AS DATE)" if timezone_mode == "IST" else "CAST(to_timestamp(req_ts) AT TIME ZONE 'UTC' AS DATE)"
    daily_sql = base_cte + f"""
    , ordered AS (
        SELECT device_id, req_ts,
               LEAD(req_ts) OVER (PARTITION BY device_id ORDER BY req_ts) AS next_ts
        FROM clean
    ), gaps AS (
        SELECT device_id,
               req_ts,
               LEAST(GREATEST(next_ts - req_ts, 0), {int(cap_seconds)}) AS watch_sec
        FROM ordered
        WHERE next_ts IS NOT NULL
    )
    SELECT
        {tz_expr} AS date,
        COUNT(*) AS rows_with_next_event,
        COUNT(DISTINCT device_id) AS distinct_devices,
        ROUND(SUM(watch_sec) / 3600.0, 4) AS watch_hours,
        ROUND(SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT device_id), 0), 4) AS watch_minutes_per_device,
        ROUND(AVG(watch_sec), 4) AS avg_watch_sec_per_event
    FROM gaps
    GROUP BY 1
    ORDER BY 1
    """
    daily_df = run_df(daily_sql)

    status_df = pd.DataFrame()
    if status_col:
        status_sql = base_cte + """
        SELECT status_code, COUNT(*) AS rows, COUNT(DISTINCT device_id) AS distinct_devices
        FROM clean
        GROUP BY 1
        ORDER BY rows DESC
        LIMIT 50
        """
        status_df = run_df(status_sql)

    path_df = pd.DataFrame()
    if path_col:
        path_sql = base_cte + """
        SELECT
            CASE
                WHEN lower(req_path) LIKE '%.ts%' THEN 'ts_segment'
                WHEN lower(req_path) LIKE '%.m4s%' THEN 'm4s_segment'
                WHEN lower(req_path) LIKE '%.mp4%' THEN 'mp4'
                WHEN lower(req_path) LIKE '%.m3u8%' THEN 'm3u8_manifest'
                WHEN lower(req_path) LIKE '%.mpd%' THEN 'dash_manifest'
                ELSE 'other'
            END AS path_type,
            COUNT(*) AS rows,
            COUNT(DISTINCT device_id) AS distinct_devices
        FROM clean
        GROUP BY 1
        ORDER BY rows DESC
        """
        path_df = run_df(path_sql)

st.success("Diagnostics complete.")

st.subheader("4) Key metrics to send back")
k1, k2, k3, k4 = st.columns(4)
row = overview_df.iloc[0]
k1.metric("Rows with Device_ID", f"{int(row['rows_with_device_id']):,}")
k2.metric("Total Device_ID Count", f"{int(row['total_device_id_count']):,}")
watch_device = float(device_gap_df.iloc[0].get("watch_hours_device_only", 0) or 0)
k3.metric("Watch Hours device-only", f"{watch_device:,.2f}")
if not session_gap_df.empty:
    watch_session = float(session_gap_df.iloc[0].get("watch_hours_device_session", 0) or 0)
    k4.metric("Watch Hours device+session", f"{watch_session:,.2f}")
else:
    k4.metric("Watch Hours device+session", "not used")

st.markdown("### Overview")
st.dataframe(overview_df, use_container_width=True, hide_index=True)

st.markdown("### Same device + same second duplicates")
st.dataframe(dup_df, use_container_width=True, hide_index=True)

c1, c2 = st.columns(2)
with c1:
    st.markdown("### Gap diagnostics: device-only")
    st.dataframe(device_gap_df, use_container_width=True, hide_index=True)
with c2:
    st.markdown("### Gap diagnostics: device + session")
    if session_gap_df.empty:
        st.info("Session mode is disabled or no session_id was selected.")
    else:
        st.dataframe(session_gap_df, use_container_width=True, hide_index=True)

st.markdown("### Per-device watch distribution")
st.dataframe(per_device_df, use_container_width=True, hide_index=True)

st.markdown("### Top 25 devices by watch hours")
st.dataframe(top_devices_df, use_container_width=True, hide_index=True)

st.markdown("### Daily watch-hour summary")
st.dataframe(daily_df, use_container_width=True, hide_index=True, height=380)

if not status_df.empty or not path_df.empty:
    s1, s2 = st.columns(2)
    with s1:
        st.markdown("### Status breakdown")
        if status_df.empty:
            st.info("No status column selected.")
        else:
            st.dataframe(status_df, use_container_width=True, hide_index=True)
    with s2:
        st.markdown("### Path type breakdown")
        if path_df.empty:
            st.info("No path column selected.")
        else:
            st.dataframe(path_df, use_container_width=True, hide_index=True)

st.subheader("5) Download all diagnostics")
outputs = {
    "overview.csv": overview_df,
    "same_device_same_second_duplicates.csv": dup_df,
    "gap_device_only.csv": device_gap_df,
    "gap_device_session.csv": session_gap_df,
    "per_device_watch_distribution.csv": per_device_df,
    "top_devices_watch_hours.csv": top_devices_df,
    "daily_watch_summary.csv": daily_df,
    "status_breakdown.csv": status_df,
    "path_type_breakdown.csv": path_df,
}

# Simple combined text report for easy paste-back.
report_lines = []
for name, df in outputs.items():
    report_lines.append(f"\n===== {name} =====")
    if df is None or df.empty:
        report_lines.append("EMPTY")
    else:
        report_lines.append(df.head(50).to_csv(index=False))
report_text = "\n".join(report_lines)

st.download_button(
    "📥 Download paste-back diagnostic report TXT",
    data=report_text.encode("utf-8"),
    file_name="watchhour_diagnostics_report.txt",
    mime="text/plain",
)
