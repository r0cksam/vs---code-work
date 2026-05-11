import re
from pathlib import Path
from datetime import datetime, time

import duckdb
import pandas as pd
import streamlit as st


st.set_page_config(page_title="cliIP Missing Device/Session Check", layout="wide")
st.title("cliIP Missing device_id / session_id Check")

st.markdown("""
This diagnostic finds `cliIP` values where **none of that IP's rows** have `device_id` or `session_id` in `queryStr`.

Use it to answer:

> Are there IPs where we have `cliIP`, but every row for that IP is missing both `device_id` and `session_id`?
""")

folder = st.sidebar.text_input("Parquet folder", placeholder=r"D:\Veto Logs Backup or /mnt/data/logs")

if not folder:
    st.info("Enter a parquet folder in the sidebar.")
    st.stop()

root = Path(folder)
if not root.is_dir():
    st.error("Folder not found.")
    st.stop()

files = sorted(root.glob("*.parquet"))
if not files:
    st.error("No parquet files found in this folder.")
    st.stop()

file_paths = [str(f) for f in files]
st.sidebar.success(f"Found {len(file_paths):,} parquet files")

con = duckdb.connect()
con.execute("PRAGMA threads=4")

try:
    schema_df = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 1",
        [file_paths],
    ).df()
except Exception as e:
    st.error(f"Could not read parquet schema: {e}")
    st.stop()

columns = schema_df["column_name"].tolist()

def default_index(col_name: str) -> int:
    return columns.index(col_name) if col_name in columns else 0

st.sidebar.markdown("### Column mapping")
query_col = st.sidebar.selectbox("queryStr column", columns, index=default_index("queryStr"))
ip_col = st.sidebar.selectbox("cliIP column", columns, index=default_index("cliIP"))
ts_col = st.sidebar.selectbox("reqTimeSec column", columns, index=default_index("reqTimeSec"))

status_col = st.sidebar.selectbox(
    "Optional statusCode column",
    ["(none)"] + columns,
    index=(["(none)"] + columns).index("statusCode") if "statusCode" in columns else 0,
)

path_col = st.sidebar.selectbox(
    "Optional reqPath column",
    ["(none)"] + columns,
    index=(["(none)"] + columns).index("reqPath") if "reqPath" in columns else 0,
)

st.sidebar.markdown("### Date range")

try:
    min_ts, max_ts = con.execute(
        f"""
        SELECT
            MIN(TRY_CAST("{ts_col}" AS BIGINT)) AS min_ts,
            MAX(TRY_CAST("{ts_col}" AS BIGINT)) AS max_ts
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST("{ts_col}" AS BIGINT) IS NOT NULL
        """,
        [file_paths],
    ).fetchone()
except Exception as e:
    st.error(f"Could not detect timestamp range: {e}")
    st.stop()

if min_ts is None or max_ts is None:
    st.error("No valid timestamp values found.")
    st.stop()

min_date = pd.to_datetime(int(min_ts), unit="s").date()
max_date = pd.to_datetime(int(max_ts), unit="s").date()

st.sidebar.caption(f"Available: {min_date} → {max_date}")

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

start_time = st.sidebar.time_input("Start time", value=time(0, 0, 0))
end_time = st.sidebar.time_input("End time", value=time(23, 59, 59))

start_epoch = int(datetime.combine(start_date, start_time).timestamp())
end_epoch = int(datetime.combine(end_date, end_time).timestamp())

st.sidebar.markdown("### Optional filters")
only_status_200 = False
if status_col != "(none)":
    only_status_200 = st.sidebar.checkbox("Only statusCode = 200", value=False)

run = st.sidebar.button("Run cliIP missing-ID check", type="primary")

if not run:
    st.stop()

status_filter = ""
if only_status_200 and status_col != "(none)":
    status_filter = f"""AND TRY_CAST("{status_col}" AS BIGINT) = 200"""

path_select = f'CAST("{path_col}" AS VARCHAR) AS reqPath,' if path_col != "(none)" else "NULL AS reqPath,"
status_select = f'TRY_CAST("{status_col}" AS BIGINT) AS statusCode,' if status_col != "(none)" else "NULL AS statusCode,"

query = f"""
WITH base AS (
    SELECT
        CAST("{ip_col}" AS VARCHAR) AS cliIP,
        CAST("{query_col}" AS VARCHAR) AS queryStr,
        TRY_CAST("{ts_col}" AS BIGINT) AS req_ts,
        {path_select}
        {status_select}
        regexp_extract(CAST("{query_col}" AS VARCHAR), '(?:^|&)device_id=([^&]+)', 1) AS raw_device_id,
        regexp_extract(CAST("{query_col}" AS VARCHAR), '(?:^|&)session_id=([^&]+)', 1) AS raw_session_id
    FROM read_parquet(?, union_by_name=true)
    WHERE TRY_CAST("{ts_col}" AS BIGINT) BETWEEN {start_epoch} AND {end_epoch}
      AND "{ip_col}" IS NOT NULL
      AND TRIM(CAST("{ip_col}" AS VARCHAR)) <> ''
      {status_filter}
),
clean AS (
    SELECT
        cliIP,
        queryStr,
        req_ts,
        reqPath,
        statusCode,
        NULLIF(TRIM(raw_device_id), '') AS device_id,
        NULLIF(TRIM(raw_session_id), '') AS session_id,
        CASE WHEN NULLIF(TRIM(raw_device_id), '') IS NOT NULL THEN 1 ELSE 0 END AS has_device_id,
        CASE WHEN NULLIF(TRIM(raw_session_id), '') IS NOT NULL THEN 1 ELSE 0 END AS has_session_id
    FROM base
),
ip_summary AS (
    SELECT
        cliIP,
        COUNT(*) AS rows,
        SUM(has_device_id) AS rows_with_device_id,
        SUM(has_session_id) AS rows_with_session_id,
        COUNT(DISTINCT device_id) AS distinct_device_id,
        COUNT(DISTINCT session_id) AS distinct_session_id,
        MIN(req_ts) AS first_req_ts,
        MAX(req_ts) AS last_req_ts
    FROM clean
    GROUP BY cliIP
),
missing_both_ip AS (
    SELECT *
    FROM ip_summary
    WHERE rows_with_device_id = 0
      AND rows_with_session_id = 0
),
overall AS (
    SELECT
        COUNT(*) AS raw_rows_with_cliip,
        COUNT(DISTINCT cliIP) AS distinct_cliip,
        SUM(CASE WHEN device_id IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_device_id,
        SUM(CASE WHEN session_id IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_session_id,
        COUNT(DISTINCT device_id) AS distinct_device_id,
        COUNT(DISTINCT session_id) AS distinct_session_id
    FROM clean
),
missing_overall AS (
    SELECT
        COUNT(*) AS cliip_missing_both_count,
        COALESCE(SUM(rows), 0) AS rows_on_cliip_missing_both
    FROM missing_both_ip
)
SELECT
    'overall' AS section,
    o.raw_rows_with_cliip,
    o.distinct_cliip,
    o.rows_with_device_id,
    o.rows_with_session_id,
    o.distinct_device_id,
    o.distinct_session_id,
    m.cliip_missing_both_count,
    m.rows_on_cliip_missing_both,
    ROUND(100.0 * m.cliip_missing_both_count / NULLIF(o.distinct_cliip, 0), 2) AS pct_cliip_missing_both,
    ROUND(100.0 * m.rows_on_cliip_missing_both / NULLIF(o.raw_rows_with_cliip, 0), 2) AS pct_rows_on_cliip_missing_both
FROM overall o
CROSS JOIN missing_overall m
"""

with st.spinner("Checking cliIP groups..."):
    overall_df = con.execute(query, [file_paths]).df()

if overall_df.empty:
    st.warning("No data found for this range.")
    st.stop()

row = overall_df.iloc[0].to_dict()

st.subheader("Overall result")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Raw rows with cliIP", f"{int(row['raw_rows_with_cliip']):,}")
k2.metric("Distinct cliIP", f"{int(row['distinct_cliip']):,}")
k3.metric("Rows with device_id", f"{int(row['rows_with_device_id']):,}")
k4.metric("Rows with session_id", f"{int(row['rows_with_session_id']):,}")

k5, k6, k7, k8 = st.columns(4)
k5.metric("Distinct device_id", f"{int(row['distinct_device_id']):,}")
k6.metric("Distinct session_id", f"{int(row['distinct_session_id']):,}")
k7.metric("cliIP with no device/session anywhere", f"{int(row['cliip_missing_both_count']):,}")
k8.metric("Rows on those cliIP", f"{int(row['rows_on_cliip_missing_both']):,}")

st.markdown("### Key percentages")
p1, p2 = st.columns(2)
p1.metric("% cliIP missing both IDs", f"{float(row['pct_cliip_missing_both']):,.2f}%")
p2.metric("% rows on cliIP missing both IDs", f"{float(row['pct_rows_on_cliip_missing_both']):,.2f}%")

st.info("""
Interpretation:
- If `% rows on cliIP missing both IDs` is high, many requests can only be identified by IP/UA/path-style logic.
- If it is low, most traffic has at least some ID evidence somewhere on the same cliIP.
""")

detail_query = f"""
WITH base AS (
    SELECT
        CAST("{ip_col}" AS VARCHAR) AS cliIP,
        CAST("{query_col}" AS VARCHAR) AS queryStr,
        TRY_CAST("{ts_col}" AS BIGINT) AS req_ts,
        {path_select}
        {status_select}
        regexp_extract(CAST("{query_col}" AS VARCHAR), '(?:^|&)device_id=([^&]+)', 1) AS raw_device_id,
        regexp_extract(CAST("{query_col}" AS VARCHAR), '(?:^|&)session_id=([^&]+)', 1) AS raw_session_id
    FROM read_parquet(?, union_by_name=true)
    WHERE TRY_CAST("{ts_col}" AS BIGINT) BETWEEN {start_epoch} AND {end_epoch}
      AND "{ip_col}" IS NOT NULL
      AND TRIM(CAST("{ip_col}" AS VARCHAR)) <> ''
      {status_filter}
),
clean AS (
    SELECT
        cliIP,
        req_ts,
        reqPath,
        statusCode,
        NULLIF(TRIM(raw_device_id), '') AS device_id,
        NULLIF(TRIM(raw_session_id), '') AS session_id,
        CASE WHEN NULLIF(TRIM(raw_device_id), '') IS NOT NULL THEN 1 ELSE 0 END AS has_device_id,
        CASE WHEN NULLIF(TRIM(raw_session_id), '') IS NOT NULL THEN 1 ELSE 0 END AS has_session_id
    FROM base
),
ip_summary AS (
    SELECT
        cliIP,
        COUNT(*) AS rows,
        SUM(has_device_id) AS rows_with_device_id,
        SUM(has_session_id) AS rows_with_session_id,
        COUNT(DISTINCT device_id) AS distinct_device_id,
        COUNT(DISTINCT session_id) AS distinct_session_id,
        MIN(req_ts) AS first_req_ts,
        MAX(req_ts) AS last_req_ts
    FROM clean
    GROUP BY cliIP
)
SELECT *
FROM ip_summary
WHERE rows_with_device_id = 0
  AND rows_with_session_id = 0
ORDER BY rows DESC
"""

with st.spinner("Loading cliIP list with no device/session anywhere..."):
    missing_df = con.execute(detail_query, [file_paths]).df()

if not missing_df.empty:
    missing_df["first_seen"] = pd.to_datetime(missing_df["first_req_ts"], unit="s", errors="coerce")
    missing_df["last_seen"] = pd.to_datetime(missing_df["last_req_ts"], unit="s", errors="coerce")
    missing_df = missing_df.drop(columns=["first_req_ts", "last_req_ts"])

st.subheader("cliIP values where no row has device_id or session_id")
st.dataframe(missing_df.head(1000), use_container_width=True, hide_index=True, height=420)

st.download_button(
    "Download full missing-both cliIP list CSV",
    data=missing_df.to_csv(index=False).encode("utf-8"),
    file_name="cliip_with_no_device_or_session_anywhere.csv",
    mime="text/csv",
)

st.markdown("### Distribution of missing-both cliIP by row volume")

if not missing_df.empty:
    bins = [0, 1, 5, 20, 100, 1000, 10000, float("inf")]
    labels = ["1", "2-5", "6-20", "21-100", "101-1K", "1K-10K", "10K+"]
    dist = pd.cut(missing_df["rows"], bins=bins, labels=labels).value_counts().sort_index().reset_index()
    dist.columns = ["Rows per cliIP", "cliIP count"]
    st.dataframe(dist, use_container_width=True, hide_index=True)
else:
    st.success("No cliIP values found where all rows are missing both device_id and session_id.")

report = f"""cliIP Missing Device/Session Check
Date range: {start_date} {start_time} to {end_date} {end_time}
Files: {len(file_paths)}

Raw rows with cliIP: {int(row['raw_rows_with_cliip']):,}
Distinct cliIP: {int(row['distinct_cliip']):,}
Rows with device_id: {int(row['rows_with_device_id']):,}
Rows with session_id: {int(row['rows_with_session_id']):,}
Distinct device_id: {int(row['distinct_device_id']):,}
Distinct session_id: {int(row['distinct_session_id']):,}

cliIP with no device_id/session_id anywhere: {int(row['cliip_missing_both_count']):,}
Rows on those cliIP: {int(row['rows_on_cliip_missing_both']):,}
% cliIP missing both IDs: {float(row['pct_cliip_missing_both']):.2f}%
% rows on cliIP missing both IDs: {float(row['pct_rows_on_cliip_missing_both']):.2f}%
"""

st.download_button(
    "Download paste-back TXT report",
    data=report.encode("utf-8"),
    file_name="cliip_missing_ids_report.txt",
    mime="text/plain",
)
