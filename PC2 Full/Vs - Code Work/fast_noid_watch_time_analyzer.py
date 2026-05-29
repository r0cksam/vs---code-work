import os
from pathlib import Path
from datetime import datetime, time as dtime
from typing import List

import duckdb
import pandas as pd
import streamlit as st


APP_TITLE = "Fast No-ID Watch Time Analyzer"

st.set_page_config(page_title=APP_TITLE, page_icon="⚡", layout="wide")

st.markdown("""
<style>
.block-container { padding-top: 1rem; }
div[data-testid="stMetric"] {
    background: rgba(128,128,128,0.08);
    padding: 0.75rem;
    border-radius: 0.8rem;
    border: 1px solid rgba(128,128,128,0.18);
}
</style>
""", unsafe_allow_html=True)


def _dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    return repr([str(Path(f)).replace("\\", "/") for f in files])


@st.cache_data(show_spinner=False)
def scan_parquet_files(root_folder: str, recursive: bool = True) -> List[str]:
    root = Path(root_folder).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    if recursive:
        return sorted(str(p) for p in root.rglob("*.parquet"))
    return sorted(str(p) for p in root.glob("*.parquet"))


@st.cache_data(show_spinner=False)
def get_columns(files: List[str]) -> List[str]:
    con = duckdb.connect()
    df = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 1",
        [files],
    ).df()
    return df["column_name"].tolist()


@st.cache_data(show_spinner=False)
def get_epoch_range(files: List[str], req_time_col: str):
    con = duckdb.connect()
    qts = _dq(req_time_col)
    row = con.execute(
        f"""
        SELECT
            MIN(TRY_CAST({qts} AS BIGINT)) AS min_ts,
            MAX(TRY_CAST({qts} AS BIGINT)) AS max_ts
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) IS NOT NULL
        """,
        [files],
    ).fetchone()
    return row[0], row[1]


def epoch_to_date(ts):
    return pd.to_datetime(int(ts), unit="s").date()


def dt_to_epoch(d, t):
    return int(datetime.combine(d, t).timestamp())


def stream_key_sql(path_col: str) -> str:
    p = _dq(path_col)
    return f"""
    COALESCE(
        NULLIF(regexp_extract(CAST({p} AS VARCHAR), '(vglive-sk-[0-9]+)', 1), ''),
        NULLIF(regexp_extract(CAST({p} AS VARCHAR), '^([^/]+)/', 1), ''),
        NULLIF(CAST({p} AS VARCHAR), ''),
        '__unknown_stream__'
    )
    """


@st.cache_data(show_spinner=False)
def fast_noid_watch_time(
    files: List[str],
    req_time_col: str,
    query_col: str,
    cliip_col: str,
    ua_col: str,
    path_col: str,
    start_epoch: int,
    end_epoch: int,
    status_col: str = "",
    only_200: bool = False,
):
    """Fast watch-time estimator.

    Current rule:
      - no max-gap cap
      - no 30-minute inactivity split

    Therefore, for each timeline:
      SUM(next_ts - current_ts) = MAX(req_ts) - MIN(req_ts)

    This avoids LEAD()/sorting every event.
    """

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    try:
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    parquet_sql = _path_list_sql(files)
    ts = _dq(req_time_col)
    qs = _dq(query_col)
    ip = _dq(cliip_col)
    ua = _dq(ua_col)
    stream_expr = stream_key_sql(path_col)

    status_filter = ""
    if only_200 and status_col:
        status_filter = f"AND TRY_CAST({_dq(status_col)} AS BIGINT) = 200"

    create_sql = f"""
    CREATE TEMP TABLE timeline_events AS
    WITH raw AS (
        SELECT
            TRY_CAST({ts} AS BIGINT) AS req_ts,
            CAST({qs} AS VARCHAR) AS queryStr,
            NULLIF(TRIM(CAST({ip} AS VARCHAR)), '') AS cliIP,
            NULLIF(TRIM(CAST({ua} AS VARCHAR)), '') AS UA,
            {stream_expr} AS stream_key,
            regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])device_id=([^&]*)', 1) AS raw_device_id,
            regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])session_id=([^&]*)', 1) AS raw_session_id
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
          {status_filter}
    ),
    clean AS (
        SELECT
            req_ts,
            CAST(to_timestamp(req_ts) AS DATE) AS event_date,
            cliIP,
            UA,
            stream_key,
            NULLIF(TRIM(raw_device_id), '') AS device_id,
            NULLIF(TRIM(raw_session_id), '') AS session_id
        FROM raw
    ),
    prepared AS (
        SELECT
            req_ts,
            event_date,
            cliIP,
            UA,
            stream_key,
            device_id,
            session_id,

            CASE
                WHEN device_id IS NOT NULL OR session_id IS NOT NULL THEN 'id_sample'
                ELSE 'no_id_estimated'
            END AS row_group,

            CASE
                WHEN cliIP IS NOT NULL AND UA IS NOT NULL THEN cliIP || '|' || UA
                WHEN cliIP IS NOT NULL THEN cliIP
                WHEN UA IS NOT NULL THEN UA
                ELSE NULL
            END AS cliip_ua_viewer,

            CASE
                WHEN device_id IS NOT NULL THEN 'device:' || device_id
                ELSE NULL
            END AS device_viewer,

            CASE
                WHEN device_id IS NOT NULL AND session_id IS NOT NULL
                    THEN 'id|device:' || device_id || '|sid:' || session_id || '|stream:' || stream_key
                WHEN device_id IS NOT NULL
                    THEN 'id|device:' || device_id || '|no_sid|stream:' || stream_key
                WHEN session_id IS NOT NULL AND cliIP IS NOT NULL AND UA IS NOT NULL
                    THEN 'id|sid:' || session_id || '|viewer:' || cliIP || '|' || UA || '|stream:' || stream_key
                WHEN cliIP IS NOT NULL AND UA IS NOT NULL
                    THEN 'noid|' || cliIP || '|' || UA || '|stream:' || stream_key
                WHEN cliIP IS NOT NULL
                    THEN 'noid|' || cliIP || '|stream:' || stream_key
                WHEN UA IS NOT NULL
                    THEN 'noid|' || UA || '|stream:' || stream_key
                ELSE NULL
            END AS timeline_key
        FROM clean
    )
    SELECT DISTINCT
        timeline_key,
        row_group,
        cliip_ua_viewer,
        device_viewer,
        device_id,
        session_id,
        cliIP,
        UA,
        stream_key,
        event_date,
        req_ts
    FROM prepared
    WHERE timeline_key IS NOT NULL
      AND req_ts IS NOT NULL
    """
    con.execute(create_sql)

    con.execute("""
    CREATE TEMP TABLE timeline_duration AS
    SELECT
        timeline_key,
        row_group,
        ANY_VALUE(cliip_ua_viewer) AS cliip_ua_viewer,
        ANY_VALUE(device_viewer) AS device_viewer,
        ANY_VALUE(stream_key) AS stream_key,
        MIN(req_ts) AS first_ts,
        MAX(req_ts) AS last_ts,
        COUNT(*) AS timeline_events,
        GREATEST(MAX(req_ts) - MIN(req_ts), 0) AS watch_sec
    FROM timeline_events
    GROUP BY timeline_key, row_group
    """)

    con.execute("""
    CREATE TEMP TABLE daily_timeline_duration AS
    SELECT
        event_date,
        timeline_key,
        row_group,
        ANY_VALUE(cliip_ua_viewer) AS cliip_ua_viewer,
        ANY_VALUE(device_viewer) AS device_viewer,
        ANY_VALUE(stream_key) AS stream_key,
        COUNT(*) AS timeline_events,
        GREATEST(MAX(req_ts) - MIN(req_ts), 0) AS watch_sec
    FROM timeline_events
    GROUP BY event_date, timeline_key, row_group
    """)

    summary_query = """
    SELECT
        (SELECT COUNT(*) FROM timeline_events) AS rows_used_after_dedup,
        (SELECT COUNT(DISTINCT cliip_ua_viewer) FROM timeline_events WHERE cliip_ua_viewer IS NOT NULL) AS distinct_cliip_ua_viewer,
        (SELECT COUNT(DISTINCT device_id) FROM timeline_events WHERE device_id IS NOT NULL) AS distinct_device_id,
        (SELECT COUNT(DISTINCT session_id) FROM timeline_events WHERE session_id IS NOT NULL) AS distinct_session_id,
        (SELECT COUNT(DISTINCT cliIP) FROM timeline_events WHERE cliIP IS NOT NULL) AS distinct_cliip,
        (SELECT COUNT(DISTINCT stream_key) FROM timeline_events) AS distinct_stream_key,

        (SELECT COUNT(*) FROM timeline_duration) AS timeline_count,
        (SELECT COUNT(*) FROM timeline_duration WHERE row_group = 'id_sample') AS id_sample_timeline_count,
        (SELECT COUNT(*) FROM timeline_duration WHERE row_group = 'no_id_estimated') AS no_id_timeline_count,

        (SELECT COUNT(*) FROM timeline_events WHERE device_id IS NOT NULL) AS rows_with_device_id,
        (SELECT COUNT(*) FROM timeline_events WHERE device_id IS NULL) AS rows_without_device_id,

        COALESCE((SELECT SUM(watch_sec) / 3600.0 FROM timeline_duration), 0) AS total_watch_hours,
        COALESCE((SELECT SUM(watch_sec) / 3600.0 FROM timeline_duration WHERE row_group = 'id_sample'), 0) AS id_sample_watch_hours,
        COALESCE((SELECT SUM(watch_sec) / 3600.0 FROM timeline_duration WHERE row_group = 'no_id_estimated'), 0) AS no_id_estimated_watch_hours,

        COALESCE((SELECT SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0) FROM timeline_duration), 0) AS estimated_watch_min_per_cliip_ua_viewer,
        COALESCE((SELECT SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT device_viewer), 0) FROM timeline_duration WHERE row_group = 'id_sample'), 0) AS device_id_sample_watch_min_per_device,
        COALESCE((SELECT SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0) FROM timeline_duration WHERE row_group = 'no_id_estimated'), 0) AS no_id_watch_min_per_cliip_ua_viewer,

        COALESCE((SELECT AVG(watch_sec) FROM timeline_duration), 0) AS avg_timeline_watch_sec,
        COALESCE((SELECT MAX(watch_sec) FROM timeline_duration), 0) AS max_timeline_watch_sec
    """
    summary_row = con.execute(summary_query).fetchone()

    summary_cols = [
        "Rows Used After Dedup",
        "Distinct cliIP+UA Viewer",
        "Distinct device_id",
        "Distinct session_id",
        "Distinct cliIP",
        "Distinct stream_key",
        "Timeline Count",
        "ID Sample Timeline Count",
        "No-ID Timeline Count",
        "Rows with device_id",
        "Rows without device_id",
        "Total Watch Hours",
        "Device/Session Sample Watch Hours",
        "No-ID Estimated Watch Hours",
        "Estimated Watch Min / cliIP+UA Viewer",
        "Device_ID Sample Watch Min / Device",
        "No-ID Watch Min / cliIP+UA Viewer",
        "Average Timeline Watch Seconds",
        "Max Timeline Watch Seconds",
    ]
    summary_df = pd.DataFrame([dict(zip(summary_cols, summary_row))])

    daily_df = con.execute("""
    SELECT
        event_date AS Date,
        SUM(timeline_events) AS "Rows Used After Dedup",
        COUNT(DISTINCT cliip_ua_viewer) AS "Distinct cliIP+UA Viewer",
        COUNT(DISTINCT device_viewer) AS "Distinct Device_ID Sample",
        COUNT(DISTINCT stream_key) AS "Distinct stream_key",
        COUNT(*) AS "Timeline Count",
        ROUND(SUM(watch_sec) / 3600.0, 4) AS "Total Watch Hours",
        ROUND(SUM(CASE WHEN row_group = 'id_sample' THEN watch_sec ELSE 0 END) / 3600.0, 4) AS "Device/Session Sample Watch Hours",
        ROUND(SUM(CASE WHEN row_group = 'no_id_estimated' THEN watch_sec ELSE 0 END) / 3600.0, 4) AS "No-ID Estimated Watch Hours",
        ROUND(SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0), 2) AS "Estimated Watch Min / cliIP+UA Viewer"
    FROM daily_timeline_duration
    GROUP BY 1
    ORDER BY 1
    """).df()

    top_df = con.execute("""
    SELECT
        row_group,
        timeline_key,
        timeline_events AS rows,
        ROUND(watch_sec / 3600.0, 4) AS watch_hours,
        stream_key,
        cliip_ua_viewer,
        device_viewer,
        to_timestamp(first_ts) AS first_seen,
        to_timestamp(last_ts) AS last_seen
    FROM timeline_duration
    ORDER BY watch_sec DESC
    LIMIT 200
    """).df()

    return summary_df, daily_df, top_df


st.title("⚡ Fast No-ID Watch Time Analyzer")
st.caption("Optimized for your current rule: no max-gap cap and no 30-minute inactivity split.")

with st.sidebar:
    st.header("Input")
    folder = st.text_input("Parquet folder", placeholder=r"D:\Veto Logs Backup")
    recursive = st.checkbox("Scan recursively", value=True)

if not folder:
    st.info("Enter a parquet folder in the sidebar.")
    st.stop()

files = scan_parquet_files(folder, recursive=recursive)
if not files:
    st.error("No parquet files found.")
    st.stop()

st.sidebar.success(f"{len(files):,} parquet files found")

columns = get_columns(files)
if not columns:
    st.error("Could not read columns.")
    st.stop()

def idx(name):
    return columns.index(name) if name in columns else 0

with st.sidebar:
    st.header("Column mapping")
    req_time_col = st.selectbox("Timestamp column", columns, index=idx("reqTimeSec"))
    query_col = st.selectbox("Query string column", columns, index=idx("queryStr"))
    cliip_col = st.selectbox("cliIP column", columns, index=idx("cliIP"))
    ua_col = st.selectbox("UA column", columns, index=idx("UA"))
    path_col = st.selectbox("reqPath column", columns, index=idx("reqPath"))

    status_options = ["(none)"] + columns
    status_col = st.selectbox("Optional statusCode column", status_options, index=status_options.index("statusCode") if "statusCode" in columns else 0)
    only_200 = st.checkbox("Only statusCode = 200", value=False) if status_col != "(none)" else False

min_ts, max_ts = get_epoch_range(files, req_time_col)
if min_ts is None or max_ts is None:
    st.error("No valid timestamp range found.")
    st.stop()

min_date = epoch_to_date(min_ts)
max_date = epoch_to_date(max_ts)

st.subheader("Available date range")
c1, c2, c3 = st.columns(3)
c1.metric("Available From", str(pd.to_datetime(int(min_ts), unit="s")))
c2.metric("Available To", str(pd.to_datetime(int(max_ts), unit="s")))
c3.metric("Files", f"{len(files):,}")

with st.sidebar:
    st.header("Date / time range")
    dr = st.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(dr, tuple) and len(dr) == 2:
        start_d, end_d = dr
    else:
        start_d, end_d = min_date, max_date

    start_t = st.time_input("Start time", dtime(0, 0, 0))
    end_t = st.time_input("End time", dtime(23, 59, 59))

start_epoch = dt_to_epoch(start_d, start_t)
end_epoch = dt_to_epoch(end_d, end_t)

st.markdown("### Fast logic used")
st.code(
"""No cap and no inactivity break:
  For each timeline:
    watch_seconds = MAX(reqTimeSec) - MIN(reqTimeSec)

Rows with device/session:
  timeline = device_id + session_id + stream_key

Rows without device/session:
  timeline = cliIP + UA + stream_key

This avoids slow LEAD()/sorting over every row.""",
    language="text",
)

if st.button("Run fast watch-time calculation", type="primary"):
    with st.spinner("Building compact timeline table and calculating watch time..."):
        summary_df, daily_df, top_df = fast_noid_watch_time(
            files=files,
            req_time_col=req_time_col,
            query_col=query_col,
            cliip_col=cliip_col,
            ua_col=ua_col,
            path_col=path_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            status_col="" if status_col == "(none)" else status_col,
            only_200=only_200,
        )

    s = summary_df.iloc[0].to_dict()

    st.subheader("Main summary")
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Rows Used After Dedup", f"{int(s['Rows Used After Dedup'] or 0):,}")
    a2.metric("Distinct cliIP+UA Viewer", f"{int(s['Distinct cliIP+UA Viewer'] or 0):,}")
    a3.metric("Distinct device_id", f"{int(s['Distinct device_id'] or 0):,}")
    a4.metric("Timeline Count", f"{int(s['Timeline Count'] or 0):,}")

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Total Watch Hours", f"{float(s['Total Watch Hours'] or 0):,.2f}")
    b2.metric("Device/Session Sample Watch Hours", f"{float(s['Device/Session Sample Watch Hours'] or 0):,.2f}")
    b3.metric("No-ID Estimated Watch Hours", f"{float(s['No-ID Estimated Watch Hours'] or 0):,.2f}")
    b4.metric("Estimated Watch Min / cliIP+UA Viewer", f"{float(s['Estimated Watch Min / cliIP+UA Viewer'] or 0):,.2f}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Device_ID Sample Watch Min / Device", f"{float(s['Device_ID Sample Watch Min / Device'] or 0):,.2f}")
    c2.metric("No-ID Watch Min / cliIP+UA Viewer", f"{float(s['No-ID Watch Min / cliIP+UA Viewer'] or 0):,.2f}")
    c3.metric("Avg Timeline Watch Sec", f"{float(s['Average Timeline Watch Seconds'] or 0):,.2f}")
    c4.metric("Max Timeline Watch Sec", f"{float(s['Max Timeline Watch Seconds'] or 0):,.0f}")

    st.subheader("Full summary table")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.subheader("Daily summary")
    st.dataframe(daily_df, use_container_width=True, hide_index=True, height=420)
    st.download_button(
        "Download daily summary CSV",
        data=daily_df.to_csv(index=False).encode("utf-8"),
        file_name="fast_daily_watch_summary.csv",
        mime="text/csv",
    )

    st.subheader("Top timelines by watch hours")
    st.caption("Inspect these to catch overcounting from very long continuous no-ID timelines.")
    st.dataframe(top_df, use_container_width=True, hide_index=True, height=420)
    st.download_button(
        "Download top timelines CSV",
        data=top_df.to_csv(index=False).encode("utf-8"),
        file_name="fast_top_watch_timelines.csv",
        mime="text/csv",
    )

    report = summary_df.to_string(index=False) + "\n\nDaily summary:\n" + daily_df.to_string(index=False)
    st.download_button(
        "Download paste-back TXT report",
        data=report.encode("utf-8"),
        file_name="fast_noid_watch_time_report.txt",
        mime="text/plain",
    )
