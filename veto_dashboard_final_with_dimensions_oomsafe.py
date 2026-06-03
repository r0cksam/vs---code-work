import os
import re
import urllib.parse
from pathlib import Path
from datetime import datetime, time as dtime
from typing import List, Dict, Tuple

import duckdb
import pandas as pd
import streamlit as st


APP_TITLE = "Veto Dashboard"

st.set_page_config(page_title=APP_TITLE, page_icon="📺", layout="wide")

st.markdown(
    """
    <style>
        .block-container { padding-top: 1rem; }
        div[data-testid="stMetric"] {
            background: rgba(128,128,128,0.08);
            padding: 0.75rem;
            border-radius: 0.8rem;
            border: 1px solid rgba(128,128,128,0.18);
        }
        .small-note { color: #777; font-size: 0.88rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    return repr([str(Path(f)).replace("\\", "/") for f in files])


def _sql_str(value: str) -> str:
    """Escape string literal for DuckDB SQL."""
    return str(value).replace("'", "''")


def configure_duckdb(
    con,
    temp_dir: str,
    memory_limit: str = "8GB",
    threads: int = 2,
    max_temp_size: str = "200GiB",
):
    """Apply safer DuckDB settings for large parquet scans.

    Important for 25GB+ data:
    - temp_directory should be a LOCAL drive with enough free space, not network Z:\ \n    - preserve_insertion_order=false reduces memory pressure
    - fewer threads can reduce memory use
    """
    temp = Path(temp_dir).expanduser().resolve()
    temp.mkdir(parents=True, exist_ok=True)
    temp_sql = _sql_str(str(temp).replace("\\", "/"))

    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET memory_limit='{_sql_str(memory_limit)}'")
    con.execute(f"SET temp_directory='{temp_sql}'")
    con.execute(f"SET max_temp_directory_size='{_sql_str(max_temp_size)}'")
    try:
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass


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
    if not files:
        return []
    con = duckdb.connect()
    df = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 1",
        [files],
    ).df()
    return df["column_name"].tolist()


@st.cache_data(show_spinner=False)
def get_epoch_range(files: List[str], req_time_col: str):
    if not files:
        return None, None
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


def status_filter_sql(status_col: str, only_200: bool) -> str:
    if only_200 and status_col:
        return f"AND TRY_CAST({_dq(status_col)} AS BIGINT) = 200"
    return ""


def build_timeline_events_sql(
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
) -> str:
    """Create a compact timeline_events temp table.

    Timeline logic:
      ID rows:
        device_id + session_id + stream_key where IDs are available.
      No-ID rows:
        cliIP + UA + stream_key.

    stream_key is derived from reqPath:
      vglive-sk-xxxxx if present, else first folder, else full reqPath.
    """
    parquet_sql = _path_list_sql(files)
    ts = _dq(req_time_col)
    qs = _dq(query_col)
    ip = _dq(cliip_col)
    ua = _dq(ua_col)
    stream_expr = stream_key_sql(path_col)
    sf = status_filter_sql(status_col, only_200)

    return f"""
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
          {sf}
    ),
    clean AS (
        SELECT
            req_ts,
            CAST(to_timestamp(req_ts) AS DATE) AS event_date,
            cliIP,
            UA,
            stream_key,
            NULLIF(TRIM(raw_device_id), '') AS device_id,
            NULLIF(TRIM(raw_session_id), '') AS session_id,
            CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|&)device_id=') THEN 1 ELSE 0 END AS has_device_id_key,
            CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|&)session_id=') THEN 1 ELSE 0 END AS has_session_id_key
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
            has_device_id_key,
            has_session_id_key,

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
        has_device_id_key,
        has_session_id_key,
        cliIP,
        UA,
        stream_key,
        event_date,
        req_ts
    FROM prepared
    WHERE timeline_key IS NOT NULL
      AND req_ts IS NOT NULL
    """


# ─────────────────────────────────────────────
# Watch-time calculation
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def calculate_watch_time(
    files: List[str],
    req_time_col: str,
    query_col: str,
    cliip_col: str,
    ua_col: str,
    path_col: str,
    start_epoch: int,
    end_epoch: int,
    watch_mode: str,
    cap_seconds: int,
    status_col: str = "",
    only_200: bool = False,
    duckdb_temp_dir: str = "duckdb_temp",
    duckdb_memory_limit: str = "8GB",
    duckdb_threads: int = 2,
    duckdb_max_temp_size: str = "200GiB",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Calculate watch time.

    Modes:
      No cap / fast:
        watch_sec = MAX(req_ts) - MIN(req_ts) per timeline.
        This is valid when there is no max-gap cap and no inactivity split.

      Capped:
        watch_sec = MIN(next_req_ts - current_req_ts, cap_seconds)
        inside each timeline.
    """
    con = duckdb.connect(database=":memory:")
    configure_duckdb(
        con,
        temp_dir=duckdb_temp_dir,
        memory_limit=duckdb_memory_limit,
        threads=duckdb_threads,
        max_temp_size=duckdb_max_temp_size,
    )

    con.execute(
        build_timeline_events_sql(
            files, req_time_col, query_col, cliip_col, ua_col, path_col,
            start_epoch, end_epoch, status_col, only_200
        )
    )

    if watch_mode == "No cap / fast":
        con.execute("""
        CREATE TEMP TABLE watch_rows AS
        SELECT
            timeline_key,
            row_group,
            ANY_VALUE(cliip_ua_viewer) AS cliip_ua_viewer,
            ANY_VALUE(device_viewer) AS device_viewer,
            ANY_VALUE(stream_key) AS stream_key,
            MIN(req_ts) AS first_ts,
            MAX(req_ts) AS last_ts,
            COUNT(*) AS event_rows,
            GREATEST(MAX(req_ts) - MIN(req_ts), 0) AS watch_sec
        FROM timeline_events
        GROUP BY timeline_key, row_group
        """)

        con.execute("""
        CREATE TEMP TABLE daily_watch_rows AS
        SELECT
            event_date,
            timeline_key,
            row_group,
            ANY_VALUE(cliip_ua_viewer) AS cliip_ua_viewer,
            ANY_VALUE(device_viewer) AS device_viewer,
            ANY_VALUE(stream_key) AS stream_key,
            COUNT(*) AS event_rows,
            GREATEST(MAX(req_ts) - MIN(req_ts), 0) AS watch_sec
        FROM timeline_events
        GROUP BY event_date, timeline_key, row_group
        """)
    else:
        cap = int(cap_seconds)
        con.execute(f"""
        CREATE TEMP TABLE sequenced AS
        SELECT
            *,
            LEAD(req_ts) OVER (
                PARTITION BY timeline_key
                ORDER BY req_ts
            ) AS next_ts
        FROM timeline_events
        """)

        con.execute(f"""
        CREATE TEMP TABLE watch_rows AS
        SELECT
            timeline_key,
            row_group,
            cliip_ua_viewer,
            device_viewer,
            stream_key,
            req_ts AS first_ts,
            next_ts AS last_ts,
            1 AS event_rows,
            LEAST(GREATEST(next_ts - req_ts, 0), {cap}) AS watch_sec
        FROM sequenced
        WHERE next_ts IS NOT NULL
        """)

        con.execute(f"""
        CREATE TEMP TABLE daily_watch_rows AS
        SELECT
            event_date,
            timeline_key,
            row_group,
            cliip_ua_viewer,
            device_viewer,
            stream_key,
            1 AS event_rows,
            LEAST(GREATEST(next_ts - req_ts, 0), {cap}) AS watch_sec
        FROM sequenced
        WHERE next_ts IS NOT NULL
        """)

    summary_query = """
    SELECT
        (SELECT COUNT(*) FROM timeline_events) AS rows_used_after_dedup,
        (SELECT COUNT(DISTINCT cliip_ua_viewer) FROM timeline_events WHERE cliip_ua_viewer IS NOT NULL) AS distinct_cliip_ua_viewer,
        (SELECT COUNT(DISTINCT device_id) FROM timeline_events WHERE device_id IS NOT NULL) AS distinct_device_id,
        (SELECT COUNT(DISTINCT session_id) FROM timeline_events WHERE session_id IS NOT NULL) AS distinct_session_id,
        (SELECT COUNT(DISTINCT cliIP) FROM timeline_events WHERE cliIP IS NOT NULL) AS distinct_cliip,
        (SELECT COUNT(DISTINCT UA) FROM timeline_events WHERE UA IS NOT NULL) AS distinct_ua,
        (SELECT COUNT(DISTINCT stream_key) FROM timeline_events) AS distinct_stream_key,

        (SELECT COUNT(DISTINCT timeline_key) FROM timeline_events) AS timeline_count,
        (SELECT COUNT(DISTINCT timeline_key) FROM timeline_events WHERE row_group = 'id_sample') AS id_sample_timeline_count,
        (SELECT COUNT(DISTINCT timeline_key) FROM timeline_events WHERE row_group = 'no_id_estimated') AS no_id_timeline_count,

        (SELECT COUNT(*) FROM timeline_events WHERE device_id IS NOT NULL) AS rows_with_device_id,
        (SELECT COUNT(*) FROM timeline_events WHERE device_id IS NULL) AS rows_without_device_id,
        (SELECT COUNT(*) FROM timeline_events WHERE session_id IS NOT NULL) AS rows_with_session_id,
        (SELECT COUNT(*) FROM timeline_events WHERE session_id IS NULL) AS rows_without_session_id,
        (SELECT COUNT(*) FROM timeline_events WHERE device_id IS NOT NULL AND session_id IS NOT NULL) AS rows_with_both_device_session,
        (SELECT COUNT(*) FROM timeline_events WHERE has_device_id_key = 1 AND device_id IS NULL) AS rows_device_id_key_blank,
        (SELECT COUNT(*) FROM timeline_events WHERE has_session_id_key = 1 AND session_id IS NULL) AS rows_session_id_key_blank,

        COALESCE((SELECT SUM(watch_sec) / 3600.0 FROM watch_rows), 0) AS total_watch_hours,
        COALESCE((SELECT SUM(watch_sec) / 3600.0 FROM watch_rows WHERE row_group = 'id_sample'), 0) AS id_sample_watch_hours,
        COALESCE((SELECT SUM(watch_sec) / 3600.0 FROM watch_rows WHERE row_group = 'no_id_estimated'), 0) AS no_id_estimated_watch_hours,

        COALESCE((SELECT SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0) FROM watch_rows), 0) AS estimated_watch_min_per_cliip_ua_viewer,
        COALESCE((SELECT SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT device_viewer), 0) FROM watch_rows WHERE row_group = 'id_sample'), 0) AS device_id_sample_watch_min_per_device,
        COALESCE((SELECT SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0) FROM watch_rows WHERE row_group = 'no_id_estimated'), 0) AS no_id_watch_min_per_cliip_ua_viewer,

        COALESCE((SELECT AVG(watch_sec) FROM watch_rows), 0) AS avg_gap_or_timeline_sec,
        COALESCE((SELECT MAX(watch_sec) FROM watch_rows), 0) AS max_gap_or_timeline_sec
    """

    summary_row = con.execute(summary_query).fetchone()
    summary_cols = [
        "Rows Used After Dedup",
        "Distinct cliIP+UA Viewer",
        "Distinct device_id",
        "Distinct session_id",
        "Distinct cliIP",
        "Distinct UA",
        "Distinct stream_key",
        "Timeline Count",
        "ID Sample Timeline Count",
        "No-ID Timeline Count",
        "Rows with device_id",
        "Rows without device_id",
        "Rows with session_id",
        "Rows without session_id",
        "Rows with both device_id + session_id",
        "Rows where device_id= is blank",
        "Rows where session_id= is blank",
        "Total Watch Hours",
        "Device/Session Sample Watch Hours",
        "No-ID Estimated Watch Hours",
        "Estimated Watch Min / cliIP+UA Viewer",
        "Device_ID Sample Watch Min / Device",
        "No-ID Watch Min / cliIP+UA Viewer",
        "Average Seconds Used",
        "Max Seconds Used",
    ]
    summary_df = pd.DataFrame([dict(zip(summary_cols, summary_row))])

    daily_df = con.execute("""
    SELECT
        event_date AS Date,
        SUM(event_rows) AS "Rows Used",
        COUNT(DISTINCT cliip_ua_viewer) AS "Distinct cliIP+UA Viewer",
        COUNT(DISTINCT device_viewer) AS "Distinct Device_ID Sample",
        COUNT(DISTINCT stream_key) AS "Distinct stream_key",
        COUNT(DISTINCT timeline_key) AS "Timeline Count",
        ROUND(SUM(watch_sec) / 3600.0, 4) AS "Total Watch Hours",
        ROUND(SUM(CASE WHEN row_group = 'id_sample' THEN watch_sec ELSE 0 END) / 3600.0, 4) AS "Device/Session Sample Watch Hours",
        ROUND(SUM(CASE WHEN row_group = 'no_id_estimated' THEN watch_sec ELSE 0 END) / 3600.0, 4) AS "No-ID Estimated Watch Hours",
        ROUND(SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0), 2) AS "Estimated Watch Min / cliIP+UA Viewer"
    FROM daily_watch_rows
    GROUP BY 1
    ORDER BY 1
    """).df()

    top_df = con.execute("""
    SELECT
        row_group,
        timeline_key,
        SUM(event_rows) AS rows,
        ROUND(SUM(watch_sec) / 3600.0, 4) AS watch_hours,
        ANY_VALUE(stream_key) AS stream_key,
        ANY_VALUE(cliip_ua_viewer) AS cliip_ua_viewer,
        ANY_VALUE(device_viewer) AS device_viewer,
        to_timestamp(MIN(first_ts)) AS first_seen,
        to_timestamp(MAX(last_ts)) AS last_seen
    FROM watch_rows
    GROUP BY row_group, timeline_key
    ORDER BY SUM(watch_sec) DESC
    LIMIT 200
    """).df()

    return summary_df, daily_df, top_df


@st.cache_data(show_spinner=False)
def calculate_watch_time_by_dimension(
    files: List[str],
    req_time_col: str,
    query_col: str,
    cliip_col: str,
    ua_col: str,
    path_col: str,
    start_epoch: int,
    end_epoch: int,
    watch_mode: str,
    cap_seconds: int,
    dimension_source: str,
    dimension_value: str,
    status_col: str = "",
    only_200: bool = False,
    top_n: int = 100,
    duckdb_temp_dir: str = "duckdb_temp",
    duckdb_memory_limit: str = "8GB",
    duckdb_threads: int = 2,
    duckdb_max_temp_size: str = "200GiB",
) -> pd.DataFrame:
    """Group watch time by a selected dimension.

    Supported dimensions:
      - stream_key derived from reqPath
      - row_group: id_sample vs no_id_estimated
      - queryStr key: channel, category_name, content_title, platform, etc.
      - raw parquet column: country, city, state, reqHost, rspContentType, etc.

    Watch modes match the main dashboard:
      - No cap / fast: MAX(req_ts)-MIN(req_ts) per timeline+dimension
      - Capped gaps: MIN(next_ts-current_ts, cap_seconds), assigned to current row's dimension
    """
    con = duckdb.connect(database=":memory:")
    configure_duckdb(
        con,
        temp_dir=duckdb_temp_dir,
        memory_limit=duckdb_memory_limit,
        threads=duckdb_threads,
        max_temp_size=duckdb_max_temp_size,
    )

    parquet_sql = _path_list_sql(files)
    ts = _dq(req_time_col)
    qs = _dq(query_col)
    ip = _dq(cliip_col)
    ua = _dq(ua_col)
    stream_expr = stream_key_sql(path_col)
    sf = status_filter_sql(status_col, only_200)

    raw_dimension_select = "NULL AS raw_dimension_value,"
    if dimension_source == "Raw column":
        raw_dimension_select = f"CAST({_dq(dimension_value)} AS VARCHAR) AS raw_dimension_value,"

    key = re.sub(r"[^A-Za-z0-9_\-]", "", dimension_value or "")
    if dimension_source == "QueryStr key":
        dim_expr = f"NULLIF(TRIM(regexp_extract(COALESCE(queryStr, ''), '(?:^|[?&]){key}=([^&]*)', 1)), '')"
    elif dimension_source == "Raw column":
        dim_expr = "NULLIF(TRIM(raw_dimension_value), '')"
    elif dimension_source == "stream_key":
        dim_expr = "stream_key"
    elif dimension_source == "row_group":
        dim_expr = "row_group"
    else:
        dim_expr = "stream_key"

    create_sql = f"""
    CREATE TEMP TABLE dimension_events AS
    WITH raw AS (
        SELECT
            TRY_CAST({ts} AS BIGINT) AS req_ts,
            CAST({qs} AS VARCHAR) AS queryStr,
            NULLIF(TRIM(CAST({ip} AS VARCHAR)), '') AS cliIP,
            NULLIF(TRIM(CAST({ua} AS VARCHAR)), '') AS UA,
            {stream_expr} AS stream_key,
            {raw_dimension_select}
            regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])device_id=([^&]*)', 1) AS raw_device_id,
            regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])session_id=([^&]*)', 1) AS raw_session_id
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
          {sf}
    ),
    clean AS (
        SELECT
            req_ts,
            CAST(to_timestamp(req_ts) AS DATE) AS event_date,
            cliIP,
            UA,
            stream_key,
            queryStr,
            raw_dimension_value,
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
            COALESCE({dim_expr}, '(unknown)') AS dimension_value,

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
        stream_key,
        dimension_value,
        event_date,
        req_ts
    FROM prepared
    WHERE timeline_key IS NOT NULL
      AND req_ts IS NOT NULL
    """
    con.execute(create_sql)

    if watch_mode == "No cap / fast":
        con.execute("""
        CREATE TEMP TABLE dimension_watch_rows AS
        SELECT
            dimension_value,
            timeline_key,
            row_group,
            ANY_VALUE(cliip_ua_viewer) AS cliip_ua_viewer,
            ANY_VALUE(device_viewer) AS device_viewer,
            ANY_VALUE(stream_key) AS stream_key,
            COUNT(*) AS event_rows,
            GREATEST(MAX(req_ts) - MIN(req_ts), 0) AS watch_sec
        FROM dimension_events
        GROUP BY dimension_value, timeline_key, row_group
        """)
    else:
        cap = int(cap_seconds)
        con.execute(f"""
        CREATE TEMP TABLE dimension_seq AS
        SELECT
            *,
            LEAD(req_ts) OVER (PARTITION BY timeline_key ORDER BY req_ts) AS next_ts
        FROM dimension_events
        """)
        con.execute(f"""
        CREATE TEMP TABLE dimension_watch_rows AS
        SELECT
            dimension_value,
            timeline_key,
            row_group,
            cliip_ua_viewer,
            device_viewer,
            stream_key,
            1 AS event_rows,
            LEAST(GREATEST(next_ts - req_ts, 0), {cap}) AS watch_sec
        FROM dimension_seq
        WHERE next_ts IS NOT NULL
        """)

    result = con.execute(f"""
    SELECT
        dimension_value AS "Dimension",
        SUM(event_rows) AS "Rows Used",
        COUNT(DISTINCT timeline_key) AS "Timeline Count",
        COUNT(DISTINCT cliip_ua_viewer) AS "Distinct cliIP+UA Viewer",
        COUNT(DISTINCT device_viewer) AS "Distinct Device_ID Sample",
        COUNT(DISTINCT stream_key) AS "Distinct stream_key",
        ROUND(SUM(watch_sec) / 3600.0, 4) AS "Total Watch Hours",
        ROUND(SUM(CASE WHEN row_group = 'id_sample' THEN watch_sec ELSE 0 END) / 3600.0, 4) AS "Device/Session Sample Watch Hours",
        ROUND(SUM(CASE WHEN row_group = 'no_id_estimated' THEN watch_sec ELSE 0 END) / 3600.0, 4) AS "No-ID Estimated Watch Hours",
        ROUND(SUM(watch_sec) / 60.0 / NULLIF(COUNT(DISTINCT cliip_ua_viewer), 0), 2) AS "Watch Min / cliIP+UA Viewer",
        ROUND(100.0 * SUM(watch_sec) / NULLIF(SUM(SUM(watch_sec)) OVER (), 0), 2) AS "% of Watch Time"
    FROM dimension_watch_rows
    GROUP BY 1
    ORDER BY "Total Watch Hours" DESC
    LIMIT {int(top_n)}
    """).df()
    return result


# ─────────────────────────────────────────────
# Distinct count / queryStr analysis
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def distinct_count_one_column(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
) -> int:
    con = duckdb.connect()
    qcol = _dq(column)
    qts = _dq(req_time_col)
    return int(con.execute(
        f"""
        SELECT COUNT(DISTINCT {qcol})
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
        """
    ).fetchone()[0] or 0)


@st.cache_data(show_spinner=False)
def top_values_for_column(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    limit: int = 50,
) -> pd.DataFrame:
    con = duckdb.connect()
    qcol = _dq(column)
    qts = _dq(req_time_col)
    return con.execute(
        f"""
        SELECT COALESCE(CAST({qcol} AS VARCHAR), '(null)') AS value, COUNT(*) AS rows
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
        GROUP BY 1
        ORDER BY rows DESC
        LIMIT {int(limit)}
        """
    ).df()


@st.cache_data(show_spinner=False)
def query_string_key_counts(
    files: List[str],
    query_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    sample_limit: int = 0,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect()
    qcol = _dq(query_col)
    qts = _dq(req_time_col)
    limit_sql = f"LIMIT {int(sample_limit)}" if sample_limit and sample_limit > 0 else ""
    raw_df = con.execute(
        f"""
        SELECT CAST({qcol} AS VARCHAR) AS raw_value, COUNT(*) AS rows
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND {qcol} IS NOT NULL
        GROUP BY 1
        ORDER BY rows DESC
        {limit_sql}
        """
    ).df()

    if raw_df.empty:
        return pd.DataFrame(columns=["Key", "Distinct Values", "Filled Rows"]), pd.DataFrame()

    key_values: Dict[str, set] = {}
    key_rows: Dict[str, int] = {}
    top_records = []

    for _, row in raw_df.iterrows():
        raw = str(row["raw_value"] or "")
        cnt = int(row["rows"] or 0)
        try:
            pairs = urllib.parse.parse_qsl(raw, keep_blank_values=True)
        except Exception:
            pairs = []
        for k, v in pairs:
            if not k:
                continue
            key_values.setdefault(k, set()).add(v)
            key_rows[k] = key_rows.get(k, 0) + cnt
            top_records.append({"Key": k, "Value": v, "Rows": cnt})

    key_df = pd.DataFrame([
        {"Key": k, "Distinct Values": len(vals), "Filled Rows": key_rows.get(k, 0)}
        for k, vals in key_values.items()
    ]).sort_values(["Filled Rows", "Distinct Values"], ascending=False)

    top_df = pd.DataFrame(top_records)
    if not top_df.empty:
        top_df = top_df.groupby(["Key", "Value"], as_index=False)["Rows"].sum().sort_values(["Key", "Rows"], ascending=[True, False])

    return key_df, top_df


# ─────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────

st.title("📺 Veto Dashboard")
st.caption("Watch-time, distinct counts, and query-string analysis for CDN/player logs.")

with st.sidebar:
    st.header("1) Folder / files")
    folder = st.text_input("Parquet folder", placeholder=r"D:\Veto Logs Backup")
    recursive = st.checkbox("Scan recursively", value=True)

if not folder:
    st.info("Enter a parquet folder in the sidebar.")
    st.stop()

files_all = scan_parquet_files(folder, recursive=recursive)
if not files_all:
    st.error("No parquet files found.")
    st.stop()

with st.sidebar:
    selected_files = st.multiselect(
        "Select parquet files",
        files_all,
        default=files_all,
        format_func=lambda x: Path(x).name,
    )

if not selected_files:
    st.warning("Select at least one file.")
    st.stop()

columns = get_columns(selected_files)
if not columns:
    st.error("Could not read columns from selected files.")
    st.stop()

def default_idx(name: str) -> int:
    return columns.index(name) if name in columns else 0

with st.sidebar:
    st.header("2) Column mapping")
    req_time_col = st.selectbox("Timestamp column", columns, index=default_idx("reqTimeSec"))
    query_col = st.selectbox("Query string column", columns, index=default_idx("queryStr"))
    cliip_col = st.selectbox("cliIP column", columns, index=default_idx("cliIP"))
    ua_col = st.selectbox("UA column", columns, index=default_idx("UA"))
    path_col = st.selectbox("reqPath column", columns, index=default_idx("reqPath"))

    status_options = ["(none)"] + columns
    status_col_choice = st.selectbox(
        "Optional statusCode column",
        status_options,
        index=status_options.index("statusCode") if "statusCode" in columns else 0,
    )
    only_200 = st.checkbox("Only statusCode = 200", value=False) if status_col_choice != "(none)" else False
    status_col = "" if status_col_choice == "(none)" else status_col_choice

min_ts, max_ts = get_epoch_range(selected_files, req_time_col)
if min_ts is None or max_ts is None:
    st.error("No valid timestamp range found.")
    st.stop()

min_date = epoch_to_date(min_ts)
max_date = epoch_to_date(max_ts)

st.subheader("Available date range")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Available From", str(pd.to_datetime(int(min_ts), unit="s")))
c2.metric("Available To", str(pd.to_datetime(int(max_ts), unit="s")))
c3.metric("Selected Files", f"{len(selected_files):,}")
c4.metric("Columns", f"{len(columns):,}")

with st.sidebar:
    st.header("3) Date / time range")
    date_range = st.date_input("Choose date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
    else:
        start_d, end_d = min_date, max_date

    start_t = st.time_input("Start time", dtime(0, 0, 0))
    end_t = st.time_input("End time", dtime(23, 59, 59))

    st.header("4) Watch-time mode")
    watch_mode = st.radio(
        "Watch-time calculation",
        ["No cap / fast", "Capped gaps"],
        index=0,
        help="No cap / fast uses MAX(reqTimeSec)-MIN(reqTimeSec) per timeline. Capped gaps uses next-request gaps with a max cap."
    )
    cap_seconds = st.number_input("Cap seconds between requests", min_value=1, max_value=86400, value=60, step=1, disabled=(watch_mode == "No cap / fast"))

    st.header("5) DuckDB OOM safety")
    duckdb_temp_dir = st.text_input(
        "DuckDB temp folder (use LOCAL drive, not network)",
        value=str((Path.cwd() / "duckdb_temp").resolve()),
        help="Use a local disk with large free space, for example D:\\duckdb_temp. Avoid Z:\\ network paths.",
    )
    duckdb_memory_limit = st.text_input("DuckDB memory limit", value="8GB")
    duckdb_threads = st.number_input("DuckDB threads", min_value=1, max_value=16, value=2, step=1)
    duckdb_max_temp_size = st.text_input("DuckDB max temp spill size", value="200GiB")

start_epoch = dt_to_epoch(start_d, start_t)
end_epoch = dt_to_epoch(end_d, end_t)

tabs = st.tabs(["Watch Time", "Watch Time by Dimension", "Distinct Counts", "Query String / Column Distinct"])

with tabs[0]:
    st.markdown("### Watch-time logic")
    if watch_mode == "No cap / fast":
        st.code(
"""No cap / fast mode:
  For each timeline:
    watch_seconds = MAX(reqTimeSec) - MIN(reqTimeSec)

Rows with device/session:
  timeline = device_id + session_id + stream_key

Rows without device/session:
  timeline = cliIP + UA + stream_key

stream_key is derived from reqPath.""",
            language="text",
        )
    else:
        st.code(
f"""Capped gaps mode:
  For each timeline:
    gap = next reqTimeSec - current reqTimeSec
    watch_seconds = MIN(gap, {int(cap_seconds)})

Rows with device/session:
  timeline = device_id + session_id + stream_key

Rows without device/session:
  timeline = cliIP + UA + stream_key

stream_key is derived from reqPath.""",
            language="text",
        )

    if st.button("Calculate watch time", type="primary"):
        with st.spinner("Calculating watch-time metrics..."):
            summary_df, daily_df, top_df = calculate_watch_time(
                files=selected_files,
                req_time_col=req_time_col,
                query_col=query_col,
                cliip_col=cliip_col,
                ua_col=ua_col,
                path_col=path_col,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                watch_mode=watch_mode,
                cap_seconds=int(cap_seconds),
                status_col=status_col,
                only_200=only_200,
                duckdb_temp_dir=duckdb_temp_dir,
                duckdb_memory_limit=duckdb_memory_limit,
                duckdb_threads=int(duckdb_threads),
                duckdb_max_temp_size=duckdb_max_temp_size,
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
        c3.metric("Average Seconds Used", f"{float(s['Average Seconds Used'] or 0):,.2f}")
        c4.metric("Max Seconds Used", f"{float(s['Max Seconds Used'] or 0):,.0f}")

        st.subheader("ID fill / raw counts")
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Rows with device_id", f"{int(s['Rows with device_id'] or 0):,}")
        d2.metric("Rows without device_id", f"{int(s['Rows without device_id'] or 0):,}")
        d3.metric("Rows with session_id", f"{int(s['Rows with session_id'] or 0):,}")
        d4.metric("Rows without session_id", f"{int(s['Rows without session_id'] or 0):,}")

        e1, e2, e3, e4 = st.columns(4)
        e1.metric("Rows with both IDs", f"{int(s['Rows with both device_id + session_id'] or 0):,}")
        e2.metric("device_id= blank", f"{int(s['Rows where device_id= is blank'] or 0):,}")
        e3.metric("session_id= blank", f"{int(s['Rows where session_id= is blank'] or 0):,}")
        e4.metric("Distinct stream_key", f"{int(s['Distinct stream_key'] or 0):,}")

        st.subheader("Full summary")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        st.subheader("Daily summary")
        st.dataframe(daily_df, use_container_width=True, hide_index=True, height=420)
        st.download_button(
            "Download daily summary CSV",
            data=daily_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_daily_watch_summary.csv",
            mime="text/csv",
        )

        st.subheader("Top timelines by watch hours")
        st.caption("Use this to validate overcounting, especially in no-cap mode.")
        st.dataframe(top_df, use_container_width=True, hide_index=True, height=420)
        st.download_button(
            "Download top timelines CSV",
            data=top_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_top_watch_timelines.csv",
            mime="text/csv",
        )


with tabs[1]:
    st.subheader("Watch Time by Dimension")
    st.caption("Group watch hours by channel, category, content title, platform, country, city, stream key, or any raw column.")

    dim_source = st.radio(
        "Dimension source",
        ["QueryStr key", "Raw column", "stream_key", "row_group"],
        horizontal=True,
        help="QueryStr key extracts values like channel/category_name/content_title from queryStr. Raw column uses a direct parquet column like country/city/state/reqHost."
    )

    common_qs_keys = [
        "channel",
        "category_name",
        "content_title",
        "content_type",
        "platform",
        "device",
        "session_id",
        "device_id",
    ]

    if dim_source == "QueryStr key":
        dim_value = st.selectbox("QueryStr key", common_qs_keys, index=0)
    elif dim_source == "Raw column":
        preferred = [c for c in ["country", "city", "state", "reqHost", "rspContentType", "statusCode", "UA", "cliIP", "reqPath"] if c in columns]
        rest = [c for c in columns if c not in preferred]
        raw_options = preferred + rest
        dim_value = st.selectbox("Raw column", raw_options, index=0)
    elif dim_source == "stream_key":
        dim_value = "stream_key"
        st.info("stream_key is calculated from reqPath: vglive-sk-xxxxx if found, otherwise the first folder in reqPath.")
    else:
        dim_value = "row_group"
        st.info("row_group separates ID sample rows from no-ID estimated rows.")

    top_n = st.number_input("Top N dimension values", min_value=10, max_value=1000, value=100, step=10)

    if st.button("Calculate watch time by dimension", type="primary"):
        with st.spinner("Calculating grouped watch-time table..."):
            dim_df = calculate_watch_time_by_dimension(
                files=selected_files,
                req_time_col=req_time_col,
                query_col=query_col,
                cliip_col=cliip_col,
                ua_col=ua_col,
                path_col=path_col,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                watch_mode=watch_mode,
                cap_seconds=int(cap_seconds),
                dimension_source=dim_source,
                dimension_value=dim_value,
                status_col=status_col,
                only_200=only_200,
                top_n=int(top_n),
                duckdb_temp_dir=duckdb_temp_dir,
                duckdb_memory_limit=duckdb_memory_limit,
                duckdb_threads=int(duckdb_threads),
                duckdb_max_temp_size=duckdb_max_temp_size,
            )

        st.markdown(f"#### Watch time grouped by `{dim_value}`")
        st.dataframe(dim_df, use_container_width=True, hide_index=True, height=520)
        st.download_button(
            "Download watch time by dimension CSV",
            data=dim_df.to_csv(index=False).encode("utf-8"),
            file_name=f"watch_time_by_{dim_value}.csv",
            mime="text/csv",
        )

        if not dim_df.empty:
            st.markdown("#### Top values")
            chart_df = dim_df.head(25).set_index("Dimension")[["Total Watch Hours"]]
            st.bar_chart(chart_df)

with tabs[2]:
    st.subheader("Click-to-load distinct counts")
    st.caption("Each card runs only when clicked.")

    search = st.text_input("Filter columns", "")
    filtered_cols = [c for c in columns if search.lower() in c.lower()] if search else columns

    cols_per_row = 4
    for i in range(0, len(filtered_cols), cols_per_row):
        row_cols = st.columns(cols_per_row)
        for j, col_name in enumerate(filtered_cols[i:i + cols_per_row]):
            with row_cols[j]:
                st.markdown(f"**{col_name}**")
                key = f"distinct_{col_name}_{i}_{j}"
                if st.button("Load", key=key):
                    with st.spinner(f"Counting distinct `{col_name}`..."):
                        cnt = distinct_count_one_column(selected_files, col_name, req_time_col, start_epoch, end_epoch)
                    st.metric("Distinct", f"{cnt:,}")

with tabs[3]:
    st.subheader("Query String / Column distinct")
    selected_col = st.selectbox("Choose column", columns, index=default_idx(query_col))

    force_qs = st.checkbox("Force query-string parsing", value=(selected_col == query_col))

    if st.button("Analyze selected column"):
        if force_qs:
            with st.spinner("Parsing query strings..."):
                key_df, top_df = query_string_key_counts(selected_files, selected_col, req_time_col, start_epoch, end_epoch)
            st.markdown("#### Parsed key distinct counts")
            st.dataframe(key_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download key distinct CSV",
                data=key_df.to_csv(index=False).encode("utf-8"),
                file_name="query_string_key_distinct_counts.csv",
                mime="text/csv",
            )

            if not top_df.empty:
                key_choice = st.selectbox("Show top values for key", sorted(top_df["Key"].unique()))
                st.dataframe(top_df[top_df["Key"] == key_choice].head(100), use_container_width=True, hide_index=True)
        else:
            with st.spinner("Counting distinct/top values..."):
                cnt = distinct_count_one_column(selected_files, selected_col, req_time_col, start_epoch, end_epoch)
                topv = top_values_for_column(selected_files, selected_col, req_time_col, start_epoch, end_epoch, 100)
            st.metric(f"Distinct {selected_col}", f"{cnt:,}")
            st.dataframe(topv, use_container_width=True, hide_index=True)
