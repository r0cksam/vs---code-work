import os
import re
from pathlib import Path
from datetime import datetime, time as dtime
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import pytz
except ImportError:
    pytz = None

APP_TITLE = "Veto Dashboard"
REQ_TIME_COL_DEFAULT = "reqTimeSec"
QUERY_COL_DEFAULT = "queryStr"
IP_COL_DEFAULT = "cliIP"
UA_COL_DEFAULT = "UA"

st.set_page_config(page_title=APP_TITLE, page_icon="📺", layout="wide")

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.0rem; }
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


def _dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    safe = [str(Path(f)).replace("\\", "/") for f in files]
    return repr(safe)


def _tz_name(tz_mode: str) -> str:
    return "Asia/Kolkata" if tz_mode == "IST" else "UTC"


def date_time_to_epoch(selected_date, selected_time, tz_mode: str) -> int:
    dt = datetime.combine(selected_date, selected_time)
    if pytz is None:
        return int(dt.timestamp())
    tz = pytz.timezone(_tz_name(tz_mode))
    return int(tz.localize(dt).timestamp())


def epoch_to_dt(epoch: int, tz_mode: str) -> datetime:
    if pytz is None:
        return datetime.fromtimestamp(epoch)
    tz = pytz.timezone(_tz_name(tz_mode))
    return datetime.fromtimestamp(epoch, tz=tz)


@st.cache_data(show_spinner=False)
def scan_parquet_files(root_folder: str) -> List[str]:
    root = Path(root_folder).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if name.lower().endswith(".parquet"):
                files.append(str(Path(dirpath) / name))
    return sorted(files)


@st.cache_data(show_spinner=False)
def read_schema(files: List[str]) -> pd.DataFrame:
    if not files or duckdb is None:
        return pd.DataFrame(columns=["column_name", "data_type"])
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
    except Exception:
        pass
    query = f"DESCRIBE SELECT * FROM read_parquet({_path_list_sql(files)}, union_by_name=true) LIMIT 0"
    return con.execute(query).df()[["column_name", "column_type"]].rename(columns={"column_type": "data_type"})


@st.cache_data(show_spinner=False)
def get_reqtime_range(files: List[str], req_time_col: str) -> Tuple[Optional[int], Optional[int]]:
    if not files or duckdb is None:
        return None, None
    con = duckdb.connect(database=":memory:")
    qcol = _dq(req_time_col)
    query = f"""
        SELECT
            MIN(TRY_CAST({qcol} AS BIGINT)) AS min_epoch,
            MAX(TRY_CAST({qcol} AS BIGINT)) AS max_epoch
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qcol} AS BIGINT) IS NOT NULL
    """
    row = con.execute(query).fetchone()
    if not row or row[0] is None or row[1] is None:
        return None, None
    return int(row[0]), int(row[1])


@st.cache_data(show_spinner=False)
def count_rows(files: List[str], req_time_col: str, start_epoch: int, end_epoch: int) -> int:
    con = duckdb.connect(database=":memory:")
    qcol = _dq(req_time_col)
    query = f"""
        SELECT COUNT(*)
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qcol} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
    """
    return int(con.execute(query).fetchone()[0] or 0)


@st.cache_data(show_spinner=False)
def distinct_count_one_column(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
) -> Dict[str, int]:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass
    qcol = _dq(column)
    time_col = _dq(req_time_col)
    query = f"""
        SELECT
            COUNT(*) AS rows_in_range,
            COUNT({qcol}) AS filled_rows,
            COUNT(DISTINCT {qcol}) AS distinct_count
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({time_col} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
    """
    row = con.execute(query).fetchone()
    return {
        "rows_in_range": int(row[0] or 0),
        "filled_rows": int(row[1] or 0),
        "distinct_count": int(row[2] or 0),
    }


@st.cache_data(show_spinner=False)
def top_values_one_column(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    top_n: int = 50,
) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    qcol = _dq(column)
    time_col = _dq(req_time_col)
    query = f"""
        SELECT COALESCE(CAST({qcol} AS VARCHAR), '(null)') AS value, COUNT(*) AS rows
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({time_col} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
        GROUP BY 1
        ORDER BY rows DESC
        LIMIT {int(top_n)}
    """
    return con.execute(query).df()


def watch_base_cte(
    files: List[str],
    req_time_col: str,
    query_col: str,
    ip_col: str,
    ua_col: str,
    start_epoch: int,
    end_epoch: int,
    tz_mode: str,
) -> str:
    """Base CTE for raw counts and uncapped watch-hour calculation.

    Identity method in this version:
      - Primary viewer = cliIP + UA, because these fields are available on almost all rows.
      - device_id and session_id from queryStr are metadata/enrichment when available.
      - Rows are never identified primarily by device_id, because device_id is sparse in these logs.

    Watch time has NO max-gap cap:
        watch_sec = GREATEST(next_ts - req_ts, 0)

    Session handling in this no-inactivity-break version:
      - Timeline is built per cliIP+UA viewer.
      - NO 30-minute or inactivity break is applied.
      - A new timeline session starts only when a new non-empty queryStr session_id appears
        and it differs from the last known non-empty session_id for that same viewer.
      - If session_id is missing, the viewer timeline is continuous across the selected range.
    """
    ts = _dq(req_time_col)
    qs = _dq(query_col)
    ip = _dq(ip_col)
    ua = _dq(ua_col)
    parquet_sql = _path_list_sql(files)
    timezone = _tz_name(tz_mode)
    return f"""
    WITH base AS (
        SELECT
            TRY_CAST({ts} AS BIGINT) AS req_ts,
            CAST({qs} AS VARCHAR) AS query_str,
            CAST({ip} AS VARCHAR) AS cliip_raw,
            CAST({ua} AS VARCHAR) AS ua_raw
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
    ),
    parsed AS (
        SELECT
            req_ts,
            CAST(to_timestamp(req_ts) AT TIME ZONE '{timezone}' AS DATE) AS event_date,
            NULLIF(TRIM(regexp_extract(query_str, '(?:^|[?&])device_id=([^&]+)', 1)), '') AS device_id,
            NULLIF(TRIM(regexp_extract(query_str, '(?:^|[?&])session_id=([^&]+)', 1)), '') AS session_id,
            NULLIF(TRIM(cliip_raw), '') AS cliip,
            NULLIF(TRIM(ua_raw), '') AS ua
        FROM base
        WHERE req_ts IS NOT NULL
    ),
    clean AS (
        SELECT
            *,
            CASE
                WHEN cliip IS NOT NULL AND ua IS NOT NULL THEN 'ipua:' || cliip || '|' || ua
                WHEN cliip IS NOT NULL THEN 'ip:' || cliip
                WHEN ua IS NOT NULL THEN 'ua:' || ua
                ELSE NULL
            END AS viewer_id,
            CASE
                WHEN cliip IS NOT NULL AND ua IS NOT NULL THEN 'cliIP_UA'
                WHEN cliip IS NOT NULL THEN 'cliIP_only'
                WHEN ua IS NOT NULL THEN 'UA_only'
                ELSE 'no_viewer'
            END AS viewer_type
        FROM parsed
    ),
    dedup_raw AS (
        SELECT DISTINCT
            viewer_id,
            viewer_type,
            device_id,
            cliip,
            ua,
            session_id,
            req_ts,
            event_date
        FROM clean
        WHERE viewer_id IS NOT NULL
          AND req_ts IS NOT NULL
    ),
    session_marked AS (
        SELECT
            *,
            LAG(req_ts) OVER (PARTITION BY viewer_id ORDER BY req_ts) AS prev_ts,
            arg_max(session_id, CASE WHEN session_id IS NOT NULL THEN req_ts ELSE NULL END) OVER (
                PARTITION BY viewer_id
                ORDER BY req_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS last_known_session_before
        FROM dedup_raw
    ),
    sessionized AS (
        SELECT
            *,
            CASE
                WHEN prev_ts IS NULL THEN 1
                WHEN session_id IS NOT NULL
                     AND last_known_session_before IS NOT NULL
                     AND session_id <> last_known_session_before THEN 1
                ELSE 0
            END AS timeline_session_start
        FROM session_marked
    ),
    dedup AS (
        SELECT
            viewer_id,
            viewer_type,
            device_id,
            cliip,
            ua,
            session_id,
            'timeline:' || CAST(
                SUM(timeline_session_start) OVER (
                    PARTITION BY viewer_id
                    ORDER BY req_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS VARCHAR
            ) AS session_key,
            CASE WHEN session_id IS NOT NULL THEN 'queryStr_session_id_seen' ELSE 'continuous_cliIP_UA_timeline_no_inactivity_break' END AS session_source,
            req_ts,
            event_date
        FROM sessionized
    ),
    sequenced AS (
        SELECT
            *,
            LEAD(req_ts) OVER (
                PARTITION BY viewer_id, session_key
                ORDER BY req_ts
            ) AS next_ts
        FROM dedup
    ),
    watch AS (
        SELECT
            *,
            GREATEST(next_ts - req_ts, 0) AS watch_sec_est
        FROM sequenced
        WHERE next_ts IS NOT NULL
    )
    """


@st.cache_data(show_spinner=False)
def watch_summary_uncapped(
    files: List[str],
    req_time_col: str,
    query_col: str,
    ip_col: str,
    ua_col: str,
    start_epoch: int,
    end_epoch: int,
    tz_mode: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    cte = watch_base_cte(files, req_time_col, query_col, ip_col, ua_col, start_epoch, end_epoch, tz_mode)

    summary_query = cte + """
    SELECT
        (SELECT COUNT(*) FROM clean) AS raw_rows,
        (SELECT COUNT(DISTINCT device_id) FROM clean WHERE device_id IS NOT NULL) AS raw_distinct_device_id,
        (SELECT COUNT(DISTINCT cliip) FROM clean WHERE cliip IS NOT NULL) AS raw_distinct_cliip,
        (SELECT COUNT(DISTINCT ua) FROM clean WHERE ua IS NOT NULL) AS raw_distinct_ua,
        (SELECT COUNT(DISTINCT viewer_id) FROM clean WHERE viewer_id IS NOT NULL) AS raw_distinct_cliip_ua_viewer,
        (SELECT SUM(CASE WHEN device_id IS NOT NULL THEN 1 ELSE 0 END) FROM clean) AS raw_rows_with_device_id,
        (SELECT SUM(CASE WHEN device_id IS NULL AND cliip IS NOT NULL THEN 1 ELSE 0 END) FROM clean) AS raw_rows_without_device_id_with_cliip,

        (SELECT COUNT(*) FROM dedup) AS rows_used_after_dedup,
        (SELECT COUNT(DISTINCT viewer_id) FROM dedup) AS total_viewer_count,
        (SELECT COUNT(DISTINCT device_id) FROM dedup WHERE device_id IS NOT NULL) AS total_device_id_count,
        (SELECT COUNT(DISTINCT cliip) FROM dedup WHERE device_id IS NULL AND cliip IS NOT NULL) AS cliip_only_metadata_count,
        (SELECT COUNT(DISTINCT session_id) FROM dedup WHERE session_id IS NOT NULL) AS querystr_session_count,
        (SELECT COUNT(DISTINCT session_key) FROM dedup) AS timeline_session_count,
        (SELECT COUNT(DISTINCT session_key) FROM dedup WHERE session_source = 'continuous_cliIP_UA_timeline_no_inactivity_break') AS inferred_session_count,
        (SELECT SUM(CASE WHEN device_id IS NOT NULL THEN 1 ELSE 0 END) FROM dedup) AS rows_with_device_id_after_dedup,
        (SELECT SUM(CASE WHEN device_id IS NULL THEN 1 ELSE 0 END) FROM dedup) AS rows_without_device_id_after_dedup,

        COALESCE((SELECT SUM(watch_sec_est) / 3600.0 FROM watch), 0) AS total_watch_hours,
        COALESCE((SELECT SUM(CASE WHEN device_id IS NOT NULL THEN watch_sec_est ELSE 0 END) / 3600.0 FROM watch), 0) AS watch_hours_on_rows_with_device_id,
        COALESCE((SELECT SUM(CASE WHEN device_id IS NULL THEN watch_sec_est ELSE 0 END) / 3600.0 FROM watch), 0) AS watch_hours_on_rows_without_device_id,
        COALESCE((SELECT SUM(watch_sec_est) / 60.0 / NULLIF(COUNT(DISTINCT viewer_id), 0) FROM watch), 0) AS watch_minutes_per_viewer,
        COALESCE((SELECT SUM(CASE WHEN device_id IS NOT NULL THEN watch_sec_est ELSE 0 END) / 60.0 / NULLIF(COUNT(DISTINCT device_id), 0) FROM watch WHERE device_id IS NOT NULL), 0) AS device_id_sample_watch_minutes_per_device,
        COALESCE((SELECT SUM(CASE WHEN device_id IS NULL THEN watch_sec_est ELSE 0 END) / 60.0 / NULLIF(COUNT(DISTINCT viewer_id), 0) FROM watch WHERE device_id IS NULL), 0) AS no_device_rows_watch_minutes_per_viewer,
        COALESCE((SELECT AVG(watch_sec_est) FROM watch), 0) AS avg_gap_sec_used,
        COALESCE((SELECT MAX(watch_sec_est) FROM watch), 0) AS max_gap_sec_used
    """

    row = con.execute(summary_query).fetchone()
    cols = [
        "Raw Rows", "Raw Distinct Device_ID", "Raw Distinct cliIP", "Raw Distinct UA", "Raw Distinct cliIP+UA Viewer",
        "Raw Rows with Device_ID", "Raw Rows without Device_ID + cliIP",
        "Rows Used After Dedup", "Total Viewer Count", "Total Device_ID Count", "cliIP Metadata Count Without Device_ID",
        "QueryStr Session Count", "Timeline Session Count", "No-Session Timeline Count",
        "Rows with Device_ID", "Rows without Device_ID", "Total Watch Hours",
        "Watch Hours on Rows with Device_ID", "Watch Hours on Rows without Device_ID", "Estimated Watch Minutes / Viewer (cliIP+UA)",
        "Device_ID Sample Watch Minutes / Device", "No-Device Rows Watch Minutes / Viewer",
        "Average Gap Seconds Used", "Max Gap Seconds Used"
    ]
    summary_df = pd.DataFrame([dict(zip(cols, row))])

    daily_query = cte + """
    SELECT
        d.event_date AS Date,
        COUNT(*) AS "Rows Used After Dedup",
        COUNT(DISTINCT d.viewer_id) AS "Total Viewer Count",
        COUNT(DISTINCT d.device_id) AS "Total Device_ID Count",
        COUNT(DISTINCT d.cliip) AS "Distinct cliIP",
        COUNT(DISTINCT d.ua) AS "Distinct UA",
        COUNT(DISTINCT d.session_key) AS "Timeline Session Count",
        COUNT(DISTINCT CASE WHEN d.session_source = 'continuous_cliIP_UA_timeline_no_inactivity_break' THEN d.session_key END) AS "No-Session Timeline Count",
        COALESCE(SUM(w.watch_sec_est) / 3600.0, 0) AS "Total Watch Hours",
        COALESCE(SUM(CASE WHEN w.device_id IS NOT NULL THEN w.watch_sec_est ELSE 0 END) / 3600.0, 0) AS "Watch Hours on Rows with Device_ID",
        COALESCE(SUM(CASE WHEN w.device_id IS NULL THEN w.watch_sec_est ELSE 0 END) / 3600.0, 0) AS "Watch Hours on Rows without Device_ID",
        COALESCE(SUM(w.watch_sec_est) / 60.0 / NULLIF(COUNT(DISTINCT d.viewer_id), 0), 0) AS "Estimated Watch Minutes / Viewer (cliIP+UA)",
        COALESCE(SUM(CASE WHEN w.device_id IS NOT NULL THEN w.watch_sec_est ELSE 0 END) / 60.0 / NULLIF(COUNT(DISTINCT CASE WHEN d.device_id IS NOT NULL THEN d.device_id END), 0), 0) AS "Device_ID Sample Watch Minutes / Device",
        COALESCE(SUM(CASE WHEN w.device_id IS NULL THEN w.watch_sec_est ELSE 0 END) / 60.0 / NULLIF(COUNT(DISTINCT CASE WHEN d.device_id IS NULL THEN d.viewer_id END), 0), 0) AS "No-Device Rows Watch Minutes / Viewer"
    FROM dedup d
    LEFT JOIN watch w
      ON d.viewer_id = w.viewer_id
     AND d.session_key = w.session_key
     AND d.req_ts = w.req_ts
    GROUP BY 1
    ORDER BY 1
    """
    daily_df = con.execute(daily_query).df()
    return summary_df, daily_df


@st.cache_data(show_spinner=False)
def query_string_key_counts(
    files: List[str],
    query_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    sample_limit: int = 0,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Parse query string values in Python from aggregated raw values.

    This is intended for queryStr-like columns. It aggregates values first to avoid parsing every row.
    """
    import urllib.parse

    con = duckdb.connect(database=":memory:")
    qcol = _dq(query_col)
    tcol = _dq(req_time_col)
    limit_sql = f"LIMIT {int(sample_limit)}" if sample_limit and sample_limit > 0 else ""
    query = f"""
        SELECT CAST({qcol} AS VARCHAR) AS raw_value, COUNT(*) AS rows
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({tcol} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND {qcol} IS NOT NULL
        GROUP BY 1
        ORDER BY rows DESC
        {limit_sql}
    """
    raw_df = con.execute(query).df()
    if raw_df.empty:
        return pd.DataFrame(columns=["Key", "Distinct Values", "Filled Rows"]), pd.DataFrame()

    key_counts: Dict[str, Dict[str, object]] = {}
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
            item = key_counts.setdefault(k, {"values": set(), "filled_rows": 0, "top": {}})
            if str(v).strip() != "":
                item["values"].add(str(v))
                item["filled_rows"] += cnt
                item["top"][str(v)] = item["top"].get(str(v), 0) + cnt

    rows = []
    for k, data in key_counts.items():
        rows.append({"Key": k, "Distinct Values": len(data["values"]), "Filled Rows": int(data["filled_rows"])})
        for val, cnt in sorted(data["top"].items(), key=lambda x: x[1], reverse=True)[:20]:
            top_records.append({"Key": k, "Value": val, "Rows": cnt})

    key_df = pd.DataFrame(rows).sort_values(["Filled Rows", "Distinct Values"], ascending=False)
    top_df = pd.DataFrame(top_records)
    return key_df, top_df


def render_metric_grid(summary: Dict[str, object], prefix: str = ""):
    st.markdown("#### Grafana-style raw counts")
    r1, r2, r3, r4, r5 = st.columns(5)
    r1.metric("Raw Rows", f"{int(summary.get('Raw Rows', 0) or 0):,}")
    r2.metric("Raw Distinct Device_ID", f"{int(summary.get('Raw Distinct Device_ID', 0) or 0):,}")
    r3.metric("Raw Distinct cliIP", f"{int(summary.get('Raw Distinct cliIP', 0) or 0):,}")
    r4.metric("Raw Distinct UA", f"{int(summary.get('Raw Distinct UA', 0) or 0):,}")
    r5.metric("Raw cliIP+UA Viewers", f"{int(summary.get('Raw Distinct cliIP+UA Viewer', 0) or 0):,}")

    rr1, rr2 = st.columns(2)
    rr1.metric("Raw Rows with Device_ID", f"{int(summary.get('Raw Rows with Device_ID', 0) or 0):,}")
    rr2.metric("Raw Rows without Device_ID + cliIP", f"{int(summary.get('Raw Rows without Device_ID + cliIP', 0) or 0):,}")

    st.markdown("#### Watch-hour calculation counts — primary viewer = cliIP + UA, no max-gap cap")
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Total Viewer Count", f"{int(summary.get('Total Viewer Count', 0) or 0):,}")
    a2.metric("Total Watch Hours", f"{float(summary.get('Total Watch Hours', 0) or 0):,.2f}")
    a3.metric("Estimated Watch Min / Viewer", f"{float(summary.get('Estimated Watch Minutes / Viewer (cliIP+UA)', 0) or 0):,.2f}")
    a4.metric("Rows Used After Dedup", f"{int(summary.get('Rows Used After Dedup', 0) or 0):,}")

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Rows with Device_ID", f"{int(summary.get('Rows with Device_ID', 0) or 0):,}")
    b2.metric("Watch Hours on Rows with Device_ID", f"{float(summary.get('Watch Hours on Rows with Device_ID', 0) or 0):,.2f}")
    b3.metric("Rows without Device_ID", f"{int(summary.get('Rows without Device_ID', 0) or 0):,}")
    b4.metric("Watch Hours on Rows without Device_ID", f"{float(summary.get('Watch Hours on Rows without Device_ID', 0) or 0):,.2f}")

    avg1, avg2 = st.columns(2)
    avg1.metric("Device_ID Sample Watch Min / Device", f"{float(summary.get('Device_ID Sample Watch Minutes / Device', 0) or 0):,.2f}")
    avg2.metric("No-Device Rows Watch Min / Viewer", f"{float(summary.get('No-Device Rows Watch Minutes / Viewer', 0) or 0):,.2f}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Device_ID Count", f"{int(summary.get('Total Device_ID Count', 0) or 0):,}")
    c2.metric("QueryStr Session Count", f"{int(summary.get('QueryStr Session Count', 0) or 0):,}")
    c3.metric("Timeline Session Count", f"{int(summary.get('Timeline Session Count', 0) or 0):,}")
    c4.metric("No-Session Timeline Count", f"{int(summary.get('No-Session Timeline Count', 0) or 0):,}")

    g1, g2 = st.columns(2)
    g1.metric("Average Gap Seconds Used", f"{float(summary.get('Average Gap Seconds Used', 0) or 0):,.2f}")
    g2.metric("Max Gap Seconds Used", f"{float(summary.get('Max Gap Seconds Used', 0) or 0):,.0f}")

    total_wh = float(summary.get("Total Watch Hours", 0) or 0)
    no_device_wh = float(summary.get("Watch Hours on Rows without Device_ID", 0) or 0)
    if total_wh > 0 and no_device_wh / total_wh > 0.5:
        st.info(
            f"{no_device_wh / total_wh * 100:.1f}% of watch hours came from rows without device_id. "
            "This is expected when device_id exists in only a small share of queryStr rows; viewer identity is still cliIP+UA."
        )


# ─────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────
st.title("📺 Veto Dashboard")
st.caption("Folder/file selection, date-range comparison, distinct counts, and no-break uncapped estimated watch hours from reqTimeSec gaps.")

if duckdb is None:
    st.error("DuckDB is required. Install it with: pip install duckdb")
    st.stop()

with st.sidebar:
    st.header("Folder & files")
    root_folder = st.text_input("Folder path", placeholder=r"D:\Veto Logs Backup or /mnt/data/logs")
    if st.button("Scan folder", type="primary") and root_folder:
        st.session_state["veto_files"] = scan_parquet_files(root_folder)
        st.session_state["veto_root"] = root_folder

    files = st.session_state.get("veto_files", [])
    if files:
        st.success(f"{len(files):,} parquet files found")
        root = Path(st.session_state.get("veto_root", root_folder))
        labels = []
        for f in files:
            try:
                labels.append(str(Path(f).relative_to(root)))
            except Exception:
                labels.append(str(f))
        label_to_file = dict(zip(labels, files))
        select_all = st.checkbox("Select all files", value=True)
        default = labels if select_all else labels[: min(10, len(labels))]
        selected_labels = st.multiselect("Selected files", options=labels, default=default)
        selected_files = [label_to_file[x] for x in selected_labels]
    else:
        selected_files = []

    st.header("Timezone")
    tz_mode = st.radio("Date/time interpretation", ["IST", "UTC"], horizontal=True)

if not selected_files:
    st.info("Enter a folder path in the sidebar, scan, then select parquet files.")
    st.stop()

schema_df = read_schema(selected_files)
if schema_df.empty:
    st.error("Could not read schema from selected files.")
    st.stop()
columns = schema_df["column_name"].tolist()

# Column selectors at top
st.markdown("### Selected data")
s1, s2, s3 = st.columns(3)
s1.metric("Selected files", f"{len(selected_files):,}")
s2.metric("Columns", f"{len(columns):,}")
s3.metric("Timezone", tz_mode)

with st.expander("Selected files", expanded=False):
    st.write(pd.DataFrame({"file": selected_files}))
with st.expander("Columns", expanded=False):
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

st.markdown("### Available date range")
req_time_col = st.selectbox(
    "Timestamp column",
    options=columns,
    index=columns.index(REQ_TIME_COL_DEFAULT) if REQ_TIME_COL_DEFAULT in columns else 0,
)
query_col = st.selectbox(
    "Query string column",
    options=columns,
    index=columns.index(QUERY_COL_DEFAULT) if QUERY_COL_DEFAULT in columns else 0,
)
ip_col = st.selectbox(
    "cliIP column",
    options=columns,
    index=columns.index(IP_COL_DEFAULT) if IP_COL_DEFAULT in columns else 0,
    help="Used with UA to create the primary viewer identity: cliIP + UA.",
)
ua_col = st.selectbox(
    "UA column",
    options=columns,
    index=columns.index(UA_COL_DEFAULT) if UA_COL_DEFAULT in columns else 0,
    help="Used with cliIP to create the primary viewer identity: cliIP + UA.",
)

min_epoch, max_epoch = get_reqtime_range(selected_files, req_time_col)
if min_epoch is None or max_epoch is None:
    st.error(f"No valid epoch seconds found in `{req_time_col}`.")
    st.stop()
min_dt = epoch_to_dt(min_epoch, tz_mode)
max_dt = epoch_to_dt(max_epoch, tz_mode)

d1, d2 = st.columns(2)
d1.metric("Available From", min_dt.strftime("%Y-%m-%d %H:%M:%S"))
d2.metric("Available To", max_dt.strftime("%Y-%m-%d %H:%M:%S"))

st.markdown("### Choose date/time range")
comparison_mode = st.checkbox("Comparison mode", value=False)

if comparison_mode:
    st.markdown("#### Range A")
    a1, a2, a3, a4 = st.columns(4)
    with a1:
        a_start_date = st.date_input("A Start date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    with a2:
        a_start_time = st.time_input("A Start time", value=dtime(0, 0, 0))
    with a3:
        a_end_date = st.date_input("A End date", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    with a4:
        a_end_time = st.time_input("A End time", value=dtime(23, 59, 59))

    st.markdown("#### Range B")
    b1, b2, b3, b4 = st.columns(4)
    with b1:
        b_start_date = st.date_input("B Start date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    with b2:
        b_start_time = st.time_input("B Start time", value=dtime(0, 0, 0))
    with b3:
        b_end_date = st.date_input("B End date", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    with b4:
        b_end_time = st.time_input("B End time", value=dtime(23, 59, 59))

    range_a = (date_time_to_epoch(a_start_date, a_start_time, tz_mode), date_time_to_epoch(a_end_date, a_end_time, tz_mode))
    range_b = (date_time_to_epoch(b_start_date, b_start_time, tz_mode), date_time_to_epoch(b_end_date, b_end_time, tz_mode))
else:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        start_date = st.date_input("Start date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    with c2:
        start_time = st.time_input("Start time", value=dtime(0, 0, 0))
    with c3:
        end_date = st.date_input("End date", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    with c4:
        end_time = st.time_input("End time", value=dtime(23, 59, 59))
    range_single = (date_time_to_epoch(start_date, start_time, tz_mode), date_time_to_epoch(end_date, end_time, tz_mode))

# Watch-hour section
st.markdown("---")
st.markdown("## Watch hours and viewer count")
st.caption("No max-gap cap and no inactivity-session break are applied. Primary viewer is cliIP + UA. Device_ID average is shown separately as a sample-only metric from rows where device_id exists.")
st.warning("No inactivity break means a long gap between two requests from the same cliIP+UA can be counted as watch time. Use this version only to inspect the uncapped/no-break result.")

if comparison_mode:
    if range_a[0] > range_a[1] or range_b[0] > range_b[1]:
        st.error("Start date/time must be before end date/time for both ranges.")
        st.stop()
    if st.button("Calculate watch hours for Range A and B", type="primary"):
        with st.spinner("Calculating Range A watch hours without gap cap ..."):
            a_summary, a_daily = watch_summary_uncapped(selected_files, req_time_col, query_col, ip_col, ua_col, range_a[0], range_a[1], tz_mode)
        with st.spinner("Calculating Range B watch hours without gap cap ..."):
            b_summary, b_daily = watch_summary_uncapped(selected_files, req_time_col, query_col, ip_col, ua_col, range_b[0], range_b[1], tz_mode)
        st.session_state["watch_a_summary"] = a_summary
        st.session_state["watch_b_summary"] = b_summary
        st.session_state["watch_a_daily"] = a_daily
        st.session_state["watch_b_daily"] = b_daily

    a_summary = st.session_state.get("watch_a_summary")
    b_summary = st.session_state.get("watch_b_summary")
    if a_summary is not None and b_summary is not None:
        a = a_summary.iloc[0].to_dict()
        b = b_summary.iloc[0].to_dict()
        left, right = st.columns(2)
        with left:
            st.markdown("### Range A")
            render_metric_grid(a)
        with right:
            st.markdown("### Range B")
            render_metric_grid(b)

        st.markdown("### Comparison table")
        comp_rows = []
        for metric in a_summary.columns:
            av = a.get(metric, 0) or 0
            bv = b.get(metric, 0) or 0
            try:
                delta = float(bv) - float(av)
                delta_pct = (delta / float(av) * 100) if float(av) else None
            except Exception:
                delta = None
                delta_pct = None
            comp_rows.append({"Metric": metric, "Range A": av, "Range B": bv, "Delta (B - A)": delta, "Delta %": delta_pct})
        comp_df = pd.DataFrame(comp_rows)
        st.dataframe(comp_df, use_container_width=True, hide_index=True)
        st.download_button("Download comparison CSV", comp_df.to_csv(index=False).encode("utf-8"), "watch_hour_comparison_uncapped.csv", "text/csv")

        st.markdown("### Daily summaries")
        da = st.session_state.get("watch_a_daily")
        db = st.session_state.get("watch_b_daily")
        dc1, dc2 = st.columns(2)
        with dc1:
            st.markdown("#### Range A daily")
            st.dataframe(da, use_container_width=True, hide_index=True, height=360)
        with dc2:
            st.markdown("#### Range B daily")
            st.dataframe(db, use_container_width=True, hide_index=True, height=360)
else:
    if range_single[0] > range_single[1]:
        st.error("Start date/time must be before end date/time.")
        st.stop()
    if st.button("Calculate watch hours", type="primary"):
        with st.spinner("Calculating watch hours without gap cap ..."):
            summary_df, daily_df = watch_summary_uncapped(selected_files, req_time_col, query_col, ip_col, ua_col, range_single[0], range_single[1], tz_mode)
        st.session_state["watch_summary"] = summary_df
        st.session_state["watch_daily"] = daily_df

    summary_df = st.session_state.get("watch_summary")
    if summary_df is not None:
        summary = summary_df.iloc[0].to_dict()
        render_metric_grid(summary)
        st.markdown("### Daily watch-hour summary")
        daily_df = st.session_state.get("watch_daily")
        st.dataframe(daily_df, use_container_width=True, hide_index=True, height=420)
        st.download_button("Download daily watch summary CSV", daily_df.to_csv(index=False).encode("utf-8"), "daily_watch_summary_uncapped.csv", "text/csv")

# Distinct cards
st.markdown("---")
st.markdown("## Boxed distinct counts")
st.caption("Click Load on a column card. The app does not calculate every column until you ask for it.")

search_col = st.text_input("Search columns for distinct cards", placeholder="device, cliIP, city, channel ...")
card_cols = columns
if search_col.strip():
    card_cols = [c for c in columns if search_col.strip().lower() in c.lower()]

# Limit render to keep Streamlit page usable
show_n_cards = st.slider("Number of matching columns to show", min_value=4, max_value=min(80, max(4, len(card_cols))), value=min(24, max(4, len(card_cols))), step=4)
card_cols = card_cols[:show_n_cards]

active_range = range_a if comparison_mode else range_single
active_label = "Range A" if comparison_mode else "Selected range"

for row_start in range(0, len(card_cols), 4):
    cols4 = st.columns(4)
    for j, col_name in enumerate(card_cols[row_start:row_start + 4]):
        with cols4[j]:
            st.markdown(f"**{col_name}**")
            key_base = f"distinct_{hash(tuple(selected_files))}_{col_name}_{active_range[0]}_{active_range[1]}"
            if st.button("Load", key=f"btn_{key_base}"):
                with st.spinner(f"Counting distinct `{col_name}` ..."):
                    if comparison_mode:
                        a_res = distinct_count_one_column(selected_files, col_name, req_time_col, range_a[0], range_a[1])
                        b_res = distinct_count_one_column(selected_files, col_name, req_time_col, range_b[0], range_b[1])
                        st.session_state[key_base] = {"A": a_res, "B": b_res}
                    else:
                        st.session_state[key_base] = distinct_count_one_column(selected_files, col_name, req_time_col, active_range[0], active_range[1])
            res = st.session_state.get(key_base)
            if res is None:
                st.caption("Not loaded")
            elif comparison_mode:
                st.metric("A distinct", f"{res['A']['distinct_count']:,}")
                st.metric("B distinct", f"{res['B']['distinct_count']:,}", delta=f"{res['B']['distinct_count'] - res['A']['distinct_count']:,}")
            else:
                st.metric("Distinct", f"{res['distinct_count']:,}")
                st.caption(f"Filled rows: {res['filled_rows']:,}")

# Query string / normal column distinct
st.markdown("---")
st.markdown("## Query String and normal column distinct")
qsd1, qsd2, qsd3 = st.columns([2, 1, 1])
with qsd1:
    qd_col = st.selectbox("Column to analyze", options=columns, index=columns.index(query_col) if query_col in columns else 0, key="qd_col")
with qsd2:
    force_qs = st.checkbox("Force query-string parse", value=(qd_col == query_col))
with qsd3:
    top_n = st.number_input("Top values", min_value=10, max_value=500, value=50, step=10)

if st.button("Run selected column analysis", type="primary"):
    if force_qs:
        with st.spinner("Parsing query-string values ..."):
            key_df, top_df = query_string_key_counts(selected_files, qd_col, req_time_col, active_range[0], active_range[1])
        st.session_state["qs_key_df"] = key_df
        st.session_state["qs_top_df"] = top_df
        st.session_state["normal_top_df"] = None
    else:
        with st.spinner("Counting normal column distinct and top values ..."):
            one = distinct_count_one_column(selected_files, qd_col, req_time_col, active_range[0], active_range[1])
            top_df = top_values_one_column(selected_files, qd_col, req_time_col, active_range[0], active_range[1], int(top_n))
        st.session_state["normal_one"] = one
        st.session_state["normal_top_df"] = top_df
        st.session_state["qs_key_df"] = None
        st.session_state["qs_top_df"] = None

if st.session_state.get("qs_key_df") is not None:
    st.markdown("### Parsed query-string keys")
    key_df = st.session_state["qs_key_df"]
    top_df = st.session_state["qs_top_df"]
    st.dataframe(key_df, use_container_width=True, hide_index=True, height=350)
    st.download_button("Download query-string key counts CSV", key_df.to_csv(index=False).encode("utf-8"), "query_string_key_counts.csv", "text/csv")
    if top_df is not None and not top_df.empty:
        key_pick = st.selectbox("Show top values for key", options=sorted(top_df["Key"].unique().tolist()))
        st.dataframe(top_df[top_df["Key"] == key_pick].head(int(top_n)), use_container_width=True, hide_index=True)
elif st.session_state.get("normal_top_df") is not None:
    one = st.session_state.get("normal_one", {})
    n1, n2, n3 = st.columns(3)
    n1.metric("Rows in range", f"{one.get('rows_in_range', 0):,}")
    n2.metric("Filled rows", f"{one.get('filled_rows', 0):,}")
    n3.metric("Distinct count", f"{one.get('distinct_count', 0):,}")
    st.dataframe(st.session_state["normal_top_df"], use_container_width=True, hide_index=True, height=350)
    st.download_button("Download top values CSV", st.session_state["normal_top_df"].to_csv(index=False).encode("utf-8"), f"top_values_{qd_col}.csv", "text/csv")

st.markdown("---")
st.caption("Watch-hour method in this version: primary viewer = cliIP + UA, device_id/session_id from queryStr are metadata when available, sessions are inferred from real session_id changes only; no inactivity break, dedup same viewer/session/timestamp, then full uncapped gap to next request inside the same timeline session. Both estimated cliIP+UA viewer average and Device_ID sample average are shown.")
