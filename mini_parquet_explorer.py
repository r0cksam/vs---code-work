import os
import re
import urllib.parse
from pathlib import Path
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Tuple

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
DEFAULT_GAP_CAP_SECONDS = 60

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.0rem; }
        div[data-testid="stMetric"] {
            background: rgba(128,128,128,0.08);
            padding: 0.8rem;
            border-radius: 0.85rem;
            border: 1px solid rgba(128,128,128,0.18);
        }
        .small-card {
            padding: 0.8rem;
            border-radius: 0.85rem;
            border: 1px solid rgba(128,128,128,0.25);
            background: rgba(128,128,128,0.06);
            min-height: 135px;
            margin-bottom: 0.8rem;
        }
        .small-card-title {
            font-weight: 700;
            font-size: 0.92rem;
            overflow-wrap: anywhere;
            margin-bottom: 0.35rem;
        }
        .small-muted { color: #777; font-size: 0.82rem; }
        div[data-testid="stSidebarContent"] { padding-top: 1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────
# General helpers
# ─────────────────────────────────────────────

def require_duckdb() -> None:
    if duckdb is None:
        st.error("DuckDB is required. Install it with: pip install duckdb")
        st.stop()


def _dq(identifier: str) -> str:
    """DuckDB-safe double-quoted identifier."""
    return '"' + str(identifier).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    safe = [str(Path(f)).replace("\\", "/") for f in files]
    return repr(safe)


def _tz_name(tz_mode: str) -> str:
    return "Asia/Kolkata" if tz_mode == "IST" else "UTC"


def epoch_to_display_dt(epoch: int, tz_mode: str) -> datetime:
    if pytz is not None:
        tz = pytz.timezone(_tz_name(tz_mode))
        return datetime.fromtimestamp(int(epoch), tz=tz)
    return datetime.fromtimestamp(int(epoch))


def date_time_to_epoch(selected_date, selected_time, tz_mode: str) -> int:
    dt = datetime.combine(selected_date, selected_time)
    if pytz is not None:
        tz = pytz.timezone(_tz_name(tz_mode))
        return int(tz.localize(dt).timestamp())
    return int(dt.timestamp())


def _display_date_expr(req_time_col: str, tz_mode: str) -> str:
    qts = _dq(req_time_col)
    if tz_mode == "IST":
        return f"CAST((to_timestamp(TRY_CAST({qts} AS BIGINT)) AT TIME ZONE 'Asia/Kolkata') AS DATE)"
    return f"CAST((to_timestamp(TRY_CAST({qts} AS BIGINT)) AT TIME ZONE 'UTC') AS DATE)"


def _query_key_expr(query_col: str, key: str) -> str:
    """Extract a key from query string using DuckDB regexp_extract."""
    q = _dq(query_col)
    safe_key = re.escape(str(key))
    # Pattern works whether query string starts directly with key or key appears after &.
    return f"regexp_extract(CAST({q} AS VARCHAR), '(?:^|&){safe_key}=([^&]+)', 1)"


def _device_expr(device_source: str, query_col: str, device_col: str, device_key: str) -> str:
    if device_source == "Parsed from query string":
        return _query_key_expr(query_col, device_key)
    if device_source == "Direct column":
        return f"CAST({_dq(device_col)} AS VARCHAR)"
    return "NULL"


def _guess_option(options: List[str], names: List[str], include_blank: bool = False) -> int:
    opts = ([""] + options) if include_blank else options
    lowered = [x.lower() for x in opts]
    for name in names:
        if name.lower() in lowered:
            return lowered.index(name.lower())
    for idx, opt in enumerate(lowered):
        if any(name.lower() in opt for name in names if name):
            return idx
    return 0


# ─────────────────────────────────────────────
# File/schema/date helpers
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def scan_parquet_files(root_folder: str) -> List[str]:
    root = Path(root_folder).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    files: List[str] = []
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
        con.execute("PRAGMA enable_object_cache")
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
    qts = _dq(req_time_col)
    query = f"""
        SELECT COUNT(*)
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
    """
    return int(con.execute(query).fetchone()[0] or 0)


# ─────────────────────────────────────────────
# Distinct count helpers
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def distinct_count_one_column(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
) -> Dict[str, object]:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass
    qcol = _dq(column)
    qts = _dq(req_time_col)
    query = f"""
        SELECT
            COUNT(*) AS rows_in_range,
            COUNT({qcol}) AS filled_rows,
            COUNT(DISTINCT {qcol}) AS distinct_count
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
    """
    row = con.execute(query).fetchone()
    rows_in_range = int(row[0] or 0)
    filled_rows = int(row[1] or 0)
    distinct_count = int(row[2] or 0)
    return {
        "Column": column,
        "Rows": rows_in_range,
        "Filled Rows": filled_rows,
        "Distinct Count": distinct_count,
        "Fill %": round(filled_rows / rows_in_range * 100, 2) if rows_in_range else 0.0,
    }


@st.cache_data(show_spinner=False)
def compare_distinct_count_one_column(
    files: List[str],
    column: str,
    req_time_col: str,
    a_start_epoch: int,
    a_end_epoch: int,
    b_start_epoch: int,
    b_end_epoch: int,
) -> Dict[str, object]:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass
    qcol = _dq(column)
    qts = _dq(req_time_col)
    filter_a = f"TRY_CAST({qts} AS BIGINT) BETWEEN {int(a_start_epoch)} AND {int(a_end_epoch)}"
    filter_b = f"TRY_CAST({qts} AS BIGINT) BETWEEN {int(b_start_epoch)} AND {int(b_end_epoch)}"
    query = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN {filter_a} THEN {qcol} END) AS a_distinct,
            COUNT(DISTINCT CASE WHEN {filter_b} THEN {qcol} END) AS b_distinct
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE ({filter_a}) OR ({filter_b})
    """
    a_count, b_count = con.execute(query).fetchone()
    a_count = int(a_count or 0)
    b_count = int(b_count or 0)
    delta = b_count - a_count
    return {
        "Column": column,
        "Range A Distinct": a_count,
        "Range B Distinct": b_count,
        "Delta (B - A)": delta,
        "Delta %": round(delta / a_count * 100, 2) if a_count else None,
    }


@st.cache_data(show_spinner=False)
def top_values_one_column(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    limit: int = 50,
) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    qcol = _dq(column)
    qts = _dq(req_time_col)
    query = f"""
        SELECT
            COALESCE(CAST({qcol} AS VARCHAR), '(null)') AS value,
            COUNT(*) AS count
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
        GROUP BY 1
        ORDER BY count DESC
        LIMIT {int(limit)}
    """
    return con.execute(query).df()


# ─────────────────────────────────────────────
# Query-string helpers
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def sample_column_values(
    files: List[str],
    column: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    limit: int = 1000,
) -> List[str]:
    con = duckdb.connect(database=":memory:")
    qcol = _dq(column)
    qts = _dq(req_time_col)
    query = f"""
        SELECT CAST({qcol} AS VARCHAR) AS value
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND {qcol} IS NOT NULL
        LIMIT {int(limit)}
    """
    df = con.execute(query).df()
    return df["value"].dropna().astype(str).tolist()


def looks_like_querystring(values: List[str], min_hits: int = 5) -> bool:
    hits = 0
    for val in values[:200]:
        s = str(val)
        if "=" in s and ("&" in s or s.count("=") >= 1):
            hits += 1
        if hits >= min_hits:
            return True
    return False


@st.cache_data(show_spinner=False)
def querystring_distinct_counts(
    files: List[str],
    query_string_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect(database=":memory:")
    qcol = _dq(query_string_col)
    qts = _dq(req_time_col)
    query = f"""
        SELECT
            CAST({qcol} AS VARCHAR) AS raw_query_string,
            COUNT(*) AS row_count
        FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND {qcol} IS NOT NULL
          AND TRIM(CAST({qcol} AS VARCHAR)) <> ''
        GROUP BY 1
    """
    raw_df = con.execute(query).df()
    if raw_df.empty:
        return pd.DataFrame(columns=["Key", "Distinct Count", "Filled Rows", "Fill %"]), pd.DataFrame()

    total_rows = int(raw_df["row_count"].sum())
    key_value_counts: Dict[str, Dict[str, int]] = {}
    key_filled_rows: Dict[str, int] = {}

    for _, row in raw_df.iterrows():
        raw = str(row["raw_query_string"])
        weight = int(row["row_count"] or 0)
        try:
            parsed_pairs = urllib.parse.parse_qsl(raw, keep_blank_values=True)
        except Exception:
            parsed_pairs = []
        seen_keys_in_raw = set()
        for key, value in parsed_pairs:
            key = str(key)
            value = urllib.parse.unquote_plus(str(value))
            key_value_counts.setdefault(key, {})[value] = key_value_counts.setdefault(key, {}).get(value, 0) + weight
            seen_keys_in_raw.add(key)
        for key in seen_keys_in_raw:
            key_filled_rows[key] = key_filled_rows.get(key, 0) + weight

    overview_rows = []
    top_rows = []
    for key, values in key_value_counts.items():
        filled = int(key_filled_rows.get(key, 0))
        overview_rows.append({
            "Key": key,
            "Distinct Count": len(values),
            "Filled Rows": filled,
            "Fill %": round(filled / total_rows * 100, 2) if total_rows else 0,
        })
        for value, count in sorted(values.items(), key=lambda x: x[1], reverse=True)[:100]:
            top_rows.append({"Key": key, "Value": value, "Count": count})

    overview_df = pd.DataFrame(overview_rows).sort_values(["Filled Rows", "Distinct Count"], ascending=False)
    top_df = pd.DataFrame(top_rows)
    return overview_df, top_df


@st.cache_data(show_spinner=False)
def compare_querystring_distinct_counts(
    files: List[str],
    query_string_col: str,
    req_time_col: str,
    a_start_epoch: int,
    a_end_epoch: int,
    b_start_epoch: int,
    b_end_epoch: int,
) -> pd.DataFrame:
    a_overview, _ = querystring_distinct_counts(files, query_string_col, req_time_col, a_start_epoch, a_end_epoch)
    b_overview, _ = querystring_distinct_counts(files, query_string_col, req_time_col, b_start_epoch, b_end_epoch)
    a = a_overview.set_index("Key") if not a_overview.empty else pd.DataFrame()
    b = b_overview.set_index("Key") if not b_overview.empty else pd.DataFrame()
    keys = sorted(set(a.index.tolist() if not a.empty else []) | set(b.index.tolist() if not b.empty else []))
    rows = []
    for key in keys:
        av = int(a.loc[key, "Distinct Count"]) if not a.empty and key in a.index else 0
        bv = int(b.loc[key, "Distinct Count"]) if not b.empty and key in b.index else 0
        delta = bv - av
        rows.append({
            "Key": key,
            "Range A Distinct": av,
            "Range B Distinct": bv,
            "Delta (B - A)": delta,
            "Delta %": round(delta / av * 100, 2) if av else None,
        })
    return pd.DataFrame(rows).sort_values("Delta (B - A)", ascending=False)


# ─────────────────────────────────────────────
# Watch-hour helpers
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def watch_summary_for_range(
    files: List[str],
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    tz_mode: str,
    device_source: str,
    query_col: str,
    device_col: str,
    device_key: str = "device_id",
    cliip_col: str = "cliIP",
    session_key: str = "session_id",
    gap_cap_seconds: int = 60,
) -> Tuple[dict, pd.DataFrame]:
    """
    Recommended Veto watch-hour estimate.

    Viewer identity:
      1. real device_id when available
      2. cliIP fallback only when device_id is missing

    Watch grouping:
      viewer_id + session_id when session_id exists, otherwise viewer_id only.

    Watch seconds:
      next reqTimeSec gap inside the group, capped at gap_cap_seconds.

    Speed improvement:
      deduplicates same watch_group_id + same timestamp before LEAD(). Your diagnostics
      showed many same-second duplicates that add zero watch seconds but make sorting heavy.
    """
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    parquet_sql = _path_list_sql(files)
    q_ts = _dq(req_time_col)
    time_filter = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}"
    device_sql = _device_expr(device_source, query_col, device_col, device_key)
    session_sql = _query_key_expr(query_col, session_key) if query_col and session_key else "NULL"
    ip_sql = f"CAST({_dq(cliip_col)} AS VARCHAR)" if cliip_col else "NULL"
    event_date_sql = _display_date_expr(req_time_col, tz_mode)
    cap = int(gap_cap_seconds)

    base_cte = f"""
        WITH base AS (
            SELECT
                TRY_CAST({q_ts} AS BIGINT) AS event_ts,
                {event_date_sql} AS event_date,
                NULLIF(TRIM(COALESCE(CAST({device_sql} AS VARCHAR), '')), '') AS raw_device_id,
                NULLIF(TRIM(COALESCE(CAST({ip_sql} AS VARCHAR), '')), '') AS raw_cliip,
                NULLIF(TRIM(COALESCE(CAST({session_sql} AS VARCHAR), '')), '') AS raw_session_id
            FROM read_parquet({parquet_sql}, union_by_name=true)
            WHERE {time_filter}
              AND TRY_CAST({q_ts} AS BIGINT) IS NOT NULL
        ), identified AS (
            SELECT
                *,
                CASE
                    WHEN raw_device_id IS NOT NULL THEN raw_device_id
                    WHEN raw_cliip IS NOT NULL THEN 'ip:' || raw_cliip
                    ELSE NULL
                END AS viewer_id,
                CASE
                    WHEN raw_device_id IS NOT NULL THEN 'device_id'
                    WHEN raw_cliip IS NOT NULL THEN 'cliIP_fallback'
                    ELSE 'missing'
                END AS viewer_type,
                COALESCE(raw_session_id, '') AS session_id
            FROM base
        ), row_counts AS (
            SELECT
                COUNT(*) AS rows_in_range,
                SUM(CASE WHEN viewer_type = 'device_id' THEN 1 ELSE 0 END) AS rows_with_device_id,
                SUM(CASE WHEN viewer_type = 'cliIP_fallback' THEN 1 ELSE 0 END) AS rows_using_cliip_fallback,
                SUM(CASE WHEN viewer_type = 'missing' THEN 1 ELSE 0 END) AS rows_missing_viewer
            FROM identified
        ), filtered AS (
            SELECT
                *,
                CASE
                    WHEN session_id <> '' THEN viewer_id || '|session:' || session_id
                    ELSE viewer_id
                END AS watch_group_id
            FROM identified
            WHERE viewer_id IS NOT NULL
        ), dedup AS (
            SELECT
                watch_group_id,
                viewer_id,
                viewer_type,
                session_id,
                event_ts,
                MIN(event_date) AS event_date
            FROM filtered
            GROUP BY 1, 2, 3, 4, 5
        ), seq AS (
            SELECT
                *,
                LEAD(event_ts) OVER (PARTITION BY watch_group_id ORDER BY event_ts) AS next_event_ts
            FROM dedup
        ), scored AS (
            SELECT
                *,
                CASE
                    WHEN next_event_ts IS NULL THEN 0
                    WHEN next_event_ts - event_ts < 0 THEN 0
                    WHEN next_event_ts - event_ts > {cap} THEN {cap}
                    ELSE next_event_ts - event_ts
                END AS watch_sec_est
            FROM seq
        )
    """

    summary_query = base_cte + """
        SELECT
            MAX(rc.rows_in_range) AS rows_in_range,
            MAX(rc.rows_with_device_id) AS rows_with_device_id,
            MAX(rc.rows_using_cliip_fallback) AS rows_using_cliip_fallback,
            MAX(rc.rows_missing_viewer) AS rows_missing_viewer,
            COUNT(*) AS rows_used_for_watch_after_dedup,
            COUNT(DISTINCT CASE WHEN viewer_type = 'device_id' THEN viewer_id END) AS total_device_ids,
            COUNT(DISTINCT CASE WHEN viewer_type = 'cliIP_fallback' THEN viewer_id END) AS total_cliip_fallback_viewers,
            COUNT(DISTINCT viewer_id) AS total_viewers,
            COUNT(DISTINCT CASE WHEN session_id <> '' THEN session_id END) AS total_sessions,
            ROUND(SUM(watch_sec_est) / 3600.0, 4) AS total_watch_hours,
            ROUND(SUM(CASE WHEN viewer_type = 'device_id' THEN watch_sec_est ELSE 0 END) / 3600.0, 4) AS device_id_watch_hours,
            ROUND(SUM(CASE WHEN viewer_type = 'cliIP_fallback' THEN watch_sec_est ELSE 0 END) / 3600.0, 4) AS cliip_fallback_watch_hours,
            ROUND(SUM(watch_sec_est) / 60.0 / NULLIF(COUNT(DISTINCT viewer_id), 0), 2) AS watch_minutes_per_viewer,
            MIN(event_date) AS first_date,
            MAX(event_date) AS last_date
        FROM scored
        CROSS JOIN row_counts rc
    """
    row = con.execute(summary_query).fetchone()
    summary = {
        "Rows in Range": int(row[0] or 0),
        "Rows with Device_ID": int(row[1] or 0),
        "Rows using cliIP fallback": int(row[2] or 0),
        "Rows missing viewer": int(row[3] or 0),
        "Rows Used For Watch After Dedup": int(row[4] or 0),
        "Total Device_ID Count": int(row[5] or 0),
        "Total cliIP Fallback Viewer Count": int(row[6] or 0),
        "Total Viewer Count": int(row[7] or 0),
        "Total Session Count": int(row[8] or 0),
        "Total Watch Hours": float(row[9] or 0),
        "Device_ID Watch Hours": float(row[10] or 0),
        "cliIP Fallback Watch Hours": float(row[11] or 0),
        "Watch Minutes / Viewer": float(row[12] or 0),
        "First Date": row[13],
        "Last Date": row[14],
    }

    daily_query = base_cte + """
        SELECT
            event_date AS Date,
            COUNT(*) AS "Rows Used After Dedup",
            COUNT(DISTINCT viewer_id) AS "Total Viewer Count",
            COUNT(DISTINCT CASE WHEN viewer_type = 'device_id' THEN viewer_id END) AS "Total Device_ID Count",
            COUNT(DISTINCT CASE WHEN viewer_type = 'cliIP_fallback' THEN viewer_id END) AS "cliIP Fallback Viewer Count",
            ROUND(SUM(watch_sec_est) / 3600.0, 4) AS "Total Watch Hours",
            ROUND(SUM(CASE WHEN viewer_type = 'device_id' THEN watch_sec_est ELSE 0 END) / 3600.0, 4) AS "Device_ID Watch Hours",
            ROUND(SUM(CASE WHEN viewer_type = 'cliIP_fallback' THEN watch_sec_est ELSE 0 END) / 3600.0, 4) AS "cliIP Fallback Watch Hours",
            ROUND(SUM(watch_sec_est) / 60.0 / NULLIF(COUNT(DISTINCT viewer_id), 0), 2) AS "Watch Minutes / Viewer"
        FROM scored
        GROUP BY 1
        ORDER BY 1
    """
    daily_df = con.execute(daily_query).df()
    return summary, daily_df


@st.cache_data(show_spinner=False)
def compare_watch_summary_for_ranges(
    files: List[str],
    req_time_col: str,
    a_start_epoch: int,
    a_end_epoch: int,
    b_start_epoch: int,
    b_end_epoch: int,
    tz_mode: str,
    device_source: str,
    query_col: str,
    device_col: str,
    device_key: str = "device_id",
    cliip_col: str = "cliIP",
    session_key: str = "session_id",
    gap_cap_seconds: int = 60,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    a_summary, a_daily = watch_summary_for_range(
        files, req_time_col, a_start_epoch, a_end_epoch, tz_mode,
        device_source, query_col, device_col, device_key, cliip_col, session_key, gap_cap_seconds,
    )
    b_summary, b_daily = watch_summary_for_range(
        files, req_time_col, b_start_epoch, b_end_epoch, tz_mode,
        device_source, query_col, device_col, device_key, cliip_col, session_key, gap_cap_seconds,
    )
    metrics = [
        "Rows in Range",
        "Rows with Device_ID",
        "Rows using cliIP fallback",
        "Rows missing viewer",
        "Rows Used For Watch After Dedup",
        "Total Device_ID Count",
        "Device_ID Watch Hours",
        "Total cliIP Fallback Viewer Count",
        "cliIP Fallback Watch Hours",
        "Total Viewer Count",
        "Total Session Count",
        "Total Watch Hours",
        "Watch Minutes / Viewer",
    ]
    rows = []
    for metric in metrics:
        av = a_summary.get(metric, 0) or 0
        bv = b_summary.get(metric, 0) or 0
        delta = bv - av
        rows.append({
            "Metric": metric,
            "Range A": av,
            "Range B": bv,
            "Delta (B - A)": delta,
            "Delta %": round(delta / av * 100, 2) if av else None,
        })
    return pd.DataFrame(rows), a_daily, b_daily


# ─────────────────────────────────────────────
# Sidebar: file selection
# ─────────────────────────────────────────────

require_duckdb()

with st.sidebar:
    st.title("📁 Files")
    root_folder = st.text_input("Folder location", value=st.session_state.get("root_folder", ""), placeholder=r"D:\Veto Logs Backup")
    scan_clicked = st.button("Scan folder", type="primary")

    if scan_clicked or (root_folder and root_folder != st.session_state.get("root_folder")):
        st.session_state["root_folder"] = root_folder
        with st.spinner("Scanning parquet files ..."):
            st.session_state["all_parquet_files"] = scan_parquet_files(root_folder)

    all_files = st.session_state.get("all_parquet_files", [])
    if all_files:
        st.success(f"Found {len(all_files):,} parquet files")
        select_all = st.checkbox("Select all files", value=True, key="select_all_files")
        if select_all:
            selected_files = all_files
            st.caption("All files selected")
        else:
            file_labels = [str(Path(f).relative_to(Path(root_folder))) if root_folder and Path(root_folder).exists() else Path(f).name for f in all_files]
            label_to_file = dict(zip(file_labels, all_files))
            selected_labels = st.multiselect("Select files", options=file_labels, default=file_labels[: min(20, len(file_labels))])
            selected_files = [label_to_file[x] for x in selected_labels]
    else:
        selected_files = []
        st.info("Enter a folder and scan.")


# ─────────────────────────────────────────────
# Main top controls
# ─────────────────────────────────────────────

st.title("📺 Veto Dashboard")

if not selected_files:
    st.info("Use the left sidebar to select parquet files.")
    st.stop()

schema_df = read_schema(selected_files)
if schema_df.empty:
    st.error("Could not read schema from selected parquet files.")
    st.stop()

columns = schema_df["column_name"].tolist()

req_time_index = columns.index(REQ_TIME_COL_DEFAULT) if REQ_TIME_COL_DEFAULT in columns else 0
req_time_col = st.selectbox("Timestamp column", options=columns, index=req_time_index, key="req_time_col")

tz_mode = st.radio("Timezone for date/time selection", ["IST", "UTC"], horizontal=True, key="tz_mode")

min_epoch, max_epoch = get_reqtime_range(selected_files, req_time_col)
if min_epoch is None or max_epoch is None:
    st.error(f"Could not detect available date range from `{req_time_col}`.")
    st.stop()

min_dt = epoch_to_display_dt(min_epoch, tz_mode)
max_dt = epoch_to_display_dt(max_epoch, tz_mode)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Selected files", f"{len(selected_files):,}")
m2.metric("Columns", f"{len(columns):,}")
m3.metric("Available from", min_dt.strftime("%Y-%m-%d %H:%M:%S"))
m4.metric("Available to", max_dt.strftime("%Y-%m-%d %H:%M:%S"))

with st.expander("Selected files", expanded=False):
    st.dataframe(pd.DataFrame({"file": selected_files}), use_container_width=True, hide_index=True, height=260)

with st.expander("Columns", expanded=False):
    st.dataframe(schema_df, use_container_width=True, hide_index=True, height=260)

st.markdown("---")
comparison_mode = st.toggle("Comparison mode", value=False, help="Compare two dates/date ranges side by side.")

if comparison_mode:
    st.subheader("Choose date/time ranges")
    ca, cb = st.columns(2)
    with ca:
        st.markdown("#### Range A")
        a_dates = st.date_input(
            "Range A date range",
            value=(min_dt.date(), min(max_dt.date(), min_dt.date())),
            min_value=min_dt.date(),
            max_value=max_dt.date(),
            key="range_a_dates",
        )
        a_start_date, a_end_date = a_dates if isinstance(a_dates, tuple) and len(a_dates) == 2 else (min_dt.date(), min_dt.date())
        a_start_time = st.time_input("Range A start time", value=dtime(0, 0), key="range_a_start_time")
        a_end_time = st.time_input("Range A end time", value=dtime(23, 59), key="range_a_end_time")
    with cb:
        st.markdown("#### Range B")
        b_dates = st.date_input(
            "Range B date range",
            value=(max_dt.date(), max_dt.date()),
            min_value=min_dt.date(),
            max_value=max_dt.date(),
            key="range_b_dates",
        )
        b_start_date, b_end_date = b_dates if isinstance(b_dates, tuple) and len(b_dates) == 2 else (max_dt.date(), max_dt.date())
        b_start_time = st.time_input("Range B start time", value=dtime(0, 0), key="range_b_start_time")
        b_end_time = st.time_input("Range B end time", value=dtime(23, 59), key="range_b_end_time")

    a_start_epoch = date_time_to_epoch(a_start_date, a_start_time, tz_mode)
    a_end_epoch = date_time_to_epoch(a_end_date, a_end_time, tz_mode)
    b_start_epoch = date_time_to_epoch(b_start_date, b_start_time, tz_mode)
    b_end_epoch = date_time_to_epoch(b_end_date, b_end_time, tz_mode)

    range_a_label = f"{a_start_date} {a_start_time} → {a_end_date} {a_end_time}"
    range_b_label = f"{b_start_date} {b_start_time} → {b_end_date} {b_end_time}"

    with st.spinner("Counting rows for both ranges ..."):
        rows_a = count_rows(selected_files, req_time_col, a_start_epoch, a_end_epoch)
        rows_b = count_rows(selected_files, req_time_col, b_start_epoch, b_end_epoch)
    r1, r2, r3 = st.columns(3)
    r1.metric("Range A rows", f"{rows_a:,}")
    r2.metric("Range B rows", f"{rows_b:,}", delta=f"{rows_b - rows_a:,}")
    r3.metric("Row delta %", f"{((rows_b - rows_a) / rows_a * 100):,.2f}%" if rows_a else "—")
else:
    st.subheader("Choose date/time range")
    default_start = min_dt.date()
    default_end = max_dt.date()
    date_range = st.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=min_dt.date(),
        max_value=max_dt.date(),
        key="single_date_range",
    )
    start_date, end_date = date_range if isinstance(date_range, tuple) and len(date_range) == 2 else (default_start, default_end)
    t1, t2 = st.columns(2)
    with t1:
        start_time = st.time_input("Start time", value=dtime(0, 0), key="single_start_time")
    with t2:
        end_time = st.time_input("End time", value=dtime(23, 59), key="single_end_time")
    start_epoch = date_time_to_epoch(start_date, start_time, tz_mode)
    end_epoch = date_time_to_epoch(end_date, end_time, tz_mode)
    range_label = f"{start_date} {start_time} → {end_date} {end_time}"
    with st.spinner("Counting rows for selected range ..."):
        selected_rows = count_rows(selected_files, req_time_col, start_epoch, end_epoch)
    st.metric("Rows in selected range", f"{selected_rows:,}")


# ─────────────────────────────────────────────
# Watch-hour section
# ─────────────────────────────────────────────

st.markdown("---")
st.header("Watch hours and viewer count")
st.caption(
    "Recommended method: use device_id from queryStr first, use cliIP only when device_id is missing, "
    "then calculate watch time within viewer + session when session_id exists."
)

none_plus_cols = [""] + columns
with st.expander("Viewer identity and watch-hour settings", expanded=True):
    s1, s2, s3 = st.columns(3)
    with s1:
        device_source = st.radio("Device_ID source", ["Parsed from query string", "Direct column"], horizontal=False, key="watch_device_source")
        default_qs_idx = _guess_option(columns, ["queryStr", "qrystr", "query_string"], include_blank=True)
        watch_qs_col = st.selectbox("Query string column", options=none_plus_cols, index=default_qs_idx, key="watch_qs_col")
        watch_device_key = st.text_input("Device_ID query-string key", value="device_id", key="watch_device_key")
    with s2:
        default_device_idx = _guess_option(columns, ["device_id", "deviceId", "device"], include_blank=True)
        watch_device_col = st.selectbox(
            "Device_ID direct column",
            options=none_plus_cols,
            index=default_device_idx,
            key="watch_device_col",
            disabled=device_source != "Direct column",
        )
        watch_session_key = st.text_input("Session query-string key", value="session_id", key="watch_session_key")
    with s3:
        default_ip_idx = _guess_option(columns, ["cliIP", "client_ip", "ip"], include_blank=True)
        watch_cliip_col = st.selectbox("cliIP fallback column", options=none_plus_cols, index=default_ip_idx, key="watch_cliip_col")
        gap_cap = st.number_input(
            "Max seconds counted between two requests",
            min_value=10,
            max_value=600,
            value=DEFAULT_GAP_CAP_SECONDS,
            step=10,
            key="watch_gap_cap",
            help="Safety limit. Example: if requests are 5 minutes apart, count only this many seconds as watch time.",
        )

if device_source == "Parsed from query string" and not watch_qs_col:
    st.warning("Select a query-string column to extract device_id/session_id.")
elif device_source == "Direct column" and not watch_device_col:
    st.warning("Select a direct Device_ID column.")
elif not watch_cliip_col:
    st.warning("Select cliIP fallback column. It is needed for rows where Device_ID is missing.")
else:
    if st.button("▶️ Calculate watch hours", type="primary", key="watch_run"):
        with st.spinner("Calculating viewer/session watch hours ..."):
            if comparison_mode:
                compare_df, daily_a, daily_b = compare_watch_summary_for_ranges(
                    selected_files,
                    req_time_col,
                    a_start_epoch,
                    a_end_epoch,
                    b_start_epoch,
                    b_end_epoch,
                    tz_mode,
                    device_source,
                    watch_qs_col,
                    watch_device_col,
                    watch_device_key,
                    watch_cliip_col,
                    watch_session_key,
                    int(gap_cap),
                )
                st.session_state["watch_payload"] = {
                    "mode": "compare",
                    "key": (tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, watch_cliip_col, watch_session_key, int(gap_cap)),
                    "compare": compare_df,
                    "daily_a": daily_a,
                    "daily_b": daily_b,
                }
            else:
                summary, daily = watch_summary_for_range(
                    selected_files,
                    req_time_col,
                    start_epoch,
                    end_epoch,
                    tz_mode,
                    device_source,
                    watch_qs_col,
                    watch_device_col,
                    watch_device_key,
                    watch_cliip_col,
                    watch_session_key,
                    int(gap_cap),
                )
                st.session_state["watch_payload"] = {
                    "mode": "single",
                    "key": (tuple(selected_files), req_time_col, start_epoch, end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, watch_cliip_col, watch_session_key, int(gap_cap)),
                    "summary": summary,
                    "daily": daily,
                }

    expected_watch_key = (
        tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, watch_cliip_col, watch_session_key, int(gap_cap)
    ) if comparison_mode else (
        tuple(selected_files), req_time_col, start_epoch, end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, watch_cliip_col, watch_session_key, int(gap_cap)
    )
    watch_payload = st.session_state.get("watch_payload")

    def metric_value(metric_map: dict, metric: str, side: str):
        return metric_map.get(metric, {}).get(side, 0) if isinstance(metric_map.get(metric), dict) else 0

    if watch_payload and watch_payload.get("key") == expected_watch_key:
        if watch_payload.get("mode") == "compare":
            comp = watch_payload["compare"]
            metric_map = {row["Metric"]: row.to_dict() for _, row in comp.iterrows()}
            left, right = st.columns(2)
            with left:
                st.markdown("#### Range A")
                st.caption(range_a_label)
                a1, a2 = st.columns(2)
                a1.metric("Total Device_ID Count", f"{int(metric_value(metric_map, 'Total Device_ID Count', 'Range A')):,}")
                a2.metric("Device_ID Watch Hours", f"{float(metric_value(metric_map, 'Device_ID Watch Hours', 'Range A')):,.2f}")
                a3, a4 = st.columns(2)
                a3.metric("cliIP Fallback Viewers", f"{int(metric_value(metric_map, 'Total cliIP Fallback Viewer Count', 'Range A')):,}")
                a4.metric("cliIP Fallback Watch Hours", f"{float(metric_value(metric_map, 'cliIP Fallback Watch Hours', 'Range A')):,.2f}")
                a5, a6 = st.columns(2)
                a5.metric("Total Watch Hours", f"{float(metric_value(metric_map, 'Total Watch Hours', 'Range A')):,.2f}")
                a6.metric("Watch Minutes / Viewer", f"{float(metric_value(metric_map, 'Watch Minutes / Viewer', 'Range A')):,.2f}")
                st.metric("Rows with Device_ID", f"{int(metric_value(metric_map, 'Rows with Device_ID', 'Range A')):,}")
                st.metric("Rows using cliIP fallback", f"{int(metric_value(metric_map, 'Rows using cliIP fallback', 'Range A')):,}")
            with right:
                st.markdown("#### Range B")
                st.caption(range_b_label)
                b1, b2 = st.columns(2)
                b1.metric("Total Device_ID Count", f"{int(metric_value(metric_map, 'Total Device_ID Count', 'Range B')):,}", delta=f"{int(metric_value(metric_map, 'Total Device_ID Count', 'Delta (B - A)')):,}")
                b2.metric("Device_ID Watch Hours", f"{float(metric_value(metric_map, 'Device_ID Watch Hours', 'Range B')):,.2f}", delta=f"{float(metric_value(metric_map, 'Device_ID Watch Hours', 'Delta (B - A)')):,.2f}")
                b3, b4 = st.columns(2)
                b3.metric("cliIP Fallback Viewers", f"{int(metric_value(metric_map, 'Total cliIP Fallback Viewer Count', 'Range B')):,}", delta=f"{int(metric_value(metric_map, 'Total cliIP Fallback Viewer Count', 'Delta (B - A)')):,}")
                b4.metric("cliIP Fallback Watch Hours", f"{float(metric_value(metric_map, 'cliIP Fallback Watch Hours', 'Range B')):,.2f}", delta=f"{float(metric_value(metric_map, 'cliIP Fallback Watch Hours', 'Delta (B - A)')):,.2f}")
                b5, b6 = st.columns(2)
                b5.metric("Total Watch Hours", f"{float(metric_value(metric_map, 'Total Watch Hours', 'Range B')):,.2f}", delta=f"{float(metric_value(metric_map, 'Total Watch Hours', 'Delta (B - A)')):,.2f}")
                b6.metric("Watch Minutes / Viewer", f"{float(metric_value(metric_map, 'Watch Minutes / Viewer', 'Range B')):,.2f}", delta=f"{float(metric_value(metric_map, 'Watch Minutes / Viewer', 'Delta (B - A)')):,.2f}")
                st.metric("Rows with Device_ID", f"{int(metric_value(metric_map, 'Rows with Device_ID', 'Range B')):,}", delta=f"{int(metric_value(metric_map, 'Rows with Device_ID', 'Delta (B - A)')):,}")
                st.metric("Rows using cliIP fallback", f"{int(metric_value(metric_map, 'Rows using cliIP fallback', 'Range B')):,}", delta=f"{int(metric_value(metric_map, 'Rows using cliIP fallback', 'Delta (B - A)')):,}")

            st.markdown("#### Watch comparison table")
            st.dataframe(comp, use_container_width=True, hide_index=True, height=390)
            st.download_button(
                "Download watch comparison CSV",
                data=comp.to_csv(index=False).encode("utf-8"),
                file_name="veto_watch_hour_comparison.csv",
                mime="text/csv",
            )
            da, db = st.tabs(["Range A daily watch hours", "Range B daily watch hours"])
            with da:
                st.dataframe(watch_payload["daily_a"], use_container_width=True, hide_index=True, height=360)
            with db:
                st.dataframe(watch_payload["daily_b"], use_container_width=True, hide_index=True, height=360)
        else:
            summary = watch_payload["summary"]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Viewer Count", f"{int(summary.get('Total Viewer Count', 0)):,}")
            k2.metric("Total Watch Hours", f"{float(summary.get('Total Watch Hours', 0)):,.2f}")
            k3.metric("Total Device_ID Count", f"{int(summary.get('Total Device_ID Count', 0)):,}")
            k4.metric("Watch Minutes / Viewer", f"{float(summary.get('Watch Minutes / Viewer', 0)):,.2f}")

            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Rows with Device_ID", f"{int(summary.get('Rows with Device_ID', 0)):,}")
            p2.metric("Device_ID Watch Hours", f"{float(summary.get('Device_ID Watch Hours', 0)):,.2f}")
            p3.metric("Rows using cliIP fallback", f"{int(summary.get('Rows using cliIP fallback', 0)):,}")
            p4.metric("cliIP Fallback Watch Hours", f"{float(summary.get('cliIP Fallback Watch Hours', 0)):,.2f}")

            q1, q2, q3, q4 = st.columns(4)
            q1.metric("cliIP Fallback Viewers", f"{int(summary.get('Total cliIP Fallback Viewer Count', 0)):,}")
            q2.metric("Session Count", f"{int(summary.get('Total Session Count', 0)):,}")
            q3.metric("Rows after dedup", f"{int(summary.get('Rows Used For Watch After Dedup', 0)):,}")
            q4.metric("Rows missing viewer", f"{int(summary.get('Rows missing viewer', 0)):,}")

            st.caption(range_label)
            st.markdown("#### Daily watch-hour summary")
            daily = watch_payload["daily"]
            st.dataframe(daily, use_container_width=True, hide_index=True, height=380)
            st.download_button(
                "Download daily watch-hour summary CSV",
                data=daily.to_csv(index=False).encode("utf-8"),
                file_name="veto_daily_watch_hours.csv",
                mime="text/csv",
            )
    elif watch_payload:
        st.info("Selections changed. Calculate watch hours again for the current files/date range.")
    else:
        st.info("Click **Calculate watch hours** to show viewer counts and estimated watch hours.")


# ─────────────────────────────────────────────
# Boxed distinct counts
# ─────────────────────────────────────────────

st.markdown("---")
st.header("Boxed distinct counts")
st.caption("Each column loads only when you click its Load button. This avoids scanning all columns at once.")

search_col = st.text_input("Search columns", placeholder="type part of a column name ...", key="distinct_col_search")
cols_to_show = [c for c in columns if search_col.lower() in c.lower()] if search_col else columns
max_cards = st.slider("How many column cards to show", min_value=12, max_value=min(max(len(cols_to_show), 12), 200), value=min(36, max(len(cols_to_show), 12)), step=12)
cols_to_show = cols_to_show[:max_cards]

card_cols = st.columns(3)
for idx, col in enumerate(cols_to_show):
    with card_cols[idx % 3]:
        st.markdown(f"<div class='small-card'><div class='small-card-title'>{col}</div>", unsafe_allow_html=True)
        state_key = f"distinct_card_{hash((tuple(selected_files), col, req_time_col, comparison_mode))}"
        if st.button("Load", key=f"load_distinct_{idx}_{col}"):
            with st.spinner(f"Loading distinct count for {col} ..."):
                if comparison_mode:
                    st.session_state[state_key] = compare_distinct_count_one_column(
                        selected_files, col, req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch
                    )
                else:
                    st.session_state[state_key] = distinct_count_one_column(selected_files, col, req_time_col, start_epoch, end_epoch)
        result = st.session_state.get(state_key)
        if result:
            if comparison_mode:
                st.metric("Range A", f"{int(result.get('Range A Distinct', 0)):,}")
                st.metric("Range B", f"{int(result.get('Range B Distinct', 0)):,}", delta=f"{int(result.get('Delta (B - A)', 0)):,}")
            else:
                st.metric("Distinct", f"{int(result.get('Distinct Count', 0)):,}")
                st.caption(f"Filled rows: {int(result.get('Filled Rows', 0)):,} | Fill: {float(result.get('Fill %', 0)):,.2f}%")
        else:
            st.markdown("<div class='small-muted'>Not loaded yet</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Query-string / normal column distinct
# ─────────────────────────────────────────────

st.markdown("---")
st.header("Query String and column distinct")
qsd_col = st.selectbox(
    "Select query-string or normal column",
    options=columns,
    index=columns.index("queryStr") if "queryStr" in columns else 0,
    key="qsd_col",
)
force_parse = st.checkbox("Force query-string parsing", value=False, key="force_qs_parse")

if st.button("Run selected column distinct", type="primary", key="run_qsd"):
    range_for_sample = (a_start_epoch, a_end_epoch) if comparison_mode else (start_epoch, end_epoch)
    with st.spinner("Checking selected column type ..."):
        samples = sample_column_values(selected_files, qsd_col, req_time_col, range_for_sample[0], range_for_sample[1], limit=500)
    should_parse = force_parse or looks_like_querystring(samples)

    if should_parse:
        with st.spinner("Parsing query-string values ..."):
            if comparison_mode:
                out = compare_querystring_distinct_counts(selected_files, qsd_col, req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch)
                st.session_state["qsd_payload"] = {"mode": "compare_qs", "data": out, "col": qsd_col}
            else:
                overview, top = querystring_distinct_counts(selected_files, qsd_col, req_time_col, start_epoch, end_epoch)
                st.session_state["qsd_payload"] = {"mode": "single_qs", "overview": overview, "top": top, "col": qsd_col}
    else:
        with st.spinner("Running fast direct distinct count ..."):
            if comparison_mode:
                direct = compare_distinct_count_one_column(selected_files, qsd_col, req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch)
                top_a = top_values_one_column(selected_files, qsd_col, req_time_col, a_start_epoch, a_end_epoch, limit=50)
                top_b = top_values_one_column(selected_files, qsd_col, req_time_col, b_start_epoch, b_end_epoch, limit=50)
                st.session_state["qsd_payload"] = {"mode": "compare_direct", "direct": direct, "top_a": top_a, "top_b": top_b, "col": qsd_col}
            else:
                direct = distinct_count_one_column(selected_files, qsd_col, req_time_col, start_epoch, end_epoch)
                top = top_values_one_column(selected_files, qsd_col, req_time_col, start_epoch, end_epoch, limit=100)
                st.session_state["qsd_payload"] = {"mode": "single_direct", "direct": direct, "top": top, "col": qsd_col}

payload = st.session_state.get("qsd_payload")
if payload:
    st.subheader(f"Results for `{payload.get('col')}`")
    mode = payload.get("mode")
    if mode == "single_qs":
        st.markdown("#### Parsed query-string key distinct counts")
        st.dataframe(payload["overview"], use_container_width=True, hide_index=True, height=350)
        st.download_button("Download query-string key distinct CSV", data=payload["overview"].to_csv(index=False).encode("utf-8"), file_name="querystring_key_distinct_counts.csv", mime="text/csv")
        if not payload["top"].empty:
            keys = payload["overview"]["Key"].tolist()
            chosen_key = st.selectbox("Show top values for key", options=keys, key="qsd_key_top")
            st.dataframe(payload["top"][payload["top"]["Key"] == chosen_key], use_container_width=True, hide_index=True, height=300)
    elif mode == "compare_qs":
        st.markdown("#### Parsed query-string key comparison")
        st.dataframe(payload["data"], use_container_width=True, hide_index=True, height=430)
        st.download_button("Download query-string comparison CSV", data=payload["data"].to_csv(index=False).encode("utf-8"), file_name="querystring_key_distinct_comparison.csv", mime="text/csv")
    elif mode == "single_direct":
        d = payload["direct"]
        c1, c2, c3 = st.columns(3)
        c1.metric("Distinct Count", f"{int(d.get('Distinct Count', 0)):,}")
        c2.metric("Filled Rows", f"{int(d.get('Filled Rows', 0)):,}")
        c3.metric("Fill %", f"{float(d.get('Fill %', 0)):,.2f}%")
        st.markdown("#### Top values")
        st.dataframe(payload["top"], use_container_width=True, hide_index=True, height=350)
    elif mode == "compare_direct":
        d = payload["direct"]
        c1, c2, c3 = st.columns(3)
        c1.metric("Range A Distinct", f"{int(d.get('Range A Distinct', 0)):,}")
        c2.metric("Range B Distinct", f"{int(d.get('Range B Distinct', 0)):,}", delta=f"{int(d.get('Delta (B - A)', 0)):,}")
        c3.metric("Delta %", f"{d.get('Delta %') if d.get('Delta %') is not None else '—'}")
        ta, tb = st.tabs(["Range A top values", "Range B top values"])
        with ta:
            st.dataframe(payload["top_a"], use_container_width=True, hide_index=True, height=330)
        with tb:
            st.dataframe(payload["top_b"], use_container_width=True, hide_index=True, height=330)

st.markdown("---")
st.caption("Veto Dashboard mini app — Streamlit + DuckDB + Parquet")
