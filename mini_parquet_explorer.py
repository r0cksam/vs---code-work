import os
import urllib.parse
from pathlib import Path
from datetime import datetime, time as dtime
from typing import List, Tuple

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

st.set_page_config(page_title=APP_TITLE, page_icon="🧮", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.2rem; }
        div[data-testid="stMetric"] { background: rgba(128,128,128,0.08); padding: 0.8rem; border-radius: 0.8rem; }
        div[data-testid="stSidebarContent"] { padding-top: 1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _dq(identifier: str) -> str:
    """DuckDB-safe quoted identifier."""
    return '"' + str(identifier).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    """Return a Python-list SQL literal DuckDB accepts in read_parquet([...])."""
    safe = [str(Path(f)).replace("\\", "/") for f in files]
    return repr(safe)


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
    query = f"DESCRIBE SELECT * FROM read_parquet({_path_list_sql(files)}, union_by_name=true) LIMIT 0"
    return con.execute(query).df()[["column_name", "column_type"]].rename(columns={"column_type": "data_type"})


@st.cache_data(show_spinner=False)
def get_reqtime_range(files: List[str], req_time_col: str) -> Tuple[int | None, int | None]:
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


def epoch_to_display_dt(epoch: int, tz_name: str) -> datetime:
    if pytz is None:
        return datetime.fromtimestamp(epoch)
    tz = pytz.timezone("Asia/Kolkata") if tz_name == "IST" else pytz.utc
    return datetime.fromtimestamp(epoch, tz=tz)


def date_time_to_epoch(selected_date, selected_time, tz_name: str) -> int:
    dt = datetime.combine(selected_date, selected_time)
    if pytz is None:
        # Fallback: interpret as local naive timestamp.
        return int(dt.timestamp())
    tz = pytz.timezone("Asia/Kolkata") if tz_name == "IST" else pytz.utc
    return int(tz.localize(dt).timestamp())


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
) -> dict:
    """Exact distinct count for one column inside the selected reqTimeSec range."""
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass
    parquet_sql = _path_list_sql(files)
    q_col = _dq(column)
    q_ts = _dq(req_time_col)
    time_filter = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}"
    query = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {q_col} IS NOT NULL AND TRIM(CAST({q_col} AS VARCHAR)) <> '' THEN 1 ELSE 0 END) AS filled_rows,
            COUNT(DISTINCT CASE WHEN {q_col} IS NOT NULL AND TRIM(CAST({q_col} AS VARCHAR)) <> '' THEN CAST({q_col} AS VARCHAR) END) AS distinct_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE {time_filter}
    """
    total_rows, filled_rows, distinct_count = con.execute(query).fetchone()
    total_rows = int(total_rows or 0)
    filled_rows = int(filled_rows or 0)
    distinct_count = int(distinct_count or 0)
    return {
        "Column": column,
        "Distinct Count": distinct_count,
        "Filled Rows": filled_rows,
        "Total Rows": total_rows,
        "Fill %": round((filled_rows / total_rows * 100), 2) if total_rows else 0.0,
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
) -> dict:
    """Exact Range A vs Range B distinct count for one column."""
    a = distinct_count_one_column(files, column, req_time_col, a_start_epoch, a_end_epoch)
    b = distinct_count_one_column(files, column, req_time_col, b_start_epoch, b_end_epoch)
    a_distinct = int(a.get("Distinct Count", 0) or 0)
    b_distinct = int(b.get("Distinct Count", 0) or 0)
    delta = b_distinct - a_distinct
    return {
        "Column": column,
        "Range A Distinct": a_distinct,
        "Range B Distinct": b_distinct,
        "Delta (B - A)": delta,
        "Delta %": round(delta / a_distinct * 100, 2) if a_distinct else None,
        "Range A Filled Rows": int(a.get("Filled Rows", 0) or 0),
        "Range B Filled Rows": int(b.get("Filled Rows", 0) or 0),
    }


def _default_query_string_column(columns: List[str]) -> str:
    for name in ["queryStr", "qrystr", "query_string", "querystring"]:
        for col in columns:
            if col.lower() == name.lower():
                return col
    for col in columns:
        if "query" in col.lower():
            return col
    return ""


def _default_device_column(columns: List[str]) -> str:
    for name in ["device_id", "deviceId", "device", "clientDeviceId"]:
        for col in columns:
            if col.lower() == name.lower():
                return col
    for col in columns:
        if "device" in col.lower():
            return col
    return ""


def _default_ip_column(columns: List[str]) -> str:
    for name in ["cliIP", "clientIP", "client_ip", "ip", "IP"]:
        for col in columns:
            if col.lower() == name.lower():
                return col
    for col in columns:
        low = col.lower()
        if low in {"ip", "clientip"} or "cliip" in low or "client_ip" in low:
            return col
    return ""


def _query_key_expr(query_col: str, key: str) -> str:
    if not query_col or not key:
        return "NULL"
    # Matches raw query strings such as a=1&device_id=x or URLs ending in ?device_id=x.
    safe_key = str(key).replace("'", "''")
    return f"regexp_extract(CAST({_dq(query_col)} AS VARCHAR), '(?:^|[?&]){safe_key}=([^&]+)', 1)"


def _device_expr(device_source: str, query_col: str, device_col: str, device_key: str) -> str:
    if device_source == "Parsed from query string":
        return _query_key_expr(query_col, device_key)
    if device_source == "Direct column":
        return f"CAST({_dq(device_col)} AS VARCHAR)" if device_col else "NULL"
    return "NULL"


def _display_date_expr(req_time_col: str, tz_name: str) -> str:
    zone = "Asia/Kolkata" if tz_name == "IST" else "UTC"
    return f"CAST(to_timestamp(TRY_CAST({_dq(req_time_col)} AS BIGINT)) AT TIME ZONE '{zone}' AS DATE)"


@st.cache_data(show_spinner=False)
def watch_summary_for_range(
    files: List[str],
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    tz_name: str,
    device_source: str,
    query_col: str,
    device_col: str,
    device_key: str = "device_id",
    gap_cap_seconds: int = 60,
    cliip_col: str = "",
    session_key: str = "session_id",
) -> Tuple[dict, pd.DataFrame]:
    """
    Recommended Veto watch-hour estimate from request logs.

    Viewer identity:
      1) real device_id when present
      2) cliIP fallback when device_id is missing

    Watch grouping:
      viewer_id + session_id when session_id exists, otherwise viewer_id only.

    Watch seconds:
      next reqTimeSec gap inside the group, capped at gap_cap_seconds, then summed / 3600.
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
    event_date_sql = _display_date_expr(req_time_col, tz_name)
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
        ), filtered AS (
            SELECT
                *,
                CASE
                    WHEN session_id <> '' THEN viewer_id || '|session:' || session_id
                    ELSE viewer_id
                END AS watch_group_id
            FROM identified
            WHERE viewer_id IS NOT NULL
        ), seq AS (
            SELECT
                *,
                LEAD(event_ts) OVER (PARTITION BY watch_group_id ORDER BY event_ts) AS next_event_ts
            FROM filtered
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
            COUNT(*) AS rows_used_for_watch,
            SUM(CASE WHEN viewer_type = 'device_id' THEN 1 ELSE 0 END) AS rows_with_device_id,
            SUM(CASE WHEN viewer_type = 'cliIP_fallback' THEN 1 ELSE 0 END) AS rows_using_cliip_fallback,
            COUNT(DISTINCT CASE WHEN viewer_type = 'device_id' THEN viewer_id END) AS total_device_ids,
            COUNT(DISTINCT CASE WHEN viewer_type = 'cliIP_fallback' THEN viewer_id END) AS total_cliip_fallback_viewers,
            COUNT(DISTINCT viewer_id) AS total_viewers,
            COUNT(DISTINCT CASE WHEN session_id <> '' THEN session_id END) AS total_sessions,
            ROUND(SUM(watch_sec_est) / 3600.0, 4) AS total_watch_hours,
            ROUND(SUM(watch_sec_est) / 3600.0 / NULLIF(COUNT(DISTINCT viewer_id), 0), 4) AS watch_hours_per_viewer,
            MIN(event_date) AS first_date,
            MAX(event_date) AS last_date
        FROM scored
    """
    row = con.execute(summary_query).fetchone()
    summary = {
        "Rows Used For Watch": int(row[0] or 0),
        "Rows with Device_ID": int(row[1] or 0),
        "Rows using cliIP fallback": int(row[2] or 0),
        "Total Device_ID Count": int(row[3] or 0),
        "Total cliIP Fallback Viewer Count": int(row[4] or 0),
        "Total Viewer Count": int(row[5] or 0),
        "Total Session Count": int(row[6] or 0),
        "Total Watch Hours": float(row[7] or 0),
        "Watch Minutes / Viewer": float(row[8] or 0),
        "First Date": row[9],
        "Last Date": row[10],
    }

    daily_query = base_cte + """
        SELECT
            event_date AS Date,
            COUNT(*) AS Rows,
            SUM(CASE WHEN viewer_type = 'device_id' THEN 1 ELSE 0 END) AS "Rows with Device_ID",
            SUM(CASE WHEN viewer_type = 'cliIP_fallback' THEN 1 ELSE 0 END) AS "Rows using cliIP fallback",
            COUNT(DISTINCT CASE WHEN viewer_type = 'device_id' THEN viewer_id END) AS "Total Device_ID Count",
            COUNT(DISTINCT CASE WHEN viewer_type = 'cliIP_fallback' THEN viewer_id END) AS "cliIP Fallback Viewer Count",
            COUNT(DISTINCT viewer_id) AS "Total Viewer Count",
            COUNT(DISTINCT CASE WHEN session_id <> '' THEN session_id END) AS "Session Count",
            ROUND(SUM(watch_sec_est) / 3600.0, 4) AS "Total Watch Hours",
            ROUND(SUM(watch_sec_est) / 60.0 / NULLIF(COUNT(DISTINCT viewer_id), 0), 2) AS watch_minutes_per_viewer,
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
    tz_name: str,
    device_source: str,
    query_col: str,
    device_col: str,
    device_key: str = "device_id",
    gap_cap_seconds: int = 60,
    cliip_col: str = "",
    session_key: str = "session_id",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    a_summary, a_daily = watch_summary_for_range(files, req_time_col, a_start_epoch, a_end_epoch, tz_name, device_source, query_col, device_col, device_key, gap_cap_seconds, cliip_col, session_key)
    b_summary, b_daily = watch_summary_for_range(files, req_time_col, b_start_epoch, b_end_epoch, tz_name, device_source, query_col, device_col, device_key, gap_cap_seconds, cliip_col, session_key)
    metrics = [
        "Rows Used For Watch",
        "Rows with Device_ID",
        "Rows using cliIP fallback",
        "Total Device_ID Count",
        "Total cliIP Fallback Viewer Count",
        "Total Viewer Count",
        "Total Session Count",
        "Total Watch Hours",
        "Watch Minutes / Viewer"
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


def distinct_counts_for_columns_live(
    files: List[str],
    columns: List[str],
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
) -> pd.DataFrame:
    """Count exact distinct values and update the Streamlit table after each column."""
    if not files or not columns:
        return pd.DataFrame(columns=["Column", "Distinct Count", "Status"])

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    parquet_sql = _path_list_sql(files)
    time_filter = f"TRY_CAST({_dq(req_time_col)} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}"

    rows = []
    progress = st.progress(0, text="Starting distinct counts ...")
    status_box = st.empty()
    table_box = st.empty()

    for idx, col in enumerate(columns, start=1):
        progress.progress(int((idx - 1) / max(len(columns), 1) * 100), text=f"Processing {idx}/{len(columns)}: {col}")
        status_box.info(f"Loading distinct count for `{col}` ...")
        qcol = _dq(col)
        try:
            query = f"""
                SELECT COUNT(DISTINCT {qcol}) AS distinct_count
                FROM read_parquet({parquet_sql}, union_by_name=true)
                WHERE {time_filter}
            """
            distinct_count = con.execute(query).fetchone()[0]
            rows.append({"Column": col, "Distinct Count": int(distinct_count or 0), "Status": "OK"})
        except Exception as exc:
            rows.append({"Column": col, "Distinct Count": None, "Status": f"Error: {exc}"})

        live_df = pd.DataFrame(rows).sort_values("Distinct Count", ascending=False, na_position="last")
        table_box.dataframe(live_df, use_container_width=True, hide_index=True, height=520)
        progress.progress(int(idx / max(len(columns), 1) * 100), text=f"Completed {idx}/{len(columns)} columns")

    progress.progress(100, text="✅ Distinct count complete")
    status_box.success("Distinct counts completed.")
    return pd.DataFrame(rows).sort_values("Distinct Count", ascending=False, na_position="last")


def compare_distinct_counts_for_columns_live(
    files: List[str],
    columns: List[str],
    req_time_col: str,
    a_start_epoch: int,
    a_end_epoch: int,
    b_start_epoch: int,
    b_end_epoch: int,
) -> pd.DataFrame:
    """Count exact distinct values for two date ranges and update the table after each column."""
    if not files or not columns:
        return pd.DataFrame(columns=["Column", "Range A Distinct", "Range B Distinct", "Delta", "Delta %", "Status"])

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    parquet_sql = _path_list_sql(files)
    q_ts = _dq(req_time_col)
    filter_a = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(a_start_epoch)} AND {int(a_end_epoch)}"
    filter_b = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(b_start_epoch)} AND {int(b_end_epoch)}"

    rows = []
    progress = st.progress(0, text="Starting comparison distinct counts ...")
    status_box = st.empty()
    table_box = st.empty()

    for idx, col in enumerate(columns, start=1):
        progress.progress(int((idx - 1) / max(len(columns), 1) * 100), text=f"Comparing {idx}/{len(columns)}: {col}")
        status_box.info(f"Loading Range A vs Range B distinct count for `{col}` ...")
        qcol = _dq(col)
        try:
            query = f"""
                SELECT
                    COUNT(DISTINCT CASE WHEN {filter_a} THEN {qcol} END) AS range_a_distinct,
                    COUNT(DISTINCT CASE WHEN {filter_b} THEN {qcol} END) AS range_b_distinct
                FROM read_parquet({parquet_sql}, union_by_name=true)
                WHERE ({filter_a}) OR ({filter_b})
            """
            a_count, b_count = con.execute(query).fetchone()
            a_count = int(a_count or 0)
            b_count = int(b_count or 0)
            delta = b_count - a_count
            delta_pct = round((delta / a_count * 100), 2) if a_count else None
            rows.append({
                "Column": col,
                "Range A Distinct": a_count,
                "Range B Distinct": b_count,
                "Delta (B - A)": delta,
                "Delta %": delta_pct,
                "Status": "OK",
            })
        except Exception as exc:
            rows.append({
                "Column": col,
                "Range A Distinct": None,
                "Range B Distinct": None,
                "Delta (B - A)": None,
                "Delta %": None,
                "Status": f"Error: {exc}",
            })

        live_df = pd.DataFrame(rows).sort_values("Delta (B - A)", ascending=False, na_position="last")
        table_box.dataframe(live_df, use_container_width=True, hide_index=True, height=520)
        progress.progress(int(idx / max(len(columns), 1) * 100), text=f"Completed {idx}/{len(columns)} columns")

    progress.progress(100, text="✅ Comparison complete")
    status_box.success("Comparison distinct counts completed.")
    return pd.DataFrame(rows).sort_values("Delta (B - A)", ascending=False, na_position="last")


@st.cache_data(show_spinner=False)
def querystring_distinct_counts(
    files: List[str],
    query_string_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse a URL query-string column and return distinct counts per parsed key.

    This follows the same idea as the source explorer's Query String Analyzer:
    first aggregate raw query-string values with counts, then parse key=value pairs
    using urllib.parse.parse_qsl while preserving the row weight in _count.
    """
    if not files or not query_string_col:
        empty_overview = pd.DataFrame(columns=["Key", "Distinct Count", "Filled Rows", "Fill %"])
        return empty_overview, pd.DataFrame()

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    parquet_sql = _path_list_sql(files)
    q_qs = _dq(query_string_col)
    q_ts = _dq(req_time_col)
    time_filter = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}"

    query = f"""
        SELECT
            COALESCE(CAST({q_qs} AS VARCHAR), '') AS raw_query_string,
            COUNT(*) AS row_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE {time_filter}
          AND {q_qs} IS NOT NULL
          AND TRIM(CAST({q_qs} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY row_count DESC
    """
    raw_df = con.execute(query).df()
    if raw_df.empty:
        empty_overview = pd.DataFrame(columns=["Key", "Distinct Count", "Filled Rows", "Fill %"])
        return empty_overview, pd.DataFrame()

    records = []
    progress = st.progress(0, text="Parsing query strings ...")
    total_raw = len(raw_df)

    for idx, row in enumerate(raw_df.itertuples(index=False), start=1):
        if idx == 1 or idx % 1000 == 0 or idx == total_raw:
            progress.progress(int(idx / max(total_raw, 1) * 100), text=f"Parsing query strings ... {idx:,}/{total_raw:,}")
        raw_val = "" if pd.isna(row.raw_query_string) else str(row.raw_query_string)
        try:
            parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
        except Exception:
            parsed = {}
        parsed["_raw"] = raw_val
        parsed["_count"] = int(row.row_count or 0)
        records.append(parsed)

    progress.empty()
    parsed_df = pd.DataFrame.from_records(records).fillna("")
    if parsed_df.empty:
        empty_overview = pd.DataFrame(columns=["Key", "Distinct Count", "Filled Rows", "Fill %"])
        return empty_overview, parsed_df

    total_rows = int(parsed_df["_count"].sum())
    overview_rows = []
    for key in [c for c in parsed_df.columns if c not in {"_raw", "_count"}]:
        non_empty = parsed_df[parsed_df[key].astype(str).str.strip() != ""]
        filled_rows = int(non_empty["_count"].sum()) if not non_empty.empty else 0
        overview_rows.append({
            "Key": key,
            "Distinct Count": int(non_empty[key].astype(str).nunique()) if not non_empty.empty else 0,
            "Filled Rows": filled_rows,
            "Fill %": round((filled_rows / total_rows * 100), 2) if total_rows else 0.0,
        })

    overview_df = pd.DataFrame(overview_rows).sort_values(
        ["Filled Rows", "Distinct Count", "Key"], ascending=[False, False, True]
    )
    return overview_df, parsed_df



@st.cache_data(show_spinner=False)
def direct_column_distinct_summary(
    files: List[str],
    value_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    top_n: int = 100,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fast path for normal columns such as cliIP: distinct count + top values without query-string parsing."""
    empty_summary = pd.DataFrame(columns=["Column", "Distinct Count", "Filled Rows", "Total Rows", "Fill %"])
    empty_top = pd.DataFrame(columns=["value", "count", "% of filled rows"])
    if not files or not value_col:
        return empty_summary, empty_top

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    parquet_sql = _path_list_sql(files)
    q_val = _dq(value_col)
    q_ts = _dq(req_time_col)
    time_filter = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}"

    summary_query = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {q_val} IS NOT NULL AND TRIM(CAST({q_val} AS VARCHAR)) <> '' THEN 1 ELSE 0 END) AS filled_rows,
            COUNT(DISTINCT CASE WHEN {q_val} IS NOT NULL AND TRIM(CAST({q_val} AS VARCHAR)) <> '' THEN CAST({q_val} AS VARCHAR) END) AS distinct_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE {time_filter}
    """
    total_rows, filled_rows, distinct_count = con.execute(summary_query).fetchone()
    total_rows = int(total_rows or 0)
    filled_rows = int(filled_rows or 0)
    distinct_count = int(distinct_count or 0)

    summary_df = pd.DataFrame([{
        "Column": value_col,
        "Distinct Count": distinct_count,
        "Filled Rows": filled_rows,
        "Total Rows": total_rows,
        "Fill %": round((filled_rows / total_rows * 100), 2) if total_rows else 0.0,
    }])

    top_query = f"""
        SELECT CAST({q_val} AS VARCHAR) AS value, COUNT(*) AS count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE {time_filter}
          AND {q_val} IS NOT NULL
          AND TRIM(CAST({q_val} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY count DESC
        LIMIT {int(top_n)}
    """
    top_df = con.execute(top_query).df()
    if not top_df.empty:
        top_df["% of filled rows"] = (top_df["count"] / max(filled_rows, 1) * 100).round(2)
    else:
        top_df = empty_top
    return summary_df, top_df


@st.cache_data(show_spinner=False)
def querystring_probe(
    files: List[str],
    value_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    sample_n: int = 2000,
) -> dict:
    """Sample a column and detect whether it looks like URL query-string data."""
    if not files or not value_col:
        return {"sampled": 0, "query_like": 0, "query_like_pct": 0.0}
    con = duckdb.connect(database=":memory:")
    parquet_sql = _path_list_sql(files)
    q_val = _dq(value_col)
    q_ts = _dq(req_time_col)
    time_filter = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}"
    query = f"""
        SELECT CAST({q_val} AS VARCHAR) AS value
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE {time_filter}
          AND {q_val} IS NOT NULL
          AND TRIM(CAST({q_val} AS VARCHAR)) <> ''
        LIMIT {int(sample_n)}
    """
    df = con.execute(query).df()
    if df.empty:
        return {"sampled": 0, "query_like": 0, "query_like_pct": 0.0}
    s = df["value"].astype(str)
    # queryStr usually has key=value pairs and often & separators. cliIP values should not match this.
    query_like = int(s.str.contains(r"(^|[?&])[^=&\s]+=[^&]*", regex=True, na=False).sum())
    return {
        "sampled": int(len(s)),
        "query_like": query_like,
        "query_like_pct": round(query_like / max(len(s), 1) * 100, 2),
    }


def merge_querystring_overviews_for_compare(a_df: pd.DataFrame, b_df: pd.DataFrame) -> pd.DataFrame:
    """Merge parsed query-string key summaries for Range A and Range B."""
    a = a_df.rename(columns={
        "Distinct Count": "Range A Distinct",
        "Filled Rows": "Range A Filled Rows",
        "Fill %": "Range A Fill %",
    }) if a_df is not None and not a_df.empty else pd.DataFrame(columns=["Key", "Range A Distinct", "Range A Filled Rows", "Range A Fill %"])
    b = b_df.rename(columns={
        "Distinct Count": "Range B Distinct",
        "Filled Rows": "Range B Filled Rows",
        "Fill %": "Range B Fill %",
    }) if b_df is not None and not b_df.empty else pd.DataFrame(columns=["Key", "Range B Distinct", "Range B Filled Rows", "Range B Fill %"])
    out = a.merge(b, on="Key", how="outer").fillna(0)
    for c in ["Range A Distinct", "Range A Filled Rows", "Range B Distinct", "Range B Filled Rows"]:
        if c in out.columns:
            out[c] = out[c].astype(int)
    out["Delta Distinct (B - A)"] = out["Range B Distinct"] - out["Range A Distinct"]
    out["Delta Filled Rows (B - A)"] = out["Range B Filled Rows"] - out["Range A Filled Rows"]
    out["Delta Distinct %"] = out.apply(
        lambda r: round((r["Delta Distinct (B - A)"] / r["Range A Distinct"] * 100), 2) if r["Range A Distinct"] else None,
        axis=1,
    )
    return out.sort_values(["Delta Filled Rows (B - A)", "Range B Filled Rows"], ascending=[False, False])


def merge_direct_summaries_for_compare(a_df: pd.DataFrame, b_df: pd.DataFrame) -> pd.DataFrame:
    """Merge normal-column distinct summaries for Range A and Range B."""
    if a_df is None or a_df.empty:
        a_df = pd.DataFrame([{"Column": "", "Distinct Count": 0, "Filled Rows": 0, "Total Rows": 0, "Fill %": 0.0}])
    if b_df is None or b_df.empty:
        b_df = pd.DataFrame([{"Column": "", "Distinct Count": 0, "Filled Rows": 0, "Total Rows": 0, "Fill %": 0.0}])
    a = a_df.rename(columns={
        "Distinct Count": "Range A Distinct",
        "Filled Rows": "Range A Filled Rows",
        "Total Rows": "Range A Total Rows",
        "Fill %": "Range A Fill %",
    })
    b = b_df.rename(columns={
        "Distinct Count": "Range B Distinct",
        "Filled Rows": "Range B Filled Rows",
        "Total Rows": "Range B Total Rows",
        "Fill %": "Range B Fill %",
    })
    out = a.merge(b, on="Column", how="outer").fillna(0)
    for c in ["Range A Distinct", "Range A Filled Rows", "Range A Total Rows", "Range B Distinct", "Range B Filled Rows", "Range B Total Rows"]:
        if c in out.columns:
            out[c] = out[c].astype(int)
    out["Delta Distinct (B - A)"] = out["Range B Distinct"] - out["Range A Distinct"]
    out["Delta Filled Rows (B - A)"] = out["Range B Filled Rows"] - out["Range A Filled Rows"]
    out["Delta Distinct %"] = out.apply(
        lambda r: round((r["Delta Distinct (B - A)"] / r["Range A Distinct"] * 100), 2) if r["Range A Distinct"] else None,
        axis=1,
    )
    return out


def querystring_top_values(parsed_df: pd.DataFrame, key: str, top_n: int = 100) -> pd.DataFrame:
    """Return weighted top values for one parsed query-string key."""
    if parsed_df is None or parsed_df.empty or key not in parsed_df.columns:
        return pd.DataFrame(columns=["value", "count", "% of filled rows"])
    sub = parsed_df[parsed_df[key].astype(str).str.strip() != ""].copy()
    if sub.empty:
        return pd.DataFrame(columns=["value", "count", "% of filled rows"])
    out = (
        sub.groupby(key, dropna=False)["_count"]
        .sum()
        .reset_index()
        .rename(columns={key: "value", "_count": "count"})
        .sort_values("count", ascending=False)
        .head(int(top_n))
    )
    total = int(sub["_count"].sum())
    out["% of filled rows"] = (out["count"] / total * 100).round(2) if total else 0
    return out


st.title("📺 Veto Dashboard")
st.caption("Folder/file selection is on the left. Available date range, selected files, columns, and comparison controls stay at the top.")

if duckdb is None:
    st.error("DuckDB is required. Install it with: pip install duckdb")
    st.stop()

with st.sidebar:
    st.header("Folder & files")
    root_folder = st.text_input("Folder path", placeholder=r"D:\data\logs or /mnt/data/logs", key="mini_root_input")
    scan_clicked = st.button("🔍 Scan folder", type="primary", use_container_width=True)

    if scan_clicked and root_folder:
        st.session_state["mini_files"] = scan_parquet_files(root_folder)
        st.session_state["mini_root"] = root_folder
        st.session_state.pop("mini_result_df", None)
        st.session_state.pop("mini_qs_overview_df", None)
        st.session_state.pop("mini_qs_parsed_df", None)

    files = st.session_state.get("mini_files", [])
    active_root = st.session_state.get("mini_root", root_folder)

    if files:
        st.success(f"{len(files):,} parquet file(s) found")
        root_path = Path(active_root).expanduser()
        file_labels = []
        for f in files:
            try:
                file_labels.append(str(Path(f).relative_to(root_path)))
            except Exception:
                file_labels.append(str(f))
        label_to_file = dict(zip(file_labels, files))

        select_all_files = st.checkbox("Select all files", value=True, key="mini_select_all_files")
        default_labels = file_labels if select_all_files else file_labels[: min(10, len(file_labels))]
        selected_labels = st.multiselect(
            "Parquet files",
            options=file_labels,
            default=default_labels,
            key="mini_selected_file_labels",
            help="Move this selection from the main page into the left bar.",
        )
        selected_files = [label_to_file[x] for x in selected_labels]
        st.caption(f"Selected: {len(selected_files):,} file(s)")
    else:
        selected_files = []
        if root_folder:
            st.info("Click Scan folder to load parquet files.")

    st.markdown("---")
    st.header("Settings")
    tz_mode = st.radio("Timezone", ["IST", "UTC"], horizontal=True, key="mini_tz_mode")

if not selected_files:
    st.info("Start by entering a folder path in the sidebar, scanning it, and selecting parquet files.")
    st.stop()

schema_df = read_schema(selected_files)
if schema_df.empty:
    st.error("Could not read schema from selected parquet files.")
    st.stop()

columns = schema_df["column_name"].tolist()

# ─────────────────────────────────────────────
# Top panel: available date range as-is
# ─────────────────────────────────────────────
st.subheader("Available date range")
req_time_col = st.selectbox(
    "Timestamp column",
    options=columns,
    index=columns.index(REQ_TIME_COL_DEFAULT) if REQ_TIME_COL_DEFAULT in columns else 0,
    help="This column should contain Unix epoch seconds. Default target is reqTimeSec.",
)

min_epoch, max_epoch = get_reqtime_range(selected_files, req_time_col)
if min_epoch is None or max_epoch is None:
    st.error(f"No valid epoch seconds found in `{req_time_col}`.")
    st.stop()

min_dt = epoch_to_display_dt(min_epoch, tz_mode)
max_dt = epoch_to_display_dt(max_epoch, tz_mode)

r1, r2, r3, r4 = st.columns(4)
r1.metric("Selected files", f"{len(selected_files):,}")
r2.metric("Columns", f"{len(columns):,}")
r3.metric("Available from", min_dt.strftime("%Y-%m-%d %H:%M:%S"))
r4.metric("Available to", max_dt.strftime("%Y-%m-%d %H:%M:%S"))
st.markdown("### Choose date/time range")
comparison_mode = st.toggle(
    "Comparison mode — compare two dates/date ranges on one screen",
    value=False,
    key="mini_comparison_mode",
)

if not comparison_mode:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        start_date = st.date_input("Start date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date(), key="mini_start_date")
    with c2:
        start_time = st.time_input("Start time", value=dtime(0, 0, 0), key="mini_start_time")
    with c3:
        end_date = st.date_input("End date", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date(), key="mini_end_date")
    with c4:
        end_time = st.time_input("End time", value=dtime(23, 59, 59), key="mini_end_time")

    start_epoch = date_time_to_epoch(start_date, start_time, tz_mode)
    end_epoch = date_time_to_epoch(end_date, end_time, tz_mode)
    range_label = f"{start_date} {start_time} → {end_date} {end_time}"

    if start_epoch > end_epoch:
        st.error("Start date/time must be before end date/time.")
        st.stop()

    a_start_epoch = start_epoch
    a_end_epoch = end_epoch
    b_start_epoch = None
    b_end_epoch = None
    range_a_label = range_label
    range_b_label = ""
else:
    st.caption("Range A is the baseline. Range B is compared against it. Delta = Range B - Range A.")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("#### Range A / baseline")
        a1, a2 = st.columns(2)
        with a1:
            a_start_date = st.date_input("A start date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date(), key="mini_a_start_date")
            a_end_date = st.date_input("A end date", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date(), key="mini_a_end_date")
        with a2:
            a_start_time = st.time_input("A start time", value=dtime(0, 0, 0), key="mini_a_start_time")
            a_end_time = st.time_input("A end time", value=dtime(23, 59, 59), key="mini_a_end_time")
    with col_b:
        st.markdown("#### Range B / compare")
        default_b_start = max_dt.date()
        default_b_end = max_dt.date()
        b1, b2 = st.columns(2)
        with b1:
            b_start_date = st.date_input("B start date", value=default_b_start, min_value=min_dt.date(), max_value=max_dt.date(), key="mini_b_start_date")
            b_end_date = st.date_input("B end date", value=default_b_end, min_value=min_dt.date(), max_value=max_dt.date(), key="mini_b_end_date")
        with b2:
            b_start_time = st.time_input("B start time", value=dtime(0, 0, 0), key="mini_b_start_time")
            b_end_time = st.time_input("B end time", value=dtime(23, 59, 59), key="mini_b_end_time")

    a_start_epoch = date_time_to_epoch(a_start_date, a_start_time, tz_mode)
    a_end_epoch = date_time_to_epoch(a_end_date, a_end_time, tz_mode)
    b_start_epoch = date_time_to_epoch(b_start_date, b_start_time, tz_mode)
    b_end_epoch = date_time_to_epoch(b_end_date, b_end_time, tz_mode)

    if a_start_epoch > a_end_epoch or b_start_epoch > b_end_epoch:
        st.error("Each comparison range must have start date/time before end date/time.")
        st.stop()

    start_epoch = a_start_epoch
    end_epoch = a_end_epoch
    range_a_label = f"{a_start_date} {a_start_time} → {a_end_date} {a_end_time}"
    range_b_label = f"{b_start_date} {b_start_time} → {b_end_date} {b_end_time}"
    range_label = range_a_label

    with st.spinner("Counting rows for both comparison ranges ..."):
        a_rows = count_rows(selected_files, req_time_col, a_start_epoch, a_end_epoch)
        b_rows = count_rows(selected_files, req_time_col, b_start_epoch, b_end_epoch)
    ca, cb, cd = st.columns(3)
    ca.metric("Range A rows", f"{a_rows:,}")
    cb.metric("Range B rows", f"{b_rows:,}")
    cd.metric("Row delta B - A", f"{b_rows - a_rows:,}")

with st.expander("Schema", expanded=False):
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

st.markdown("---")
st.subheader("Distinct counts — click a column to load")
st.caption(
    "Each column is shown as a separate box. Click **Load** on the column you need. "
    "While it runs, the box shows a loading spinner. Results are stored until files or date range changes."
)

card_key = (
    "compare", tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch,
) if comparison_mode else ("single", tuple(selected_files), req_time_col, start_epoch, end_epoch)

if st.session_state.get("mini_distinct_card_key") != card_key:
    st.session_state["mini_distinct_card_results"] = {}
    st.session_state["mini_distinct_card_key"] = card_key

if st.button("🧹 Clear loaded distinct cards", key="mini_clear_distinct_cards"):
    st.session_state["mini_distinct_card_results"] = {}
    st.rerun()

card_search = st.text_input("Search columns to show as boxes", placeholder="type a column name ...", key="mini_distinct_card_search")
visible_columns = columns
if card_search:
    visible_columns = [c for c in columns if card_search.lower() in c.lower()]

st.caption(f"Showing {len(visible_columns):,} of {len(columns):,} column boxes.")
results_store = st.session_state.setdefault("mini_distinct_card_results", {})

for row_start in range(0, len(visible_columns), 3):
    row_cols = st.columns(3)
    for box, col in zip(row_cols, visible_columns[row_start:row_start + 3]):
        with box:
            with st.container(border=True):
                st.markdown(f"**{col}**")
                loaded = results_store.get(col)
                if loaded is None:
                    st.caption("Not loaded")
                    if st.button("Load", key=f"mini_load_distinct_{hash((card_key, col))}"):
                        with st.spinner(f"Loading `{col}` distinct count ..."):
                            try:
                                if comparison_mode:
                                    loaded = compare_distinct_count_one_column(
                                        selected_files, col, req_time_col,
                                        a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch,
                                    )
                                else:
                                    loaded = distinct_count_one_column(
                                        selected_files, col, req_time_col, start_epoch, end_epoch,
                                    )
                            except Exception as exc:
                                loaded = {"Column": col, "Error": str(exc)}
                            results_store[col] = loaded
                            st.session_state["mini_distinct_card_results"] = results_store
                        st.rerun()
                elif "Error" in loaded:
                    st.error(loaded["Error"])
                    if st.button("Retry", key=f"mini_retry_distinct_{hash((card_key, col))}"):
                        results_store.pop(col, None)
                        st.session_state["mini_distinct_card_results"] = results_store
                        st.rerun()
                elif comparison_mode:
                    st.metric("Range A", f"{int(loaded.get('Range A Distinct', 0)):,}")
                    st.metric("Range B", f"{int(loaded.get('Range B Distinct', 0)):,}", delta=f"{int(loaded.get('Delta (B - A)', 0)):,}")
                    if loaded.get("Delta %") is not None:
                        st.caption(f"Delta %: {loaded.get('Delta %')}%")
                else:
                    st.metric("Distinct", f"{int(loaded.get('Distinct Count', 0)):,}")
                    st.caption(f"Filled rows: {int(loaded.get('Filled Rows', 0)):,} | Fill: {loaded.get('Fill %', 0)}%")

if results_store:
    loaded_df = pd.DataFrame(list(results_store.values()))
    loaded_df = loaded_df[[c for c in loaded_df.columns if c != "Error"] + (["Error"] if "Error" in loaded_df.columns else [])]
    with st.expander("Loaded distinct results table", expanded=False):
        st.dataframe(loaded_df, use_container_width=True, hide_index=True, height=360)
        st.download_button(
            "Download loaded distinct cards CSV",
            data=loaded_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_loaded_distinct_comparison.csv" if comparison_mode else "veto_loaded_distinct_counts.csv",
            mime="text/csv",
        )
else:
    st.info("No column distinct cards loaded yet. Click **Load** inside any column box.")


st.markdown("---")
st.subheader("Query String / column distinct counts")
st.caption(
    "For real query-string columns, this parses values like `a=1&b=2` and calculates distinct counts for every parsed key. "
    "For normal columns such as `cliIP`, it automatically uses a faster direct distinct-count path instead of parsing."
)

query_candidates = [c for c in columns if c.lower() in {"querystr", "qrystr", "query_string", "querystring"}]
query_candidates += [c for c in columns if "query" in c.lower() and c not in query_candidates]
query_options = query_candidates + [c for c in columns if c not in query_candidates]

qs_col = st.selectbox(
    "Query string column",
    options=query_options,
    index=0,
    key="mini_qs_col",
    help="Example value: session_id=abc&device_id=xyz&channel=news",
)

force_parse = st.checkbox(
    "Force query-string parsing",
    value=False,
    key="mini_qs_force_parse",
    help="Leave off for auto mode. Auto mode uses direct distinct counting for normal columns such as cliIP.",
)
run_qs = st.button("Run query-string / column distinct counts", type="secondary")

if run_qs:
    probe_start = a_start_epoch if comparison_mode else start_epoch
    probe_end = a_end_epoch if comparison_mode else end_epoch
    with st.spinner(f"Checking `{qs_col}` data type ..."):
        probe = querystring_probe(selected_files, qs_col, req_time_col, probe_start, probe_end)

    should_parse = force_parse or probe.get("query_like_pct", 0) >= 20

    if comparison_mode:
        if should_parse:
            st.info(
                f"`{qs_col}` looks like query-string data "
                f"({probe.get('query_like', 0):,}/{probe.get('sampled', 0):,} sampled values matched). "
                "Parsing Range A and Range B now ..."
            )
            with st.spinner("Parsing query strings for Range A ..."):
                a_overview_df, a_parsed_df = querystring_distinct_counts(
                    selected_files, qs_col, req_time_col, a_start_epoch, a_end_epoch
                )
            with st.spinner("Parsing query strings for Range B ..."):
                b_overview_df, b_parsed_df = querystring_distinct_counts(
                    selected_files, qs_col, req_time_col, b_start_epoch, b_end_epoch
                )
            qs_overview_df = merge_querystring_overviews_for_compare(a_overview_df, b_overview_df)
            st.session_state["mini_qs_mode"] = "compare_parsed"
            st.session_state["mini_qs_overview_df"] = qs_overview_df
            st.session_state["mini_qs_parsed_df"] = None
            st.session_state["mini_qs_parsed_a_df"] = a_parsed_df
            st.session_state["mini_qs_parsed_b_df"] = b_parsed_df
            st.session_state["mini_qs_direct_top_df"] = None
            st.session_state["mini_qs_direct_top_a_df"] = None
            st.session_state["mini_qs_direct_top_b_df"] = None
        else:
            st.info(
                f"`{qs_col}` does not look like query-string data "
                f"({probe.get('query_like', 0):,}/{probe.get('sampled', 0):,} sampled values matched). "
                "Using fast direct comparison."
            )
            with st.spinner("Calculating direct distincts for Range A ..."):
                a_summary_df, a_top_df = direct_column_distinct_summary(
                    selected_files, qs_col, req_time_col, a_start_epoch, a_end_epoch, top_n=100
                )
            with st.spinner("Calculating direct distincts for Range B ..."):
                b_summary_df, b_top_df = direct_column_distinct_summary(
                    selected_files, qs_col, req_time_col, b_start_epoch, b_end_epoch, top_n=100
                )
            qs_overview_df = merge_direct_summaries_for_compare(a_summary_df, b_summary_df)
            st.session_state["mini_qs_mode"] = "compare_direct"
            st.session_state["mini_qs_overview_df"] = qs_overview_df
            st.session_state["mini_qs_parsed_df"] = None
            st.session_state["mini_qs_parsed_a_df"] = None
            st.session_state["mini_qs_parsed_b_df"] = None
            st.session_state["mini_qs_direct_top_df"] = None
            st.session_state["mini_qs_direct_top_a_df"] = a_top_df
            st.session_state["mini_qs_direct_top_b_df"] = b_top_df

        st.session_state["mini_qs_key"] = (
            "compare", tuple(selected_files), qs_col, req_time_col,
            a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, force_parse,
        )
    else:
        if should_parse:
            st.info(
                f"`{qs_col}` looks like query-string data "
                f"({probe.get('query_like', 0):,}/{probe.get('sampled', 0):,} sampled values matched). Parsing keys now ..."
            )
            qs_overview_df, qs_parsed_df = querystring_distinct_counts(
                selected_files,
                qs_col,
                req_time_col,
                start_epoch,
                end_epoch,
            )
            st.session_state["mini_qs_mode"] = "parsed"
            st.session_state["mini_qs_overview_df"] = qs_overview_df
            st.session_state["mini_qs_parsed_df"] = qs_parsed_df
            st.session_state["mini_qs_parsed_a_df"] = None
            st.session_state["mini_qs_parsed_b_df"] = None
            st.session_state["mini_qs_direct_top_df"] = None
            st.session_state["mini_qs_direct_top_a_df"] = None
            st.session_state["mini_qs_direct_top_b_df"] = None
        else:
            st.info(
                f"`{qs_col}` does not look like query-string data "
                f"({probe.get('query_like', 0):,}/{probe.get('sampled', 0):,} sampled values matched). "
                "Using fast direct distinct count."
            )
            qs_overview_df, direct_top_df = direct_column_distinct_summary(
                selected_files,
                qs_col,
                req_time_col,
                start_epoch,
                end_epoch,
                top_n=100,
            )
            st.session_state["mini_qs_mode"] = "direct"
            st.session_state["mini_qs_overview_df"] = qs_overview_df
            st.session_state["mini_qs_parsed_df"] = None
            st.session_state["mini_qs_parsed_a_df"] = None
            st.session_state["mini_qs_parsed_b_df"] = None
            st.session_state["mini_qs_direct_top_df"] = direct_top_df
            st.session_state["mini_qs_direct_top_a_df"] = None
            st.session_state["mini_qs_direct_top_b_df"] = None

        st.session_state["mini_qs_key"] = ("single", tuple(selected_files), qs_col, req_time_col, start_epoch, end_epoch, force_parse)

    st.session_state["mini_qs_col_used"] = qs_col
    st.session_state["mini_qs_probe"] = probe

qs_overview_df = st.session_state.get("mini_qs_overview_df")
qs_parsed_df = st.session_state.get("mini_qs_parsed_df")
qs_col_used = st.session_state.get("mini_qs_col_used")
qs_key = st.session_state.get("mini_qs_key")
current_qs_key = (
    "compare", tuple(selected_files), qs_col, req_time_col,
    a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, force_parse,
) if comparison_mode else ("single", tuple(selected_files), qs_col, req_time_col, start_epoch, end_epoch, force_parse)

if qs_overview_df is not None and qs_key == current_qs_key:
    qs_mode = st.session_state.get("mini_qs_mode", "parsed")
    qs_probe = st.session_state.get("mini_qs_probe", {})

    if qs_overview_df.empty:
        st.warning(f"No values found in `{qs_col_used or qs_col}` for the selected date range.")

    elif qs_mode == "compare_direct":
        st.success(
            f"Fast direct comparison used for `{qs_col_used or qs_col}`. "
            f"Query-like sample match: {qs_probe.get('query_like_pct', 0)}%."
        )
        d1, d2, d3 = st.columns(3)
        d1.metric("Column", str(qs_col_used or qs_col))
        d2.metric("Range A distinct", f"{int(qs_overview_df['Range A Distinct'].iloc[0]):,}")
        d3.metric("Range B distinct", f"{int(qs_overview_df['Range B Distinct'].iloc[0]):,}")
        st.caption(f"Range A: {range_a_label}  |  Range B: {range_b_label}")

        st.dataframe(qs_overview_df, use_container_width=True, hide_index=True, height=140)
        st.download_button(
            "Download column comparison summary CSV",
            data=qs_overview_df.to_csv(index=False).encode("utf-8"),
            file_name=f"mini_comparison_distinct_summary_{qs_col_used or qs_col}.csv",
            mime="text/csv",
        )

        st.markdown("#### Top values by range")
        ta, tb = st.tabs(["Range A top values", "Range B top values"])
        with ta:
            a_top = st.session_state.get("mini_qs_direct_top_a_df")
            if a_top is not None and not a_top.empty:
                st.dataframe(a_top, use_container_width=True, hide_index=True, height=360)
                st.download_button(
                    "Download Range A top values CSV",
                    data=a_top.to_csv(index=False).encode("utf-8"),
                    file_name=f"range_a_top_values_{qs_col_used or qs_col}.csv",
                    mime="text/csv",
                )
            else:
                st.info("No non-empty values found for Range A.")
        with tb:
            b_top = st.session_state.get("mini_qs_direct_top_b_df")
            if b_top is not None and not b_top.empty:
                st.dataframe(b_top, use_container_width=True, hide_index=True, height=360)
                st.download_button(
                    "Download Range B top values CSV",
                    data=b_top.to_csv(index=False).encode("utf-8"),
                    file_name=f"range_b_top_values_{qs_col_used or qs_col}.csv",
                    mime="text/csv",
                )
            else:
                st.info("No non-empty values found for Range B.")

    elif qs_mode == "compare_parsed":
        q1, q2, q3 = st.columns(3)
        parsed_a_df = st.session_state.get("mini_qs_parsed_a_df")
        parsed_b_df = st.session_state.get("mini_qs_parsed_b_df")
        q1.metric("Parsed keys", f"{len(qs_overview_df):,}")
        q2.metric("Range A raw strings", f"{len(parsed_a_df) if parsed_a_df is not None else 0:,}")
        q3.metric("Range B raw strings", f"{len(parsed_b_df) if parsed_b_df is not None else 0:,}")
        st.caption(f"Range A: {range_a_label}  |  Range B: {range_b_label}")

        qs_search = st.text_input("Search parsed keys", placeholder="device_id, session_id, channel ...", key="mini_qs_search")
        qs_show_df = qs_overview_df.copy()
        if qs_search:
            qs_show_df = qs_show_df[qs_show_df["Key"].str.contains(qs_search, case=False, na=False)]

        st.dataframe(qs_show_df, use_container_width=True, hide_index=True, height=380)
        st.download_button(
            "Download query-string comparison CSV",
            data=qs_overview_df.to_csv(index=False).encode("utf-8"),
            file_name="mini_comparison_querystring_distinct_counts_by_key.csv",
            mime="text/csv",
        )

        st.markdown("#### Top values for a parsed key by range")
        key_options = qs_overview_df["Key"].tolist()
        selected_key = st.selectbox("Parsed key", options=key_options, key="mini_qs_key_pick")
        top_n = st.number_input("Top values", min_value=5, max_value=1000, value=100, step=5, key="mini_qs_top_n")
        ta, tb = st.tabs(["Range A top values", "Range B top values"])
        with ta:
            top_a = querystring_top_values(st.session_state.get("mini_qs_parsed_a_df"), selected_key, int(top_n))
            st.dataframe(top_a, use_container_width=True, hide_index=True, height=360)
            st.download_button(
                "Download Range A selected key top values CSV",
                data=top_a.to_csv(index=False).encode("utf-8"),
                file_name=f"range_a_querystring_top_values_{selected_key}.csv",
                mime="text/csv",
            )
        with tb:
            top_b = querystring_top_values(st.session_state.get("mini_qs_parsed_b_df"), selected_key, int(top_n))
            st.dataframe(top_b, use_container_width=True, hide_index=True, height=360)
            st.download_button(
                "Download Range B selected key top values CSV",
                data=top_b.to_csv(index=False).encode("utf-8"),
                file_name=f"range_b_querystring_top_values_{selected_key}.csv",
                mime="text/csv",
            )

    elif qs_mode == "direct":
        st.success(
            f"Fast direct distinct count used for `{qs_col_used or qs_col}`. "
            f"Query-like sample match: {qs_probe.get('query_like_pct', 0)}%."
        )
        d1, d2, d3 = st.columns(3)
        d1.metric("Column", str(qs_col_used or qs_col))
        d2.metric("Distinct values", f"{int(qs_overview_df['Distinct Count'].iloc[0]):,}")
        d3.metric("Filled rows", f"{int(qs_overview_df['Filled Rows'].iloc[0]):,}")

        st.dataframe(qs_overview_df, use_container_width=True, hide_index=True, height=120)
        st.download_button(
            "Download column distinct summary CSV",
            data=qs_overview_df.to_csv(index=False).encode("utf-8"),
            file_name=f"mini_distinct_summary_{qs_col_used or qs_col}.csv",
            mime="text/csv",
        )

        st.markdown("#### Top values")
        top_values_df = st.session_state.get("mini_qs_direct_top_df")
        if top_values_df is not None and not top_values_df.empty:
            st.dataframe(top_values_df, use_container_width=True, hide_index=True, height=360)
            st.download_button(
                "Download top values CSV",
                data=top_values_df.to_csv(index=False).encode("utf-8"),
                file_name=f"mini_top_values_{qs_col_used or qs_col}.csv",
                mime="text/csv",
            )
        else:
            st.info("No non-empty values found for top values.")

    else:
        q1, q2, q3 = st.columns(3)
        q1.metric("Parsed keys", f"{len(qs_overview_df):,}")
        q2.metric("Unique raw query strings", f"{len(qs_parsed_df):,}" if qs_parsed_df is not None else "0")
        q3.metric("Column parsed", str(qs_col_used or qs_col))

        qs_search = st.text_input("Search parsed keys", placeholder="device_id, session_id, channel ...", key="mini_qs_search")
        qs_show_df = qs_overview_df.copy()
        if qs_search:
            qs_show_df = qs_show_df[qs_show_df["Key"].str.contains(qs_search, case=False, na=False)]

        st.dataframe(qs_show_df, use_container_width=True, hide_index=True, height=360)
        st.download_button(
            "Download query-string key distinct counts CSV",
            data=qs_overview_df.to_csv(index=False).encode("utf-8"),
            file_name="mini_querystring_distinct_counts_by_key.csv",
            mime="text/csv",
        )

        st.markdown("#### Top values for a parsed key")
        key_options = qs_overview_df["Key"].tolist()
        selected_key = st.selectbox("Parsed key", options=key_options, key="mini_qs_key_pick")
        top_n = st.number_input("Top values", min_value=5, max_value=1000, value=100, step=5, key="mini_qs_top_n")
        top_values_df = querystring_top_values(qs_parsed_df, selected_key, int(top_n))
        st.dataframe(top_values_df, use_container_width=True, hide_index=True, height=360)
        st.download_button(
            "Download selected key top values CSV",
            data=top_values_df.to_csv(index=False).encode("utf-8"),
            file_name=f"mini_querystring_top_values_{selected_key}.csv",
            mime="text/csv",
        )
elif qs_overview_df is not None and qs_key != current_qs_key:
    st.info("Selections changed. Run query-string distinct counts again for the current file/date range.")
else:
    if comparison_mode:
        st.info("Click **Run query-string / column distinct counts** to compare the selected column between Range A and Range B.")
    else:
        st.info("Click **Run query-string / column distinct counts** to parse the selected query-string column.")


# ─────────────────────────────────────────────
# Watch-hour and device summary
# ─────────────────────────────────────────────
st.markdown("---")
st.subheader("Watch hours and viewer count")
st.caption(
    "Recommended method: use device_id first, use cliIP only when device_id is missing, "
    "then calculate watch time within viewer + session when session_id exists. Each request gap is capped at 60 seconds by default."
)

with st.expander("Viewer identity and watch-hour settings", expanded=True):
    default_qs = _default_query_string_column(columns)
    default_device_col = _default_device_column(columns)
    default_ip_col = _default_ip_column(columns)
    none_plus_cols = [""] + columns
    source_default = 0 if default_qs else (1 if default_device_col else 0)
    device_source = st.radio(
        "Primary Device_ID source",
        ["Parsed from query string", "Direct column"],
        index=source_default,
        horizontal=True,
        key="watch_device_source",
        help="Recommended: Parsed from query string when device_id is inside queryStr.",
    )

    w1, w2, w3, w4 = st.columns(4)
    with w1:
        watch_qs_col = st.selectbox(
            "Query string column",
            options=none_plus_cols,
            index=none_plus_cols.index(default_qs) if default_qs in none_plus_cols else 0,
            key="watch_qs_col",
            help="Used to extract device_id and session_id.",
        )
    with w2:
        watch_device_key = st.text_input(
            "Device_ID key",
            value="device_id",
            key="watch_device_key",
            disabled=device_source != "Parsed from query string",
        )
    with w3:
        watch_session_key = st.text_input(
            "Session key",
            value="session_id",
            key="watch_session_key",
            help="If present, watch gaps are calculated inside viewer + session. If missing in a row, viewer-only grouping is used.",
        )
    with w4:
        watch_cliip_col = st.selectbox(
            "cliIP fallback column",
            options=none_plus_cols,
            index=none_plus_cols.index(default_ip_col) if default_ip_col in none_plus_cols else 0,
            key="watch_cliip_col",
            help="Used only when device_id is missing. Your mapping test showed cliIP fallback is reasonably safe for most rows.",
        )

    w5, w6 = st.columns(2)
    with w5:
        watch_device_col = st.selectbox(
            "Device_ID direct column",
            options=none_plus_cols,
            index=none_plus_cols.index(default_device_col) if default_device_col in none_plus_cols else 0,
            key="watch_device_col",
            disabled=device_source != "Direct column",
        )
    with w6:
        gap_cap = st.number_input(
            "Max seconds counted between two requests",
            min_value=10,
            max_value=600,
            value=60,
            step=10,
            key="watch_gap_cap",
            help="60 seconds prevents idle gaps from becoming fake watch time.",
        )

if device_source == "Parsed from query string" and not watch_qs_col:
    st.warning("Select a query-string column to extract Device_ID/session_id.")
elif device_source == "Direct column" and not watch_device_col:
    st.warning("Select a direct Device_ID column to calculate watch hours.")
elif not watch_cliip_col:
    st.warning("Select cliIP fallback column. It is needed for rows where Device_ID is missing.")
else:
    if st.button("▶️ Calculate watch hours", type="primary", key="watch_run"):
        with st.spinner("Calculating recommended viewer/session watch hours ..."):
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
                    int(gap_cap),
                    watch_cliip_col,
                    watch_session_key,
                )
                st.session_state["watch_payload"] = {
                    "mode": "compare",
                    "key": (tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, int(gap_cap), watch_cliip_col, watch_session_key),
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
                    int(gap_cap),
                    watch_cliip_col,
                    watch_session_key,
                )
                st.session_state["watch_payload"] = {
                    "mode": "single",
                    "key": (tuple(selected_files), req_time_col, start_epoch, end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, int(gap_cap), watch_cliip_col, watch_session_key),
                    "summary": summary,
                    "daily": daily,
                }

    expected_watch_key = (
        tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, int(gap_cap), watch_cliip_col, watch_session_key
    ) if comparison_mode else (
        tuple(selected_files), req_time_col, start_epoch, end_epoch, tz_mode, device_source, watch_qs_col, watch_device_col, watch_device_key, int(gap_cap), watch_cliip_col, watch_session_key
    )
    watch_payload = st.session_state.get("watch_payload")

    if watch_payload and watch_payload.get("key") == expected_watch_key:
        if watch_payload.get("mode") == "compare":
            comp = watch_payload["compare"]
            metric_map = {row["Metric"]: row for _, row in comp.iterrows()}
            left, right = st.columns(2)
            with left:
                st.markdown("#### Range A")
                st.caption(range_a_label)
                st.metric("Total Viewer Count", f"{int(metric_map['Total Viewer Count']['Range A']):,}")
                st.metric("Total Device_ID Count", f"{int(metric_map['Total Device_ID Count']['Range A']):,}")
                st.metric("cliIP Fallback Viewers", f"{int(metric_map['Total cliIP Fallback Viewer Count']['Range A']):,}")
                st.metric("Total Watch Hours", f"{float(metric_map['Total Watch Hours']['Range A']):,.2f}")
                st.metric("Watch Minutes / Viewer", f"{float(metric_map['Watch Minutes / Viewer']['Range A']):,.2f}")
                st.metric("Rows using cliIP fallback", f"{int(metric_map['Rows using cliIP fallback']['Range A']):,}")
            with right:
                st.markdown("#### Range B")
                st.caption(range_b_label)
                st.metric("Total Viewer Count", f"{int(metric_map['Total Viewer Count']['Range B']):,}", delta=f"{int(metric_map['Total Viewer Count']['Delta (B - A)']):,}")
                st.metric("Total Device_ID Count", f"{int(metric_map['Total Device_ID Count']['Range B']):,}", delta=f"{int(metric_map['Total Device_ID Count']['Delta (B - A)']):,}")
                st.metric("cliIP Fallback Viewers", f"{int(metric_map['Total cliIP Fallback Viewer Count']['Range B']):,}", delta=f"{int(metric_map['Total cliIP Fallback Viewer Count']['Delta (B - A)']):,}")
                st.metric("Total Watch Hours", f"{float(metric_map['Total Watch Hours']['Range B']):,.2f}", delta=f"{float(metric_map['Total Watch Hours']['Delta (B - A)']):,.2f}")
                st.metric("Watch Minutes / Viewer",f"{float(metric_map['Watch Minutes / Viewer']['Range B']):,.2f}",delta=f"{float(metric_map['Watch Minutes / Viewer']['Delta (B - A)']):,.2f}",)
                st.metric("Rows using cliIP fallback", f"{int(metric_map['Rows using cliIP fallback']['Range B']):,}", delta=f"{int(metric_map['Rows using cliIP fallback']['Delta (B - A)']):,}")

            st.markdown("#### Comparison table")
            st.dataframe(comp, use_container_width=True, hide_index=True, height=330)
            st.download_button(
                "Download watch comparison CSV",
                data=comp.to_csv(index=False).encode("utf-8"),
                file_name="veto_recommended_watch_hour_comparison.csv",
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

            j1, j2, j3, j4 = st.columns(4)
            j1.metric("cliIP Fallback Viewers", f"{int(summary.get('Total cliIP Fallback Viewer Count', 0)):,}")
            j2.metric("Rows with Device_ID", f"{int(summary.get('Rows with Device_ID', 0)):,}")
            j3.metric("Rows using cliIP fallback", f"{int(summary.get('Rows using cliIP fallback', 0)):,}")
            j4.metric("Session Count", f"{int(summary.get('Total Session Count', 0)):,}")

            st.caption(range_label)
            st.markdown("#### Daily watch-hour summary")
            daily = watch_payload["daily"]
            st.dataframe(daily, use_container_width=True, hide_index=True, height=380)
            st.download_button(
                "Download daily watch-hour summary CSV",
                data=daily.to_csv(index=False).encode("utf-8"),
                file_name="veto_recommended_daily_watch_hours.csv",
                mime="text/csv",
            )
    elif watch_payload:
        st.info("Selections changed. Calculate watch hours again for the current files/date range.")

        st.info("Click **Calculate watch hours** to show recommended viewer/session watch hours for the selected date range.")
