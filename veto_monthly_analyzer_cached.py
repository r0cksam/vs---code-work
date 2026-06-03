import os
from pathlib import Path
from datetime import datetime, date, time as dtime
from typing import List, Tuple

import duckdb
import pandas as pd
import streamlit as st


APP_TITLE = "Veto Monthly Analyzer - Cached"
IST_TZ = "Asia/Kolkata"
IST_OFFSET_SECONDS = 19800  # +05:30


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


def dt_to_epoch_ist(d: date, t: dtime) -> int:
    return int(pd.Timestamp(datetime.combine(d, t), tz=IST_TZ).timestamp())


def epoch_to_ist_text(ts: int) -> str:
    return str(pd.to_datetime(int(ts), unit="s", utc=True).tz_convert(IST_TZ))


def sec_to_hhmmss(sec: int) -> str:
    sec = int(sec or 0) % 86400
    return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


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
def get_epoch_range(files: List[str], req_time_col: str) -> Tuple[int, int]:
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


def distinct_expr(col_sql: str, exact_distinct: bool) -> str:
    if exact_distinct:
        return f"COUNT(DISTINCT {col_sql})"
    return f"APPROX_COUNT_DISTINCT({col_sql})"


def build_monthly_cache(
    files: List[str],
    cache_file: str,
    req_time_col: str,
    query_col: str,
    cliip_col: str,
    ua_col: str,
    path_col: str,
    start_epoch: int,
    end_epoch: int,
    exact_distinct: bool,
    status_col: str = "",
    only_200: bool = False,
):
    """One-time monthly aggregation from raw parquet to a small daily summary parquet.

    Optimizations:
    - Filter with epoch seconds.
    - Group IST date with integer math:
      FLOOR((reqTimeSec + 19800) / 86400)
    - Only read required columns.
    - Option to use approximate distinct counts for speed.
    """
    con = duckdb.connect()
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
    path = _dq(path_col)

    status_select = "NULL AS status_code"
    status_filter = ""
    if status_col:
        status_select = f"TRY_CAST({_dq(status_col)} AS BIGINT) AS status_code"
        if only_200:
            status_filter = f"AND TRY_CAST({_dq(status_col)} AS BIGINT) = 200"

    d_cliip = distinct_expr("cliIP", exact_distinct)
    d_ua = distinct_expr("UA", exact_distinct)
    d_device = distinct_expr("device_id", exact_distinct)
    d_session = distinct_expr("session_id", exact_distinct)
    d_channel = distinct_expr("channel", exact_distinct)
    d_category = distinct_expr("category_name", exact_distinct)
    d_content = distinct_expr("content_title", exact_distinct)
    d_platform = distinct_expr("platform", exact_distinct)
    d_stream = distinct_expr("stream_key", exact_distinct)
    d_reqpath = distinct_expr("reqPath", exact_distinct)
    d_status = distinct_expr("status_code", exact_distinct)

    cache_path = str(Path(cache_file).replace("\\", "/") if hasattr(Path(cache_file), "replace") else cache_file)
    cache_path = str(Path(cache_file)).replace("\\", "/")

    query = f"""
    COPY (
        WITH raw AS (
            SELECT
                TRY_CAST({ts} AS BIGINT) AS req_ts,
                CAST(FLOOR((TRY_CAST({ts} AS BIGINT) + {IST_OFFSET_SECONDS}) / 86400) AS BIGINT) AS ist_day_id,

                CAST({qs} AS VARCHAR) AS queryStr,
                NULLIF(TRIM(CAST({ip} AS VARCHAR)), '') AS cliIP,
                NULLIF(TRIM(CAST({ua} AS VARCHAR)), '') AS UA,
                NULLIF(TRIM(CAST({path} AS VARCHAR)), '') AS reqPath,
                {status_select},

                regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])device_id=([^&]*)', 1) AS raw_device_id,
                regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])session_id=([^&]*)', 1) AS raw_session_id,
                regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])channel=([^&]*)', 1) AS raw_channel,
                regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])category_name=([^&]*)', 1) AS raw_category_name,
                regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])content_title=([^&]*)', 1) AS raw_content_title,
                regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])platform=([^&]*)', 1) AS raw_platform
            FROM read_parquet({parquet_sql}, union_by_name=true)
            WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
              AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
              {status_filter}
        ),
        clean AS (
            SELECT
                *,
                NULLIF(TRIM(raw_device_id), '') AS device_id,
                NULLIF(TRIM(raw_session_id), '') AS session_id,
                NULLIF(TRIM(raw_channel), '') AS channel,
                NULLIF(TRIM(raw_category_name), '') AS category_name,
                NULLIF(TRIM(raw_content_title), '') AS content_title,
                NULLIF(TRIM(raw_platform), '') AS platform,

                CASE WHEN queryStr IS NULL OR TRIM(queryStr) = '' OR LOWER(TRIM(queryStr)) IN ('null', '(null)', 'none', 'nan') THEN 1 ELSE 0 END AS is_empty_querystr,
                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '^m=[^&]+$') THEN 1 ELSE 0 END AS is_m_only_querystr,

                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])device_id=') THEN 1 ELSE 0 END AS has_device_id_key,
                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])session_id=') THEN 1 ELSE 0 END AS has_session_id_key,
                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])channel=') THEN 1 ELSE 0 END AS has_channel_key,
                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])category_name=') THEN 1 ELSE 0 END AS has_category_key,
                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])content_title=') THEN 1 ELSE 0 END AS has_content_title_key,
                CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])platform=') THEN 1 ELSE 0 END AS has_platform_key,

                COALESCE(
                    NULLIF(regexp_extract(CAST(reqPath AS VARCHAR), '(vglive-sk-[0-9]+)', 1), ''),
                    NULLIF(regexp_extract(CAST(reqPath AS VARCHAR), '^([^/]+)/', 1), ''),
                    NULLIF(CAST(reqPath AS VARCHAR), ''),
                    '__unknown_stream__'
                ) AS stream_key
            FROM raw
        ),
        classified AS (
            SELECT
                *,
                CASE
                    WHEN is_empty_querystr = 1 THEN 'empty_queryStr'
                    WHEN is_m_only_querystr = 1 THEN 'm_only'
                    WHEN device_id IS NOT NULL OR session_id IS NOT NULL
                         OR has_channel_key = 1 OR has_category_key = 1
                         OR has_content_title_key = 1 OR has_platform_key = 1
                        THEN 'rich_metadata'
                    ELSE 'misc_queryStr'
                END AS querystr_type
            FROM clean
        )
        SELECT
            ist_day_id,
            MIN(req_ts) AS min_req_ts,
            MAX(req_ts) AS max_req_ts,
            COUNT(*) AS total_row_count,

            {d_cliip} AS distinct_cliip_count,
            COUNT(*) FILTER (WHERE cliIP IS NOT NULL) AS rows_with_cliip,

            {d_ua} AS distinct_ua_count,
            COUNT(*) FILTER (WHERE UA IS NOT NULL) AS rows_with_ua,

            {d_device} AS distinct_device_id_count,
            COUNT(*) FILTER (WHERE device_id IS NOT NULL) AS rows_with_device_id,
            COUNT(*) FILTER (WHERE device_id IS NULL) AS rows_without_device_id,
            COUNT(*) FILTER (WHERE has_device_id_key = 1 AND device_id IS NULL) AS rows_device_id_blank,
            COUNT(*) FILTER (WHERE has_device_id_key = 0) AS rows_device_id_key_absent,

            {d_session} AS distinct_session_id_count,
            COUNT(*) FILTER (WHERE session_id IS NOT NULL) AS rows_with_session_id,
            COUNT(*) FILTER (WHERE session_id IS NULL) AS rows_without_session_id,
            COUNT(*) FILTER (WHERE has_session_id_key = 1 AND session_id IS NULL) AS rows_session_id_blank,
            COUNT(*) FILTER (WHERE has_session_id_key = 0) AS rows_session_id_key_absent,

            COUNT(*) FILTER (WHERE device_id IS NOT NULL AND session_id IS NOT NULL) AS rows_with_both_device_session,

            COUNT(*) FILTER (WHERE is_empty_querystr = 1) AS empty_querystr_rows,
            COUNT(*) FILTER (WHERE is_m_only_querystr = 1) AS m_only_querystr_rows,
            COUNT(*) FILTER (WHERE querystr_type = 'rich_metadata') AS rich_metadata_querystr_rows,
            COUNT(*) FILTER (WHERE querystr_type = 'misc_queryStr') AS misc_querystr_rows,

            {d_channel} AS distinct_channel_count,
            COUNT(*) FILTER (WHERE channel IS NOT NULL) AS rows_with_channel,
            {d_category} AS distinct_category_count,
            COUNT(*) FILTER (WHERE category_name IS NOT NULL) AS rows_with_category,
            {d_content} AS distinct_content_title_count,
            COUNT(*) FILTER (WHERE content_title IS NOT NULL) AS rows_with_content_title,
            {d_platform} AS distinct_platform_count,
            COUNT(*) FILTER (WHERE platform IS NOT NULL) AS rows_with_platform,

            {d_stream} AS distinct_stream_key_count,
            {d_reqpath} AS distinct_reqpath_count,

            COUNT(*) FILTER (WHERE LOWER(COALESCE(reqPath, '')) LIKE '%.m3u8%') AS m3u8_rows,
            COUNT(*) FILTER (WHERE LOWER(COALESCE(reqPath, '')) LIKE '%.ts%') AS ts_segment_rows,

            {d_status} AS distinct_status_count,

            {'TRUE' if exact_distinct else 'FALSE'} AS exact_distinct_used,
            {int(start_epoch)} AS cache_start_epoch,
            {int(end_epoch)} AS cache_end_epoch
        FROM classified
        GROUP BY ist_day_id
        ORDER BY ist_day_id
    ) TO '{cache_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    con.execute(query)


@st.cache_data(show_spinner=False)
def load_cache(cache_file: str):
    con = duckdb.connect()
    df = con.execute("SELECT * FROM read_parquet(?) ORDER BY ist_day_id", [cache_file]).df()
    if df.empty:
        return df, pd.DataFrame(), pd.DataFrame()

    df["Date"] = pd.to_datetime(df["ist_day_id"], unit="D", origin="unix").dt.date.astype(str)
    df["Available From IST"] = df["min_req_ts"].apply(lambda x: sec_to_hhmmss((int(x) + IST_OFFSET_SECONDS) % 86400))
    df["Available To IST"] = df["max_req_ts"].apply(lambda x: sec_to_hhmmss((int(x) + IST_OFFSET_SECONDS) % 86400))

    rename = {
        "total_row_count": "Total Row Count",
        "distinct_cliip_count": "Distinct cliIP Count",
        "rows_with_cliip": "Rows with cliIP",
        "distinct_ua_count": "Distinct UA Count",
        "rows_with_ua": "Rows with UA",
        "distinct_device_id_count": "Distinct Device_ID Count",
        "rows_with_device_id": "Rows with Device_ID",
        "rows_without_device_id": "Rows without Device_ID",
        "rows_device_id_blank": "Rows where device_id= Blank",
        "rows_device_id_key_absent": "Rows where device_id Key Absent",
        "distinct_session_id_count": "Distinct Session_ID Count",
        "rows_with_session_id": "Rows with Session_ID",
        "rows_without_session_id": "Rows without Session_ID",
        "rows_session_id_blank": "Rows where session_id= Blank",
        "rows_session_id_key_absent": "Rows where session_id Key Absent",
        "rows_with_both_device_session": "Rows with both Device_ID + Session_ID",
        "empty_querystr_rows": "Empty QueryStr Rows",
        "m_only_querystr_rows": "m= Only QueryStr Rows",
        "rich_metadata_querystr_rows": "Rich Metadata QueryStr Rows",
        "misc_querystr_rows": "Misc QueryStr Rows",
        "distinct_channel_count": "Distinct Channel Count",
        "rows_with_channel": "Rows with Channel",
        "distinct_category_count": "Distinct Category Count",
        "rows_with_category": "Rows with Category",
        "distinct_content_title_count": "Distinct Content Title Count",
        "rows_with_content_title": "Rows with Content Title",
        "distinct_platform_count": "Distinct Platform Count",
        "rows_with_platform": "Rows with Platform",
        "distinct_stream_key_count": "Distinct Stream Key Count",
        "distinct_reqpath_count": "Distinct reqPath Count",
        "m3u8_rows": "m3u8 Rows",
        "ts_segment_rows": "TS Segment Rows",
        "distinct_status_count": "Distinct Status Count",
    }
    df = df.rename(columns=rename)

    pct_map = {
        "Rows with Device_ID": "% with Device_ID",
        "Rows without Device_ID": "% without Device_ID",
        "Rows with Session_ID": "% with Session_ID",
        "Rows without Session_ID": "% without Session_ID",
        "Empty QueryStr Rows": "% Empty QueryStr",
        "m= Only QueryStr Rows": "% m= Only QueryStr",
        "Rich Metadata QueryStr Rows": "% Rich Metadata QueryStr",
        "Misc QueryStr Rows": "% Misc QueryStr",
        "Rows with Channel": "% with Channel",
        "Rows with Category": "% with Category",
        "Rows with Content Title": "% with Content Title",
        "Rows with Platform": "% with Platform",
        "m3u8 Rows": "% m3u8",
        "TS Segment Rows": "% TS Segment",
    }
    for raw_col, pct_col in pct_map.items():
        if raw_col in df.columns:
            df[pct_col] = (df[raw_col] / df["Total Row Count"] * 100).round(2)

    df["Partial Day Note"] = df.apply(
        lambda r: (
            f"Partial data: {r['Available From IST']} → {r['Available To IST']}"
            if str(r["Available From IST"]) > "00:05:00" or str(r["Available To IST"]) < "23:55:00"
            else "Full/near-full day"
        ),
        axis=1,
    )

    front = ["Date", "Available From IST", "Available To IST", "Partial Day Note"]
    df = df[front + [c for c in df.columns if c not in front and c not in ["ist_day_id", "min_req_ts", "max_req_ts"]]]

    metric_order = [
        "Partial Day Note",
        "Available From IST",
        "Available To IST",
        "Total Row Count",
        "Distinct cliIP Count",
        "Rows with cliIP",
        "Distinct UA Count",
        "Rows with UA",
        "Distinct Device_ID Count",
        "Rows with Device_ID",
        "% with Device_ID",
        "Rows without Device_ID",
        "% without Device_ID",
        "Rows where device_id= Blank",
        "Rows where device_id Key Absent",
        "Distinct Session_ID Count",
        "Rows with Session_ID",
        "% with Session_ID",
        "Rows without Session_ID",
        "% without Session_ID",
        "Rows where session_id= Blank",
        "Rows where session_id Key Absent",
        "Rows with both Device_ID + Session_ID",
        "Empty QueryStr Rows",
        "% Empty QueryStr",
        "m= Only QueryStr Rows",
        "% m= Only QueryStr",
        "Rich Metadata QueryStr Rows",
        "% Rich Metadata QueryStr",
        "Misc QueryStr Rows",
        "% Misc QueryStr",
        "Distinct Channel Count",
        "Rows with Channel",
        "% with Channel",
        "Distinct Category Count",
        "Rows with Category",
        "% with Category",
        "Distinct Content Title Count",
        "Rows with Content Title",
        "% with Content Title",
        "Distinct Platform Count",
        "Rows with Platform",
        "% with Platform",
        "Distinct Stream Key Count",
        "Distinct reqPath Count",
        "m3u8 Rows",
        "% m3u8",
        "TS Segment Rows",
        "% TS Segment",
        "Distinct Status Count",
    ]
    metrics = [m for m in metric_order if m in df.columns]
    transposed = df.set_index("Date")[metrics].T.reset_index().rename(columns={"index": "Metric"})
    partial = df[["Date", "Available From IST", "Available To IST", "Partial Day Note", "Total Row Count"]].copy()
    return df, transposed, partial


st.title("⚡ Veto Monthly Analyzer - Cached")
st.caption("Build a small monthly daily-summary cache once, then analyze the month instantly.")

with st.sidebar:
    st.header("1) Raw parquet input")
    folder = st.text_input("Parquet folder", placeholder=r"D:\Veto Logs Backup")
    recursive = st.checkbox("Scan recursively", value=True)

if not folder:
    st.info("Enter a parquet folder in the sidebar.")
    st.stop()

files_all = scan_parquet_files(folder, recursive)
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
    st.error("Could not read columns.")
    st.stop()

def idx(name: str) -> int:
    return columns.index(name) if name in columns else 0

with st.sidebar:
    st.header("2) Column mapping")
    req_time_col = st.selectbox("Timestamp column", columns, index=idx("reqTimeSec"))
    query_col = st.selectbox("Query string column", columns, index=idx("queryStr"))
    cliip_col = st.selectbox("cliIP column", columns, index=idx("cliIP"))
    ua_col = st.selectbox("UA column", columns, index=idx("UA"))
    path_col = st.selectbox("reqPath column", columns, index=idx("reqPath"))

    status_options = ["(none)"] + columns
    status_choice = st.selectbox(
        "Optional statusCode column",
        status_options,
        index=status_options.index("statusCode") if "statusCode" in columns else 0,
    )
    status_col = "" if status_choice == "(none)" else status_choice
    only_200 = st.checkbox("Only statusCode = 200", value=False) if status_col else False

min_ts, max_ts = get_epoch_range(selected_files, req_time_col)
if min_ts is None or max_ts is None:
    st.error("No valid timestamp range found.")
    st.stop()

st.subheader("Raw data available range")
r1, r2, r3, r4 = st.columns(4)
r1.metric("From IST", epoch_to_ist_text(min_ts))
r2.metric("To IST", epoch_to_ist_text(max_ts))
r3.metric("Files selected", f"{len(selected_files):,}")
r4.metric("Columns", f"{len(columns):,}")

min_date = pd.to_datetime(int(min_ts), unit="s", utc=True).tz_convert(IST_TZ).date()
max_date = pd.to_datetime(int(max_ts), unit="s", utc=True).tz_convert(IST_TZ).date()

with st.sidebar:
    st.header("3) Month / date range")
    date_range = st.date_input(
        "Month/date range in IST",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
    else:
        start_d, end_d = min_date, max_date

    start_t = st.time_input("Start time IST", dtime(0, 0, 0))
    end_t = st.time_input("End time IST", dtime(23, 59, 59))

    st.header("4) Cache")
    cache_dir = st.text_input("Cache folder", value=str(Path(folder) / "_veto_cache"))
    exact_distinct = st.checkbox(
        "Use exact distinct counts",
        value=False,
        help="Exact is slower. Approx is much faster and usually good for month-level scanning.",
    )

start_epoch = dt_to_epoch_ist(start_d, start_t)
end_epoch = dt_to_epoch_ist(end_d, end_t)

Path(cache_dir).mkdir(parents=True, exist_ok=True)
cache_name = f"daily_quality_{start_d}_{end_d}_{'exact' if exact_distinct else 'approx'}"
if only_200:
    cache_name += "_status200"
cache_file = str(Path(cache_dir) / f"{cache_name}.parquet")

st.info(
    "Recommended for 25 GB/month: click **Build/Rebuild monthly cache** once. "
    "After that, use **Load existing cache** for instant analysis."
)

c1, c2 = st.columns(2)

with c1:
    build = st.button("Build/Rebuild monthly cache", type="primary")
with c2:
    load = st.button("Load existing cache")

if build:
    with st.spinner("Scanning raw parquet and building monthly daily-summary cache. This is the slow one-time step..."):
        build_monthly_cache(
            files=selected_files,
            cache_file=cache_file,
            req_time_col=req_time_col,
            query_col=query_col,
            cliip_col=cliip_col,
            ua_col=ua_col,
            path_col=path_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            exact_distinct=exact_distinct,
            status_col=status_col,
            only_200=only_200,
        )
    st.success(f"Cache built: {cache_file}")
    load = True

if load:
    if not Path(cache_file).exists():
        st.error(f"Cache file not found: {cache_file}. Build it first.")
        st.stop()

    daily_df, transposed_df, partial_df = load_cache(cache_file)

    if daily_df.empty:
        st.warning("Cache is empty.")
        st.stop()

    st.subheader("Cache info")
    i1, i2, i3 = st.columns(3)
    i1.metric("Cache file", Path(cache_file).name)
    i2.metric("Days", f"{len(daily_df):,}")
    i3.metric("Cache size MB", f"{Path(cache_file).stat().st_size / 1024 / 1024:,.2f}")

    st.subheader("Partial-day availability check")
    st.dataframe(partial_df, use_container_width=True, hide_index=True)

    partial_notes = partial_df[partial_df["Partial Day Note"] != "Full/near-full day"]
    if not partial_notes.empty:
        st.warning("Some days have partial data. Check the table above before comparing daily counts.")

    st.subheader("Monthly table: metrics as rows, days as columns")
    st.dataframe(transposed_df, use_container_width=True, hide_index=True, height=900)

    st.download_button(
        "Download monthly table CSV",
        data=transposed_df.to_csv(index=False).encode("utf-8"),
        file_name="veto_monthly_quality_table_transposed.csv",
        mime="text/csv",
    )

    with st.expander("Show row-wise daily summary"):
        st.dataframe(daily_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download row-wise daily summary CSV",
            data=daily_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_monthly_quality_table_rowwise.csv",
            mime="text/csv",
        )
