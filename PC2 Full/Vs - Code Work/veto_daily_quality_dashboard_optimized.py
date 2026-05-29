import os
from pathlib import Path
from datetime import datetime, time as dtime
from typing import List

import duckdb
import pandas as pd
import streamlit as st


APP_TITLE = "Veto"
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
    return con.execute(
        f"""
        SELECT
            MIN(TRY_CAST({qts} AS BIGINT)) AS min_ts,
            MAX(TRY_CAST({qts} AS BIGINT)) AS max_ts
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) IS NOT NULL
        """,
        [files],
    ).fetchone()


def epoch_to_ist_date(ts: int):
    return pd.to_datetime(int(ts), unit="s", utc=True).tz_convert(IST_TZ).date()


def epoch_to_ist_text(ts: int) -> str:
    return str(pd.to_datetime(int(ts), unit="s", utc=True).tz_convert(IST_TZ))


def dt_to_epoch_ist(d, t) -> int:
    ts = pd.Timestamp(datetime.combine(d, t), tz=IST_TZ)
    return int(ts.timestamp())


def sec_to_hhmmss(sec: int) -> str:
    sec = int(sec or 0) % 86400
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


@st.cache_data(show_spinner=False)
def daily_quality_summary_optimized(
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
    """Optimized daily quality table.

    Performance idea:
    - Use epoch seconds for filtering.
    - Create IST day using integer math:
        ist_day_id = FLOOR((req_ts + 19800) / 86400)
      This avoids expensive timezone conversion per row.
    - Only after aggregation, convert day/time for display in Pandas.
    """
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    try:
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    ts = _dq(req_time_col)
    qs = _dq(query_col)
    ip = _dq(cliip_col)
    ua = _dq(ua_col)
    path = _dq(path_col)
    parquet_sql = _path_list_sql(files)

    status_select = "NULL AS status_code"
    status_filter = ""
    if status_col:
        status_select = f"TRY_CAST({_dq(status_col)} AS BIGINT) AS status_code"
        if only_200:
            status_filter = f"AND TRY_CAST({_dq(status_col)} AS BIGINT) = 200"

    query = f"""
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
        COUNT(*) AS "Total Row Count",
        MIN(req_ts) AS min_req_ts,
        MAX(req_ts) AS max_req_ts,

        COUNT(DISTINCT cliIP) AS "Distinct cliIP Count",
        COUNT(*) FILTER (WHERE cliIP IS NOT NULL) AS "Rows with cliIP",

        COUNT(DISTINCT UA) AS "Distinct UA Count",
        COUNT(*) FILTER (WHERE UA IS NOT NULL) AS "Rows with UA",

        COUNT(DISTINCT device_id) AS "Distinct Device_ID Count",
        COUNT(*) FILTER (WHERE device_id IS NOT NULL) AS "Rows with Device_ID",
        COUNT(*) FILTER (WHERE device_id IS NULL) AS "Rows without Device_ID",
        COUNT(*) FILTER (WHERE has_device_id_key = 1 AND device_id IS NULL) AS "Rows where device_id= Blank",
        COUNT(*) FILTER (WHERE has_device_id_key = 0) AS "Rows where device_id Key Absent",

        COUNT(DISTINCT session_id) AS "Distinct Session_ID Count",
        COUNT(*) FILTER (WHERE session_id IS NOT NULL) AS "Rows with Session_ID",
        COUNT(*) FILTER (WHERE session_id IS NULL) AS "Rows without Session_ID",
        COUNT(*) FILTER (WHERE has_session_id_key = 1 AND session_id IS NULL) AS "Rows where session_id= Blank",
        COUNT(*) FILTER (WHERE has_session_id_key = 0) AS "Rows where session_id Key Absent",

        COUNT(*) FILTER (WHERE device_id IS NOT NULL AND session_id IS NOT NULL) AS "Rows with both Device_ID + Session_ID",

        COUNT(*) FILTER (WHERE is_empty_querystr = 1) AS "Empty QueryStr Rows",
        COUNT(*) FILTER (WHERE is_m_only_querystr = 1) AS "m= Only QueryStr Rows",
        COUNT(*) FILTER (WHERE querystr_type = 'rich_metadata') AS "Rich Metadata QueryStr Rows",
        COUNT(*) FILTER (WHERE querystr_type = 'misc_queryStr') AS "Misc QueryStr Rows",

        COUNT(DISTINCT channel) AS "Distinct Channel Count",
        COUNT(*) FILTER (WHERE channel IS NOT NULL) AS "Rows with Channel",
        COUNT(DISTINCT category_name) AS "Distinct Category Count",
        COUNT(*) FILTER (WHERE category_name IS NOT NULL) AS "Rows with Category",
        COUNT(DISTINCT content_title) AS "Distinct Content Title Count",
        COUNT(*) FILTER (WHERE content_title IS NOT NULL) AS "Rows with Content Title",
        COUNT(DISTINCT platform) AS "Distinct Platform Count",
        COUNT(*) FILTER (WHERE platform IS NOT NULL) AS "Rows with Platform",

        COUNT(DISTINCT stream_key) AS "Distinct Stream Key Count",
        COUNT(DISTINCT reqPath) AS "Distinct reqPath Count",

        COUNT(*) FILTER (WHERE LOWER(COALESCE(reqPath, '')) LIKE '%.m3u8%') AS "m3u8 Rows",
        COUNT(*) FILTER (WHERE LOWER(COALESCE(reqPath, '')) LIKE '%.ts%') AS "TS Segment Rows",

        COUNT(DISTINCT status_code) AS "Distinct Status Count"
    FROM classified
    GROUP BY ist_day_id
    ORDER BY ist_day_id
    """

    daily_df = con.execute(query).df()

    if daily_df.empty:
        return daily_df, pd.DataFrame(), pd.DataFrame()

    # Format date/time after aggregation, not per row.
    daily_df["Date"] = (pd.to_datetime(daily_df["ist_day_id"], unit="D", origin="unix")).dt.date.astype(str)
    daily_df["Available From IST"] = daily_df["min_req_ts"].apply(lambda x: sec_to_hhmmss((int(x) + IST_OFFSET_SECONDS) % 86400))
    daily_df["Available To IST"] = daily_df["max_req_ts"].apply(lambda x: sec_to_hhmmss((int(x) + IST_OFFSET_SECONDS) % 86400))

    daily_df = daily_df.drop(columns=["ist_day_id", "min_req_ts", "max_req_ts"])

    # Move display columns to front
    front = ["Date", "Available From IST", "Available To IST"]
    daily_df = daily_df[front + [c for c in daily_df.columns if c not in front]]

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
        daily_df[pct_col] = (daily_df[raw_col] / daily_df["Total Row Count"] * 100).round(2)

    daily_df["Partial Day Note"] = daily_df.apply(
        lambda r: (
            f"Partial data: {r['Available From IST']} → {r['Available To IST']}"
            if str(r["Available From IST"]) > "00:05:00" or str(r["Available To IST"]) < "23:55:00"
            else "Full/near-full day"
        ),
        axis=1,
    )

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

    existing_metrics = [m for m in metric_order if m in daily_df.columns]
    transposed_df = daily_df.set_index("Date")[existing_metrics].T.reset_index()
    transposed_df = transposed_df.rename(columns={"index": "Metric"})

    partial_df = daily_df[["Date", "Available From IST", "Available To IST", "Partial Day Note", "Total Row Count"]].copy()

    return daily_df, transposed_df, partial_df


def sec_to_hhmmss(sec: int) -> str:
    sec = int(sec or 0) % 86400
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


st.title("Veto")
st.caption("IST daily table using epoch math for faster grouping.")

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

min_date = epoch_to_ist_date(min_ts)
max_date = epoch_to_ist_date(max_ts)

st.subheader("Available range in IST")
a1, a2, a3, a4 = st.columns(4)
a1.metric("Available From IST", epoch_to_ist_text(min_ts))
a2.metric("Available To IST", epoch_to_ist_text(max_ts))
a3.metric("Selected Files", f"{len(selected_files):,}")
a4.metric("Columns", f"{len(columns):,}")

with st.sidebar:
    st.header("3) Date range in IST")
    date_range = st.date_input("Choose date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
    else:
        start_d, end_d = min_date, max_date

    start_t = st.time_input("Start time IST", dtime(0, 0, 0))
    end_t = st.time_input("End time IST", dtime(23, 59, 59))

start_epoch = dt_to_epoch_ist(start_d, start_t)
end_epoch = dt_to_epoch_ist(end_d, end_t)

st.info(
    "info"
)

if st.button("Build optimized daily table", type="primary"):
    with st.spinner("Building optimized IST daily quality table..."):
        daily_df, transposed_df, partial_df = daily_quality_summary_optimized(
            files=selected_files,
            req_time_col=req_time_col,
            query_col=query_col,
            cliip_col=cliip_col,
            ua_col=ua_col,
            path_col=path_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            status_col=status_col,
            only_200=only_200,
        )

    if daily_df.empty:
        st.warning("No rows found for the selected range.")
        st.stop()

    st.subheader("Partial-day availability check")
    st.dataframe(partial_df, use_container_width=True, hide_index=True)

    partial_notes = partial_df[partial_df["Partial Day Note"] != "Full/near-full day"]
    if not partial_notes.empty:
        st.warning("Some days have partial data. Check the table above before comparing daily counts.")

    st.subheader("Daily table: metrics as rows, dates as columns")
    st.dataframe(transposed_df, use_container_width=True, hide_index=True, height=900)

    st.download_button(
        "Download optimized daily table CSV",
        data=transposed_df.to_csv(index=False).encode("utf-8"),
        file_name="veto_daily_quality_table_optimized_transposed.csv",
        mime="text/csv",
    )

    with st.expander("Show normal row-wise daily data"):
        st.dataframe(daily_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download optimized row-wise daily data CSV",
            data=daily_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_daily_quality_table_optimized_rowwise.csv",
            mime="text/csv",
        )
