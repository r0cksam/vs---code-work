from pathlib import Path
from datetime import datetime, time as dtime
from typing import List

import duckdb
import pandas as pd
import streamlit as st

APP_TITLE = "Veto Simple Daily Metrics"
IST_TZ = "Asia/Kolkata"
IST_OFFSET_SECONDS = 19800

st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")


def _dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    return repr([str(Path(f)).replace("\\", "/") for f in files])


def sec_to_hhmmss(sec: int) -> str:
    sec = int(sec or 0) % 86400
    return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


def dt_to_epoch_ist(d, t) -> int:
    return int(pd.Timestamp(datetime.combine(d, t), tz=IST_TZ).timestamp())


def epoch_to_ist_date(ts: int):
    return pd.to_datetime(int(ts), unit="s", utc=True).tz_convert(IST_TZ).date()


def epoch_to_ist_text(ts: int) -> str:
    return str(pd.to_datetime(int(ts), unit="s", utc=True).tz_convert(IST_TZ))


@st.cache_data(show_spinner=False)
def scan_parquet_files(root_folder: str, recursive: bool = True) -> List[str]:
    root = Path(root_folder).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    return sorted(str(p) for p in (root.rglob("*.parquet") if recursive else root.glob("*.parquet")))


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
        SELECT MIN(TRY_CAST({qts} AS BIGINT)), MAX(TRY_CAST({qts} AS BIGINT))
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) IS NOT NULL
        """,
        [files],
    ).fetchone()


@st.cache_data(show_spinner=False)
def simple_daily_metrics(
    files: List[str],
    req_time_col: str,
    query_col: str,
    cliip_col: str,
    ua_col: str,
    asn_col: str,
    start_epoch: int,
    end_epoch: int,
    exact_distinct: bool = True,
):
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
    asn = _dq(asn_col)
    parquet_sql = _path_list_sql(files)
    distinct = "COUNT(DISTINCT {})" if exact_distinct else "APPROX_COUNT_DISTINCT({})"

    query = f"""
    WITH raw AS (
        SELECT
            TRY_CAST({ts} AS BIGINT) AS req_ts,
            CAST(FLOOR((TRY_CAST({ts} AS BIGINT) + {IST_OFFSET_SECONDS}) / 86400) AS BIGINT) AS ist_day_id,
            CAST({qs} AS VARCHAR) AS queryStr,
            NULLIF(TRIM(CAST({ip} AS VARCHAR)), '') AS cliIP,
            NULLIF(TRIM(CAST({ua} AS VARCHAR)), '') AS UA,
            NULLIF(TRIM(CAST({asn} AS VARCHAR)), '') AS asn_value,
            regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])device_id=([^&]*)', 1) AS raw_device_id,
            regexp_extract(CAST({qs} AS VARCHAR), '(?:^|[?&])session_id=([^&]*)', 1) AS raw_session_id
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
    ),
    clean AS (
        SELECT
            *,
            NULLIF(TRIM(raw_device_id), '') AS device_id,
            NULLIF(TRIM(raw_session_id), '') AS session_id,
            CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])device_id=') THEN 1 ELSE 0 END AS has_device_id_key,
            CASE WHEN regexp_matches(COALESCE(queryStr, ''), '(^|[?&])session_id=') THEN 1 ELSE 0 END AS has_session_id_key
        FROM raw
    )
    SELECT
        ist_day_id,
        MIN(req_ts) AS min_req_ts,
        MAX(req_ts) AS max_req_ts,

        COUNT(*) AS "Total Row Count",
        {distinct.format("cliIP")} AS "Distinct cliIP Count",
        {distinct.format("UA")} AS "Distinct UA Count",

        COUNT(*) FILTER (WHERE device_id IS NOT NULL) AS "Rows with Device_ID Value",
        COUNT(*) FILTER (WHERE device_id IS NULL) AS "Rows without Device_ID Value",
        COUNT(*) FILTER (WHERE has_device_id_key = 1 AND device_id IS NULL) AS "Rows where device_id= Empty",
        COUNT(*) FILTER (WHERE has_device_id_key = 1 AND device_id IS NOT NULL) AS "Rows where device_id= Non-Empty",
        COUNT(*) FILTER (WHERE has_device_id_key = 0) AS "Rows where device_id Key Absent",
        {distinct.format("device_id")} AS "Unique Device_ID Count",

        COUNT(*) FILTER (WHERE session_id IS NOT NULL) AS "Rows with Session_ID Value",
        COUNT(*) FILTER (WHERE session_id IS NULL) AS "Rows without Session_ID Value",
        COUNT(*) FILTER (WHERE has_session_id_key = 1 AND session_id IS NULL) AS "Rows where session_id= Empty",
        COUNT(*) FILTER (WHERE has_session_id_key = 1 AND session_id IS NOT NULL) AS "Rows where session_id= Non-Empty",
        COUNT(*) FILTER (WHERE has_session_id_key = 0) AS "Rows where session_id Key Absent",
        {distinct.format("session_id")} AS "Unique Session_ID Count",

        {distinct.format("asn_value")} AS "Distinct ASN Count"
    FROM clean
    GROUP BY ist_day_id
    ORDER BY ist_day_id
    """

    df = con.execute(query).df()
    if df.empty:
        return df, pd.DataFrame(), pd.DataFrame()

    df["Date"] = pd.to_datetime(df["ist_day_id"], unit="D", origin="unix").dt.date.astype(str)
    df["Available From IST"] = df["min_req_ts"].apply(lambda x: sec_to_hhmmss((int(x) + IST_OFFSET_SECONDS) % 86400))
    df["Available To IST"] = df["max_req_ts"].apply(lambda x: sec_to_hhmmss((int(x) + IST_OFFSET_SECONDS) % 86400))
    df["Partial Day Note"] = df.apply(
        lambda r: f"Partial data: {r['Available From IST']} → {r['Available To IST']}"
        if str(r["Available From IST"]) > "00:05:00" or str(r["Available To IST"]) < "23:55:00"
        else "Full/near-full day",
        axis=1,
    )

    df = df.drop(columns=["ist_day_id", "min_req_ts", "max_req_ts"])
    front = ["Date", "Available From IST", "Available To IST", "Partial Day Note"]
    df = df[front + [c for c in df.columns if c not in front]]

    metric_order = [
        "Available From IST",
        "Available To IST",
        "Partial Day Note",
        "Total Row Count",
        "Distinct cliIP Count",
        "Distinct UA Count",
        "Rows with Device_ID Value",
        "Rows without Device_ID Value",
        "Rows where device_id= Empty",
        "Rows where device_id= Non-Empty",
        "Rows where device_id Key Absent",
        "Unique Device_ID Count",
        "Rows with Session_ID Value",
        "Rows without Session_ID Value",
        "Rows where session_id= Empty",
        "Rows where session_id= Non-Empty",
        "Rows where session_id Key Absent",
        "Unique Session_ID Count",
        "Distinct ASN Count",
    ]

    transposed = df.set_index("Date")[metric_order].T.reset_index().rename(columns={"index": "Metric"})
    partial = df[["Date", "Available From IST", "Available To IST", "Partial Day Note", "Total Row Count"]].copy()
    return df, transposed, partial


st.title("📊 Veto Simple Daily Metrics")
st.caption("Only the metrics requested: date/time, rows, cliIP, UA, device/session keys, unique IDs, ASN.")

with st.sidebar:
    st.header("1) Folder / files")
    folder = st.text_input("Parquet folder", placeholder=r"D:\\Veto Logs Backup")
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
    asn_col = st.selectbox("ASN column", columns, index=idx("asn"))
    exact_distinct = st.checkbox("Use exact distinct counts", value=True)

min_ts, max_ts = get_epoch_range(selected_files, req_time_col)
if min_ts is None or max_ts is None:
    st.error("No valid timestamp range found.")
    st.stop()

min_date = epoch_to_ist_date(min_ts)
max_date = epoch_to_ist_date(max_ts)

st.subheader("Available range in IST")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Available From IST", epoch_to_ist_text(min_ts))
c2.metric("Available To IST", epoch_to_ist_text(max_ts))
c3.metric("Selected Files", f"{len(selected_files):,}")
c4.metric("Columns", f"{len(columns):,}")

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

st.info("Uses epoch filtering and optimized IST grouping: FLOOR((reqTimeSec + 19800) / 86400).")

if st.button("Build simple daily metrics table", type="primary"):
    with st.spinner("Building simple daily metrics table..."):
        daily_df, transposed_df, partial_df = simple_daily_metrics(
            files=selected_files,
            req_time_col=req_time_col,
            query_col=query_col,
            cliip_col=cliip_col,
            ua_col=ua_col,
            asn_col=asn_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            exact_distinct=exact_distinct,
        )

    if daily_df.empty:
        st.warning("No rows found.")
        st.stop()

    st.subheader("Partial-day availability check")
    st.dataframe(partial_df, use_container_width=True, hide_index=True)

    if not partial_df[partial_df["Partial Day Note"] != "Full/near-full day"].empty:
        st.warning("Some days have partial data. Check the available time range before comparing counts.")

    st.subheader("Daily table: metrics as rows, dates as columns")
    st.dataframe(transposed_df, use_container_width=True, hide_index=True, height=720)

    st.download_button(
        "Download simple daily table CSV",
        data=transposed_df.to_csv(index=False).encode("utf-8"),
        file_name="veto_simple_daily_metrics_transposed.csv",
        mime="text/csv",
    )

    with st.expander("Show row-wise version"):
        st.dataframe(daily_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download row-wise CSV",
            data=daily_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_simple_daily_metrics_rowwise.csv",
            mime="text/csv",
        )
