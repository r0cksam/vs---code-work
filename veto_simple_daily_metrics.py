from pathlib import Path
from datetime import datetime, time as dtime
from typing import List

import duckdb
import pandas as pd
import streamlit as st

APP_TITLE = "Veto Simple Daily Metrics - UTC"
SECONDS_PER_DAY = 86400

st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")

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


def epoch_to_utc_dt(ts: int) -> pd.Timestamp:
    return pd.to_datetime(int(ts), unit="s", utc=True)


def epoch_to_utc_text(ts: int) -> str:
    return epoch_to_utc_dt(ts).strftime("%d/%m/%y %H:%M:%S")


def epoch_to_utc_date(ts: int):
    return epoch_to_utc_dt(ts).date()


def dt_to_epoch_utc(d, t) -> int:
    return int(pd.Timestamp(datetime.combine(d, t), tz="UTC").timestamp())


def day_id_to_date_text(day_id: int) -> str:
    return pd.to_datetime(int(day_id), unit="D", origin="unix").strftime("%d/%m/%y")


def sec_to_hhmmss(sec: int) -> str:
    sec = int(sec or 0) % SECONDS_PER_DAY
    return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


def epoch_to_time_utc(ts: int) -> str:
    return sec_to_hhmmss(int(ts) % SECONDS_PER_DAY)


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
        SELECT
            MIN(TRY_CAST({qts} AS BIGINT)) AS min_ts,
            MAX(TRY_CAST({qts} AS BIGINT)) AS max_ts
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST({qts} AS BIGINT) IS NOT NULL
        """,
        [files],
    ).fetchone()


# ─────────────────────────────────────────────
# Date availability
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def utc_day_availability(
    files: List[str],
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
):
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")

    ts = _dq(req_time_col)
    parquet_sql = _path_list_sql(files)

    df = con.execute(
        f"""
        SELECT
            CAST(FLOOR(TRY_CAST({ts} AS BIGINT) / 86400) AS BIGINT) AS utc_day_id,
            COUNT(*) AS "Total Row Count",
            MIN(TRY_CAST({ts} AS BIGINT)) AS min_req_ts,
            MAX(TRY_CAST({ts} AS BIGINT)) AS max_req_ts
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).df()

    if df.empty:
        return df

    df["Date"] = df["utc_day_id"].apply(day_id_to_date_text)
    df["Available From UTC"] = df["min_req_ts"].apply(epoch_to_utc_text)
    df["Available To UTC"] = df["max_req_ts"].apply(epoch_to_utc_text)
    df["Partial Day Note"] = df.apply(
        lambda r: (
            f"Partial data: {epoch_to_time_utc(r['min_req_ts'])} → {epoch_to_time_utc(r['max_req_ts'])} UTC"
            if epoch_to_time_utc(r["min_req_ts"]) > "00:05:00" or epoch_to_time_utc(r["max_req_ts"]) < "23:55:00"
            else "Full/near-full UTC day"
        ),
        axis=1,
    )

    return df[["Date", "Available From UTC", "Available To UTC", "Partial Day Note", "Total Row Count"]]


# ─────────────────────────────────────────────
# Daily metrics
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def simple_daily_metrics_utc(
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
            CAST(FLOOR(TRY_CAST({ts} AS BIGINT) / 86400) AS BIGINT) AS utc_day_id,
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
        utc_day_id,
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
    GROUP BY utc_day_id
    ORDER BY utc_day_id
    """

    df = con.execute(query).df()

    if df.empty:
        return df, pd.DataFrame(), pd.DataFrame()

    df["Date"] = df["utc_day_id"].apply(day_id_to_date_text)
    df["Available From UTC"] = df["min_req_ts"].apply(epoch_to_utc_text)
    df["Available To UTC"] = df["max_req_ts"].apply(epoch_to_utc_text)
    df["Partial Day Note"] = df.apply(
        lambda r: (
            f"Partial data: {epoch_to_time_utc(r['min_req_ts'])} → {epoch_to_time_utc(r['max_req_ts'])} UTC"
            if epoch_to_time_utc(r["min_req_ts"]) > "00:05:00" or epoch_to_time_utc(r["max_req_ts"]) < "23:55:00"
            else "Full/near-full UTC day"
        ),
        axis=1,
    )

    df = df.drop(columns=["utc_day_id", "min_req_ts", "max_req_ts"])

    front = ["Date", "Available From UTC", "Available To UTC", "Partial Day Note"]
    df = df[front + [c for c in df.columns if c not in front]]

    metric_order = [
        "Available From UTC",
        "Available To UTC",
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
    partial = df[["Date", "Available From UTC", "Available To UTC", "Partial Day Note", "Total Row Count"]].copy()

    return df, transposed, partial


# ─────────────────────────────────────────────
# Details sections: daily style
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def value_counts_by_utc_day(
    files: List[str],
    value_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    top_n: int = 500,
):
    """Return daily-style value counts.

    Output:
    - matrix: rows = values, columns = UTC dates, cells = row_count
    - rowwise: Date/value/row_count table
    - total_unique: unique count for full range
    """
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    try:
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    vcol = _dq(value_col)
    tcol = _dq(req_time_col)
    parquet_sql = _path_list_sql(files)

    # First get top values over the whole selected range, so UA/city tables do not explode.
    top_values_df = con.execute(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(CAST({vcol} AS VARCHAR)), ''), '(blank/null)') AS value,
            COUNT(*) AS total_row_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({tcol} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({tcol} AS BIGINT) IS NOT NULL
        GROUP BY 1
        ORDER BY total_row_count DESC
        LIMIT {int(top_n)}
        """
    ).df()

    total_unique = con.execute(
        f"""
        SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(CAST({vcol} AS VARCHAR)), ''), '(blank/null)'))
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({tcol} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({tcol} AS BIGINT) IS NOT NULL
        """
    ).fetchone()[0]

    if top_values_df.empty:
        return pd.DataFrame(), pd.DataFrame(), int(total_unique or 0), top_values_df

    # Register top values to filter daily table.
    con.register("top_values", top_values_df[["value"]])

    rowwise = con.execute(
        f"""
        WITH base AS (
            SELECT
                CAST(FLOOR(TRY_CAST({tcol} AS BIGINT) / 86400) AS BIGINT) AS utc_day_id,
                COALESCE(NULLIF(TRIM(CAST({vcol} AS VARCHAR)), ''), '(blank/null)') AS value
            FROM read_parquet({parquet_sql}, union_by_name=true)
            WHERE TRY_CAST({tcol} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
              AND TRY_CAST({tcol} AS BIGINT) IS NOT NULL
        )
        SELECT
            b.utc_day_id,
            b.value,
            COUNT(*) AS row_count
        FROM base b
        INNER JOIN top_values t ON b.value = t.value
        GROUP BY 1, 2
        ORDER BY 1, row_count DESC
        """
    ).df()

    if rowwise.empty:
        return pd.DataFrame(), rowwise, int(total_unique or 0), top_values_df

    rowwise["Date"] = rowwise["utc_day_id"].apply(day_id_to_date_text)
    rowwise = rowwise[["Date", "value", "row_count"]]

    matrix = rowwise.pivot_table(
        index="value",
        columns="Date",
        values="row_count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Add total row count and sort by total desc.
    date_cols = [c for c in matrix.columns if c != "value"]
    matrix["Total Row Count"] = matrix[date_cols].sum(axis=1)
    matrix = matrix.sort_values("Total Row Count", ascending=False)
    matrix = matrix[["value", "Total Row Count"] + date_cols]

    return matrix, rowwise, int(total_unique or 0), top_values_df


def render_daily_style_detail_tab(
    title: str,
    file_prefix: str,
    files: List[str],
    value_col: str,
    req_time_col: str,
    start_epoch: int,
    end_epoch: int,
    top_n: int,
    availability_df: pd.DataFrame,
):
    st.subheader(title)

    st.markdown("#### Date/time availability in UTC")
    st.dataframe(availability_df, use_container_width=True, hide_index=True)

    with st.spinner(f"Loading {title}..."):
        matrix, rowwise, total_unique, top_values = value_counts_by_utc_day(
            files=files,
            value_col=value_col,
            req_time_col=req_time_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            top_n=int(top_n),
        )

    m1, m2, m3 = st.columns(3)
    m1.metric("Total unique values in selected range", f"{total_unique:,}")
    m2.metric("Top values shown", f"{len(matrix):,}")
    m3.metric("Rows represented by shown values", f"{int(matrix['Total Row Count'].sum()) if not matrix.empty else 0:,}")

    st.markdown("#### Daily table: values as rows, UTC dates as columns")
    st.dataframe(matrix, use_container_width=True, hide_index=True, height=700)

    st.download_button(
        f"Download {title} daily matrix CSV",
        data=matrix.to_csv(index=False).encode("utf-8"),
        file_name=f"{file_prefix}_daily_matrix_utc.csv",
        mime="text/csv",
    )

    with st.expander("Show row-wise value/date counts"):
        st.dataframe(rowwise, use_container_width=True, hide_index=True)
        st.download_button(
            f"Download {title} row-wise CSV",
            data=rowwise.to_csv(index=False).encode("utf-8"),
            file_name=f"{file_prefix}_rowwise_utc.csv",
            mime="text/csv",
        )


# ─────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────

st.title("📊 Veto Simple Daily Metrics - UTC")
st.caption(
    "UTC version. Dates/times are displayed as dd/mm/yy hh:mm:ss. "
    "All detail sections use daily-style tables."
)

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
    city_col = st.selectbox("City column", columns, index=idx("city"))
    state_col = st.selectbox("State column", columns, index=idx("state"))
    country_col = st.selectbox("Country column", columns, index=idx("country"))

    exact_distinct = st.checkbox(
        "Use exact distinct counts",
        value=True,
        help="For very large month data, turn off to use approximate distinct counts faster for the daily summary.",
    )

    top_n = st.number_input(
        "Top values to show per detail section",
        min_value=10,
        max_value=100000,
        value=500,
        step=100,
    )

min_ts, max_ts = get_epoch_range(selected_files, req_time_col)
if min_ts is None or max_ts is None:
    st.error("No valid timestamp range found.")
    st.stop()

min_date = epoch_to_utc_date(min_ts)
max_date = epoch_to_utc_date(max_ts)

st.subheader("Available range in UTC")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Available From UTC", epoch_to_utc_text(min_ts))
c2.metric("Available To UTC", epoch_to_utc_text(max_ts))
c3.metric("Selected Files", f"{len(selected_files):,}")
c4.metric("Columns", f"{len(columns):,}")

with st.sidebar:
    st.header("3) Date range in UTC")
    date_range = st.date_input(
        "Choose date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
    else:
        start_d, end_d = min_date, max_date

    start_t = st.time_input("Start time UTC", dtime(0, 0, 0))
    end_t = st.time_input("End time UTC", dtime(23, 59, 59))

start_epoch = dt_to_epoch_utc(start_d, start_t)
end_epoch = dt_to_epoch_utc(end_d, end_t)

st.info(
    "UTC optimized grouping: `FLOOR(reqTimeSec / 86400)`. "
    "Displayed date/time format is `dd/mm/yy hh:mm:ss`."
)

# Build availability once for all tabs.
availability_df = utc_day_availability(
    files=selected_files,
    req_time_col=req_time_col,
    start_epoch=start_epoch,
    end_epoch=end_epoch,
)

tabs = st.tabs(["Daily Metrics", "ASN Details", "UA Details", "City Details", "State Details", "Country Details"])


with tabs[0]:
    if st.button("Build daily metrics table", type="primary"):
        with st.spinner("Building UTC daily metrics table..."):
            daily_df, transposed_df, partial_df = simple_daily_metrics_utc(
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

        st.subheader("Date/time availability in UTC")
        st.dataframe(partial_df, use_container_width=True, hide_index=True)

        if not partial_df[partial_df["Partial Day Note"] != "Full/near-full UTC day"].empty:
            st.warning("Some days have partial data. Check the available UTC time range before comparing counts.")

        st.subheader("Daily table: metrics as rows, UTC dates as columns")
        st.dataframe(transposed_df, use_container_width=True, hide_index=True, height=720)

        st.download_button(
            "Download daily metrics CSV",
            data=transposed_df.to_csv(index=False).encode("utf-8"),
            file_name="veto_daily_metrics_utc_transposed.csv",
            mime="text/csv",
        )

        with st.expander("Show row-wise daily metrics"):
            st.dataframe(daily_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download row-wise daily metrics CSV",
                data=daily_df.to_csv(index=False).encode("utf-8"),
                file_name="veto_daily_metrics_utc_rowwise.csv",
                mime="text/csv",
            )


with tabs[1]:
    if st.button("Load ASN daily table", type="primary"):
        render_daily_style_detail_tab(
            title="ASN daily value counts",
            file_prefix="asn",
            files=selected_files,
            value_col=asn_col,
            req_time_col=req_time_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            top_n=int(top_n),
            availability_df=availability_df,
        )


with tabs[2]:
    if st.button("Load UA daily table", type="primary"):
        render_daily_style_detail_tab(
            title="UA daily value counts",
            file_prefix="ua",
            files=selected_files,
            value_col=ua_col,
            req_time_col=req_time_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            top_n=int(top_n),
            availability_df=availability_df,
        )


with tabs[3]:
    if st.button("Load City daily table", type="primary"):
        render_daily_style_detail_tab(
            title="City daily value counts",
            file_prefix="city",
            files=selected_files,
            value_col=city_col,
            req_time_col=req_time_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            top_n=int(top_n),
            availability_df=availability_df,
        )


with tabs[4]:
    if st.button("Load State daily table", type="primary"):
        render_daily_style_detail_tab(
            title="State daily value counts",
            file_prefix="state",
            files=selected_files,
            value_col=state_col,
            req_time_col=req_time_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            top_n=int(top_n),
            availability_df=availability_df,
        )


with tabs[5]:
    if st.button("Load Country daily table", type="primary"):
        render_daily_style_detail_tab(
            title="Country daily value counts",
            file_prefix="country",
            files=selected_files,
            value_col=country_col,
            req_time_col=req_time_col,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            top_n=int(top_n),
            availability_df=availability_df,
        )
