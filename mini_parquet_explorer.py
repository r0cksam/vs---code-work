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

APP_TITLE = "Mini Parquet Explorer"
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


st.title("🧮 Mini Parquet Explorer")
st.caption("Folder/file selection is on the left. The available reqTimeSec date range appears at the top before running distinct counts.")

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
st.caption(f"Shown in {tz_mode}. Raw available epoch range: `{min_epoch}` → `{max_epoch}`.")

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
st.subheader("Distinct counts — lazy load")
if comparison_mode:
    st.caption("Click run and the table updates one column at a time with Range A, Range B, and delta values.")
    run_query = st.button("▶️ Lazy load comparison distinct counts", type="primary")
else:
    st.caption("Click run and the table below updates one column at a time while the query is processing.")
    run_query = st.button("▶️ Lazy load distinct counts for every column", type="primary")

if run_query:
    if comparison_mode:
        with st.spinner("Counting rows for comparison ranges ..."):
            a_rows = count_rows(selected_files, req_time_col, a_start_epoch, a_end_epoch)
            b_rows = count_rows(selected_files, req_time_col, b_start_epoch, b_end_epoch)
        st.session_state["mini_total_rows"] = {"A": a_rows, "B": b_rows}

        m1, m2, m3 = st.columns(3)
        m1.metric("Range A rows", f"{a_rows:,}")
        m2.metric("Range B rows", f"{b_rows:,}")
        m3.metric("Status", "Processing")

        result_df = compare_distinct_counts_for_columns_live(
            selected_files, columns, req_time_col,
            a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch,
        )
        st.session_state["mini_result_df"] = result_df
        st.session_state["mini_result_key"] = (
            "compare", tuple(selected_files), req_time_col,
            a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch,
        )
    else:
        with st.spinner("Counting rows in selected date range ..."):
            total_rows = count_rows(selected_files, req_time_col, start_epoch, end_epoch)
        st.session_state["mini_total_rows"] = total_rows

        m1, m2, m3 = st.columns(3)
        m1.metric("Rows in date range", f"{total_rows:,}")
        m2.metric("Columns to process", f"{len(columns):,}")
        m3.metric("Status", "Processing")

        result_df = distinct_counts_for_columns_live(selected_files, columns, req_time_col, start_epoch, end_epoch)
        st.session_state["mini_result_df"] = result_df
        st.session_state["mini_result_key"] = ("single", tuple(selected_files), req_time_col, start_epoch, end_epoch)
    st.rerun()

result_df = st.session_state.get("mini_result_df")
result_key = st.session_state.get("mini_result_key")
current_key = (
    "compare", tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch,
) if comparison_mode else ("single", tuple(selected_files), req_time_col, start_epoch, end_epoch)

if result_df is not None and result_key == current_key:
    if comparison_mode:
        totals = st.session_state.get("mini_total_rows", {"A": 0, "B": 0})
        m1, m2, m3 = st.columns(3)
        m1.metric("Range A rows", f"{int(totals.get('A', 0)):,}")
        m2.metric("Range B rows", f"{int(totals.get('B', 0)):,}")
        m3.metric("Columns compared", f"{len(result_df):,}")
        st.caption(f"Range A: {range_a_label}  |  Range B: {range_b_label}")
    else:
        total_rows = st.session_state.get("mini_total_rows", 0)
        m1, m2, m3 = st.columns(3)
        m1.metric("Rows in date range", f"{total_rows:,}")
        m2.metric("Columns counted", f"{len(result_df):,}")
        m3.metric("Completed", "Yes")

    search = st.text_input("Search result columns", placeholder="type a column name ...", key="mini_distinct_search")
    show_df = result_df.copy()
    if search:
        show_df = show_df[show_df["Column"].str.contains(search, case=False, na=False)]

    st.dataframe(show_df, use_container_width=True, hide_index=True, height=560)
    st.download_button(
        "Download distinct counts CSV",
        data=result_df.to_csv(index=False).encode("utf-8"),
        file_name="mini_comparison_distinct_counts_by_column.csv" if comparison_mode else "mini_distinct_counts_by_column.csv",
        mime="text/csv",
    )
elif result_df is not None and result_key != current_key:
    st.info("Selections changed. Run lazy distinct counts again for the current file/date range.")
else:
    if comparison_mode:
        st.info("Click **Lazy load comparison distinct counts** to compare Range A vs Range B as each column finishes.")
    else:
        st.info("Click **Lazy load distinct counts for every column** to show results as each column finishes.")


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
# Grafana-style output tables from the sample workbook
# ─────────────────────────────────────────────
st.markdown("---")
st.subheader("Grafana-style daily tables")
st.caption(
    "This section follows your sample workbook: raw mapped rows, daily distinct IP/device summary, "
    "80% consumption concentration, and content watched by IP/device. It uses the same master date range above."
)


def _guess_option(options: List[str], guesses: List[str], allow_blank: bool = False) -> int:
    opts = ([""] + options) if allow_blank else options
    lowered = [str(x).lower() for x in opts]
    for guess in guesses:
        gl = guess.lower()
        for idx, val in enumerate(lowered):
            if val == gl:
                return idx
    for guess in guesses:
        gl = guess.lower()
        for idx, val in enumerate(lowered):
            if gl and gl in val:
                return idx
    return 0


def _varchar_expr(col: str) -> str:
    return f"CAST({_dq(col)} AS VARCHAR)" if col else "NULL"


def _numeric_expr(col: str) -> str:
    return f"TRY_CAST({_dq(col)} AS DOUBLE)" if col else "0.0"


def _qs_extract_expr(qs_col: str, key: str) -> str:
    if not qs_col or not key:
        return "NULL"
    # Supports raw query strings with or without a leading ?. Values remain URL-encoded, same as the fast DuckDB path.
    return f"regexp_extract(CAST({_dq(qs_col)} AS VARCHAR), '(?:^|[?&]){key}=([^&]+)', 1)"


def _source_expr(source_type: str, source_col: str, qs_col: str, qs_key: str) -> str:
    if source_type == "Parsed query-string key":
        return _qs_extract_expr(qs_col, qs_key)
    if source_type == "Direct column":
        return _varchar_expr(source_col)
    return "NULL"


def _event_date_expr(req_time_col: str, tz_name: str) -> str:
    zone = "Asia/Kolkata" if tz_name == "IST" else "UTC"
    return f"CAST(to_timestamp(TRY_CAST({_dq(req_time_col)} AS BIGINT)) AT TIME ZONE '{zone}' AS DATE)"


def _build_grafana_base_sql(
    files: List[str],
    req_time_col: str,
    start_ep: int,
    end_ep: int,
    tz_name: str,
    ip_col: str,
    qs_col: str,
    device_source: str,
    device_col: str,
    device_qs_key: str,
    duration_col: str,
    size_col: str,
    market_col: str,
    city_col: str,
    country_col: str,
    channel_source: str,
    channel_col: str,
    channel_qs_key: str,
    content_source: str,
    content_col: str,
    content_qs_key: str,
) -> str:
    q_ts = _dq(req_time_col)
    time_filter = f"TRY_CAST({q_ts} AS BIGINT) BETWEEN {int(start_ep)} AND {int(end_ep)}"
    ip_expr = _varchar_expr(ip_col)
    device_expr = _source_expr(device_source, device_col, qs_col, device_qs_key)
    channel_expr = _source_expr(channel_source, channel_col, qs_col, channel_qs_key)
    content_expr = _source_expr(content_source, content_col, qs_col, content_qs_key)
    return f"""
        WITH base AS (
            SELECT
                {_event_date_expr(req_time_col, tz_name)} AS event_date,
                COALESCE(NULLIF(TRIM({ip_expr}), ''), '') AS ip,
                COALESCE(NULLIF(TRIM({device_expr}), ''), '') AS device_id,
                COALESCE({_numeric_expr(duration_col)}, 0.0) AS duration_sec,
                COALESCE({_numeric_expr(size_col)}, 0.0) AS total_size,
                COALESCE(NULLIF(TRIM({_varchar_expr(market_col)}), ''), '') AS market,
                COALESCE(NULLIF(TRIM({_varchar_expr(city_col)}), ''), '') AS city,
                COALESCE(NULLIF(TRIM({_varchar_expr(country_col)}), ''), '') AS country,
                COALESCE(NULLIF(TRIM({channel_expr}), ''), '') AS channel_name,
                COALESCE(NULLIF(TRIM({content_expr}), ''), NULLIF(TRIM({channel_expr}), ''), '') AS content_name
            FROM read_parquet({_path_list_sql(files)}, union_by_name=true)
            WHERE {time_filter}
        )
    """


def _grafana_tables_for_range(base_sql: str, consumption_label: str) -> dict:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass

    raw_query = base_sql + """
        SELECT
            event_date AS Date,
            ip AS IP,
            device_id AS Device_ID,
            duration_sec AS "Duration (S)",
            total_size AS "Total Size",
            market AS Market,
            city AS City,
            country AS Country,
            channel_name AS "Channel Name",
            content_name AS "Content Name"
        FROM base
        ORDER BY event_date
        LIMIT 1000
    """
    daily_query = base_sql + """
        SELECT
            event_date AS Date,
            COUNT(*) AS Rows,
            COUNT(DISTINCT NULLIF(ip, '')) AS "Distinct Count IP",
            COUNT(DISTINCT NULLIF(device_id, '')) AS "Distinct Count Device",
            ROUND(SUM(duration_sec) / 3600.0, 4) AS "Total Watch Hour",
            ROUND(SUM(duration_sec) / 3600.0 / NULLIF(COUNT(DISTINCT NULLIF(ip, '')), 0), 4) AS "Watch Hour / IP",
            ROUND(SUM(duration_sec) / 3600.0 / NULLIF(COUNT(DISTINCT NULLIF(device_id, '')), 0), 4) AS "Watch Hour / Device",
            ROUND(SUM(total_size), 2) AS "Total Size",
            ROUND(SUM(total_size) / NULLIF(COUNT(DISTINCT NULLIF(device_id, '')), 0), 2) AS "Total Size / Device",
            ROUND(SUM(total_size) / NULLIF(COUNT(DISTINCT NULLIF(ip, '')), 0), 2) AS "Total Size / IP"
        FROM base
        GROUP BY 1
        ORDER BY 1
    """
    content_query = base_sql + """
        SELECT
            event_date AS Date,
            COALESCE(NULLIF(content_name, ''), '(unknown)') AS "Content Name",
            COUNT(*) AS Requests,
            COUNT(DISTINCT NULLIF(ip, '')) AS "Distinct IP",
            COUNT(DISTINCT NULLIF(device_id, '')) AS "Distinct Device",
            ROUND(SUM(duration_sec) / 3600.0, 4) AS "Total Watch Hour",
            ROUND(SUM(duration_sec) / 3600.0 / NULLIF(COUNT(DISTINCT NULLIF(ip, '')), 0), 4) AS "Watch Hour / IP",
            ROUND(SUM(duration_sec) / 3600.0 / NULLIF(COUNT(DISTINCT NULLIF(device_id, '')), 0), 4) AS "Watch Hour / Device",
            ROUND(SUM(total_size), 2) AS "Total Size"
        FROM base
        GROUP BY 1, 2
        ORDER BY 1, "Total Watch Hour" DESC, Requests DESC
        LIMIT 5000
    """
    by_ip_query = base_sql + """
        SELECT ip AS entity, COUNT(*) AS requests, SUM(duration_sec) AS consumption_sec, SUM(total_size) AS total_size
        FROM base
        WHERE ip <> ''
        GROUP BY 1
        ORDER BY consumption_sec DESC, requests DESC
    """
    by_device_query = base_sql + """
        SELECT device_id AS entity, COUNT(*) AS requests, SUM(duration_sec) AS consumption_sec, SUM(total_size) AS total_size
        FROM base
        WHERE device_id <> ''
        GROUP BY 1
        ORDER BY consumption_sec DESC, requests DESC
    """

    raw_df = con.execute(raw_query).df()
    daily_df = con.execute(daily_query).df()
    content_df = con.execute(content_query).df()
    by_ip_df = con.execute(by_ip_query).df()
    by_device_df = con.execute(by_device_query).df()

    # If no duration column was mapped, consumption_sec will be zero. Fall back to request count for the 80% curve.
    def _concentration(entity_df: pd.DataFrame, entity_name: str) -> dict:
        if entity_df.empty:
            return {"Entity": entity_name, "Total Entities": 0, f"Entities for 80% of {consumption_label}": 0, "% Entities for 80%": 0.0, "Total Consumption": 0.0}
        work = entity_df.copy()
        if float(work["consumption_sec"].sum() or 0) <= 0:
            work["consumption"] = work["requests"].astype(float)
            label_total = float(work["consumption"].sum())
        else:
            work["consumption"] = work["consumption_sec"].astype(float)
            label_total = float(work["consumption"].sum()) / 3600.0
        work = work.sort_values("consumption", ascending=False).reset_index(drop=True)
        total = float(work["consumption"].sum() or 0)
        if total <= 0:
            n80 = 0
        else:
            n80 = int((work["consumption"].cumsum() < total * 0.8).sum() + 1)
        return {
            "Entity": entity_name,
            "Total Entities": int(len(work)),
            f"Entities for 80% of {consumption_label}": n80,
            "% Entities for 80%": round(n80 / max(len(work), 1) * 100, 2),
            "Total Consumption": round(label_total, 4),
        }

    concentration_df = pd.DataFrame([
        _concentration(by_ip_df, "IP"),
        _concentration(by_device_df, "Device_ID"),
    ])

    answer_rows = []
    if not daily_df.empty:
        total_ip = int(daily_df["Distinct Count IP"].sum())
        total_device = int(daily_df["Distinct Count Device"].sum())
        ip_watch = float(daily_df["Watch Hour / IP"].mean() or 0)
        dev_watch = float(daily_df["Watch Hour / Device"].mean() or 0)
        answer_rows.append({
            "Question": "IP and Device consumption are same?",
            "Answer": "Close" if abs(ip_watch - dev_watch) <= max(ip_watch, dev_watch, 1) * 0.05 else "Different",
            "Notes": f"Avg watch hour/IP={ip_watch:.4f}; avg watch hour/device={dev_watch:.4f}; daily IP distinct sum={total_ip:,}; daily device distinct sum={total_device:,}.",
        })
        for _, row in concentration_df.iterrows():
            answer_rows.append({
                "Question": f"What % of {row['Entity']} consume 80% of {consumption_label}?",
                "Answer": f"{row['% Entities for 80%']}%",
                "Notes": f"{int(row[f'Entities for 80% of {consumption_label}']):,} of {int(row['Total Entities']):,} {row['Entity']} values account for 80%.",
            })
        answer_rows.append({
            "Question": "Content watched by IP and Device",
            "Answer": f"{len(content_df):,} date/content rows",
            "Notes": "See the Content watched table below.",
        })
    answers_df = pd.DataFrame(answer_rows)

    return {
        "raw": raw_df,
        "daily": daily_df,
        "concentration": concentration_df,
        "answers": answers_df,
        "content": content_df,
        "by_ip": by_ip_df.head(500),
        "by_device": by_device_df.head(500),
    }


with st.expander("Build Grafana-style tables", expanded=False):
    st.markdown("#### Column mapping")
    st.caption("Map your parquet columns to the sample Excel fields. Blank optional fields are allowed.")

    none_plus_cols = [""] + columns
    g1, g2, g3 = st.columns(3)
    with g1:
        grafana_ip_col = st.selectbox("IP column", options=none_plus_cols, index=_guess_option(columns, ["cliIP", "client_ip", "ip"], True), key="grafana_ip_col")
        grafana_duration_col = st.selectbox("Duration seconds column", options=none_plus_cols, index=_guess_option(columns, ["duration", "duration_sec", "watch_sec", "watchTime", "transferTimeMSec", "downloadTime"], True), key="grafana_duration_col", help="Use the field that represents watch duration in seconds. If blank/zero, 80% concentration falls back to request count.")
        grafana_size_col = st.selectbox("Total size column", options=none_plus_cols, index=_guess_option(columns, ["total_size", "size", "bytes", "responseSize"], True), key="grafana_size_col")
    with g2:
        grafana_qs_col = st.selectbox("Query string column", options=none_plus_cols, index=_guess_option(columns, ["queryStr", "qrystr", "query_string"], True), key="grafana_qs_col")
        grafana_device_source = st.radio("Device_ID source", ["Parsed query-string key", "Direct column", "None"], horizontal=True, key="grafana_device_source")
        grafana_device_col = st.selectbox("Device_ID direct column", options=none_plus_cols, index=_guess_option(columns, ["device_id", "deviceId", "device"], True), key="grafana_device_col")
        grafana_device_qs_key = st.text_input("Device_ID query-string key", value="device_id", key="grafana_device_qs_key")
    with g3:
        grafana_market_col = st.selectbox("Market column", options=none_plus_cols, index=_guess_option(columns, ["market"], True), key="grafana_market_col")
        grafana_city_col = st.selectbox("City column", options=none_plus_cols, index=_guess_option(columns, ["city", "city_name"], True), key="grafana_city_col")
        grafana_country_col = st.selectbox("Country column", options=none_plus_cols, index=_guess_option(columns, ["country", "country_code"], True), key="grafana_country_col")

    c1, c2 = st.columns(2)
    with c1:
        grafana_channel_source = st.radio("Channel Name source", ["Parsed query-string key", "Direct column", "None"], horizontal=True, key="grafana_channel_source")
        grafana_channel_col = st.selectbox("Channel direct column", options=none_plus_cols, index=_guess_option(columns, ["channel", "channel_name", "Channel Name"], True), key="grafana_channel_col")
        grafana_channel_qs_key = st.text_input("Channel query-string key", value="channel", key="grafana_channel_qs_key")
    with c2:
        grafana_content_source = st.radio("Content Name source", ["Parsed query-string key", "Direct column", "None"], horizontal=True, key="grafana_content_source")
        grafana_content_col = st.selectbox("Content direct column", options=none_plus_cols, index=_guess_option(columns, ["content_title", "content_name", "title", "content"], True), key="grafana_content_col")
        grafana_content_qs_key = st.text_input("Content query-string key", value="content_title", key="grafana_content_qs_key")

    consumption_label = "watch time" if grafana_duration_col else "requests"
    run_grafana = st.button("▶️ Build sample-style tables", type="primary", key="grafana_run")

    if run_grafana:
        def _run_one_range(label: str, start_ep: int, end_ep: int):
            base_sql = _build_grafana_base_sql(
                selected_files,
                req_time_col,
                start_ep,
                end_ep,
                tz_mode,
                grafana_ip_col,
                grafana_qs_col,
                grafana_device_source,
                grafana_device_col,
                grafana_device_qs_key,
                grafana_duration_col,
                grafana_size_col,
                grafana_market_col,
                grafana_city_col,
                grafana_country_col,
                grafana_channel_source,
                grafana_channel_col,
                grafana_channel_qs_key,
                grafana_content_source,
                grafana_content_col,
                grafana_content_qs_key,
            )
            with st.spinner(f"Building Grafana-style tables for {label} ..."):
                return _grafana_tables_for_range(base_sql, consumption_label)

        if comparison_mode:
            payload_a = _run_one_range("Range A", a_start_epoch, a_end_epoch)
            payload_b = _run_one_range("Range B", b_start_epoch, b_end_epoch)
            st.session_state["grafana_payload"] = {
                "mode": "compare",
                "key": (tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, tz_mode),
                "A": payload_a,
                "B": payload_b,
            }
        else:
            payload = _run_one_range("selected range", start_epoch, end_epoch)
            st.session_state["grafana_payload"] = {
                "mode": "single",
                "key": (tuple(selected_files), req_time_col, start_epoch, end_epoch, tz_mode),
                "single": payload,
            }

    grafana_payload = st.session_state.get("grafana_payload")
    expected_key = (tuple(selected_files), req_time_col, a_start_epoch, a_end_epoch, b_start_epoch, b_end_epoch, tz_mode) if comparison_mode else (tuple(selected_files), req_time_col, start_epoch, end_epoch, tz_mode)
    if grafana_payload and grafana_payload.get("key") == expected_key:
        def _render_payload(payload: dict, prefix: str):
            k1, k2, k3, k4 = st.columns(4)
            daily = payload.get("daily", pd.DataFrame())
            k1.metric("Dates", f"{daily['Date'].nunique():,}" if not daily.empty else "0")
            k2.metric("Distinct IP sum", f"{int(daily['Distinct Count IP'].sum()):,}" if not daily.empty else "0")
            k3.metric("Distinct Device sum", f"{int(daily['Distinct Count Device'].sum()):,}" if not daily.empty else "0")
            k4.metric("Watch hours", f"{float(daily['Total Watch Hour'].sum()):,.2f}" if not daily.empty else "0")

            st.markdown("##### Table 1 — Raw mapped rows preview")
            st.dataframe(payload.get("raw", pd.DataFrame()), use_container_width=True, hide_index=True, height=260)
            st.download_button("Download Table 1 raw CSV", data=payload.get("raw", pd.DataFrame()).to_csv(index=False).encode("utf-8"), file_name=f"{prefix}_table1_raw.csv", mime="text/csv", key=f"{prefix}_raw_dl")

            st.markdown("##### Table 2 — Daily summary")
            st.dataframe(payload.get("daily", pd.DataFrame()), use_container_width=True, hide_index=True, height=260)
            st.download_button("Download Table 2 daily summary CSV", data=payload.get("daily", pd.DataFrame()).to_csv(index=False).encode("utf-8"), file_name=f"{prefix}_table2_daily_summary.csv", mime="text/csv", key=f"{prefix}_daily_dl")

            st.markdown("##### Questions from sample")
            st.dataframe(payload.get("answers", pd.DataFrame()), use_container_width=True, hide_index=True, height=180)
            st.markdown("##### 80% consumption distribution")
            st.dataframe(payload.get("concentration", pd.DataFrame()), use_container_width=True, hide_index=True, height=120)

            st.markdown("##### Table 3 — Content watched by IP and Device")
            st.dataframe(payload.get("content", pd.DataFrame()), use_container_width=True, hide_index=True, height=360)
            st.download_button("Download Table 3 content watched CSV", data=payload.get("content", pd.DataFrame()).to_csv(index=False).encode("utf-8"), file_name=f"{prefix}_table3_content_watched.csv", mime="text/csv", key=f"{prefix}_content_dl")

            with st.expander("Top IP and Device consumption details", expanded=False):
                bi, bd = st.tabs(["Top IP", "Top Device_ID"])
                with bi:
                    st.dataframe(payload.get("by_ip", pd.DataFrame()), use_container_width=True, hide_index=True, height=300)
                with bd:
                    st.dataframe(payload.get("by_device", pd.DataFrame()), use_container_width=True, hide_index=True, height=300)

        if grafana_payload.get("mode") == "compare":
            tab_a, tab_b = st.tabs(["Range A sample tables", "Range B sample tables"])
            with tab_a:
                st.caption(range_a_label)
                _render_payload(grafana_payload["A"], "range_a")
            with tab_b:
                st.caption(range_b_label)
                _render_payload(grafana_payload["B"], "range_b")
        else:
            st.caption(range_label)
            _render_payload(grafana_payload["single"], "selected_range")
    elif grafana_payload:
        st.info("Selections changed. Build sample-style tables again for the current file/date range.")
    else:
        st.info("Click **Build sample-style tables** to create the raw, daily, 80%, and content tables from your Excel sample.")
