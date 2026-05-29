from pathlib import Path
from datetime import datetime, time as dtime
from typing import List, Optional

import duckdb
import pandas as pd
import streamlit as st

APP_TITLE = "Veto Device ASN State Comparison"
st.set_page_config(page_title=APP_TITLE, page_icon="📡", layout="wide")

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


def _dq(x: str) -> str:
    return '"' + str(x).replace('"', '""') + '"'


def _path_list_sql(files: List[str]) -> str:
    return repr([str(Path(f)).replace("\\", "/") for f in files])


def epoch_to_utc_text(ts: int) -> str:
    return pd.to_datetime(int(ts), unit="s", utc=True).strftime("%d/%m/%y %H:%M:%S")


def epoch_to_utc_date(ts: int):
    return pd.to_datetime(int(ts), unit="s", utc=True).date()


def dt_to_epoch_utc(d, t) -> int:
    return int(pd.Timestamp(datetime.combine(d, t), tz="UTC").timestamp())


def clean_value_expr(col_sql: str) -> str:
    return f"COALESCE(NULLIF(TRIM(CAST({col_sql} AS VARCHAR)), ''), '(blank/null)')"


def clean_asn_expr(col_sql: str) -> str:
    return f"regexp_replace(LOWER(TRIM(CAST({col_sql} AS VARCHAR))), '^as', '')"


def setup_con():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("SET preserve_insertion_order=false")
    try:
        con.execute("PRAGMA enable_object_cache")
    except Exception:
        pass
    return con


@st.cache_data(show_spinner=False)
def scan_parquet_files(folder: str, recursive: bool = True) -> List[str]:
    root = Path(folder).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    return sorted(str(p) for p in (root.rglob("*.parquet") if recursive else root.glob("*.parquet")))


@st.cache_data(show_spinner=False)
def get_columns(files: List[str]) -> List[str]:
    con = duckdb.connect()
    return con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 1",
        [files],
    ).df()["column_name"].tolist()


@st.cache_data(show_spinner=False)
def get_epoch_range(files: List[str], req_time_col: str):
    con = duckdb.connect()
    ts = _dq(req_time_col)
    return con.execute(
        f"""
        SELECT MIN(TRY_CAST({ts} AS BIGINT)), MAX(TRY_CAST({ts} AS BIGINT))
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) IS NOT NULL
        """,
        [files],
    ).fetchone()


def load_asn_mapping(uploaded_file) -> Optional[pd.DataFrame]:
    """Load decoded ASN CSV/XLSX. Recommended columns: asn, as_name, as_country, as_domain, asn_type."""
    if uploaded_file is None:
        return None
    if uploaded_file.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
    if "asn" not in df.columns:
        st.warning("ASN decoded file must contain a column named `asn`.")
        return None
    preferred_cols = [
        "asn", "as_name", "as_country", "as_domain", "asn_type",
        "total_ipv4_ips", "total_ipv6_ips", "requests", "pct_rows",
        "lookup_status", "updated_at", "lookup_source",
    ]
    keep_cols = [c for c in preferred_cols if c in df.columns]
    extra_cols = [c for c in df.columns if c not in keep_cols]
    out = df[keep_cols + extra_cols].copy()
    out["asn_key"] = out["asn"].astype(str).str.strip().str.lower().str.replace(r"^as", "", regex=True)
    for col in out.columns:
        if col != "asn_key":
            out[col] = out[col].astype(str).str.strip()
    out = out.drop_duplicates("asn_key")
    front = ["asn_key"] + [c for c in preferred_cols if c in out.columns]
    rest = [c for c in out.columns if c not in front]
    return out[front + rest]


def join_asn(df: pd.DataFrame, mapping: Optional[pd.DataFrame], col: str = "asn_key") -> pd.DataFrame:
    if mapping is None or df.empty or col not in df.columns:
        return df
    out = df.copy()
    out[col] = out[col].astype(str).str.strip().str.lower().str.replace(r"^as", "", regex=True)
    meta_cols = [c for c in mapping.columns if c != "asn_key"]
    merged = out.merge(mapping[["asn_key"] + meta_cols], how="left", left_on=col, right_on="asn_key")
    preferred_order = ["asn_key", "asn", "as_name", "as_country", "as_domain", "asn_type", "row_count", "distinct_cliip"]
    front = [c for c in preferred_order if c in merged.columns]
    rest = [c for c in merged.columns if c not in front]
    return merged[front + rest]


@st.cache_data(show_spinner=False)
def range_summary(files, req_time_col, cliip_col, ua_col, asn_col, state_col, city_col, country_col,
                  start_epoch, end_epoch, exact_distinct):
    con = setup_con()
    parquet_sql = _path_list_sql(files)
    ts, cli, ua = _dq(req_time_col), _dq(cliip_col), _dq(ua_col)
    asn, state, city, country = _dq(asn_col), _dq(state_col), _dq(city_col), _dq(country_col)
    distinct = "COUNT(DISTINCT {})" if exact_distinct else "APPROX_COUNT_DISTINCT({})"
    q = f"""
    WITH base AS (
        SELECT
            TRY_CAST({ts} AS BIGINT) AS req_ts,
            {clean_value_expr(cli)} AS cliIP,
            {clean_value_expr(ua)} AS UA,
            {clean_asn_expr(asn)} AS asn_key,
            {clean_value_expr(state)} AS state_value,
            {clean_value_expr(city)} AS city_value,
            {clean_value_expr(country)} AS country_value
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
    )
    SELECT
        COUNT(*) AS row_count,
        MIN(req_ts) AS min_req_ts,
        MAX(req_ts) AS max_req_ts,
        {distinct.format('cliIP')} AS distinct_cliip,
        {distinct.format('UA')} AS distinct_ua,
        {distinct.format('asn_key')} AS distinct_asn,
        {distinct.format('state_value')} AS distinct_state,
        {distinct.format('city_value')} AS distinct_city,
        {distinct.format('country_value')} AS distinct_country
    FROM base
    """
    df = con.execute(q).df()
    if not df.empty:
        df["available_from_utc"] = df["min_req_ts"].apply(lambda x: epoch_to_utc_text(x) if pd.notna(x) else "")
        df["available_to_utc"] = df["max_req_ts"].apply(lambda x: epoch_to_utc_text(x) if pd.notna(x) else "")
    return df


@st.cache_data(show_spinner=False)
def top_dimension(files, req_time_col, value_col, start_epoch, end_epoch, top_n):
    con = setup_con()
    parquet_sql = _path_list_sql(files)
    ts, v = _dq(req_time_col), _dq(value_col)
    return con.execute(f"""
        SELECT {clean_value_expr(v)} AS value, COUNT(*) AS row_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
        GROUP BY 1
        ORDER BY row_count DESC
        LIMIT {int(top_n)}
    """).df()


@st.cache_data(show_spinner=False)
def top_asn(files, req_time_col, asn_col, start_epoch, end_epoch, top_n):
    con = setup_con()
    parquet_sql = _path_list_sql(files)
    ts, asn = _dq(req_time_col), _dq(asn_col)
    return con.execute(f"""
        SELECT {clean_asn_expr(asn)} AS asn_key, {clean_value_expr(asn)} AS asn_raw, COUNT(*) AS row_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
        GROUP BY 1, 2
        ORDER BY row_count DESC
        LIMIT {int(top_n)}
    """).df()


@st.cache_data(show_spinner=False)
def pair_counts(files, req_time_col, left_col, right_col, start_epoch, end_epoch, top_n, left_asn=False, right_asn=False):
    con = setup_con()
    parquet_sql = _path_list_sql(files)
    ts, left, right = _dq(req_time_col), _dq(left_col), _dq(right_col)
    left_expr = clean_asn_expr(left) if left_asn else clean_value_expr(left)
    right_expr = clean_asn_expr(right) if right_asn else clean_value_expr(right)
    return con.execute(f"""
        SELECT {left_expr} AS left_value, {right_expr} AS right_value, COUNT(*) AS row_count
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
        GROUP BY 1, 2
        ORDER BY row_count DESC
        LIMIT {int(top_n)}
    """).df()


@st.cache_data(show_spinner=False)
def device_asn_state(files, req_time_col, ua_col, asn_col, state_col, cliip_col,
                     start_epoch, end_epoch, top_n, exact_distinct):
    con = setup_con()
    parquet_sql = _path_list_sql(files)
    ts, ua, asn = _dq(req_time_col), _dq(ua_col), _dq(asn_col)
    state, cli = _dq(state_col), _dq(cliip_col)
    distinct = "COUNT(DISTINCT {})" if exact_distinct else "APPROX_COUNT_DISTINCT({})"
    return con.execute(f"""
        SELECT
            {clean_value_expr(ua)} AS UA,
            {clean_asn_expr(asn)} AS asn_key,
            {clean_value_expr(state)} AS state,
            COUNT(*) AS row_count,
            {distinct.format(clean_value_expr(cli))} AS distinct_cliip
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY row_count DESC
        LIMIT {int(top_n)}
    """).df()


@st.cache_data(show_spinner=False)
def device_asn_state_city(files, req_time_col, ua_col, asn_col, state_col, city_col, country_col, cliip_col,
                          start_epoch, end_epoch, top_n, exact_distinct):
    con = setup_con()
    parquet_sql = _path_list_sql(files)
    ts, ua, asn = _dq(req_time_col), _dq(ua_col), _dq(asn_col)
    state, city, country, cli = _dq(state_col), _dq(city_col), _dq(country_col), _dq(cliip_col)
    distinct = "COUNT(DISTINCT {})" if exact_distinct else "APPROX_COUNT_DISTINCT({})"
    return con.execute(f"""
        SELECT
            {clean_value_expr(ua)} AS UA,
            {clean_asn_expr(asn)} AS asn_key,
            {clean_value_expr(state)} AS state,
            {clean_value_expr(city)} AS city,
            {clean_value_expr(country)} AS country,
            COUNT(*) AS row_count,
            {distinct.format(clean_value_expr(cli))} AS distinct_cliip
        FROM read_parquet({parquet_sql}, union_by_name=true)
        WHERE TRY_CAST({ts} AS BIGINT) BETWEEN {int(start_epoch)} AND {int(end_epoch)}
          AND TRY_CAST({ts} AS BIGINT) IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY row_count DESC
        LIMIT {int(top_n)}
    """).df()


def compare_tables(a, b, keys):
    if a is None or b is None or a.empty or b.empty:
        return pd.DataFrame()
    aa = a.copy().rename(columns={"row_count": "row_count_A"})
    bb = b.copy().rename(columns={"row_count": "row_count_B"})
    out = aa.merge(bb, how="outer", on=keys)
    out["row_count_A"] = out["row_count_A"].fillna(0).astype("int64")
    out["row_count_B"] = out["row_count_B"].fillna(0).astype("int64")
    out["delta_rows_B_minus_A"] = out["row_count_B"] - out["row_count_A"]
    out["delta_pct_vs_A"] = (out["delta_rows_B_minus_A"] / out["row_count_A"].replace(0, pd.NA) * 100).round(2).fillna(0)
    return out.sort_values("row_count_B", ascending=False)


def render_summary(label, df):
    if df.empty:
        st.warning(f"No data for {label}")
        return
    r = df.iloc[0]
    st.markdown(f"#### {label}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows", f"{int(r['row_count'] or 0):,}")
    c2.metric("Distinct cliIP", f"{int(r['distinct_cliip'] or 0):,}")
    c3.metric("Distinct UA/devices", f"{int(r['distinct_ua'] or 0):,}")
    c4.metric("Distinct ASN", f"{int(r['distinct_asn'] or 0):,}")
    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Distinct State", f"{int(r['distinct_state'] or 0):,}")
    c6.metric("Distinct City", f"{int(r['distinct_city'] or 0):,}")
    c7.metric("From UTC", r["available_from_utc"])
    c8.metric("To UTC", r["available_to_utc"])


def show_table(title, df, filename, height=500):
    st.subheader(title)
    st.dataframe(df, use_container_width=True, hide_index=True, height=height)
    st.download_button(
        f"Download {title} CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
    )


# UI
st.title("📡 Veto Device / ASN / State Comparison")
st.caption("Compare single day/range or two ranges by UA/device, ASN metadata, state, city, country, and cliIP relationships.")

with st.sidebar:
    st.header("1) Parquet input")
    folder = st.text_input("Parquet folder", placeholder=r"D:\Veto Logs Backup")
    recursive = st.checkbox("Scan recursively", value=True)

if not folder:
    st.info("Enter parquet folder in sidebar.")
    st.stop()

files_all = scan_parquet_files(folder, recursive)
if not files_all:
    st.error("No parquet files found.")
    st.stop()

with st.sidebar:
    selected_files = st.multiselect("Select parquet files", files_all, default=files_all, format_func=lambda x: Path(x).name)

if not selected_files:
    st.warning("Select at least one parquet file.")
    st.stop()

columns = get_columns(selected_files)
if not columns:
    st.error("Could not read columns.")
    st.stop()


def idx(name):
    return columns.index(name) if name in columns else 0


with st.sidebar:
    st.header("2) Column mapping")
    req_time_col = st.selectbox("Timestamp column", columns, index=idx("reqTimeSec"))
    cliip_col = st.selectbox("cliIP column", columns, index=idx("cliIP"))
    ua_col = st.selectbox("UA / device column", columns, index=idx("UA"))
    asn_col = st.selectbox("ASN column", columns, index=idx("asn"))
    state_col = st.selectbox("State column", columns, index=idx("state"))
    city_col = st.selectbox("City column", columns, index=idx("city"))
    country_col = st.selectbox("Country column", columns, index=idx("country"))
    exact_distinct = st.checkbox("Use exact distinct counts", value=False, help="Approx distinct is faster for big month data.")
    top_n = st.number_input("Top N rows", min_value=10, max_value=10000, value=100, step=10)

min_ts, max_ts = get_epoch_range(selected_files, req_time_col)
if min_ts is None or max_ts is None:
    st.error("No valid timestamp range found.")
    st.stop()

min_date = epoch_to_utc_date(min_ts)
max_date = epoch_to_utc_date(max_ts)

r1, r2, r3 = st.columns(3)
r1.metric("Available From UTC", epoch_to_utc_text(min_ts))
r2.metric("Available To UTC", epoch_to_utc_text(max_ts))
r3.metric("Selected Files", f"{len(selected_files):,}")

with st.sidebar:
    st.header("3) Compare ranges")
    compare_mode = st.radio("Mode", ["Single range", "Compare two ranges"], index=0)

    st.markdown("#### Range A")
    a_dates = st.date_input("Range A date(s)", value=(min_date, min_date), min_value=min_date, max_value=max_date, key="a_dates")
    if isinstance(a_dates, tuple) and len(a_dates) == 2:
        a_start_d, a_end_d = a_dates
    else:
        a_start_d = a_end_d = min_date
    a_start_t = st.time_input("Range A start UTC", dtime(0, 0, 0), key="a_start")
    a_end_t = st.time_input("Range A end UTC", dtime(23, 59, 59), key="a_end")

    if compare_mode == "Compare two ranges":
        st.markdown("#### Range B")
        b_dates = st.date_input("Range B date(s)", value=(max_date, max_date), min_value=min_date, max_value=max_date, key="b_dates")
        if isinstance(b_dates, tuple) and len(b_dates) == 2:
            b_start_d, b_end_d = b_dates
        else:
            b_start_d = b_end_d = max_date
        b_start_t = st.time_input("Range B start UTC", dtime(0, 0, 0), key="b_start")
        b_end_t = st.time_input("Range B end UTC", dtime(23, 59, 59), key="b_end")
    else:
        b_start_d, b_end_d = a_start_d, a_end_d
        b_start_t, b_end_t = a_start_t, a_end_t

a_start_epoch = dt_to_epoch_utc(a_start_d, a_start_t)
a_end_epoch = dt_to_epoch_utc(a_end_d, a_end_t)
b_start_epoch = dt_to_epoch_utc(b_start_d, b_start_t)
b_end_epoch = dt_to_epoch_utc(b_end_d, b_end_t)

with st.sidebar:
    st.header("4) ASN decoded list")
    st.caption("Recommended columns: asn, as_name, as_country, as_domain, asn_type")
    asn_file = st.file_uploader("Upload ASN decoded CSV/XLSX", type=["csv", "xlsx", "xls"])
    asn_map_df = None
    if asn_file is not None:
        try:
            asn_map_df = load_asn_mapping(asn_file)
            if asn_map_df is not None:
                st.success(f"Loaded {len(asn_map_df):,} ASN mappings")
                st.dataframe(asn_map_df.head(5), use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"Could not read ASN file: {e}")

st.info("This dashboard compares distribution, not watch time: UA/device ↔ ASN metadata ↔ state/city/country ↔ cliIP.")

if st.button("Build comparison dashboard", type="primary"):
    with st.spinner("Calculating Range A..."):
        summary_a = range_summary(selected_files, req_time_col, cliip_col, ua_col, asn_col, state_col, city_col, country_col, a_start_epoch, a_end_epoch, exact_distinct)
        ua_a = top_dimension(selected_files, req_time_col, ua_col, a_start_epoch, a_end_epoch, int(top_n))
        state_a = top_dimension(selected_files, req_time_col, state_col, a_start_epoch, a_end_epoch, int(top_n))
        city_a = top_dimension(selected_files, req_time_col, city_col, a_start_epoch, a_end_epoch, int(top_n))
        country_a = top_dimension(selected_files, req_time_col, country_col, a_start_epoch, a_end_epoch, int(top_n))
        asn_a = top_asn(selected_files, req_time_col, asn_col, a_start_epoch, a_end_epoch, int(top_n))
        ua_asn_a = pair_counts(selected_files, req_time_col, ua_col, asn_col, a_start_epoch, a_end_epoch, int(top_n), right_asn=True)
        asn_state_a = pair_counts(selected_files, req_time_col, asn_col, state_col, a_start_epoch, a_end_epoch, int(top_n), left_asn=True)
        das_a = device_asn_state(selected_files, req_time_col, ua_col, asn_col, state_col, cliip_col, a_start_epoch, a_end_epoch, int(top_n), exact_distinct)
        das_city_a = device_asn_state_city(selected_files, req_time_col, ua_col, asn_col, state_col, city_col, country_col, cliip_col, a_start_epoch, a_end_epoch, int(top_n), exact_distinct)

    summary_b = ua_b = asn_b = state_b = city_b = country_b = None
    if compare_mode == "Compare two ranges":
        with st.spinner("Calculating Range B..."):
            summary_b = range_summary(selected_files, req_time_col, cliip_col, ua_col, asn_col, state_col, city_col, country_col, b_start_epoch, b_end_epoch, exact_distinct)
            ua_b = top_dimension(selected_files, req_time_col, ua_col, b_start_epoch, b_end_epoch, int(top_n))
            asn_b = top_asn(selected_files, req_time_col, asn_col, b_start_epoch, b_end_epoch, int(top_n))
            state_b = top_dimension(selected_files, req_time_col, state_col, b_start_epoch, b_end_epoch, int(top_n))
            city_b = top_dimension(selected_files, req_time_col, city_col, b_start_epoch, b_end_epoch, int(top_n))
            country_b = top_dimension(selected_files, req_time_col, country_col, b_start_epoch, b_end_epoch, int(top_n))

    st.subheader("Range summary")
    if compare_mode == "Compare two ranges":
        ca, cb = st.columns(2)
        with ca:
            render_summary("Range A", summary_a)
        with cb:
            render_summary("Range B", summary_b)
    else:
        render_summary("Selected range", summary_a)

    asn_a = join_asn(asn_a, asn_map_df)
    ua_asn_a = ua_asn_a.rename(columns={"left_value": "UA", "right_value": "asn_key"})
    ua_asn_a = join_asn(ua_asn_a, asn_map_df)
    asn_state_a = asn_state_a.rename(columns={"left_value": "asn_key", "right_value": "state"})
    asn_state_a = join_asn(asn_state_a, asn_map_df)
    das_a = join_asn(das_a, asn_map_df)
    das_city_a = join_asn(das_city_a, asn_map_df)

    tabs = st.tabs([
        "Top Devices / UA", "Top ASN", "Top State City Country", "Device ↔ ASN",
        "ASN ↔ State", "Device ↔ ASN ↔ State", "Device ↔ ASN ↔ State City", "Comparison Deltas"
    ])

    with tabs[0]:
        show_table("Top devices / UA", ua_a, "top_ua.csv", 500)
    with tabs[1]:
        show_table("Top ASN with decoded metadata", asn_a, "top_asn.csv", 500)
    with tabs[2]:
        c1, c2, c3 = st.columns(3)
        with c1:
            show_table("Top State", state_a, "top_state.csv", 500)
        with c2:
            show_table("Top City", city_a, "top_city.csv", 500)
        with c3:
            show_table("Top Country", country_a, "top_country.csv", 500)
    with tabs[3]:
        show_table("Device / UA ↔ ASN", ua_asn_a, "ua_asn.csv", 650)
    with tabs[4]:
        show_table("ASN ↔ State", asn_state_a, "asn_state.csv", 650)
    with tabs[5]:
        show_table("Device / UA ↔ ASN ↔ State", das_a, "device_asn_state.csv", 650)
    with tabs[6]:
        show_table("Device / UA ↔ ASN ↔ State / City / Country", das_city_a, "device_asn_state_city_country.csv", 700)
    with tabs[7]:
        if compare_mode != "Compare two ranges":
            st.info("Switch to Compare two ranges to see deltas.")
        else:
            st.subheader("Range B minus Range A")
            asn_b = join_asn(asn_b, asn_map_df)
            ua_cmp = compare_tables(ua_a, ua_b, ["value"])
            asn_cmp = compare_tables(
                asn_a[[c for c in ["asn_key", "asn_raw", "row_count"] if c in asn_a.columns]],
                asn_b[[c for c in ["asn_key", "asn_raw", "row_count"] if c in asn_b.columns]],
                ["asn_key", "asn_raw"],
            )
            asn_cmp = join_asn(asn_cmp, asn_map_df)
            state_cmp = compare_tables(state_a, state_b, ["value"])
            city_cmp = compare_tables(city_a, city_b, ["value"])
            country_cmp = compare_tables(country_a, country_b, ["value"])
            show_table("UA/device change", ua_cmp, "ua_comparison.csv", 350)
            show_table("ASN change", asn_cmp, "asn_comparison.csv", 350)
            show_table("State change", state_cmp, "state_comparison.csv", 350)
            show_table("City change", city_cmp, "city_comparison.csv", 350)
            show_table("Country change", country_cmp, "country_comparison.csv", 350)
