"""
pDash_cache_explorer.py
=======================
Lightweight explorer for the DuckDB analytics cache.

This is NOT the raw parquet explorer. It reads the compact cache tables created by:
    build_pDash_summary_cache_v2_1_fixed_counts.py

Run:
    streamlit run pDash_cache_explorer.py -- --db "D:\\Vs - Code Work\\pDash_analytics_cache.duckdb"

Use this when you want to inspect cached analytics quickly:
- table list / schemas
- date coverage
- unique values in cached dimensions
- filtered preview from behavior_device_day
- safe SQL query runner with row limit
- CSV exports
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re

import duckdb
import pandas as pd
import streamlit as st

try:
    import plotly.express as px
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

st.set_page_config(page_title="pDash Cache Explorer", page_icon="🔎", layout="wide")


def parse_cli_db() -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", default="pDash_analytics_cache.duckdb")
    args, _ = parser.parse_known_args()
    return args.db


def sql_str(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


@st.cache_data(show_spinner=False, ttl=120)
def read_df(db_path: str, query: str) -> pd.DataFrame:
    con = duckdb.connect(str(Path(db_path).expanduser().resolve()), read_only=True)
    try:
        return con.execute(query).df()
    finally:
        con.close()


@st.cache_resource(show_spinner=False)
def check_db(db_path: str) -> str:
    p = Path(db_path).expanduser().resolve()
    if not p.exists():
        st.error(f"Cache DB not found: {p}")
        st.stop()
    con = duckdb.connect(str(p), read_only=True)
    try:
        tables = con.execute("SHOW TABLES").df()["name"].astype(str).tolist()
    finally:
        con.close()
    if "behavior_device_day" not in tables:
        st.error("This DB does not contain `behavior_device_day`. Build the analytics cache first.")
        st.stop()
    return str(p)


def safe_select_query(query: str, limit: int) -> str:
    q = query.strip().rstrip(";")
    if not re.match(r"(?is)^\\s*(select|with)\\b", q):
        raise ValueError("Only SELECT / WITH queries are allowed.")
    blocked = re.search(r"(?is)\\b(insert|update|delete|drop|alter|create|copy|attach|detach|pragma|vacuum|export|import)\\b", q)
    if blocked:
        raise ValueError(f"Blocked SQL keyword: {blocked.group(1)}")
    if re.search(r"(?is)\\blimit\\s+\\d+", q):
        return q
    return f"SELECT * FROM ({q}) q LIMIT {int(limit)}"


db_path = check_db(parse_cli_db())

st.title("🔎 pDash Cache Explorer")
st.caption("Fast explorer for the validated DuckDB cache. Use raw Parquet Explorer only when you need raw rows or columns not present in this cache.")
st.code(db_path, language="text")

try:
    info = read_df(db_path, "SELECT * FROM available_dates")
    if not info.empty:
        r = info.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Available range", f"{pd.to_datetime(r['min_date']).date()} → {pd.to_datetime(r['max_date']).date()}")
        c2.metric("Cached base rows", f"{int(r['cached_rows']):,}")
        c3.metric("Active days", f"{int(r['active_days']):,}")
        c4.metric("DB size", f"{Path(db_path).stat().st_size / (1024**3):.2f} GB")
except Exception:
    pass

with st.expander("Cache quality / ID source", expanded=True):
    try:
        q = read_df(db_path, """
            SELECT device_id_source, session_id_source, requests,
                   devices AS estimated_devices,
                   sessions AS estimated_sessions
            FROM cache_quality
            ORDER BY requests DESC
        """)
        st.dataframe(q, use_container_width=True, hide_index=True)
        st.caption("Requests are exact. Device/session counts are estimated when fallback sources are used.")
    except Exception as e:
        st.info(f"cache_quality not available: {e}")

tab_tables, tab_filter, tab_unique, tab_sql = st.tabs([
    "Tables & schema",
    "Filtered cache preview",
    "Unique values",
    "SQL runner",
])

with tab_tables:
    st.subheader("Cache tables")
    tables = read_df(db_path, "SHOW TABLES")
    st.dataframe(tables, use_container_width=True, hide_index=True)

    selected_table = st.selectbox("Inspect table", tables["name"].astype(str).tolist())
    if selected_table:
        c1, c2 = st.columns([1, 2])
        with c1:
            try:
                count_df = read_df(db_path, f"SELECT COUNT(*) AS rows FROM {dq(selected_table)}")
                st.metric("Rows", f"{int(count_df.iloc[0]['rows']):,}")
            except Exception as e:
                st.warning(str(e))
            schema_df = read_df(db_path, f"DESCRIBE {dq(selected_table)}")
            st.dataframe(schema_df, use_container_width=True, hide_index=True, height=420)
        with c2:
            preview = read_df(db_path, f"SELECT * FROM {dq(selected_table)} LIMIT 200")
            st.dataframe(preview, use_container_width=True, hide_index=True, height=520)
            st.download_button(
                "Download preview CSV",
                data=preview.to_csv(index=False).encode("utf-8"),
                file_name=f"{selected_table}_preview.csv",
                mime="text/csv",
            )

with tab_filter:
    st.subheader("Filtered preview from behavior_device_day")
    date_df = read_df(db_path, "SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date FROM behavior_device_day")
    min_date = pd.to_datetime(date_df.iloc[0]["min_date"]).date()
    max_date = pd.to_datetime(date_df.iloc[0]["max_date"]).date()

    picked = st.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    start_date, end_date = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_date, max_date)

    base_where = f"event_date BETWEEN DATE {sql_str(str(start_date))} AND DATE {sql_str(str(end_date))}"

    def opts(col, where="1=1"):
        df = read_df(db_path, f"SELECT DISTINCT {dq(col)} AS v FROM behavior_device_day WHERE {where} ORDER BY 1")
        return ["All"] + [str(x) for x in df["v"].dropna().tolist()]

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        country = st.selectbox("Country", opts("country", base_where))
    where_country = base_where if country == "All" else f"{base_where} AND country={sql_str(country)}"
    with f2:
        region = st.selectbox("Region / State", opts("region", where_country))
    where_region = where_country if region == "All" else f"{where_country} AND region={sql_str(region)}"
    with f3:
        city = st.selectbox("City", opts("city", where_region))
    where_city = where_region if city == "All" else f"{where_region} AND city={sql_str(city)}"
    with f4:
        device_type = st.selectbox("Device type", opts("device_type", base_where))

    channel_contains = st.text_input("Channel contains", "")
    limit = st.number_input("Preview rows", min_value=50, max_value=10000, value=1000, step=50)

    where = where_city
    if device_type != "All":
        where += f" AND device_type={sql_str(device_type)}"
    if channel_contains.strip():
        where += f" AND channel_name ILIKE {sql_str('%' + channel_contains.strip() + '%')}"

    kpi = read_df(db_path, f"""
        SELECT
            SUM(requests) AS requests,
            COUNT(DISTINCT device_id) AS estimated_devices,
            COUNT(DISTINCT session_id) AS estimated_sessions,
            COUNT(DISTINCT channel_name) AS channels
        FROM behavior_device_day
        WHERE {where}
    """)
    if not kpi.empty:
        r = kpi.iloc[0]
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Requests", f"{int(r['requests'] or 0):,}")
        k2.metric("Est. devices", f"{int(r['estimated_devices'] or 0):,}")
        k3.metric("Est. sessions", f"{int(r['estimated_sessions'] or 0):,}")
        k4.metric("Channels", f"{int(r['channels'] or 0):,}")

    preview = read_df(db_path, f"""
        SELECT event_date, country, region, city, channel_name, device_type,
               requests, device_id_source, session_id_source
        FROM behavior_device_day
        WHERE {where}
        ORDER BY requests DESC
        LIMIT {int(limit)}
    """)
    st.dataframe(preview, use_container_width=True, hide_index=True, height=520)
    st.download_button("Download filtered preview CSV", preview.to_csv(index=False).encode("utf-8"), "cache_filtered_preview.csv", "text/csv")

    if PLOTLY_OK and not preview.empty:
        top = (
            preview.groupby(["city", "channel_name", "device_type"], as_index=False)
            .agg(requests=("requests", "sum"))
            .sort_values("requests", ascending=False)
            .head(30)
        )
        top["combo"] = top["city"].astype(str) + " | " + top["channel_name"].astype(str) + " | " + top["device_type"].astype(str)
        fig = px.bar(top.sort_values("requests"), x="requests", y="combo", color="device_type", orientation="h", title="Top cached combinations by requests")
        fig.update_layout(height=650, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

with tab_unique:
    st.subheader("Unique values in cached table")
    cols_df = read_df(db_path, "DESCRIBE behavior_device_day")
    cols = cols_df["column_name"].astype(str).tolist()
    col = st.selectbox("Column", cols, index=cols.index("channel_name") if "channel_name" in cols else 0)
    top_n = st.number_input("Top N", min_value=10, max_value=10000, value=200, step=10)
    uv = read_df(db_path, f"""
        SELECT CAST({dq(col)} AS VARCHAR) AS value,
               COUNT(*) AS cache_rows,
               SUM(requests) AS requests
        FROM behavior_device_day
        GROUP BY 1
        ORDER BY requests DESC NULLS LAST
        LIMIT {int(top_n)}
    """)
    st.dataframe(uv, use_container_width=True, hide_index=True, height=520)
    st.download_button("Download unique values CSV", uv.to_csv(index=False).encode("utf-8"), f"unique_{col}.csv", "text/csv")

with tab_sql:
    st.subheader("Safe SQL runner")
    st.caption("Read-only SELECT/WITH queries only. A LIMIT is added automatically if you do not provide one.")
    default_sql = """SELECT
    event_date,
    country,
    region,
    city,
    channel_name,
    device_type,
    SUM(requests) AS requests,
    COUNT(DISTINCT device_id) AS estimated_devices
FROM behavior_device_day
GROUP BY 1,2,3,4,5,6
ORDER BY requests DESC"""
    query = st.text_area("SQL", value=default_sql, height=260)
    limit = st.number_input("Auto LIMIT", min_value=10, max_value=100000, value=5000, step=100)
    if st.button("Run SQL", type="primary"):
        try:
            final_query = safe_select_query(query, int(limit))
            df = read_df(db_path, final_query)
            st.success(f"Returned {len(df):,} rows")
            st.dataframe(df, use_container_width=True, hide_index=True, height=560)
            st.download_button("Download SQL result CSV", df.to_csv(index=False).encode("utf-8"), "sql_result.csv", "text/csv")
        except Exception as e:
            st.error(str(e))
