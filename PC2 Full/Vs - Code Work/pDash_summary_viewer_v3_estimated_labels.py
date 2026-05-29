"""
pDash_summary_viewer_v3_estimated_labels.py
=======================
Fast Streamlit viewer for the DuckDB cache created by build_pDash_summary_cache_v2_log_fallback.py.

Run:
    streamlit run pDash_summary_viewer_v3_estimated_labels.py -- --db "D:\\Vs - Code Work\\pDash_analytics_cache.duckdb"
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

try:
    import plotly.express as px
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

st.set_page_config(page_title="pDash Summary Viewer", page_icon="⚡", layout="wide")


def parse_cli_db() -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", default="pDash_analytics_cache.duckdb")
    args, _ = parser.parse_known_args()
    return args.db


@st.cache_resource(show_spinner=False)
def get_conn(db_path: str):
    p = Path(db_path).expanduser().resolve()
    if not p.exists():
        st.error(f"Cache DB not found: {p}")
        st.stop()
    con = duckdb.connect(str(p), read_only=True)
    con.execute("PRAGMA enable_object_cache")
    return con


def sql_str(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def filters_sql(start_date, end_date, country, region, city, device_type) -> str:
    parts = [f"event_date BETWEEN DATE {sql_str(str(start_date))} AND DATE {sql_str(str(end_date))}"]
    if country != "All":
        parts.append(f"country = {sql_str(country)}")
    if region != "All":
        parts.append(f"region = {sql_str(region)}")
    if city != "All":
        parts.append(f"city = {sql_str(city)}")
    if device_type != "All":
        parts.append(f"device_type = {sql_str(device_type)}")
    return " AND ".join(parts)


@st.cache_data(show_spinner=False, ttl=300)
def read_df(db_path: str, query: str) -> pd.DataFrame:
    con = duckdb.connect(str(Path(db_path).expanduser().resolve()), read_only=True)
    try:
        return con.execute(query).df()
    finally:
        con.close()


def options_for(db_path: str, col: str, where: str = "1=1") -> list[str]:
    df = read_df(db_path, f"SELECT DISTINCT {col} AS v FROM behavior_device_day WHERE {where} ORDER BY 1")
    vals = [str(x) for x in df["v"].dropna().tolist()]
    return ["All"] + vals


db_path = parse_cli_db()
con = get_conn(db_path)

st.title("⚡ pDash Summary Viewer v3")
st.caption("Reads cached DuckDB summaries. It does not scan raw Parquet during dashboard use. Device/session metrics are labelled as estimated because most rows use cliIP + UA fallback when queryStr has no real IDs.")
st.code(str(Path(db_path).expanduser().resolve()), language="text")

try:
    date_row = con.execute("SELECT min_date, max_date, cached_rows, active_days FROM available_dates").fetchone()
except Exception:
    st.error("This DB does not look like a pDash analytics cache. Run build_pDash_summary_cache.py first.")
    st.stop()

min_date = pd.to_datetime(date_row[0]).date()
max_date = pd.to_datetime(date_row[1]).date()
cached_rows = int(date_row[2] or 0)
active_days = int(date_row[3] or 0)

m1, m2, m3 = st.columns(3)
m1.metric("Available range", f"{min_date} → {max_date}")
m2.metric("Cached base rows", f"{cached_rows:,}")
m3.metric("Active days", f"{active_days:,}")

with st.expander("Cache quality / ID source", expanded=False):
    try:
        qdf = read_df(db_path, """
            SELECT device_id_source, session_id_source, requests, devices, sessions
            FROM cache_quality
            ORDER BY requests DESC
        """)
        st.caption("Shows whether IDs came from queryStr or were estimated. Your current files mostly require estimated IDs because device_id/session_id are not raw columns.")
        st.dataframe(qdf, use_container_width=True, hide_index=True)
    except Exception as e:
        st.info(f"Cache quality table not available: {e}")

st.markdown("---")

quick = st.radio("Date range", ["Full available", "Last 7 available days", "Last 30 available days", "Current month in data", "Custom"], horizontal=True)
if quick == "Full available":
    start_date, end_date = min_date, max_date
elif quick == "Last 7 available days":
    end_date = max_date
    start_date = max(min_date, end_date - pd.Timedelta(days=6).date()) if False else max(min_date, (pd.Timestamp(end_date) - pd.Timedelta(days=6)).date())
elif quick == "Last 30 available days":
    end_date = max_date
    start_date = max(min_date, (pd.Timestamp(end_date) - pd.Timedelta(days=29)).date())
elif quick == "Current month in data":
    end_date = max_date
    start_date = max(min_date, pd.Timestamp(end_date).replace(day=1).date())
else:
    picked = st.date_input("Custom date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    start_date, end_date = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_date, max_date)

fbase = f"event_date BETWEEN DATE {sql_str(str(start_date))} AND DATE {sql_str(str(end_date))}"

c1, c2, c3, c4 = st.columns(4)
with c1:
    country = st.selectbox("Country", options_for(db_path, "country", fbase), key="country")
with c2:
    w = fbase if country == "All" else f"{fbase} AND country = {sql_str(country)}"
    region = st.selectbox("Region / State", options_for(db_path, "region", w), key="region")
with c3:
    w2 = w if region == "All" else f"{w} AND region = {sql_str(region)}"
    city = st.selectbox("City", options_for(db_path, "city", w2), key="city")
with c4:
    device_type = st.selectbox("Device type", options_for(db_path, "device_type", fbase), key="device_type")

top_n = st.slider("Top N", 5, 100, 25)
where = filters_sql(start_date, end_date, country, region, city, device_type)

st.caption(f"Selected range: **{start_date} → {end_date}**")

kpi = read_df(db_path, f"""
    SELECT
        SUM(requests) AS requests,
        COUNT(DISTINCT session_id) AS est_sessions,
        COUNT(DISTINCT device_id) AS est_devices,
        COUNT(DISTINCT country) AS countries,
        COUNT(DISTINCT region) AS regions,
        COUNT(DISTINCT city) AS cities,
        COUNT(DISTINCT channel_name) AS channels,
        COUNT(DISTINCT device_type) AS device_types
    FROM behavior_device_day
    WHERE {where}
""")
if not kpi.empty:
    vals = kpi.iloc[0]
    a,b,c,d,e,f,g,h = st.columns(8)
    a.metric("Requests", f"{int(vals['requests'] or 0):,}")
    b.metric("Est. sessions", f"{int(vals['est_sessions'] or 0):,}")
    c.metric("Est. devices", f"{int(vals['est_devices'] or 0):,}")
    d.metric("Countries", f"{int(vals['countries'] or 0):,}")
    e.metric("Regions", f"{int(vals['regions'] or 0):,}")
    f.metric("Cities", f"{int(vals['cities'] or 0):,}")
    g.metric("Channels", f"{int(vals['channels'] or 0):,}")
    h.metric("Device types", f"{int(vals['device_types'] or 0):,}")

st.info("Device/session counts are **estimated unique devices/sessions** when source is `estimated_cliIP_UA` or `estimated_device_date_hour`. Requests are exact and validated against raw parquet rows.")

st.markdown("---")
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Country", "City", "City × Channel × Device", "Device types", "India states"])

with tab1:
    df = read_df(db_path, f"""
        SELECT country,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS est_sessions,
               COUNT(DISTINCT device_id) AS est_devices,
               COUNT(DISTINCT region) AS regions,
               COUNT(DISTINCT city) AS cities,
               COUNT(DISTINCT channel_name) AS channels,
               COUNT(DISTINCT device_type) AS device_types
        FROM behavior_device_day
        WHERE {where}
        GROUP BY 1
        ORDER BY est_devices DESC, est_sessions DESC, requests DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True, height=420)
    st.download_button("Download country CSV", df.to_csv(index=False).encode("utf-8"), "country_summary.csv", "text/csv")

with tab2:
    df = read_df(db_path, f"""
        SELECT country, region, city,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS est_sessions,
               COUNT(DISTINCT device_id) AS est_devices,
               COUNT(DISTINCT channel_name) AS channels,
               COUNT(DISTINCT device_type) AS device_types
        FROM behavior_device_day
        WHERE {where}
        GROUP BY 1,2,3
        ORDER BY est_devices DESC, est_sessions DESC, requests DESC
        LIMIT {int(top_n) * 5}
    """)
    st.dataframe(df, use_container_width=True, hide_index=True, height=420)
    if PLOTLY_OK and not df.empty:
        fig = px.bar(df.head(top_n), x="est_devices", y="city", color="region", orientation="h", title="Top cities by estimated unique devices")
        fig.update_layout(height=550, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    st.download_button("Download city CSV", df.to_csv(index=False).encode("utf-8"), "city_summary.csv", "text/csv")

with tab3:
    chart_mode = st.radio("Chart mode", ["Top overall combinations", "Top per device type", "Device type totals"], horizontal=True)
    rel = read_df(db_path, f"""
        SELECT country, region, city, channel_name, device_type,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS est_sessions,
               COUNT(DISTINCT device_id) AS est_devices
        FROM behavior_device_day
        WHERE {where}
        GROUP BY 1,2,3,4,5
        ORDER BY est_devices DESC, est_sessions DESC, requests DESC
        LIMIT 5000
    """)
    st.dataframe(rel.head(1000), use_container_width=True, hide_index=True, height=420)
    if PLOTLY_OK and not rel.empty:
        if chart_mode == "Top per device type":
            plot_df = rel.sort_values(["device_type", "est_devices"], ascending=[True, False]).groupby("device_type").head(10)
            title = "Top channel/device combinations per device type"
        elif chart_mode == "Device type totals":
            plot_df = rel.groupby("device_type", as_index=False).agg(est_devices=("est_devices", "sum"), requests=("requests", "sum"), est_sessions=("est_sessions", "sum"))
            fig = px.bar(plot_df.sort_values("est_devices"), x="est_devices", y="device_type", orientation="h", title="Device type totals in current filter")
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)
            plot_df = None
        else:
            plot_df = rel.head(top_n).copy()
            title = "Top channel/device combinations by estimated unique devices"
        if plot_df is not None and not plot_df.empty:
            plot_df["combo"] = plot_df["city"].astype(str) + " | " + plot_df["channel_name"].astype(str) + " | " + plot_df["device_type"].astype(str)
            fig = px.bar(plot_df.sort_values("est_devices"), x="est_devices", y="combo", color="device_type", orientation="h", title=title)
            fig.update_layout(height=650, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)
    st.download_button("Download relation CSV", rel.to_csv(index=False).encode("utf-8"), "city_channel_device_relation.csv", "text/csv")

with tab4:
    df = read_df(db_path, f"""
        SELECT device_type,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS est_sessions,
               COUNT(DISTINCT device_id) AS est_devices,
               COUNT(DISTINCT country) AS countries,
               COUNT(DISTINCT region) AS regions,
               COUNT(DISTINCT city) AS cities,
               COUNT(DISTINCT channel_name) AS channels
        FROM behavior_device_day
        WHERE {where}
        GROUP BY 1
        ORDER BY est_devices DESC, est_sessions DESC, requests DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True, height=350)
    st.download_button("Download device types CSV", df.to_csv(index=False).encode("utf-8"), "device_type_summary.csv", "text/csv")

with tab5:
    india_where = f"{where} AND upper(country) IN ('IN', 'IND', 'INDIA')"
    df = read_df(db_path, f"""
        SELECT region AS state_region,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS est_sessions,
               COUNT(DISTINCT device_id) AS est_devices,
               COUNT(DISTINCT city) AS cities,
               COUNT(DISTINCT channel_name) AS channels,
               COUNT(DISTINCT device_type) AS device_types
        FROM behavior_device_day
        WHERE {india_where}
        GROUP BY 1
        ORDER BY est_devices DESC, est_sessions DESC, requests DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True, height=420)
    if PLOTLY_OK and not df.empty:
        fig = px.bar(df.head(top_n).sort_values("est_devices"), x="est_devices", y="state_region", orientation="h", title="India state/region list by estimated devices")
        fig.update_layout(height=550, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    st.download_button("Download India states CSV", df.to_csv(index=False).encode("utf-8"), "india_state_summary.csv", "text/csv")
