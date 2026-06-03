import streamlit as st
import plotly.express as px
import pandas as pd

from advanced_metrics import CDNAnalytics, FilterConfig

PARQUET_GLOB = r"D:\VETO Logs\parquet_output\*.parquet"

st.set_page_config(page_title="CDN Advanced Analytics", layout="wide")
st.title("CDN Advanced Analytics")

@st.cache_resource
def get_engine() -> CDNAnalytics:
    return CDNAnalytics(PARQUET_GLOB)

engine = get_engine()

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.header("Filters")

start_dt = st.sidebar.text_input("Start datetime (optional)", "")
end_dt = st.sidebar.text_input("End datetime (optional)", "")
states = tuple(st.sidebar.text_input("States comma-separated", "").split(",")) if st.sidebar.checkbox("Use state filter") else ()
hosts = tuple(st.sidebar.text_input("Hosts comma-separated", "").split(",")) if st.sidebar.checkbox("Use host filter") else ()
path_search = st.sidebar.text_input("Path contains", "")
status_raw = st.sidebar.text_input("Status codes comma-separated", "")
status_codes = tuple(int(x.strip()) for x in status_raw.split(",") if x.strip().isdigit())

filters = FilterConfig(
    start_dt=start_dt or None,
    end_dt=end_dt or None,
    states=tuple(s.strip() for s in states if s.strip()),
    hosts=tuple(h.strip() for h in hosts if h.strip()),
    status_codes=status_codes,
    path_search=path_search.strip(),
)

summary = engine.global_summary(filters).to_pandas()
row = summary.iloc[0]

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Requests", f"{int(row.get('total_requests', 0)):,}")
m2.metric("Pseudo Users", f"{int(row.get('pseudo_users', 0)):,}")
m3.metric("Error Rate %", f"{float(row.get('error_rate_pct', 0) or 0):.2f}")
m4.metric("Cache Hit %", f"{float(row.get('cache_hit_pct', 0) or 0):.2f}")
m5.metric("Avg Throughput", f"{float(row.get('avg_throughput', 0) or 0):,.0f}")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Overview",
    "QoE",
    "Pseudo Users",
    "Retries & Duplicates",
    "Bots",
    "Raw Sample",
])

with tab1:
    hourly = engine.hourly_trend(filters).to_pandas()
    if not hourly.empty:
        fig = px.line(hourly, x="hour", y="requests", markers=True)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        states_df = engine.top_states(filters).to_pandas()
        if not states_df.empty:
            st.subheader("Top States")
            st.dataframe(states_df, use_container_width=True)

    with col2:
        status_df = engine.status_breakdown(filters).to_pandas()
        if not status_df.empty:
            st.subheader("Status Breakdown")
            st.dataframe(status_df, use_container_width=True)

    top_paths = engine.top_paths(filters).to_pandas()
    if not top_paths.empty:
        st.subheader("Top Paths")
        st.dataframe(top_paths, use_container_width=True)

with tab2:
    qoe = engine.qoe_segments(filters).to_pandas()
    if not qoe.empty:
        st.subheader("QoE Segments")
        st.dataframe(qoe, use_container_width=True)

    state_qoe = engine.state_qoe(filters).to_pandas()
    if not state_qoe.empty:
        st.subheader("State QoE")
        st.dataframe(state_qoe, use_container_width=True)

    cache_df = engine.cache_breakdown(filters).to_pandas()
    if not cache_df.empty:
        st.subheader("Cache Breakdown")
        st.dataframe(cache_df, use_container_width=True)

with tab3:
    st.subheader("Top Pseudo Users")
    top_users = engine.top_pseudo_users(filters).to_pandas()
    st.dataframe(top_users, use_container_width=True)

    selected_user = st.text_input("Pseudo user key", "")
    if selected_user:
        user_summary = engine.pseudo_user_summary(selected_user, filters).to_pandas()
        timeline = engine.pseudo_user_timeline(selected_user, filters).to_pandas()
        sessions = engine.pseudo_user_sessions(selected_user, filters).to_pandas()

        st.subheader("User Summary")
        st.dataframe(user_summary, use_container_width=True)

        st.subheader("User Sessions")
        st.dataframe(sessions, use_container_width=True)

        st.subheader("User Timeline")
        st.dataframe(timeline, use_container_width=True)

with tab4:
    retries = st.number_input("Retry threshold seconds", min_value=1, max_value=10, value=2)
    if st.button("Run duplicate analysis"):
        dupes = engine.duplicate_groups(filters, rounded_seconds=1).to_pandas()
        st.dataframe(dupes, use_container_width=True)

    selected_user_retry = st.text_input("Pseudo user key for retry analysis", key="retry_user")
    if selected_user_retry:
        retry_df = engine.pseudo_user_retries(selected_user_retry, filters, threshold_seconds=retries).to_pandas()
        st.dataframe(retry_df, use_container_width=True)

with tab5:
    bots = engine.bot_like_users(filters).to_pandas()
    st.dataframe(bots, use_container_width=True)

with tab6:
    sample = engine.sample_rows(filters, limit=500).to_pandas()
    st.dataframe(sample, use_container_width=True)