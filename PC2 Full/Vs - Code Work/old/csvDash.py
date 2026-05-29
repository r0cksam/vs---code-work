import re
from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="User Session Viewer",
    page_icon="👤",
    layout="wide"
)


# ----------------------------
# Helpers
# ----------------------------
def detect_path_column(df: pd.DataFrame) -> str | None:
    candidates = ["reqPath", "rspContentType", "path", "url", "request_path"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def detect_time_column(df: pd.DataFrame) -> str | None:
    candidates = ["reqTimeSec", "timestamp", "time", "event_time"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def extract_channel(path: str) -> str:
    if pd.isna(path):
        return "Unknown"
    m = re.search(r"(vglive-sk-\d+)", str(path))
    return m.group(1) if m else "Unknown"


def extract_quality(path: str) -> str:
    if pd.isna(path):
        return "Unknown"

    path = str(path)

    m = re.search(r"(\d+p)", path)
    if m:
        return m.group(1)

    m2 = re.search(r"main_(\d+)\.m3u8", path)
    if m2:
        return f"variant_{m2.group(1)}"

    if "main.m3u8" in path:
        return "master"

    return "Unknown"


def device_type_from_ua(ua: str) -> str:
    if pd.isna(ua):
        return "Unknown"

    ua = str(ua)

    if "TV" in ua or "SmartTV" in ua or "HiSmartTV" in ua:
        return "Smart TV"
    if "Android" in ua and "ExoPlayer" in ua:
        return "Android"
    if "iPhone" in ua:
        return "iPhone"
    if "iPad" in ua:
        return "iPad"
    if "Windows" in ua:
        return "Windows"
    if "Macintosh" in ua or "Mac OS" in ua:
        return "Mac"
    return "Other"


def load_csv(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path, low_memory=False)


def build_sessions(
    df: pd.DataFrame,
    time_col: str,
    path_col: str,
    session_gap_minutes: int
) -> pd.DataFrame:
    df = df.copy()

    # time
    if time_col == "reqTimeSec":
        df["event_time"] = pd.to_datetime(
            pd.to_numeric(df[time_col], errors="coerce"),
            unit="s",
            errors="coerce"
        )
    else:
        df["event_time"] = pd.to_datetime(df[time_col], errors="coerce")

    df = df.dropna(subset=["event_time"]).sort_values("event_time").reset_index(drop=True)

    # path-based enrichments
    df["channel"] = df[path_col].apply(extract_channel)
    df["quality"] = df[path_col].apply(extract_quality)

    # device
    if "UA" in df.columns:
        df["device_type"] = df["UA"].apply(device_type_from_ua)
    else:
        df["device_type"] = "Unknown"

    # sessionization
    df["gap_min"] = df["event_time"].diff().dt.total_seconds() / 60
    df["new_session"] = df["gap_min"].isna() | (df["gap_min"] > session_gap_minutes)
    df["session_id"] = df["new_session"].cumsum().astype(int)

    return df


def session_summary(df: pd.DataFrame, path_col: str) -> pd.DataFrame:
    summary = (
        df.groupby("session_id")
        .agg(
            start_time=("event_time", "min"),
            end_time=("event_time", "max"),
            requests=("session_id", "size"),
            unique_channels=("channel", "nunique"),
            unique_paths=(path_col, "nunique"),
            top_channel=("channel", lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"),
            top_quality=("quality", lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"),
            top_device=("device_type", lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"),
        )
        .reset_index()
    )

    summary["duration_min"] = (
        (summary["end_time"] - summary["start_time"]).dt.total_seconds() / 60
    ).round(2)

    return summary.sort_values("start_time").reset_index(drop=True)


# ----------------------------
# UI
# ----------------------------
st.title("👤 Single CSV Session Viewer")

with st.sidebar:
    st.header("Settings")

    csv_path = st.text_input(
        "CSV file path",
        value=r"/mnt/data/rows_cliIP_223.233.77.55.csv"
    )

    session_gap_minutes = st.number_input(
        "Session gap (minutes)",
        min_value=1,
        max_value=240,
        value=30,
        step=1
    )

    load_btn = st.button("Load CSV", type="primary")


if not load_btn:
    st.info("Enter your CSV file path in the sidebar and click **Load CSV**.")
    st.stop()

csv_file = Path(csv_path)
if not csv_file.exists():
    st.error(f"File not found: {csv_path}")
    st.stop()

try:
    df_raw = load_csv(csv_path)
except Exception as e:
    st.error(f"Could not read CSV: {e}")
    st.stop()

if df_raw.empty:
    st.warning("CSV is empty.")
    st.stop()

path_col = detect_path_column(df_raw)
time_col = detect_time_column(df_raw)

if not path_col:
    st.error("Could not detect path column. Expected one of: reqPath, rspContentType, path, url")
    st.stop()

if not time_col:
    st.error("Could not detect time column. Expected one of: reqTimeSec, timestamp, time, event_time")
    st.stop()

df = build_sessions(
    df=df_raw,
    time_col=time_col,
    path_col=path_col,
    session_gap_minutes=session_gap_minutes
)

summary_df = session_summary(df, path_col)

# ----------------------------
# Top metrics
# ----------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{len(df):,}")
c2.metric("Sessions", f"{df['session_id'].nunique():,}")
c3.metric("Channels", f"{df['channel'].nunique():,}")
c4.metric("Path column used", path_col)

st.markdown("---")

# ----------------------------
# Session summary table
# ----------------------------
st.subheader("Session Summary")
st.dataframe(summary_df, use_container_width=True, height=350, hide_index=True)

# ----------------------------
# Pick one session
# ----------------------------
st.subheader("Inspect One Session")

session_ids = summary_df["session_id"].tolist()
selected_session = st.selectbox("Choose session_id", options=session_ids)

session_df = df[df["session_id"] == selected_session].copy()

if session_df.empty:
    st.warning("No rows in selected session.")
    st.stop()

start_time = session_df["event_time"].min()
end_time = session_df["event_time"].max()
duration_min = round((end_time - start_time).total_seconds() / 60, 2)

top_channel = session_df["channel"].mode().iloc[0] if not session_df["channel"].mode().empty else "Unknown"
top_quality = session_df["quality"].mode().iloc[0] if not session_df["quality"].mode().empty else "Unknown"
top_device = session_df["device_type"].mode().iloc[0] if not session_df["device_type"].mode().empty else "Unknown"
top_asn = (
    str(session_df["asn"].mode().iloc[0])
    if "asn" in session_df.columns and not session_df["asn"].mode().empty
    else "N/A"
)

s1, s2, s3, s4 = st.columns(4)
s1.metric("Requests", f"{len(session_df):,}")
s2.metric("Duration (min)", f"{duration_min}")
s3.metric("Top channel", top_channel)
s4.metric("Top quality", top_quality)

s5, s6 = st.columns(2)
s5.write(f"**Top device:** {top_device}")
s6.write(f"**Top ASN:** {top_asn}")

st.write(f"**Start:** {start_time}")
st.write(f"**End:** {end_time}")

# ----------------------------
# Session charts
# ----------------------------
left, right = st.columns(2)

with left:
    st.markdown("### Top Channels in Session")
    ch = (
        session_df["channel"]
        .value_counts()
        .rename_axis("channel")
        .reset_index(name="requests")
        .head(10)
    )
    st.dataframe(ch, use_container_width=True, hide_index=True)
    if not ch.empty:
        st.bar_chart(ch.set_index("channel")["requests"])

with right:
    st.markdown("### Top Paths in Session")
    paths = (
        session_df[path_col]
        .astype(str)
        .value_counts()
        .rename_axis(path_col)
        .reset_index(name="requests")
        .head(10)
    )
    st.dataframe(paths, use_container_width=True, hide_index=True)

st.markdown("### Timeline")
timeline = (
    session_df.set_index("event_time")
    .resample("1min")
    .size()
    .rename("requests")
    .reset_index()
)
if not timeline.empty:
    st.line_chart(timeline.set_index("event_time")["requests"])

# ----------------------------
# Raw rows
# ----------------------------
st.markdown("### Raw Rows for Selected Session")
st.dataframe(session_df, use_container_width=True, height=400)

# ----------------------------
# Download selected session
# ----------------------------
csv_bytes = session_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download Selected Session CSV",
    data=csv_bytes,
    file_name=f"session_{selected_session}.csv",
    mime="text/csv"
)