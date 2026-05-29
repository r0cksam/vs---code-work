import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, time, timedelta
from urllib.parse import unquote

# ====================== CONFIG ======================
LAKE_FOLDER = Path(r"D:\Veto Logs Backup\veto Stream logs\lake")
CHUNK_DURATION_SECONDS = 6
CHUNK_DURATION_HOURS   = CHUNK_DURATION_SECONDS / 3600.0

# ====================== CHANNEL MAP ======================
CHANNEL_MAP_RAW = {
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
    "vglive-sk-238731": "NDTV Marathi",
    "vglive-sk-639201": "IndiaTV Cricket",
    "vglive-sk-834057": "NDTV India",
    "indiatv": "India TV",
    "india%20tv": "India TV",
    "india tv": "India TV",
    "speednews": "India TV SpeedNews",
    "aapkiadalat": "India TV Adalat",
    "yogatv": "India TV Yoga",
    "yoga": "India TV Yoga",
    "indiatvcrk": "IndiaTV Cricket",
    "ndtv_india": "NDTV India",
    "ndtvindia": "NDTV India",
    "ctvndtvindia": "NDTV India",
    "ndtv india": "NDTV India",
    "ndtv_marathi": "NDTV Marathi",
    "ndtvmarathi": "NDTV Marathi",
    "ctvndtvmarathi": "NDTV Marathi",
    "b4u_music": "B4U Music",
    "b4umusic": "B4U Music",
    "b4um001": "B4U Music",
    "b4u_movies": "B4U Movies",
    "b4umovies": "B4U Movies",
    "b4umo001": "B4U Movies",
    "b4u_kadak": "B4U Kadak",
    "b4ukadak": "B4U Kadak",
    "b4ua001": "B4U Kadak",
    "b4u_bhojpuri": "B4U Bhojpuri",
    "b4ubhojpuri": "B4U Bhojpuri",
    "b4u bhojpuri": "B4U Bhojpuri",
    "9xm": "9XM",
    "9xm_jalwa": "9XM Jalwa",
    "9xmjalwa": "9XM Jalwa",
    "9xjalwa": "9XM Jalwa",
    "9xm_tashan": "9XM Tashan",
    "9xmtashan": "9XM Tashan",
    "9xtashan": "9XM Tashan",
    "9xm_jhakaas": "9XM Jhakaas",
    "9xmjhakaas": "9XM Jhakaas",
    "9xjhakaas": "9XM Jhakaas",
    "newsnation": "NewsNation",
    "news nation": "NewsNation",
    "newsnation_upuk": "NewsNation UP/UK",
    "newsnation upuk": "NewsNation UP/UK",
    "nnup": "NewsNation UP/UK",
    "newsnation_pbhr": "NewsNation PB/HR",
    "newsnation_mpch": "NewsNation MP/CH",
    "nnmp": "NewsNation MP/CH",
    "newsnation_brjh": "NewsNation BR/JH",
    "nnbrjh": "NewsNation BR/JH",
    "nnpunj": "NewsNation Punjab",
    "gtc_news": "GTC News",
    "gtcnews": "GTC News",
    "gtc_punjabi": "GTC Punjabi",
    "gtcpunjabi": "GTC Punjabi",
    "sanskaartv": "Sanskaar TV",
    "sanskaar": "Sanskaar TV",
    "sanskar": "Sanskaar TV",
    "satsanghtv": "Satsangh TV",
    "satsangh": "Satsangh TV",
    "satsang": "Satsangh TV",
    "shubhtv": "Shubh TV",
    "shubh": "Shubh TV",
    "manorama": "Manorama",
    "national": "DD National",
    "punjabi_shorts": "Punjabi Shorts",
    "punjabshort": "Punjabi Shorts",
    "bollywoodmasala": "Bollywood Masala",
    "bollywood masala": "Bollywood Masala",
    "vetocricketlive": "Veto Cricket Live",
    "out": "Other",                     # ← fixed: was incorrectly "manorama"
    "1080p": "Other",
    "720p": "Other",
    "480p": "Other",
    "360p": "Other",
    "master_1080": "Other",
    "master_720": "Other",
    "master_360": "Other",
    "master_504": "Other",
    "unknown": "Other",
}
CHANNEL_MAP = {k.lower(): v for k, v in CHANNEL_MAP_RAW.items()}

CARD_COLOURS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#98D8C8", "#F7C6C6", "#B5D4F4", "#C0DD97",
    "#FFB347", "#87CEEB", "#DEB887", "#90EE90", "#FFB6C1",
]

def resolve_channel(raw_id: str) -> str:
    if not raw_id or not isinstance(raw_id, str):
        return "Other"
    decoded = unquote(raw_id).strip().strip("/").strip()
    lower = decoded.lower()
    if lower in CHANNEL_MAP:
        return CHANNEL_MAP[lower]
    best, best_len = None, 0
    for key, name in CHANNEL_MAP.items():
        if lower.startswith(key) and len(key) > best_len:
            best, best_len = name, len(key)
    if best:
        return best
    for key, name in sorted(CHANNEL_MAP.items(), key=lambda x: -len(x[0])):
        if key and key in lower:
            return name
    return "Other"

@st.cache_data(ttl=86400)
def get_available_dates(lake_path):
    lake_path = Path(lake_path)
    dates = []
    for year_dir in lake_path.glob("year=*"):
        try:
            year = int(year_dir.name.split("=")[1])
            for month_dir in year_dir.glob("month=*"):
                month = int(month_dir.name.split("=")[1])
                for day_dir in month_dir.glob("day=*"):
                    day = int(day_dir.name.split("=")[1])
                    dates.append(datetime(year, month, day))
        except (ValueError, IndexError):
            continue
    if dates:
        return min(dates), max(dates)
    parquet_files = list(lake_path.glob("**/*.parquet"))
    if not parquet_files:
        return None, None
    con = duckdb.connect()
    try:
        result = con.execute(f"""
            SELECT MIN(reqTimeSec), MAX(reqTimeSec)
            FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
        """).fetchone()
        if result and result[0] and result[1]:
            return (datetime.fromtimestamp(float(result[0])),
                    datetime.fromtimestamp(float(result[1])))
    except Exception:
        pass
    finally:
        con.close()
    return None, None

def inspect_lake(lake_path):
    lake_path = Path(lake_path)
    if not list(lake_path.glob("**/*.parquet")):
        return {"error": "No parquet files found in the lake folder."}
    con = duckdb.connect()
    try:
        meta_df = con.execute(f"""
            SELECT SUM(num_rows) as total_rows
            FROM parquet_metadata('{lake_path.as_posix()}/**/*.parquet')
        """).fetchdf()
        total_rows = int(meta_df["total_rows"].iloc[0]) if not meta_df.empty else 0
        valid_ts = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
            WHERE statusCode = '200' AND reqPath LIKE '%.ts'
        """).fetchone()[0]
        return {"total_rows": total_rows, "valid_ts_rows": valid_ts}
    except Exception as e:
        return {"error": str(e)}
    finally:
        con.close()

def build_partition_filter(start_date, end_date):
    filters, current = [], start_date
    while current <= end_date:
        filters.append(f"(year = {current.year} AND month = {current.month} AND day = {current.day})")
        current += timedelta(days=1)
    return " OR ".join(filters) if filters else "1=1"

@st.cache_data(ttl=7200, show_spinner=False)
def compute_metrics(lake_path, start_date, end_date):
    lake_path = Path(lake_path)
    con = duckdb.connect()
    try:
        start_epoch = int(datetime.combine(start_date, time.min).timestamp())
        end_epoch   = int(datetime.combine(end_date,   time.max).timestamp())
        partition_filter = build_partition_filter(start_date, end_date)

        # ────────── FIXED SQL with hostname detection for B4U ──────────
        con.execute(f"""
            CREATE TEMP TABLE base_tmp AS
            SELECT
                cliIP,
                lower(
                    CASE
                        -- First, detect B4U channels by hostname (fixes the "Other" issue)
                        WHEN reqHost LIKE '%b4u-veto-m%' THEN 'b4u_movies'
                        WHEN reqHost LIKE '%b4u-veto-music%' THEN 'b4u_music'
                        WHEN reqHost LIKE '%b4u-veto-kadak%' THEN 'b4u_kadak'

                        -- Then vglive-sk IDs
                        WHEN reqPath LIKE '%vglive-sk-%'
                            THEN regexp_extract(reqPath, '(vglive-sk-[0-9]+)', 1)

                        -- Then path-based extraction (original logic)
                        WHEN reqPath LIKE '%/%' THEN
                            CASE
                                WHEN lower(split_part(ltrim(reqPath, '/'), '/', 1))
                                     IN ('v1', 'live', 'stream', 'hls', '')
                                    THEN split_part(ltrim(reqPath, '/'), '/', 2)
                                ELSE split_part(ltrim(reqPath, '/'), '/', 1)
                            END

                        ELSE
                            regexp_replace(
                                regexp_extract(reqPath, '([^/]+)\\.ts$', 1),
                                '[_-]?(1080p?|720p?|480p?|360p?|\\d+)$', ''
                            )
                    END
                ) AS channel_id
            FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
            WHERE statusCode = '200'
              AND reqPath LIKE '%.ts'
              AND CAST(reqTimeSec AS DOUBLE) BETWEEN {start_epoch} AND {end_epoch}
              AND ({partition_filter})
        """)

        raw_channel_df = con.execute("""
            SELECT
                channel_id,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                COUNT(*) AS total_chunks
            FROM base_tmp
            GROUP BY channel_id
        """).fetchdf()

        user_df = con.execute("""
            SELECT
                channel_id,
                cliIP,
                COUNT(*) AS chunks_watched
            FROM base_tmp
            GROUP BY channel_id, cliIP
            ORDER BY chunks_watched DESC
            LIMIT 20000
        """).fetchdf()

        unmapped_raw_df = raw_channel_df.copy()
        con.execute("DROP TABLE base_tmp")

        if raw_channel_df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "No data found."

        raw_channel_df["channel_name"] = raw_channel_df["channel_id"].apply(resolve_channel)
        user_df["channel_name"]        = user_df["channel_id"].apply(resolve_channel)

        unmapped_raw_df["channel_name"] = unmapped_raw_df["channel_id"].apply(resolve_channel)
        unmapped_df = unmapped_raw_df[unmapped_raw_df["channel_name"] == "Other"].copy()
        if not unmapped_df.empty:
            unmapped_df["watch_hours"] = unmapped_df["total_chunks"] * CHUNK_DURATION_HOURS
            unmapped_df = unmapped_df.sort_values("total_chunks", ascending=False)

        channel_df = raw_channel_df.groupby("channel_name", as_index=False).agg(
            unique_viewers=("unique_viewers", "sum"),
            total_chunks=("total_chunks", "sum"),
        )
        channel_df["watch_hours"] = channel_df["total_chunks"] * CHUNK_DURATION_HOURS
        channel_df["avg_chunks_per_viewer"] = channel_df["total_chunks"] / channel_df["unique_viewers"]
        channel_df["avg_watch_hours_per_viewer"] = channel_df["watch_hours"] / channel_df["unique_viewers"]
        channel_df = channel_df.sort_values("watch_hours", ascending=False).reset_index(drop=True)

        user_df["watch_hours"] = user_df["chunks_watched"] * CHUNK_DURATION_HOURS
        user_df = user_df.drop(columns=["channel_id"])

        time_range = f"{start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}"
        return channel_df, user_df, unmapped_df, time_range
    finally:
        con.close()

# ====================== STREAMLIT UI ======================
st.set_page_config(page_title="Live TV Dashboard", layout="wide")
st.title("📺 Live TV Channel Watch Dashboard")
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Settings")
    lake_input = st.text_input("Lake Folder Path", value=str(LAKE_FOLDER))

    if st.button("🔍 Inspect Lake"):
        with st.spinner("Inspecting lake…"):
            info = inspect_lake(lake_input)
            if "error" in info:
                st.error(f"Error: {info['error']}")
            else:
                st.success(f"Total rows (metadata): {info['total_rows']:,}")
                st.info(f"Valid .ts rows: {info['valid_ts_rows']:,}")

    min_dt, max_dt = get_available_dates(lake_input)
    min_default = min_dt.date() if min_dt else datetime(2026, 3, 1).date()
    max_default = max_dt.date() if max_dt else datetime(2026, 5, 1).date()

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=min_default)
    with col2:
        end_date = st.date_input("To", value=max_default)

    if start_date > end_date:
        st.error("❌ Start date cannot be after end date")
        st.stop()

    cols_per_row = st.slider("📊 Cards per row", min_value=2, max_value=8, value=6, step=1)
    st.info("📁 Hive partitioning: `year=YYYY/month=MM/day=DD`")
    run_button = st.button("🔄 Refresh Data", type="primary", use_container_width=True)
    show_debug = st.checkbox("🔎 Show unmapped channel IDs", value=False)

if run_button or "channel_df" not in st.session_state:
    with st.spinner("Processing data…"):
        result = compute_metrics(lake_input, start_date, end_date)
        channel_df, user_df, unmapped_df, time_range = result
        if channel_df.empty:
            st.error("No data found for the selected range.")
        else:
            st.session_state.channel_df = channel_df
            st.session_state.user_df = user_df
            st.session_state.unmapped_df = unmapped_df
            st.session_state.time_range = time_range
            st.success("✅ Analysis completed successfully!")

if "channel_df" in st.session_state and not st.session_state.channel_df.empty:
    df = st.session_state.channel_df
    user_df = st.session_state.user_df
    unmapped_df = st.session_state.unmapped_df
    time_range = st.session_state.time_range

    st.markdown(f"**Data Period:** {time_range}")
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Total Channels", len(df[df["channel_name"] != "Other"]))
    with c2: st.metric("Total Watch Hours", f"{df['watch_hours'].sum():,.0f}")
    with c3: st.metric("Unique Viewers", f"{df['unique_viewers'].sum():,}")
    with c4: st.metric("Total Chunks", f"{df['total_chunks'].sum():,}")

    st.markdown("---")
    st.subheader("📡 Channel Performance")

    display_df = df[df["channel_name"] != "Other"]
    other_row = df[df["channel_name"] == "Other"]

    for i in range(0, len(display_df), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, col in enumerate(cols):
            idx = i + j
            if idx < len(display_df):
                row = display_df.iloc[idx]
                colour = CARD_COLOURS[idx % len(CARD_COLOURS)]
                with col:
                    st.markdown(f"""
                    <div style="background-color:{colour}; padding:1.2rem; border-radius:15px;
                                margin-bottom:1rem; color:black;">
                        <h3 style="margin:0 0 .4rem">{row['channel_name']}</h3>
                        <p style="margin:.2rem 0"><b>{row['watch_hours']:.1f}</b> hrs watched</p>
                        <p style="margin:.2rem 0">👥 {row['unique_viewers']:,} viewers</p>
                        <p style="margin:.2rem 0">🎬 {row['total_chunks']:,} chunks</p>
                        <p style="margin:.2rem 0">Avg: {row['avg_watch_hours_per_viewer']:.2f} hrs/viewer</p>
                    </div>
                    """, unsafe_allow_html=True)

    if not other_row.empty:
        r = other_row.iloc[0]
        st.markdown(f"""
        <div style="background-color:#d3d3d3; padding:1rem; border-radius:12px;
                    margin-bottom:1rem; color:#333;">
            <b>Other / Unmapped</b> — {r['watch_hours']:.1f} hrs &nbsp;|&nbsp;
            👥 {r['unique_viewers']:,} viewers &nbsp;|&nbsp;
            🎬 {r['total_chunks']:,} chunks
        </div>
        """, unsafe_allow_html=True)

        # Debug expander with CSV export
        if show_debug:
            with st.expander("🔎 Unmapped raw channel IDs (add these to CHANNEL_MAP)"):
                if unmapped_df.empty:
                    st.success("No unmapped IDs — everything is resolved!")
                else:
                    left, _ = st.columns([1, 4])
                    with left:
                        st.download_button(
                            "📥 Export unmapped CSV",
                            data=unmapped_df[["channel_id", "unique_viewers",
                                              "total_chunks", "watch_hours"]].to_csv(index=False).encode("utf-8"),
                            file_name="unmapped_channels.csv",
                            mime="text/csv",
                            help="Download all raw IDs that landed in 'Other'"
                        )
                    st.dataframe(
                        unmapped_df[["channel_id", "unique_viewers",
                                     "total_chunks", "watch_hours"]].style.format({
                            "watch_hours":    "{:.2f} hrs",
                            "total_chunks":   "{:,}",
                            "unique_viewers": "{:,}",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )

    st.markdown("---")
    st.subheader("🏆 Top Viewers by Channel")
    selected_channel = st.selectbox("Select Channel", df["channel_name"].tolist())
    if selected_channel:
        top_users = user_df[user_df["channel_name"] == selected_channel].head(15)
        if not top_users.empty:
            st.dataframe(
                top_users[["cliIP", "watch_hours", "chunks_watched"]].style.format({
                    "watch_hours": "{:.2f} hrs",
                    "chunks_watched": "{:,}",
                }),
                use_container_width=True,
                hide_index=True,
            )

    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button(
            "📥 Channel Report", df.to_csv(index=False).encode("utf-8"),
            "channel_report.csv", "text/csv"
        )
    with col_dl2:
        st.download_button(
            "📥 User Report", user_df.to_csv(index=False).encode("utf-8"),
            "user_report.csv", "text/csv"
        )
else:
    st.info("👆 Click **Refresh Data** to load analytics.")