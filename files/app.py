"""
CDN Log Intelligence — Streamlit App
Fixed: loading survives Streamlit re-runs, chunked file loading,
lazy column reads, all state stored in session_state.
"""

import os, glob, re
import html
from datetime import timedelta
from urllib.parse import unquote

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import streamlit as st

# ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="CDN Log Intelligence", page_icon="📡",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .block-container { padding-top:1.5rem; padding-bottom:2rem; }
    .metric-card { background:#1e2130; border-radius:10px; padding:16px 20px; border:1px solid #2d3147; }
    .metric-val  { font-size:28px; font-weight:800; color:#00e5a0; }
    .metric-lbl  { font-size:12px; color:#6b7280; text-transform:uppercase; letter-spacing:.08em; }
    .metric-delta{ font-size:12px; margin-top:4px; }
    .path-hint   { background:#1e2130; border:1px solid #2d3147; border-radius:8px;
                   padding:10px 14px; font-family:monospace; font-size:12px; color:#6b7280; margin-top:6px; }
    .path-hint b { color:#00e5a0; }
</style>""", unsafe_allow_html=True)

COLORS = ["#00e5a0","#5b7fff","#a78bfa","#ffc847","#fb923c",
          "#f472b6","#38bdf8","#34d399","#ff6b6b","#e2e8f0","#6b7280","#facc15"]

WANTED_COLS = ["UA","reqTimeSec","country","city","state","statusCode",
               "bytes","totalBytes","throughput","timeToFirstByte",
               "transferTimeMSec","turnAroundTimeMSec","cacheStatus",
               "errorCode","rspContentType","reqHost","tlsVersion",
               "proto","fileSizeBucket","reqPath","reqMethod","objSize","cliIP"]

REQUIRED_COLS = ["reqTimeSec"]
SAFE_DEFAULTS = {
    "UA": "",
    "cacheStatus": "",
    "statusCode": "",
}

# ──────────────────────────────────────────────────────────────
# PATH TRANSLATOR
# ──────────────────────────────────────────────────────────────
def translate_path(p: str) -> str:
    return p.strip().strip('"').strip("'")

# ──────────────────────────────────────────────────────────────
# UA TAGGER
# ──────────────────────────────────────────────────────────────
def tag_platform(ua):
    if not ua or pd.isna(ua): return "Null/Invalid"
    u = ua.lower()
    if "dalvik" in u:
        if re.search(r"aft[a-z0-9]", ua, re.I): return "Amazon Fire TV"
        if "bravia" in u:                        return "Sony BRAVIA"
        if "mibox" in u or "mitvstick" in u:    return "Xiaomi/MiBox"
        return "Other Android TV"
    if "web0s" in u or "webos" in u:            return "LG WebOS"
    if "tizen" in u:                            return "Samsung Tizen"
    if "applecoremedia" in u:
        if "apple tv" in u:                     return "Apple TV"
        if "iphone" in u:                       return "iPhone (ACM)"
        return "Apple Other"
    if "iphone" in u:                           return "iPhone Browser"
    if "ipad" in u:                             return "iPad Browser"
    if "macintosh" in u:                        return "Mac Browser"
    if "windows" in u:                          return "Windows Browser"
    if "exoplayer" in u:
        m = re.match(r"^([^\s/]+)", ua)
        return "App: " + (m.group(0).split("/")[0].strip() if m else "ExoPlayer")
    if "lavf" in u or "ffmpeg" in u:           return "Tool: FFmpeg"
    if "curl" in u:                             return "Tool: curl"
    if "adflag" in u:                           return "Service: AdFlag"
    if "hls2disk" in u:                         return "Service: hls2disk"
    if "palo alto" in u or "xpanse" in u:       return "Scanner: Palo Alto"
    if "slackbot" in u:                         return "Bot: Slack"
    if ua.strip() in ["(null)", "-", ""]:        return "Null/Invalid"
    return "Other"

def android_ver(ua):
    m = re.search(r"Android (\d+)", ua or "")
    return m.group(1) if m else None

def device_model(ua):
    m = re.search(r"Android [\d.]+;\s*(.+?)\s*Build/", ua or "")
    return m.group(1).strip() if m else None

# ──────────────────────────────────────────────────────────────
# LOAD — file by file, stored in session_state so re-runs are safe
# ──────────────────────────────────────────────────────────────
def find_files(folder):
    files = glob.glob(os.path.join(folder,"**","*.parquet"), recursive=True)
    files += glob.glob(os.path.join(folder,"*.parquet"))
    return sorted(set(files))

def load_and_enrich(folder: str):
    """
    Loads all parquet files one by one.
    Stores result in st.session_state so Streamlit re-runs don't restart it.
    """
    files = find_files(folder)
    if not files:
        st.error("No parquet files found in that folder.")
        return False

    # Already loaded this exact folder → skip
    if (st.session_state.get("loaded_folder") == folder and
            st.session_state.get("df_full") is not None):
        return True

    # Reset state
    st.session_state.df_full       = None
    st.session_state.loaded_folder = None
    st.session_state.loaded_files  = []
    st.session_state.loading       = True

    status_box = st.empty()
    prog       = st.progress(0)

    try:
        dfs = []
        for i, f in enumerate(files):
            fname = os.path.basename(f)
            status_box.info(f"📂 Loading {i+1}/{len(files)}: {fname}")
            prog.progress((i) / len(files))
            try:
                schema_names = pq.read_schema(f).names
                cols = [c for c in WANTED_COLS if c in schema_names]
                if not cols:
                    st.warning(f"⚠ Skipped {fname}: no recognized CDN log columns found.")
                    continue
                df = pq.read_table(f, columns=cols).to_pandas()
                df["_src"] = fname
                dfs.append(df)
            except Exception as e:
                st.warning(f"⚠ Skipped {fname}: {e}")

        prog.progress(1.0)
        status_box.info("⚙️ Enriching data…")

        if not dfs:
            st.error("Could not read any files.")
            return False

        df = pd.concat(dfs, ignore_index=True)
        del dfs  # free memory

        missing_required = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing_required:
            st.error(
                "Cannot build the dashboard. Missing required column(s): "
                + ", ".join(f"`{c}`" for c in missing_required)
            )
            return False

        for col, default in SAFE_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default

        # Enrich
        df["_ts"]    = pd.to_numeric(df["reqTimeSec"], errors="coerce")
        df["_dt"]    = pd.to_datetime(df["_ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
        df["_date"]  = df["_dt"].dt.date
        df["_hour"]  = df["_dt"].dt.hour

        if df["_date"].dropna().empty:
            st.error("Cannot build the dashboard. No valid timestamps found in `reqTimeSec`.")
            return False

        df["_ua"]    = df["UA"].apply(lambda x: unquote(str(x)) if pd.notna(x) else "")
        df["_plat"]  = df["_ua"].apply(tag_platform)
        df["_aver"]  = df["_ua"].apply(android_ver)
        df["_model"] = df["_ua"].apply(device_model)

        for col in ["bytes","totalBytes","throughput","timeToFirstByte",
                    "transferTimeMSec","turnAroundTimeMSec","objSize"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["_hit"] = df["cacheStatus"].astype(str) == "1"
        df["_err"] = df["statusCode"].astype(str).str.startswith(("4","5"))

        st.session_state.df_full       = df
        st.session_state.loaded_folder = folder
        st.session_state.loaded_files  = files
        return True
    finally:
        st.session_state.loading = False
        status_box.empty()
        prog.empty()

# ──────────────────────────────────────────────────────────────
# INIT SESSION STATE
# ──────────────────────────────────────────────────────────────
for k,v in [("df_full",None),("loaded_folder",""),("loaded_files",[]),("loading",False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ──────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📡 CDN Log Intelligence")
    st.markdown("---")

    raw_path = st.text_input("Folder path (paste Windows path)",
                             placeholder=r"Z:\Veto Logs\01")
    if raw_path:
        translated = translate_path(raw_path)
        safe = html.escape(translated)
        st.markdown(f'<div class="path-hint">→ <b>{safe}</b></div>',
                    unsafe_allow_html=True)

    load_btn = st.button("Load Data", type="primary", use_container_width=True)

    st.markdown("---")
    mode = st.radio("Analysis Mode", ["Single Range", "Compare Two Ranges"])
    st.markdown("---")

# ──────────────────────────────────────────────────────────────
# TRIGGER LOAD
# ──────────────────────────────────────────────────────────────
if load_btn and raw_path:
    folder = translate_path(raw_path)
    if not os.path.isdir(folder):
        st.error(f"❌ Folder not found: `{folder}`\n\nCheck your docker-compose.yml volumes.")
        st.stop()
    if load_and_enrich(folder):
        st.rerun()  # clean re-run after load completes

# ──────────────────────────────────────────────────────────────
# LANDING
# ──────────────────────────────────────────────────────────────
df_full = st.session_state.df_full

if df_full is None:
    if st.session_state.loading:
        st.info("Loading… please wait.")
    else:
        st.markdown("""
        <div style="text-align:center;padding:80px 40px">
            <div style="font-size:60px;margin-bottom:16px">📡</div>
            <h1 style="font-size:2.2rem;font-weight:800;margin-bottom:12px">CDN Log Intelligence</h1>
            <p style="color:#6b7280;font-size:15px;max-width:480px;margin:0 auto 32px">
                Paste your Windows folder path in the sidebar and click <strong>Load Data</strong>.<br>
                Path is auto-converted — no docker-compose editing needed.
            </p>
            <div style="background:#1e2130;border:1px solid #2d3147;border-radius:12px;
                        padding:24px;max-width:520px;margin:0 auto;text-align:left">
                <div style="font-family:monospace;font-size:11px;color:#6b7280;margin-bottom:10px">EXAMPLES</div>
                <div style="font-family:monospace;font-size:13px;line-height:2.2;color:#ffc847">
                    Z:\\Veto Logs\\01<br>
                    D:\\Vs - Code Work\\cleaned_output<br>
                    C:\\Users\\you\\cdn-logs
                </div>
            </div>
        </div>""", unsafe_allow_html=True)
    st.stop()

# Sidebar filters — only rendered after data is loaded
with st.sidebar:
    all_plat = sorted(df_full["_plat"].dropna().unique())
    all_cty  = sorted(df_full["country"].dropna().unique()) if "country" in df_full.columns else []
    all_host = sorted(df_full["reqHost"].dropna().unique()) if "reqHost" in df_full.columns else []
    pf = st.multiselect("Filter: Platform",  all_plat)
    cf = st.multiselect("Filter: Country",   all_cty)
    hf = st.multiselect("Filter: Req Host",  all_host)
    if st.button("🔄 Load different folder", use_container_width=True):
        st.session_state.df_full = None
        st.session_state.loaded_folder = ""
        st.rerun()

# ──────────────────────────────────────────────────────────────
# DATE RANGE
# ──────────────────────────────────────────────────────────────
all_dates = sorted(df_full["_date"].dropna().unique())
min_d, max_d = all_dates[0], all_dates[-1]
n_files = len(st.session_state.loaded_files)

st.markdown(f"**Loaded:** `{min_d}` → `{max_d}` &nbsp;·&nbsp; "
            f"{len(all_dates)} day(s) &nbsp;·&nbsp; "
            f"{len(df_full):,} rows &nbsp;·&nbsp; {n_files} file(s)")

if mode == "Single Range":
    c1,c2 = st.columns(2)
    with c1: d_start = st.date_input("From", value=min_d, min_value=min_d, max_value=max_d)
    with c2: d_end   = st.date_input("To",   value=max_d, min_value=min_d, max_value=max_d)
    df = df_full[(df_full["_date"] >= d_start) & (df_full["_date"] <= d_end)].copy()
    df_b = None
else:
    mid = all_dates[max(0, len(all_dates)//2 - 1)]
    mid2 = all_dates[min(len(all_dates)-1, len(all_dates)//2)]
    c1,c2,c3,c4 = st.columns(4)
    with c1: d_start  = st.date_input("A — From", value=min_d, min_value=min_d, max_value=max_d)
    with c2: d_end    = st.date_input("A — To",   value=mid,   min_value=min_d, max_value=max_d)
    with c3: d_start2 = st.date_input("B — From", value=mid2,  min_value=min_d, max_value=max_d)
    with c4: d_end2   = st.date_input("B — To",   value=max_d, min_value=min_d, max_value=max_d)
    df  = df_full[(df_full["_date"] >= d_start)  & (df_full["_date"] <= d_end)].copy()
    df_b= df_full[(df_full["_date"] >= d_start2) & (df_full["_date"] <= d_end2)].copy()

def filt(frame):
    if pf: frame = frame[frame["_plat"].isin(pf)]
    if cf and "country" in frame.columns: frame = frame[frame["country"].isin(cf)]
    if hf and "reqHost" in frame.columns: frame = frame[frame["reqHost"].isin(hf)]
    return frame

df = filt(df)
if df_b is not None: df_b = filt(df_b)

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def fmt(n):
    if n is None or (isinstance(n, float) and np.isnan(n)): return "—"
    if n >= 1e9: return f"{n/1e9:.2f}B"
    if n >= 1e6: return f"{n/1e6:.2f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(int(n))

def mcard(label, val, delta=None):
    dh = ""
    if delta is not None:
        c = "#00e5a0" if delta >= 0 else "#ff6b6b"
        dh = f'<div class="metric-delta" style="color:{c}">{"▲" if delta>=0 else "▼"} {abs(delta):.1f}%</div>'
    st.markdown(f'<div class="metric-card"><div class="metric-lbl">{label}</div>'
                f'<div class="metric-val">{val}</div>{dh}</div>', unsafe_allow_html=True)

def barchart(data, x, y, title, color=None, top=15):
    d = data.nlargest(top, y) if top else data
    fig = px.bar(d, x=y, y=x, orientation="h", title=title,
                 color_discrete_sequence=[color or COLORS[0]])
    fig.update_layout(yaxis=dict(categoryorder="total ascending"),
                      height=max(300, top*28), margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"))
    fig.update_xaxes(gridcolor="#2d3147")
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)")
    return fig

def piechart(data, names, values, title):
    fig = px.pie(data, names=names, values=values, title=title,
                 color_discrete_sequence=COLORS, hole=0.4)
    fig.update_layout(height=340, margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e8f0"))
    return fig

def linechart(data, x, y, title, color=COLORS[0]):
    fig = px.line(data, x=x, y=y, title=title, color_discrete_sequence=[color])
    fig.update_traces(line_width=2)
    fig.update_layout(height=280, margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"))
    fig.update_xaxes(gridcolor="#2d3147"); fig.update_yaxes(gridcolor="#2d3147")
    return fig

def compare_line(ts_a, ts_b, x, y, title):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts_a[x], y=ts_a[y], name=f"A: {d_start}→{d_end}",   line=dict(color=COLORS[0],width=2)))
    fig.add_trace(go.Scatter(x=ts_b[x], y=ts_b[y], name=f"B: {d_start2}→{d_end2}", line=dict(color=COLORS[1],width=2)))
    fig.update_layout(title=title, height=300, margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"))
    fig.update_xaxes(gridcolor="#2d3147"); fig.update_yaxes(gridcolor="#2d3147")
    return fig

def compare_bar(data_a, data_b, col, title):
    a = data_a.groupby(col).size().reset_index(name="Range A")
    b = data_b.groupby(col).size().reset_index(name="Range B")
    m = a.merge(b, on=col, how="outer").fillna(0).sort_values("Range A",ascending=False).head(15)
    fig = go.Figure()
    fig.add_bar(x=m[col], y=m["Range A"], name=f"A: {d_start}→{d_end}",   marker_color=COLORS[0])
    fig.add_bar(x=m[col], y=m["Range B"], name=f"B: {d_start2}→{d_end2}", marker_color=COLORS[1])
    fig.update_layout(barmode="group", title=title, height=360,
                      margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"), xaxis_tickangle=-30)
    fig.update_yaxes(gridcolor="#2d3147")
    return fig

# ──────────────────────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────────────────────
tabs = st.tabs(["📊 Overview","📱 UA Analysis","🌍 Geography",
                "⚡ Performance","💾 Cache & Content","🚨 Errors & Anomalies","🔍 Raw Explorer"])

# ── OVERVIEW ──────────────────────────────────────────────────
with tabs[0]:
    st.markdown("### Key Metrics")
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    with c1: mcard("Total Requests",  fmt(len(df)))
    with c2: mcard("Unique UAs",      fmt(df["_ua"].nunique()))
    with c3: mcard("Cache Hit Rate",  f"{df['_hit'].mean()*100:.1f}%")
    with c4: mcard("Error Rate",      f"{df['_err'].mean()*100:.2f}%")
    tput = df["throughput"].mean() if "throughput" in df.columns else None
    with c5: mcard("Avg Throughput",  f"{tput/1e3:.0f} Mbps" if pd.notna(tput) else "—")
    tb = df["totalBytes"].sum()/1e12 if "totalBytes" in df.columns else 0
    with c6: mcard("Total Transferred", f"{tb:.2f} TB")

    st.markdown("---")
    st.markdown("### Requests Over Time")
    ts = df.groupby(df["_dt"].dt.floor("1h")).size().reset_index()
    ts.columns = ["hour","requests"]
    if df_b is not None:
        ts2 = df_b.groupby(df_b["_dt"].dt.floor("1h")).size().reset_index()
        ts2.columns = ["hour","requests"]
        st.plotly_chart(compare_line(ts, ts2, "hour","requests","Requests per Hour — A vs B"), use_container_width=True)
    else:
        st.plotly_chart(linechart(ts,"hour","requests","Requests per Hour"), use_container_width=True)

    c1,c2 = st.columns(2)
    with c1:
        sc = df["statusCode"].value_counts().reset_index(); sc.columns=["Status","Count"]
        st.plotly_chart(piechart(sc,"Status","Count","Status Codes"), use_container_width=True)
    with c2:
        if "reqHost" in df.columns:
            hc = df["reqHost"].value_counts().reset_index(); hc.columns=["Host","Count"]
            st.plotly_chart(barchart(hc,"Host","Count","Top Hosts",COLORS[1],10), use_container_width=True)

# ── UA ANALYSIS ───────────────────────────────────────────────
with tabs[1]:
    st.markdown("### Platform Distribution")
    plat = df.groupby("_plat").size().reset_index(name="requests").sort_values("requests",ascending=False)
    c1,c2 = st.columns([2,1])
    with c1: st.plotly_chart(barchart(plat,"_plat","requests","By Platform",top=20), use_container_width=True)
    with c2: st.plotly_chart(piechart(plat.head(10),"_plat","requests","Top 10"), use_container_width=True)

    if df_b is not None:
        st.markdown("#### A vs B — Platform")
        st.plotly_chart(compare_bar(df, df_b, "_plat","Platform Comparison"), use_container_width=True)

    st.markdown("---")
    st.markdown("### Android Device Models")
    adf = df[df["_model"].notna()]
    if len(adf):
        dm = adf.groupby("_model").size().reset_index(name="requests").sort_values("requests",ascending=False)
        c1,c2 = st.columns([3,1])
        with c1: st.plotly_chart(barchart(dm,"_model","requests","Device Models",COLORS[0],20), use_container_width=True)
        with c2: st.dataframe(dm.head(30), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### Android OS Versions")
    av = df[df["_aver"].notna()].groupby("_aver").size().reset_index(name="requests")
    av["_s"] = pd.to_numeric(av["_aver"], errors="coerce")
    av = av.sort_values("_s")
    fig = px.bar(av, x="_aver", y="requests", color_discrete_sequence=[COLORS[2]],
                 title="Android Version Distribution")
    fig.update_layout(height=300,margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font=dict(color="#e4e8f0"))
    fig.update_yaxes(gridcolor="#2d3147")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("### Top UA Strings by Volume")
    st.dataframe(df.groupby("_ua").size().reset_index(name="requests")
                   .sort_values("requests",ascending=False).head(50),
                 use_container_width=True, hide_index=True)

# ── GEOGRAPHY ─────────────────────────────────────────────────
with tabs[2]:
    if "country" not in df.columns:
        st.info("No geography columns found.")
    else:
        st.markdown("### Country")
        ct = df.groupby("country").size().reset_index(name="requests").sort_values("requests",ascending=False)
        c1,c2 = st.columns(2)
        with c1: st.plotly_chart(barchart(ct,"country","requests","By Country",COLORS[3],20), use_container_width=True)
        with c2: st.plotly_chart(piechart(ct.head(10),"country","requests","Top 10"), use_container_width=True)

        if df_b is not None:
            st.plotly_chart(compare_bar(df, df_b,"country","Country Comparison"), use_container_width=True)

        st.markdown("---")
        india = df[df["country"]=="IN"].copy()
        if len(india) and "state" in india.columns:
            st.markdown("### India — State")
            india["state_d"] = india["state"].apply(lambda x: unquote(str(x)) if pd.notna(x) else x)
            sa = india.groupby("state_d").size().reset_index(name="requests").sort_values("requests",ascending=False)
            c1,c2 = st.columns([2,1])
            with c1: st.plotly_chart(barchart(sa,"state_d","requests","Indian States",COLORS[4],25), use_container_width=True)
            with c2: st.dataframe(sa, use_container_width=True, hide_index=True)

        if "city" in df.columns:
            st.markdown("---")
            st.markdown("### City")
            ca = df[df["city"].notna()].groupby("city").size().reset_index(name="requests").sort_values("requests",ascending=False)
            st.plotly_chart(barchart(ca,"city","requests","Top Cities",COLORS[5],20), use_container_width=True)

# ── PERFORMANCE ───────────────────────────────────────────────
with tabs[3]:
    st.markdown("### Throughput & Latency")
    tput = df["throughput"] if "throughput" in df.columns else pd.Series(dtype=float)
    ttfb = df["timeToFirstByte"] if "timeToFirstByte" in df.columns else pd.Series(dtype=float)
    ttime= df["transferTimeMSec"] if "transferTimeMSec" in df.columns else pd.Series(dtype=float)
    c1,c2,c3,c4 = st.columns(4)
    with c1: mcard("Avg Throughput",   f"{tput.mean()/1e3:.1f} Mbps" if tput.notna().any() else "—")
    with c2: mcard("Median Throughput",f"{tput.median()/1e3:.1f} Mbps" if tput.notna().any() else "—")
    with c3: mcard("Avg TTFB",         f"{ttfb.mean():.0f} ms" if ttfb.notna().any() else "—")
    with c4: mcard("Avg Transfer Time",f"{ttime.mean():.0f} ms" if ttime.notna().any() else "—")

    if "throughput" in df.columns:
        st.markdown("---")
        tp = df.groupby(df["_dt"].dt.floor("1h"))["throughput"].mean().reset_index()
        tp.columns=["hour","kbps"]; tp["Mbps"]=tp["kbps"]/1e3
        if df_b is not None:
            tp2 = df_b.groupby(df_b["_dt"].dt.floor("1h"))["throughput"].mean().reset_index()
            tp2.columns=["hour","kbps"]; tp2["Mbps"]=tp2["kbps"]/1e3
            st.plotly_chart(compare_line(tp,tp2,"hour","Mbps","Throughput (Mbps) — A vs B"), use_container_width=True)
        else:
            st.plotly_chart(linechart(tp,"hour","Mbps","Avg Throughput (Mbps)",COLORS[0]), use_container_width=True)

    c1,c2 = st.columns(2)
    with c1:
        if "tlsVersion" in df.columns:
            st.plotly_chart(piechart(df.groupby("tlsVersion").size().reset_index(name="r"),"tlsVersion","r","TLS Versions"), use_container_width=True)
    with c2:
        if "proto" in df.columns:
            st.plotly_chart(piechart(df.groupby("proto").size().reset_index(name="r"),"proto","r","Protocols"), use_container_width=True)

# ── CACHE & CONTENT ───────────────────────────────────────────
with tabs[4]:
    st.markdown("### Cache")
    c1,c2,c3 = st.columns(3)
    with c1: mcard("Hit Rate",  f"{df['_hit'].mean()*100:.1f}%")
    with c2: mcard("Hits",      fmt(df["_hit"].sum()))
    with c3: mcard("Misses",    fmt((~df["_hit"]).sum()))

    ch = df.groupby(df["_dt"].dt.floor("1h"))["_hit"].mean().reset_index()
    ch.columns=["hour","rate"]; ch["pct"]=ch["rate"]*100
    if df_b is not None:
        ch2 = df_b.groupby(df_b["_dt"].dt.floor("1h"))["_hit"].mean().reset_index()
        ch2.columns=["hour","rate"]; ch2["pct"]=ch2["rate"]*100
        st.plotly_chart(compare_line(ch,ch2,"hour","pct","Cache Hit Rate (%) — A vs B"), use_container_width=True)
    else:
        st.plotly_chart(linechart(ch,"hour","pct","Cache Hit Rate (%)",COLORS[3]), use_container_width=True)

    c1,c2 = st.columns(2)
    with c1:
        if "rspContentType" in df.columns:
            st.plotly_chart(piechart(df.groupby("rspContentType").size().reset_index(name="r"),"rspContentType","r","Content Types"), use_container_width=True)
    with c2:
        if "fileSizeBucket" in df.columns:
            st.plotly_chart(piechart(df.groupby("fileSizeBucket").size().reset_index(name="r"),"fileSizeBucket","r","File Sizes"), use_container_width=True)

    if "totalBytes" in df.columns:
        st.markdown("---")
        st.markdown("### Bandwidth")
        bw = df.groupby(df["_dt"].dt.floor("1h"))["totalBytes"].sum().reset_index()
        bw.columns=["hour","bytes"]; bw["GB"]=bw["bytes"]/1e9
        if df_b is not None:
            bw2 = df_b.groupby(df_b["_dt"].dt.floor("1h"))["totalBytes"].sum().reset_index()
            bw2.columns=["hour","bytes"]; bw2["GB"]=bw2["bytes"]/1e9
            st.plotly_chart(compare_line(bw,bw2,"hour","GB","Bandwidth (GB/hr) — A vs B"), use_container_width=True)
        else:
            st.plotly_chart(linechart(bw,"hour","GB","Bandwidth per Hour (GB)",COLORS[4]), use_container_width=True)

# ── ERRORS & ANOMALIES ────────────────────────────────────────
with tabs[5]:
    st.markdown("### Error Overview")
    err = df[df["_err"]]
    c1,c2,c3 = st.columns(3)
    with c1: mcard("Total Errors", fmt(len(err)))
    with c2: mcard("Error Rate",   f"{df['_err'].mean()*100:.2f}%")
    with c3: mcard("4xx",          fmt(df["statusCode"].astype(str).str.startswith("4").sum()))

    c1,c2 = st.columns(2)
    with c1:
        se = err.groupby("statusCode").size().reset_index(name="count").sort_values("count",ascending=False)
        if len(se): st.plotly_chart(barchart(se,"statusCode","count","Status Code Errors",COLORS[8],10), use_container_width=True)
    with c2:
        if "errorCode" in df.columns:
            ec = df[(df["errorCode"].notna()) & (df["errorCode"].astype(str).str.strip() != "") & (df["errorCode"].astype(str) != "nan")]
            ec = ec.groupby("errorCode").size().reset_index(name="count").sort_values("count",ascending=False)
            if len(ec): st.plotly_chart(barchart(ec,"errorCode","count","Akamai Error Codes",COLORS[3],10), use_container_width=True)

    et = df.groupby(df["_dt"].dt.floor("1h"))["_err"].mean().reset_index()
    et.columns=["hour","rate"]; et["pct"]=et["rate"]*100
    if df_b is not None:
        et2 = df_b.groupby(df_b["_dt"].dt.floor("1h"))["_err"].mean().reset_index()
        et2.columns=["hour","rate"]; et2["pct"]=et2["rate"]*100
        st.plotly_chart(compare_line(et,et2,"hour","pct","Error Rate (%) — A vs B"), use_container_width=True)
    else:
        st.plotly_chart(linechart(et,"hour","pct","Error Rate (%)",COLORS[8]), use_container_width=True)

    st.markdown("---")
    st.markdown("### Anomaly Flags")
    flags = []
    n = df["_plat"].eq("Null/Invalid").sum()
    if n: flags.append(f"🔴 **{fmt(n)} requests** with null/invalid UA")
    n = df["_plat"].eq("Service: hls2disk").sum()
    if n: flags.append(f"⚠️ **{fmt(n)} requests** from hls2disk — stream ripping tool")
    n = df["_plat"].eq("Tool: FFmpeg").sum()
    if n: flags.append(f"⚠️ **{fmt(n)} requests** from FFmpeg/Lavf")
    n = df["_plat"].eq("Scanner: Palo Alto").sum()
    if n: flags.append(f"🛡️ **{fmt(n)} requests** from Palo Alto Cortex Xpanse")
    fut = df[pd.to_numeric(df["_aver"],errors="coerce").fillna(0) >= 16]
    if len(fut): flags.append(f"⚠️ **{fmt(len(fut))} requests** with Android 16+ (possibly spoofed)")
    spk = df.groupby(df["_dt"].dt.floor("1h"))["_err"].mean()
    sh = spk[spk > 0.1]
    if len(sh): flags.append(f"🔴 **{len(sh)} hour(s)** with >10% error rate")
    for f in flags: st.markdown(f)
    if not flags: st.success("✅ No significant anomalies detected.")

# ── RAW EXPLORER ──────────────────────────────────────────────
with tabs[6]:
    st.markdown(f"### Raw Explorer — {fmt(len(df))} rows in current view")
    all_c = list(df.columns)
    def_c = [c for c in ["_ua","_plat","_date","_hour","country","state",
                          "statusCode","throughput","timeToFirstByte",
                          "cacheStatus","reqHost","reqPath"] if c in all_c]
    cols_show = st.multiselect("Columns", all_c, default=def_c)
    search = st.text_input("UA contains…")
    show = df.copy()
    if search: show = show[show["_ua"].str.contains(search, case=False, na=False)]
    st.dataframe(show[cols_show].head(10000), use_container_width=True, hide_index=True)
    st.download_button("⬇️ Export CSV", show[cols_show].head(100000).to_csv(index=False).encode(),
                       "cdn_export.csv","text/csv")

st.markdown(f'<div style="text-align:center;color:#6b7280;font-size:11px;margin-top:24px">'
            f'CDN Log Intelligence · {fmt(len(df))} rows in view · {n_files} files loaded</div>',
            unsafe_allow_html=True)
