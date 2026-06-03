"""
parquet_explorer.py  -  Visual Parquet Explorer (multi-folder + smart browser)
===============================================================================
Run:  streamlit run parquet_explorer.py

pip install streamlit pandas pyarrow
"""

import json
import time
import threading
import urllib.parse
import re
import io
from pathlib import Path
from datetime import time as dtime
from urllib.parse import unquote_plus

import pandas as pd
import pyarrow as pa
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow.compute as pc
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak

try:
    import duckdb
    DUCKDB_OK = True
except ImportError:
    DUCKDB_OK = False

try:
    import plotly.express as px
    PLOTLY_OK = True
except ImportError:
    PLOTLY_OK = False

WATCH_GAP_CAP_SECONDS = 60


def ub_staged_progress(bar, placeholder, stages: list, fn):
    """
    Run fn() in a background thread while animating a staged progress bar.
    stages = list of (pct, label) tuples describing the phases.
    Returns the function result when done.
    """
    result_box = [None]
    error_box  = [None]

    def _run():
        try:
            result_box[0] = fn()
        except Exception as e:
            error_box[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    stage_idx = 0
    pulse     = 0
    while t.is_alive():
        pct, label = stages[min(stage_idx, len(stages) - 1)]
        dots = "." * (pulse % 4)
        bar.progress(pct, text=f"{label}{dots}")
        placeholder.caption(f"⏳ Working — step {stage_idx + 1} of {len(stages)}")
        time.sleep(0.35)
        pulse += 1
        # Advance stage every ~2 seconds
        if pulse % 6 == 0 and stage_idx < len(stages) - 2:
            stage_idx += 1

    bar.progress(100, text="✅ Done!")
    placeholder.empty()
    time.sleep(0.3)
    bar.empty()

    if error_box[0]:
        raise error_box[0]
    return result_box[0]



# ─────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Parquet Explorer",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .stMetric label  { font-size: 0.78rem; color: #888; }
    div[data-testid="stSidebarContent"] { padding-top: 1rem; }
    div[data-testid="stSidebarContent"] .stButton button {
        width: 100%;
        text-align: left;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────
DEFAULTS = {
    "selected_folders": [],
    "last_unique_col":  None,
    "last_unique_vals": [],
    "browser_root":     "",
    "scan_results":     [],
    # Query String Analyzer state
    "qsa_parsed_df":    None,
    "qsa_column":       None,
    "qsa_keys":         [],
    # User Behavior Dashboard state
    "ub_col_map": {
        "queryStr":         "queryStr",
        "reqTimeSec":       "reqTimeSec",
        "reqPath":          "reqPath",
        "UA":               "UA",
        "cliIP":            "cliIP",
        "asn":              "asn",
        "statusCode":       "statusCode",
        "transferTimeMSec": "transferTimeMSec",
        "downloadTime":     "downloadTime",
    },
    "ub_device_ids": [],
    "ub_loaded":     False,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────
# Folder scanner
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def scan_root(root: str) -> list:
    import os
    root_path = Path(root)
    results   = []

    def _walk(p: Path, depth: int):
        if depth > 4:
            return
        try:
            entries  = list(os.scandir(p))
            pq_count = sum(1 for e in entries
                           if e.is_file(follow_symlinks=False)
                           and e.name.endswith(".parquet"))
            subdirs  = [Path(e.path) for e in entries
                        if e.is_dir(follow_symlinks=False)
                        and not e.name.startswith(".")]
            if pq_count > 0:
                try:
                    rel = str(p.relative_to(root_path))
                except Exception:
                    rel = p.name
                if rel == ".":
                    rel = "(root)"
                results.append({
                    "path":    str(p),
                    "name":    p.name,
                    "n_files": pq_count,
                    "rel":     rel,
                })
            for sub in sorted(subdirs, key=lambda x: x.name):
                _walk(sub, depth + 1)
        except (PermissionError, OSError):
            pass

    _walk(root_path, 0)
    return results


# ─────────────────────────────────────────────
# Core data helpers
# ─────────────────────────────────────────────

def collect_files(folders: list) -> list:
    files = []
    for f in folders:
        p = Path(str(f).strip())
        if p.is_dir():
            files.extend(sorted(p.glob("*.parquet")))
    return files


@st.cache_data(show_spinner=False)
def get_file_columns(folder_key: str) -> dict:
    """Cache per-file schema names so we do not reread parquet schemas repeatedly."""
    files = collect_files(folder_key.split("|"))
    out = {}
    for f in files:
        try:
            out[str(f)] = set(pq.read_schema(f).names)
        except Exception:
            out[str(f)] = set()
    return out


def _parse_query_string(raw_val: str) -> dict:
    try:
        parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
    except Exception:
        parsed = {}
    parsed["_raw"] = raw_val
    parsed["_count"] = 1
    return parsed


def _iter_query_string_batches(parquet_file: Path, qrystr_col: str, batch_size: int = 50000):
    pf = pq.ParquetFile(parquet_file)
    for batch in pf.iter_batches(columns=[qrystr_col], batch_size=batch_size):
        arr = batch.column(0)
        raws = ["" if x is None else str(x) for x in arr.to_pylist()]
        if not raws:
            continue
        records = [_parse_query_string(raw) for raw in raws]
        yield pd.DataFrame.from_records(records).fillna("")


def build_mask(tbl, filters: dict):
    mask = None
    for col, vals in filters.items():
        if not vals or col not in tbl.schema.names:
            continue
        col_arr  = tbl.column(col).cast(pa.string())
        sub_mask = None
        for v in vals:
            eq       = pc.equal(col_arr, pa.scalar(str(v), pa.string()))
            sub_mask = eq if sub_mask is None else pc.or_(sub_mask, eq)
        if sub_mask is not None:
            mask = sub_mask if mask is None else pc.and_(mask, sub_mask)
    return mask


@st.cache_data(show_spinner="📋 Reading column schema from parquet files ...")
def load_schema(folder_key: str):
    folders  = folder_key.split("|")
    files    = collect_files(folders)
    all_cols = {}
    seen     = set()
    for f in files:
        fp = str(f.parent)
        if fp in seen:
            continue
        seen.add(fp)
        try:
            schema = pq.read_schema(f)
            for name in schema.names:
                if name not in all_cols:
                    all_cols[name] = str(schema.field(name).type)
        except Exception:
            continue
    return list(all_cols.keys()), all_cols


@st.cache_data(show_spinner="🔢 Counting total rows across all files ...")
def count_stats(folder_key: str):
    folders    = folder_key.split("|")
    files      = collect_files(folders)
    total_rows = 0
    for f in files:
        try:
            total_rows += pq.read_metadata(f).num_rows
        except Exception:
            pass
    n_folders = len({str(Path(f).parent) for f in files})
    return len(files), total_rows, n_folders


def unique_values(folder_key: str, column: str) -> pd.DataFrame:
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    counter = {}

    bar = st.progress(0, text=f"Scanning unique values ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"Scanning unique values ... {i+1:,} / {total:,} files")
        try:
            if column not in file_cols.get(str(f), set()):
                continue

            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=[column], batch_size=50000):
                arr = pa.array(batch.column(0)).cast(pa.string())
                vc = pc.value_counts(arr)
                if vc is None:
                    continue
                for item in vc:
                    val = item["values"].as_py()
                    val = str(val) if val is not None else "(null)"
                    counter[val] = counter.get(val, 0) + item["counts"].as_py()
        except Exception:
            continue

    bar.empty()
    if not counter:
        return pd.DataFrame(columns=["value", "count", "% of rows"])
    df = pd.DataFrame(list(counter.items()), columns=["value", "count"])
    df = df.sort_values("count", ascending=False).reset_index(drop=True)
    total_rows = df["count"].sum()
    df["% of rows"] = (df["count"] / total_rows * 100).round(2)
    return df


def apply_all_filters(tbl, filters: dict, dual: dict):
    mask = None

    if dual and (dual.get("vals_a") or dual.get("vals_b")):
        col_a, vals_a = dual["col_a"], dual["vals_a"]
        col_b, vals_b = dual["col_b"], dual["vals_b"]
        dm = None
        if vals_a and col_a in tbl.schema.names:
            arr = tbl.column(col_a).cast(pa.string())
            for v in vals_a:
                eq = pc.equal(arr, pa.scalar(str(v), pa.string()))
                dm = eq if dm is None else pc.or_(dm, eq)
        if vals_b and col_b in tbl.schema.names:
            arr = tbl.column(col_b).cast(pa.string())
            for v in vals_b:
                eq = pc.equal(arr, pa.scalar(str(v), pa.string()))
                dm = eq if dm is None else pc.or_(dm, eq)
        if dm is not None:
            mask = dm

    std = build_mask(tbl, filters)
    if std is not None:
        mask = std if mask is None else pc.and_(mask, std)

    if mask is not None:
        tbl = tbl.filter(mask)
    return tbl


def run_query(
    folder_key: str,
    sel_cols: list,
    filters: dict,
    dual: dict,
    max_rows=None,
    progress_label: str = "Scanning files",
) -> pd.DataFrame:
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    frames = []
    collected = 0

    bar = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    info = st.empty()

    needed_cols = list(dict.fromkeys(
        list(sel_cols)
        + list(filters.keys())
        + [dual.get("col_a"), dual.get("col_b")]
    ))
    needed_cols = [c for c in needed_cols if c]

    for i, f in enumerate(files):
        if max_rows is not None and collected >= max_rows:
            break

        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files  |  {collected:,} rows found")
        try:
            available_in_file = file_cols.get(str(f), set())
            avail = [c for c in needed_cols if c in available_in_file]
            if not any(c in available_in_file for c in sel_cols):
                continue
            if not avail:
                continue

            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=avail, batch_size=50000):
                tbl = pa.Table.from_batches([batch])
                tbl = apply_all_filters(tbl, filters, dual)
                if len(tbl) == 0:
                    continue

                present_sel_cols = [c for c in sel_cols if c in tbl.schema.names]
                if not present_sel_cols:
                    continue
                tbl = tbl.select(present_sel_cols)

                need = (max_rows - collected) if max_rows is not None else len(tbl)
                if need <= 0:
                    break

                chunk = tbl.slice(0, need).to_pandas()
                chunk.insert(0, "_folder", f.parent.name)
                frames.append(chunk)
                collected += len(chunk)

                if max_rows is not None and collected >= max_rows:
                    break
        except Exception:
            continue

    bar.empty()
    info.empty()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ─────────────────────────────────────────────
# Query String helpers  (NEW)
# ─────────────────────────────────────────────

def parse_qrystr_column(df: pd.DataFrame, value_col: str, count_col: str = "count") -> pd.DataFrame:
    """
    Given a DataFrame where each row is a unique query-string value + its count,
    parse every query string and return a flat DataFrame with one row per unique value,
    preserving the count column.
    """
    records = []
    for _, row in df.iterrows():
        raw_val = str(row[value_col])
        cnt     = row.get(count_col, 1)
        try:
            parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
        except Exception:
            parsed = {}
        parsed["_raw"]   = raw_val
        parsed["_count"] = cnt
        records.append(parsed)
    return pd.DataFrame(records).fillna("")


def load_qrystr_from_parquet(
    folder_key: str,
    qrystr_col: str,
    progress_label: str = "Loading query strings",
) -> pd.DataFrame:
    """
    Read the qrystr column from parquet files, return a flat parsed DataFrame.
    Each parquet row becomes one parsed record; _count=1 for raw parquet rows.
    """
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    frames = []

    bar = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files")
        try:
            if qrystr_col not in file_cols.get(str(f), set()):
                continue
            for parsed_batch_df in _iter_query_string_batches(f, qrystr_col, batch_size=50000):
                frames.append(parsed_batch_df)
        except Exception:
            continue
    bar.empty()
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)



# ─────────────────────────────────────────────
# User Behavior Dashboard helpers
# ─────────────────────────────────────────────

def _cm(col_map: dict, role: str) -> str:
    """Return the actual column name for a given role, fallback to role name."""
    return col_map.get(role, role)


def ub_extract_channel_from_path(path_s: pd.Series) -> pd.Series:
    return path_s.astype(str).str.extract(r"(vglive-sk-\d+)", expand=False)


DEVICE_SUFFIX_TOKENS = {
    "firetv", "firestick", "fireos",
    "androidtv", "android",
    "web", "webos",
    "lg", "lgtv",
    "apple", "appletv", "ios", "iphone", "ipad",
    "samsung", "samsungtv", "tizen",
    "roku", "mi", "mitv", "xiaomi",
    "sony", "bravia",
    "id", "tv", "mobile", "phone", "tablet",
}


def _norm_token(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _expand_device_tokens(*values) -> set:
    expanded = set(DEVICE_SUFFIX_TOKENS)
    for val in values:
        tok = _norm_token(val)
        if not tok:
            continue
        expanded.add(tok)
        if tok.endswith("tv") and len(tok) > 2:
            expanded.add(tok[:-2])
        if tok == "androidtv":
            expanded.add("android")
        if tok == "appletv":
            expanded.add("apple")
        if tok == "lgtv":
            expanded.add("lg")
        if tok == "samsungtv":
            expanded.add("samsung")
        if tok == "firestick":
            expanded.add("firetv")
    return expanded


def normalize_channel_name_smart(channel_raw: str, platform: str = "", device_name: str = "") -> str:
    """Return pure channel name by removing trailing device/platform token after underscore.

    Examples:
    - IndiaTV_FireTV -> IndiaTV
    - B4U_Music_AndroidTV -> B4U_Music
    - Ndtv_India -> Ndtv_India
    - 9XM_Jalwa -> 9XM_Jalwa
    """
    if pd.isna(channel_raw):
        return "Unknown"
    raw = str(channel_raw).strip()
    if raw.lower() in {"", "nan", "none", "null", "(null)"}:
        return "Unknown"
    raw = re.sub(r"\s*_\s*", "_", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    clean = raw
    tokens = _expand_device_tokens(platform, device_name)
    while "_" in clean:
        base, suffix = clean.rsplit("_", 1)
        suffix_norm = _norm_token(suffix)
        if suffix_norm in tokens:
            clean = base.strip("_ ").strip()
            continue
        break
    return clean or "Unknown"

def ub_extract_quality(path_s: pd.Series) -> pd.Series:
    s = path_s.fillna("").astype(str)
    quality = s.str.extract(r"(\d+p)", expand=False)
    variant = s.str.extract(r"main_(\d+)\.m3u8", expand=False)
    out = quality.copy()
    out = out.where(out.notna(), "variant_" + variant.astype(str))
    out = out.where(~s.str.contains("main.m3u8", na=False), "master")
    out = out.where(~s.str.contains(r"\.ts", na=False), "segment")
    out = out.fillna("unknown").replace("variant_nan", "unknown")
    return out


def ub_infer_device_type(ua_s: pd.Series, platform_s: pd.Series) -> pd.Series:
    ua = ua_s.fillna("").astype(str).str.lower()
    platform = platform_s.fillna("").astype(str).str.lower()
    out = pd.Series("Other", index=ua.index)
    tv_mask = (
        platform.str.contains("android_tv", na=False)
        | ua.str.contains("smarttv|hismarttv|bravia", na=False)
        | ua.str.contains(r"\btv\b", na=False)
    )
    out = out.mask(tv_mask, "Smart TV")
    out = out.mask(~tv_mask & ua.str.contains("android", na=False), "Android")
    out = out.mask(ua.str.contains("iphone", na=False), "iPhone")
    out = out.mask(ua.str.contains("ipad", na=False), "iPad")
    out = out.mask(ua.str.contains("windows", na=False), "Windows")
    out = out.mask(ua.str.contains("mac", na=False), "Mac")
    return out


def ub_build_sessions(df: pd.DataFrame, gap_minutes: int = 20) -> pd.DataFrame:
    df = df.sort_values(["device_id", "event_time"]).copy()
    grp = df.groupby("device_id", sort=False)
    df["prev_time"] = grp["event_time"].shift(1)
    df["gap_min"]   = (df["event_time"] - df["prev_time"]).dt.total_seconds() / 60
    first_row = df["prev_time"].isna()
    long_gap  = df["gap_min"] > gap_minutes

    if "session_id" in df.columns:
        prev_sid = grp["session_id"].shift(1)
        sid_changed = df["session_id"].fillna("__x__") != prev_sid.fillna("__x__")
    else:
        sid_changed = pd.Series(False, index=df.index)

    df["prev_channel"] = grp["channel_name"].shift(1)
    ch_changed = df["channel_name"].fillna("__x__") != df["prev_channel"].fillna("__x__")

    ip_changed  = pd.Series(False, index=df.index)
    asn_changed = pd.Series(False, index=df.index)
    if "cliIP" in df.columns:
        df["prev_ip"] = grp["cliIP"].shift(1)
        ip_changed = df["cliIP"].astype(str).fillna("__x__") != df["prev_ip"].astype(str).fillna("__x__")
    if "asn" in df.columns:
        df["prev_asn"] = grp["asn"].shift(1)
        asn_changed = df["asn"].astype(str).fillna("__x__") != df["prev_asn"].astype(str).fillna("__x__")

    df["prev_platform"] = grp["platform"].shift(1)
    plat_changed = df["platform"].fillna("__x__") != df["prev_platform"].fillna("__x__")

    ctx_score = ch_changed.astype(int) + ip_changed.astype(int) + asn_changed.astype(int) + plat_changed.astype(int)
    smart_break = (df["gap_min"] > 8) & (ctx_score >= 2)

    df["new_session"] = first_row | long_gap | sid_changed | smart_break
    df["session_no"]  = grp["new_session"].cumsum()
    df["session_key"] = df["device_id"].astype(str) + "_S" + df["session_no"].astype(str)
    return df


def ub_estimate_watch_minutes(df: pd.DataFrame, cap_seconds: int = WATCH_GAP_CAP_SECONDS) -> pd.DataFrame:
    df = df.sort_values(["device_id", "event_time"]).copy()
    next_t = df.groupby("device_id", sort=False)["event_time"].shift(-1)
    gap = (next_t - df["event_time"]).dt.total_seconds()
    df["watch_gap_sec_est"] = gap.clip(lower=0, upper=cap_seconds).fillna(0)
    df["watch_min_est"]     = df["watch_gap_sec_est"] / 60
    return df


def ub_session_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("session_key", sort=False)
        .agg(
            device_id       = ("device_id",    "first"),
            start           = ("event_time",   "min"),
            end             = ("event_time",   "max"),
            requests        = ("session_key",  "size"),
            unique_paths    = ("reqPath",      "nunique"),
            unique_channels = ("channel_name", "nunique"),
            est_watch_min   = ("watch_min_est","sum"),
            top_channel     = ("channel_name", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
            top_title       = ("content_label",lambda s: s.mode().iloc[0] if not s.mode().empty else None),
        )
        .reset_index()
    )
    out["session_duration_min"] = (out["end"] - out["start"]).dt.total_seconds() / 60
    return out.sort_values("start").reset_index(drop=True)



def ub_lookup_geo(ips: list) -> dict:
    """
    Batch-lookup IP addresses via ip-api.com (free, no key, 100/req).
    Returns dict: ip -> {country, region, city, isp, org, asn_name, lat, lon}
    Falls back gracefully if offline or rate-limited.
    """
    import requests as _req
    unique_ips = [ip for ip in set(ips)
                  if ip and str(ip) not in ("", "nan", "None", "0.0.0.0")]
    if not unique_ips:
        return {}

    result = {}
    batch_size = 100
    fields = "status,query,country,regionName,city,zip,isp,org,as,lat,lon,timezone"

    for i in range(0, len(unique_ips), batch_size):
        batch = unique_ips[i:i + batch_size]
        try:
            resp = _req.post(
                "http://ip-api.com/batch",
                json=batch,
                params={"fields": fields},
                timeout=10,
            )
            if resp.status_code == 200:
                for rec in resp.json():
                    if rec.get("status") == "success":
                        result[rec["query"]] = {
                            "country":  rec.get("country", ""),
                            "region":   rec.get("regionName", ""),
                            "city":     rec.get("city", ""),
                            "zip":      rec.get("zip", ""),
                            "isp":      rec.get("isp", ""),
                            "org":      rec.get("org", ""),
                            "asn_name": rec.get("as", ""),
                            "lat":      rec.get("lat", ""),
                            "lon":      rec.get("lon", ""),
                            "timezone": rec.get("timezone", ""),
                        }
        except Exception:
            pass  # offline or blocked — fail silently

    return result


def ub_format_geo_summary(geo: dict) -> dict:
    """
    Given geo dict for a single IP, return clean display strings.
    """
    if not geo:
        return {}
    parts = [p for p in [geo.get("city"), geo.get("region"), geo.get("country")] if p]
    return {
        "location":  ", ".join(parts) if parts else "Unknown",
        "country":   geo.get("country", ""),
        "region":    geo.get("region", ""),
        "city":      geo.get("city", ""),
        "isp":       geo.get("isp", "") or geo.get("org", ""),
        "asn_name":  geo.get("asn_name", ""),
        "timezone":  geo.get("timezone", ""),
        "lat":       geo.get("lat", ""),
        "lon":       geo.get("lon", ""),
    }


@st.cache_resource
def get_db_conn():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    return con


@st.cache_data(show_spinner="Scanning device IDs ...", ttl=1800)
def ub_get_device_ids(parquet_glob: str, qs_col: str) -> list:
    con = get_db_conn()
    query = f"""
    SELECT DISTINCT regexp_extract({qs_col}, '(?:^|&)device_id=([^&]+)', 1) AS device_id
    FROM dataset
    WHERE {qs_col} IS NOT NULL
      AND regexp_extract({qs_col}, '(?:^|&)device_id=([^&]+)', 1) <> ''
    ORDER BY 1
    """
    try:
        df = con.execute(query).df()
        return df["device_id"].astype(str).tolist()
    except Exception as e:
        return []


@st.cache_data(show_spinner="Loading device data ...", ttl=600)
def ub_load_device(parquet_glob: str, device_id: str, start_date: str, end_date: str, col_map: dict) -> pd.DataFrame:
    con  = get_db_conn()
    qs   = _cm(col_map, "queryStr")
    ts   = _cm(col_map, "reqTimeSec")
    path = _cm(col_map, "reqPath")
    ua   = _cm(col_map, "UA")
    ip   = _cm(col_map, "cliIP")
    asn  = _cm(col_map, "asn")
    sc   = _cm(col_map, "statusCode")
    tt   = _cm(col_map, "transferTimeMSec")
    dt   = _cm(col_map, "downloadTime")

    def sel(actual, alias):
        if actual and actual != alias:
            return f"{actual} AS {alias}"
        return actual if actual else f"NULL AS {alias}"

    # Only select cols that differ from None
    extra_sels = ", ".join([
        sel(ip,  "cliIP"),
        sel(asn, "asn"),
        sel(sc,  "statusCode"),
        sel(tt,  "transferTimeMSec"),
        sel(dt,  "downloadTime"),
    ])

    query = f"""
    SELECT
        {qs}            AS queryStr,
        {ts}            AS reqTimeSec,
        {path}          AS reqPath,
        {ua}            AS UA,
        {extra_sels},
        regexp_extract({qs}, '(?:^|&)device_id=([^&]+)',      1) AS device_id,
        regexp_extract({qs}, '(?:^|&)session_id=([^&]+)',     1) AS session_id,
        regexp_extract({qs}, '(?:^|&)channel=([^&]+)',        1) AS channel_param,
        regexp_extract({qs}, '(?:^|&)content_type=([^&]+)',   1) AS content_type,
        regexp_extract({qs}, '(?:^|&)content_title=([^&]+)',  1) AS content_title,
        regexp_extract({qs}, '(?:^|&)platform=([^&]+)',       1) AS platform,
        regexp_extract({qs}, '(?:^|&)device=([^&]+)',         1) AS device_name_qs,
        regexp_extract({qs}, '(?:^|&)category_name=([^&]+)',  1) AS category_name
    FROM dataset
    WHERE regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1) = ?
      AND to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE BETWEEN ? AND ?
    ORDER BY TRY_CAST({ts} AS BIGINT)
    """
    try:
        return con.execute(query, [device_id, start_date, end_date]).df()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def ub_enrich(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["reqTimeSec"] = pd.to_numeric(out["reqTimeSec"], errors="coerce")
    out["event_time"] = pd.to_datetime(out["reqTimeSec"], unit="s", errors="coerce")
    out = out.dropna(subset=["event_time"]).copy()

    for col in ["channel_param","content_type","content_title","platform","device_name_qs","category_name","device_id","session_id"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).map(unquote_plus)

    if "reqPath" in out.columns:
        out["channel_from_path"] = ub_extract_channel_from_path(out["reqPath"])
        out["quality"]           = ub_extract_quality(out["reqPath"])
    else:
        out["channel_from_path"] = ""
        out["quality"]           = "unknown"

    out["channel_name_raw"] = out["channel_param"].replace("", pd.NA).fillna(out.get("channel_from_path", pd.NA)).fillna("Unknown")
    out["channel_name"] = out.apply(
        lambda r: normalize_channel_name_smart(
            r.get("channel_name_raw", ""),
            r.get("platform", ""),
            r.get("device_name_qs", "")
        ),
        axis=1
    ).replace("", "Unknown")
    out["content_label"] = out["content_title"].replace("", pd.NA).fillna(out["channel_name"]).fillna(out.get("reqPath",""))

    ua_col = out["UA"] if "UA" in out.columns else pd.Series("", index=out.index)
    out["device_type"] = ub_infer_device_type(ua_col, out["platform"])

    for c in ["transferTimeMSec","downloadTime","statusCode","asn"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["event_date"]     = out["event_time"].dt.date
    out["time_only"]      = out["event_time"].dt.time
    out["watch_date"]     = out["event_date"]
    out["watch_time_min"] = out["event_time"].dt.hour * 60 + out["event_time"].dt.minute + out["event_time"].dt.second / 60
    return out.sort_values(["device_id","event_time"]).reset_index(drop=True)




def ub_get_device_date_range(parquet_glob: str, device_id: str, col_map: dict):
    con = get_db_conn()
    qs = _cm(col_map, "queryStr")
    ts = _cm(col_map, "reqTimeSec")
    query = f"""
    SELECT
        MIN(to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE) AS min_date,
        MAX(to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE) AS max_date
    FROM dataset
    WHERE regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1) = ?
    """
    try:
        row = con.execute(query, [device_id]).fetchone()
        return row if row else (None, None)
    except Exception:
        return (None, None)


def ub_fig_to_png_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def ub_build_pdf_report(window_df: pd.DataFrame, sess_df: pd.DataFrame, content_window: pd.DataFrame,
                        selected_device: str, start_date, end_date, top_channel: str,
                        top_content: str, est_watch_hrs, n_sessions: int) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, rightMargin=24, leftMargin=24, topMargin=24, bottomMargin=24)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"User Behavior Report - {selected_device}", styles["Title"]))
    story.append(Paragraph(f"Date range: {start_date} to {end_date}", styles["Normal"]))
    story.append(Spacer(1, 10))

    summary_data = [
        ["Metric", "Value"],
        ["Rows", f"{len(window_df):,}"],
        ["Sessions", f"{n_sessions:,}"],
        ["Estimated Watch Hours", str(est_watch_hrs)],
        ["Top Channel", str(top_channel)],
        ["Top Content", str(top_content)],
    ]
    summary_tbl = Table(summary_data, colWidths=[2.5 * inch, 3.5 * inch])
    summary_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))
    story.append(summary_tbl)
    story.append(Spacer(1, 12))

    if not window_df.empty:
        # Activity chart
        min_act = (window_df.set_index("event_time").resample("1min").size().rename("requests").reset_index())
        fig = plt.figure(figsize=(8, 3))
        plt.plot(min_act["event_time"], min_act["requests"])
        plt.title("Minute-by-minute activity")
        plt.xlabel("Time")
        plt.ylabel("Requests")
        plt.xticks(rotation=30)
        plt.tight_layout()
        story.append(Paragraph("Activity", styles["Heading2"]))
        story.append(Image(io.BytesIO(ub_fig_to_png_bytes(fig)), width=7.2 * inch, height=2.7 * inch))
        story.append(Spacer(1, 10))

        # Top channels chart
        ch = window_df["channel_name"].value_counts().head(10).sort_values(ascending=True)
        if not ch.empty:
            fig = plt.figure(figsize=(8, 3.5))
            plt.barh(ch.index.astype(str), ch.values)
            plt.title("Top Channels by Requests")
            plt.xlabel("Requests")
            plt.tight_layout()
            story.append(Paragraph("Top Channels", styles["Heading2"]))
            story.append(Image(io.BytesIO(ub_fig_to_png_bytes(fig)), width=7.2 * inch, height=3.0 * inch))
            story.append(Spacer(1, 10))

    if not content_window.empty:
        top_content_df = content_window.head(10).copy()
        fig = plt.figure(figsize=(8, 4))
        plt.barh(top_content_df["content_label"].astype(str)[::-1], top_content_df["est_watch_min"][::-1])
        plt.title("Top Content by Estimated Watch Minutes")
        plt.xlabel("Estimated Watch Minutes")
        plt.tight_layout()
        story.append(Paragraph("Top Content", styles["Heading2"]))
        story.append(Image(io.BytesIO(ub_fig_to_png_bytes(fig)), width=7.2 * inch, height=3.2 * inch))
        story.append(Spacer(1, 10))

        cols = [c for c in ["content_label", "channel_name", "requests", "est_watch_min", "sessions"] if c in top_content_df.columns]
        tbl_data = [cols] + top_content_df[cols].astype(str).values.tolist()
        tbl = Table(tbl_data, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]))
        story.append(Paragraph("Content Summary", styles["Heading2"]))
        story.append(tbl)
        story.append(Spacer(1, 10))

    if not sess_df.empty:
        story.append(PageBreak())
        top_sess = sess_df.sort_values("est_watch_min", ascending=False).head(15).copy()
        cols = [c for c in ["session_key", "start", "end", "requests", "unique_channels", "est_watch_min", "top_channel"] if c in top_sess.columns]
        tbl_data = [cols] + top_sess[cols].astype(str).values.tolist()
        tbl = Table(tbl_data, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]))
        story.append(Paragraph("Session Summary", styles["Heading2"]))
        story.append(tbl)

    doc.build(story)
    return buf.getvalue()




@st.cache_data(show_spinner="Loading global behavior coverage ...", ttl=900)
def gb_get_coverage(parquet_glob: list, col_map: dict, start_date: str, end_date: str) -> dict:
    con = get_db_conn()
    qs   = _cm(col_map, "queryStr")
    ts   = _cm(col_map, "reqTimeSec")
    query = f"""
    WITH base AS (
        SELECT {qs} AS queryStr, TRY_CAST({ts} AS BIGINT) AS req_ts
        FROM dataset
        WHERE to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE BETWEEN ? AND ?
    )
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN queryStr IS NOT NULL AND queryStr LIKE '%session_id=%' THEN 1 ELSE 0 END) AS behavior_rows,
        COUNT(DISTINCT CASE WHEN queryStr IS NOT NULL AND queryStr LIKE '%session_id=%'
              THEN regexp_extract(queryStr, '(?:^|&)session_id=([^&]+)', 1) END) AS sessions,
        COUNT(DISTINCT CASE WHEN queryStr IS NOT NULL AND queryStr LIKE '%session_id=%'
              THEN regexp_extract(queryStr, '(?:^|&)device_id=([^&]+)', 1) END) AS devices
    FROM base
    """
    row = con.execute(query, [start_date, end_date]).fetchone()
    total_rows, behavior_rows, sessions, devices = row
    coverage_pct = (behavior_rows / total_rows * 100) if total_rows else 0
    return {
        "total_rows": int(total_rows or 0),
        "behavior_rows": int(behavior_rows or 0),
        "coverage_pct": coverage_pct,
        "sessions": int(sessions or 0),
        "devices": int(devices or 0),
    }



@st.cache_data(show_spinner="Loading pure channel master ...", ttl=900)
def gb_get_channel_master_smart(parquet_glob: list, col_map: dict, start_date: str, end_date: str, top_n: int = 5000) -> pd.DataFrame:
    """Get raw channel/platform/device combos, then clean channels in Python exactly like User Behavior."""
    con = get_db_conn()
    qs = _cm(col_map, "queryStr")
    ts = _cm(col_map, "reqTimeSec")
    path = _cm(col_map, "reqPath")
    query = f"""
    SELECT
        COALESCE(NULLIF(regexp_extract({qs}, '(?:^|&)channel=([^&]+)', 1), ''),
                 NULLIF(regexp_extract({path}, '(vglive-sk-\d+)', 1), ''),
                 'Unknown') AS raw_channel,
        regexp_extract({qs}, '(?:^|&)platform=([^&]+)', 1) AS platform,
        regexp_extract({qs}, '(?:^|&)device=([^&]+)', 1) AS device_name,
        COUNT(*) AS requests,
        COUNT(DISTINCT regexp_extract({qs}, '(?:^|&)session_id=([^&]+)', 1)) AS sessions,
        COUNT(DISTINCT regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1)) AS devices
    FROM dataset
    WHERE {qs} IS NOT NULL
      AND {qs} LIKE '%session_id=%'
      AND to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY 1,2,3
    ORDER BY requests DESC
    LIMIT {int(top_n)}
    """
    df = con.execute(query).df()
    if df.empty:
        return df
    for c in ["raw_channel", "platform", "device_name"]:
        df[c] = df[c].fillna("").astype(str).map(unquote_plus)
    df["clean_channel"] = df.apply(
        lambda r: normalize_channel_name_smart(r.get("raw_channel", ""), r.get("platform", ""), r.get("device_name", "")),
        axis=1,
    )
    return (
        df.groupby(["clean_channel", "raw_channel", "platform", "device_name"], dropna=False)
          .agg(requests=("requests", "sum"), sessions=("sessions", "sum"), devices=("devices", "sum"))
          .reset_index()
          .sort_values(["requests", "sessions"], ascending=False)
    )

def gb_behavior_cte(parquet_glob: list, col_map: dict, start_date: str, end_date: str, daypart_mode: str = "Default") -> str:
    qs   = _cm(col_map, "queryStr")
    ts   = _cm(col_map, "reqTimeSec")
    path = _cm(col_map, "reqPath")
    ua   = _cm(col_map, "UA")

    if daypart_mode == "TV Style":
        daypart_case = """
            CASE
                WHEN EXTRACT('hour' FROM event_time) BETWEEN 5 AND 9 THEN 'Morning'
                WHEN EXTRACT('hour' FROM event_time) BETWEEN 10 AND 16 THEN 'Daytime'
                WHEN EXTRACT('hour' FROM event_time) BETWEEN 17 AND 21 THEN 'Evening'
                ELSE 'Late Night'
            END
        """
    else:
        daypart_case = """
            CASE
                WHEN EXTRACT('hour' FROM event_time) BETWEEN 5 AND 11 THEN 'Morning'
                WHEN EXTRACT('hour' FROM event_time) BETWEEN 12 AND 16 THEN 'Afternoon'
                WHEN EXTRACT('hour' FROM event_time) BETWEEN 17 AND 21 THEN 'Evening'
                ELSE 'Night'
            END
        """

    return f"""
    WITH behavior AS (
        SELECT
            to_timestamp(TRY_CAST({ts} AS BIGINT)) AS event_time,
            TRY_CAST({ts} AS BIGINT) AS reqTimeSec,
            {path} AS reqPath,
            {qs}   AS queryStr,
            {ua}   AS UA,
            regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1)      AS device_id,
            regexp_extract({qs}, '(?:^|&)session_id=([^&]+)', 1)     AS session_id,
            regexp_extract({qs}, '(?:^|&)channel=([^&]+)', 1)        AS channel_qs,
            regexp_extract({qs}, '(?:^|&)content_title=([^&]+)', 1)  AS content_title_qs,
            regexp_extract({qs}, '(?:^|&)platform=([^&]+)', 1)       AS platform,
            regexp_extract({qs}, '(?:^|&)device=([^&]+)', 1)         AS device_name_qs,
            regexp_extract({qs}, '(?:^|&)category_name=([^&]+)', 1)  AS category_name
        FROM dataset
        WHERE {qs} IS NOT NULL
          AND {qs} LIKE '%session_id=%'
          AND to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    ),
    enriched AS (
        SELECT
            *,
            COALESCE(NULLIF(channel_qs, ''), NULLIF(regexp_extract(reqPath, '(vglive-sk-\d+)', 1), ''), 'Unknown') AS channel_name_raw,
            regexp_replace(regexp_replace(COALESCE(NULLIF(channel_qs, ''), NULLIF(regexp_extract(reqPath, '(vglive-sk-\d+)', 1), ''), 'Unknown'), '\\s*_\\s*', '_', 'g'), '_(firetv|firestick|fireos|androidtv|android|web|webos|lg|lgtv|apple|appletv|ios|iphone|ipad|samsung|samsungtv|tizen|roku|mi|mitv|xiaomi|sony|bravia|id|tv|mobile|phone|tablet)$', '', 'i') AS channel_name,
            COALESCE(NULLIF(content_title_qs, ''), NULLIF(channel_qs, ''), NULLIF(regexp_extract(reqPath, '(vglive-sk-\d+)', 1), ''), reqPath) AS content_label,
            {daypart_case} AS day_part,
            CASE
                WHEN lower(coalesce(platform, '')) LIKE '%android_tv%' OR lower(coalesce(UA, '')) ~ 'smarttv|hismarttv|bravia|\btv\b' THEN 'Smart TV'
                WHEN lower(coalesce(UA, '')) LIKE '%android%' THEN 'Android'
                WHEN lower(coalesce(UA, '')) LIKE '%iphone%' THEN 'iPhone'
                WHEN lower(coalesce(UA, '')) LIKE '%ipad%' THEN 'iPad'
                WHEN lower(coalesce(UA, '')) LIKE '%windows%' THEN 'Windows'
                WHEN lower(coalesce(UA, '')) LIKE '%mac%' THEN 'Mac'
                ELSE 'Other'
            END AS device_type
        FROM behavior
        WHERE session_id <> '' AND device_id <> ''
    )
    """


@st.cache_data(show_spinner="Loading global behavior day-part data ...", ttl=900)
def gb_get_daypart_top(parquet_glob: list, col_map: dict, start_date: str, end_date: str, entity: str, top_n: int, daypart_mode: str = "Default", channel_mode: str = "Clean") -> pd.DataFrame:
    con = get_db_conn()
    entity_expr = ("channel_name_raw" if channel_mode == "Raw" else "channel_name") if entity == "Channel" else "content_label"
    query = gb_behavior_cte(parquet_glob, col_map, start_date, end_date, daypart_mode) + f"""
    SELECT *
    FROM (
        SELECT
            day_part,
            {entity_expr} AS entity_name,
            COUNT(*) AS requests,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT device_id) AS devices,
            ROW_NUMBER() OVER (PARTITION BY day_part ORDER BY COUNT(DISTINCT session_id) DESC, COUNT(*) DESC) AS rn
        FROM enriched
        GROUP BY 1, 2
    ) q
    WHERE rn <= {int(top_n)}
    ORDER BY day_part, rn
    """
    return con.execute(query).df()


@st.cache_data(show_spinner="Loading global stickiness data ...", ttl=900)
def gb_get_stickiness(parquet_glob: list, col_map: dict, start_date: str, end_date: str, entity: str, top_n: int, daypart_mode: str = "Default", channel_mode: str = "Clean") -> pd.DataFrame:
    con = get_db_conn()
    entity_expr = ("channel_name_raw" if channel_mode == "Raw" else "channel_name") if entity == "Channel" else "content_label"
    query = gb_behavior_cte(parquet_glob, col_map, start_date, end_date, daypart_mode) + f"""
    , seq AS (
        SELECT
            *,
            LEAD(event_time) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS next_event_time,
            LEAD({entity_expr}) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS next_entity
        FROM enriched
    )
    SELECT
        {entity_expr} AS entity_name,
        COUNT(*) AS requests,
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(DISTINCT device_id) AS devices,
        ROUND(SUM(LEAST(GREATEST(date_diff('second', event_time, next_event_time), 0), {WATCH_GAP_CAP_SECONDS})) / 60.0, 2) AS watch_min,
        ROUND(SUM(LEAST(GREATEST(date_diff('second', event_time, next_event_time), 0), {WATCH_GAP_CAP_SECONDS})) / NULLIF(COUNT(*), 0) / 60.0, 3) AS avg_watch_per_request,
        ROUND(SUM(LEAST(GREATEST(date_diff('second', event_time, next_event_time), 0), {WATCH_GAP_CAP_SECONDS})) / NULLIF(COUNT(DISTINCT session_id), 0) / 60.0, 2) AS avg_watch_per_session,
        ROUND(100.0 * AVG(CASE WHEN next_entity IS NOT NULL AND next_entity <> {entity_expr} THEN 1 ELSE 0 END), 2) AS switch_away_pct
    FROM seq
    GROUP BY 1
    HAVING COUNT(DISTINCT session_id) >= 3
    ORDER BY sessions DESC, watch_min DESC
    LIMIT {int(top_n)}
    """
    df = con.execute(query).df()
    if not df.empty:
        req_med = float(df["requests"].median()) if len(df) else 0
        wpr_med = float(df["avg_watch_per_request"].median()) if len(df) else 0
        def classify(r):
            if r["requests"] >= req_med and r["avg_watch_per_request"] < wpr_med:
                return "High traffic, low retention"
            if r["requests"] >= req_med and r["avg_watch_per_request"] >= wpr_med:
                return "Strong"
            if r["requests"] < req_med and r["avg_watch_per_request"] >= wpr_med:
                return "Niche but sticky"
            return "Low traction"
        df["label"] = df.apply(classify, axis=1)
    return df


@st.cache_data(show_spinner="Loading global retention data ...", ttl=900)
def gb_get_retention_buckets(parquet_glob: list, col_map: dict, start_date: str, end_date: str, entity: str, top_n: int, daypart_mode: str = "Default", channel_mode: str = "Clean") -> pd.DataFrame:
    con = get_db_conn()
    entity_expr = ("channel_name_raw" if channel_mode == "Raw" else "channel_name") if entity == "Channel" else "content_label"
    query = gb_behavior_cte(parquet_glob, col_map, start_date, end_date, daypart_mode) + f"""
    , seq AS (
        SELECT
            *,
            LEAD(event_time) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS next_event_time
        FROM enriched
    ),
    watches AS (
        SELECT
            session_id,
            device_id,
            {entity_expr} AS entity_name,
            SUM(LEAST(GREATEST(date_diff('second', event_time, next_event_time), 0), {WATCH_GAP_CAP_SECONDS})) / 60.0 AS watch_min
        FROM seq
        GROUP BY 1,2,3
    ),
    bucketed AS (
        SELECT
            entity_name,
            CASE
                WHEN watch_min < 1 THEN 'Bounce (<1m)'
                WHEN watch_min < 5 THEN 'Short (1-5m)'
                WHEN watch_min < 15 THEN 'Medium (5-15m)'
                ELSE 'Long (15m+)'
            END AS watch_bucket,
            COUNT(*) AS sessions
        FROM watches
        GROUP BY 1,2
    )
    SELECT *
    FROM bucketed
    WHERE entity_name IN (
        SELECT entity_name
        FROM bucketed
        GROUP BY 1
        ORDER BY SUM(sessions) DESC
        LIMIT {int(top_n)}
    )
    ORDER BY entity_name, watch_bucket
    """
    return con.execute(query).df()


@st.cache_data(show_spinner="Loading switching behavior data ...", ttl=900)
def gb_get_switching(parquet_glob: list, col_map: dict, start_date: str, end_date: str, top_n: int, daypart_mode: str = "Default", channel_mode: str = "Clean") -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    con = get_db_conn()
    cte = gb_behavior_cte(parquet_glob, col_map, start_date, end_date, daypart_mode)
    channel_expr = "channel_name_raw" if channel_mode == "Raw" else "channel_name"
    trans_query = cte + f"""
    , seq AS (
        SELECT
            *,
            LEAD({channel_expr}) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS next_channel,
            LEAD(event_time) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS next_event_time
        FROM enriched
    )
    SELECT
        {channel_expr} AS from_channel,
        next_channel AS to_channel,
        COUNT(*) AS transitions,
        ROUND(AVG(GREATEST(date_diff('second', event_time, next_event_time), 0)), 2) AS avg_gap_sec
    FROM seq
    WHERE next_channel IS NOT NULL AND next_channel <> {channel_expr}
    GROUP BY 1,2
    ORDER BY transitions DESC
    LIMIT {int(top_n)}
    """
    rate_query = cte + f"""
    , seq AS (
        SELECT
            *,
            LEAD({channel_expr}) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS next_channel
        FROM enriched
    )
    SELECT
        {channel_expr} AS channel_name,
        COUNT(*) AS events,
        ROUND(100.0 * AVG(CASE WHEN next_channel IS NOT NULL AND next_channel <> {channel_expr} THEN 1 ELSE 0 END), 2) AS switch_away_pct
    FROM seq
    GROUP BY 1
    HAVING COUNT(*) >= 20
    ORDER BY switch_away_pct DESC, events DESC
    LIMIT 20
    """
    sess_query = cte + f"""
    , seq AS (
        SELECT
            *,
            LAG({channel_expr}) OVER (PARTITION BY device_id, session_id ORDER BY event_time) AS prev_channel
        FROM enriched
    ),
    session_switches AS (
        SELECT
            session_id,
            COUNT(*) FILTER (WHERE prev_channel IS NOT NULL AND prev_channel <> {channel_expr}) AS switches
        FROM seq
        GROUP BY 1
    )
    SELECT
        CASE
            WHEN switches = 0 THEN '0'
            WHEN switches = 1 THEN '1'
            WHEN switches BETWEEN 2 AND 3 THEN '2-3'
            ELSE '4+'
        END AS switch_bucket,
        COUNT(*) AS sessions
    FROM session_switches
    GROUP BY 1
    ORDER BY CASE switch_bucket WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2-3' THEN 2 ELSE 3 END
    """
    return con.execute(trans_query).df(), con.execute(rate_query).df(), con.execute(sess_query).df()

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

with st.sidebar:
    st.title("🗂️ Parquet Explorer")
    st.markdown("---")

    mode = st.radio(
        "Folder selection mode",
        ["🔍 Browse from root", "✏️ Type paths manually"],
        horizontal=False,
        key="folder_mode",
    )

    st.markdown("---")

    if mode == "🔍 Browse from root":
        st.markdown("**Step 1 — Enter a root folder**")
        st.caption("The app will scan it and show every subfolder that has .parquet files.")

        root_input = st.text_input(
            "Root folder",
            value=st.session_state.browser_root,
            placeholder=r"e.g. D:\data  or  /mnt/data",
            key="root_input",
        )

        root_changed = root_input.strip() != st.session_state.browser_root

        if st.button("🔍 Scan", type="primary", key="btn_scan") or (
            root_input.strip() and root_changed
        ):
            rp = Path(root_input.strip())
            if not rp.is_dir():
                st.error("Folder not found.")
            else:
                st.session_state.browser_root = root_input.strip()
                with st.spinner("Scanning ..."):
                    st.session_state.scan_results = scan_root(root_input.strip())
                if not st.session_state.scan_results:
                    st.warning("No subfolders with .parquet files found.")

        results = st.session_state.scan_results

        if results:
            st.markdown(f"**Step 2 — Pick folders** ({len(results)} found)")
            st.caption("Click to toggle. Selected folders are merged for analysis.")

            ca, cb = st.columns(2)
            with ca:
                if st.button("✅ Select all", key="sel_all"):
                    st.session_state.selected_folders = [r["path"] for r in results]
                    st.rerun()
            with cb:
                if st.button("🗑️ Clear all", key="clr_all"):
                    st.session_state.selected_folders = []
                    st.rerun()

            selected_set = set(st.session_state.selected_folders)

            for r in results:
                is_selected = r["path"] in selected_set
                icon  = "✅" if is_selected else "⬜"
                label = f"{icon}  {r['rel']}  ({r['n_files']:,} files)"
                if st.button(label, key=f"fld_{r['path']}"):
                    if is_selected:
                        st.session_state.selected_folders = [
                            x for x in st.session_state.selected_folders if x != r["path"]
                        ]
                    else:
                        if r["path"] not in st.session_state.selected_folders:
                            st.session_state.selected_folders.append(r["path"])
                    st.rerun()

            if st.session_state.selected_folders:
                st.markdown("---")
                st.success(f"{len(st.session_state.selected_folders)} folder(s) selected")

    else:
        st.markdown("**Type folder paths manually**")
        st.caption("One path per box. All must contain .parquet files.")

        if "manual_folders" not in st.session_state:
            st.session_state.manual_folders = [""]

        manual = st.session_state.manual_folders
        for i in range(len(manual)):
            manual[i] = st.text_input(
                f"Folder {i+1}",
                value=manual[i],
                placeholder=r"e.g. D:\data\parquet_jan",
                key=f"manual_{i}",
            )

        mc1, mc2 = st.columns(2)
        with mc1:
            if st.button("➕ Add", key="man_add"):
                st.session_state.manual_folders.append("")
                st.rerun()
        with mc2:
            if st.button("➖ Remove", key="man_rem") and len(manual) > 1:
                st.session_state.manual_folders.pop()
                st.rerun()

        valid = []
        for f in manual:
            f = f.strip()
            if not f:
                continue
            p = Path(f)
            if not p.is_dir():
                st.error(f"Not found: {f}")
            elif not list(p.glob("*.parquet")):
                st.warning(f"No .parquet files: {f}")
            else:
                valid.append(f)
        st.session_state.selected_folders = valid

    st.markdown("---")
    st.caption("Tabs:\n1. Columns\n2. Unique Values\n3. Filter & Export\n4. Query String Analyzer")


# ─────────────────────────────────────────────
# Resolve valid folders
# ─────────────────────────────────────────────

valid_folders = []
for f in st.session_state.selected_folders:
    f = str(f).strip()
    p = Path(f)
    if p.is_dir() and list(p.glob("*.parquet")):
        valid_folders.append(f)


# ─────────────────────────────────────────────
# Welcome screen
# ─────────────────────────────────────────────
if not valid_folders:
    st.title("Welcome to Parquet Explorer")
    st.info("👈  Use the sidebar to pick your Parquet folders.")
    st.markdown("""
    **Two ways to add folders:**
    - **Browse from root** — type a root path, click Scan, then click any folder to select it
    - **Type paths manually** — paste exact folder paths one by one

    **Features:**
    - 📋 Browse all columns and data types
    - 🔍 Count + explore unique values per column
    - 📥 Export unique values list as CSV
    - 🎯 Filter rows by multiple columns (AND logic)
    - 🔗 Dual-column filter (OR between two columns)
    - 👀 Preview matching rows
    - 💾 Export filtered data as CSV
    - 📂 Multiple folders merged transparently
    - 🔎 **Query String Analyzer** — parse URL query strings, track device IDs & session IDs
    """)
    st.stop()


# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────

folder_key             = "|".join(sorted(valid_folders))
columns, col_types     = load_schema(folder_key)
n_files, n_rows, n_fol = count_stats(folder_key)

if not columns:
    st.error("Could not read any schema from the selected folders.")
    st.stop()

st.title("🗂️ Parquet Explorer")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Folders",       f"{n_fol}")
m2.metric("Parquet files", f"{n_files:,}")
m3.metric("Total rows",    f"{n_rows:,}")
m4.metric("Columns",       f"{len(columns)}")

with st.expander(f"📂 {n_fol} active folder(s)", expanded=False):
    for f in valid_folders:
        fpath = Path(f)
        cnt   = len(list(fpath.glob("*.parquet")))
        st.markdown(f"- **{fpath.name}** &nbsp; `{f}` &nbsp; ({cnt:,} files)")

st.markdown("---")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 Columns",
    "🔍 Unique Values",
    "🎯 Filter & Export",
    "🔎 Query String Analyzer",
    "📺 User Behavior",
    "🌐 Global Behavior",
])


# ══════════════════════════════════════════════
# TAB 1 — Column browser
# ══════════════════════════════════════════════
with tab1:
    st.subheader("All Columns")
    st.caption("Union of all columns across selected folders.")

    search  = st.text_input("Search column name", placeholder="type to filter ...", key="col_search")
    cols_df = pd.DataFrame({
        "Column":    columns,
        "Data type": [col_types.get(c, "") for c in columns],
    })
    if search:
        cols_df = cols_df[cols_df["Column"].str.contains(search, case=False)]

    st.dataframe(cols_df, use_container_width=True, height=520, hide_index=True)


# ══════════════════════════════════════════════
# TAB 2 — Unique values
# ══════════════════════════════════════════════
with tab2:
    st.subheader("Unique Values Explorer")
    st.caption("Pick a column, count, then explore or export.")

    col_pick = st.selectbox("Column", options=columns, key="uv_col")

    if st.button("1  Count unique values", type="secondary", key="btn_count"):
        udf_all = unique_values(folder_key, col_pick)
        st.session_state[f"udf_{col_pick}"] = udf_all
        st.session_state[f"n_{col_pick}"]   = len(udf_all)

    total_unique = st.session_state.get(f"n_{col_pick}")
    udf_cached   = st.session_state.get(f"udf_{col_pick}")

    if total_unique is not None:
        st.success(f"**{total_unique:,}** unique values in `{col_pick}`")
        st.markdown("---")

        c_inp, c_show, c_exp = st.columns([2, 1, 1])
        with c_inp:
            show_n = st.number_input(
                f"How many to show (max {total_unique:,})",
                min_value=1,
                max_value=total_unique,
                value=min(total_unique, 200),
                step=1,
                key="uv_show_n",
            )
        with c_show:
            st.markdown("<br>", unsafe_allow_html=True)
            load_btn = st.button("2  Show values", type="primary", key="btn_show")
        with c_exp:
            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Export all CSV",
                data=udf_cached.to_csv(index=False).encode(),
                file_name=f"unique_{col_pick}.csv",
                mime="text/csv",
                key="btn_exp_unique",
            )

        if load_btn:
            st.session_state["uv_result"]    = udf_cached.head(int(show_n))
            st.session_state.last_unique_col  = col_pick
            st.session_state.last_unique_vals = st.session_state["uv_result"]["value"].tolist()

        udf_result = st.session_state.get("uv_result")
        if udf_result is not None and not udf_result.empty:
            st.caption(f"Showing top {len(udf_result):,} of {total_unique:,} unique values")

            uv_search  = st.text_input("Search within values", placeholder="filter the list ...", key="uv_search")
            display_df = udf_result
            if uv_search:
                display_df = udf_result[
                    udf_result["value"].astype(str).str.contains(uv_search, case=False, na=False)
                ]
                st.caption(f"{len(display_df):,} values match your search")

            col_l, col_r = st.columns([2, 1])
            with col_l:
                st.dataframe(display_df, use_container_width=True, height=420, hide_index=True)
            with col_r:
                top10 = display_df.head(10)
                if not top10.empty:
                    st.bar_chart(top10.set_index("value")["count"])

            st.markdown("---")
            st.markdown("#### Extract all rows for a specific value")
            st.caption(
                f"Pick one or more values from `{col_pick}` — get every matching row across all files as a CSV."
            )

            pick_vals = st.multiselect(
                f"Select value(s) from `{col_pick}`",
                options=display_df["value"].tolist(),
                placeholder="Click or type to pick values ...",
                key="extract_vals",
            )

            extract_cols = st.multiselect(
                "Columns to include in extracted CSV  (leave empty = all columns)",
                options=columns,
                default=[],
                key="extract_cols",
            )

            if pick_vals:
                st.info(
                    f"Will extract all rows where `{col_pick}` is one of: "
                    + ", ".join(f"**{v}**" for v in pick_vals[:5])
                    + (f" ... (+{len(pick_vals)-5} more)" if len(pick_vals) > 5 else "")
                )

                xc1, xc2 = st.columns([1, 1])
                with xc1:
                    preview_extract = st.button("👀 Preview rows", key="btn_extract_preview", type="secondary")
                with xc2:
                    export_extract  = st.button("💾 Export rows to CSV", key="btn_extract_export", type="primary")

                out_cols = extract_cols if extract_cols else columns

                if preview_extract or export_extract:
                    df_extracted = run_query(
                        folder_key,
                        out_cols,
                        filters={col_pick: pick_vals},
                        dual={},
                        max_rows=500 if preview_extract else None,
                    )

                    if df_extracted.empty:
                        st.warning("No rows found.")
                    elif preview_extract:
                        st.caption(f"Preview — first {len(df_extracted):,} rows")
                        st.dataframe(df_extracted, use_container_width=True, height=380)
                    else:
                        csv_bytes = df_extracted.to_csv(index=False).encode()
                        fname = f"rows_{col_pick}_{'_'.join(str(v)[:20] for v in pick_vals[:3])}.csv"
                        st.download_button(
                            label=f"📥 Download  ({len(df_extracted):,} rows)",
                            data=csv_bytes,
                            file_name=fname,
                            mime="text/csv",
                            key="dl_extract",
                        )
                        st.success(f"{len(df_extracted):,} rows ready to download.")


# ══════════════════════════════════════════════
# TAB 3 — Filter & Export
# ══════════════════════════════════════════════
with tab3:
    st.subheader("Filter & Export Data")
    st.caption("AND between columns, OR within a column's values.")

    sel_cols = st.multiselect(
        "Columns to include in output",
        options=columns,
        default=columns[:min(10, len(columns))],
        key="fe_cols",
    )

    st.markdown("---")

    filter_mode = st.radio(
        "Filter mode",
        ["Standard (AND per column)", "Dual filter (OR between two columns)"],
        horizontal=True,
        key="filter_mode",
    )

    filters: dict = {}
    dual:    dict = {}

    if filter_mode == "Standard (AND per column)":
        st.caption("Pick a column and one or more values. Rows must match ALL filters.")
        n_filters = st.number_input("Number of filters", 0, 6, 1, step=1, key="n_std")

        for i in range(int(n_filters)):
            fc1, fc2 = st.columns([2, 3])
            with fc1:
                fcol = st.selectbox(f"Filter {i+1} column", options=columns, key=f"std_col_{i}")
            with fc2:
                sug = (
                    st.session_state.last_unique_vals[:2000]
                    if st.session_state.last_unique_col == fcol else []
                )
                fvals = st.multiselect(
                    f"Filter {i+1} values (OR within)",
                    options=sug,
                    key=f"std_val_{i}",
                    placeholder="Select or type values ...",
                )
                if not sug:
                    st.caption("Tip: explore this column in Unique Values tab first to get suggestions here.")
            if fvals:
                filters[fcol] = fvals

    else:
        st.caption("Rows where **(Column A = value)  OR  (Column B = value)**.")

        da1, da2 = st.columns(2)
        with da1:
            st.markdown("**Column A**")
            dual_col_a  = st.selectbox("Column A", options=columns, key="dual_col_a")
            sug_a = st.session_state.last_unique_vals[:2000] if st.session_state.last_unique_col == dual_col_a else []
            dual_vals_a = st.multiselect("Values for A", options=sug_a, placeholder="Select or type ...", key="dual_val_a")
        with da2:
            st.markdown("**Column B**")
            dual_col_b  = st.selectbox("Column B", options=columns, key="dual_col_b")
            sug_b = st.session_state.last_unique_vals[:2000] if st.session_state.last_unique_col == dual_col_b else []
            dual_vals_b = st.multiselect("Values for B", options=sug_b, placeholder="Select or type ...", key="dual_val_b")

        dual = {"col_a": dual_col_a, "vals_a": dual_vals_a, "col_b": dual_col_b, "vals_b": dual_vals_b}

        st.markdown("**Optional extra AND filter**")
        extra_col = st.selectbox("Extra column (optional)", options=["(none)"] + columns, key="extra_col")
        if extra_col != "(none)":
            sug_e = st.session_state.last_unique_vals[:2000] if st.session_state.last_unique_col == extra_col else []
            extra_vals = st.multiselect("Extra values", options=sug_e, placeholder="Select or type ...", key="extra_vals")
            if extra_vals:
                filters[extra_col] = extra_vals

    st.markdown("---")

    n_preview = st.number_input(
        "Preview rows to show",
        min_value=10, max_value=10_000, value=200, step=10,
        key="n_preview",
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        run_preview = st.button("👀 Preview", type="primary", key="btn_preview")
    with col_b:
        run_export  = st.button("💾 Export full CSV", key="btn_export")

    if run_preview:
        if not sel_cols:
            st.warning("Select at least one column.")
        else:
            df = run_query(folder_key, sel_cols, filters, dual, max_rows=int(n_preview))
            if df.empty:
                st.warning("No rows match your filters.")
            else:
                st.caption(f"Showing {len(df):,} rows")
                st.dataframe(df, use_container_width=True, height=480)

    if run_export:
        if not sel_cols:
            st.warning("Select at least one column.")
        else:
            df_all = run_query(folder_key, sel_cols, filters, dual, max_rows=None)
            if df_all.empty:
                st.warning("No rows match your filters.")
            else:
                csv_bytes = df_all.to_csv(index=False).encode()
                st.download_button(
                    label=f"📥 Download CSV  ({len(df_all):,} rows)",
                    data=csv_bytes,
                    file_name="export.csv",
                    mime="text/csv",
                    key="dl_export",
                )
                st.success(f"Ready — {len(df_all):,} rows matched.")


# ══════════════════════════════════════════════
# TAB 4 — Query String Analyzer  (NEW)
# ══════════════════════════════════════════════
with tab4:
    st.subheader("🔎 Query String Analyzer")
    st.caption(
        "Pick a column that contains URL query strings (e.g. `queryStr`, `qrystr`). "
        "The app parses every value, extracts `device_id` and `session_id`, "
        "and lets you explore their relationships."
    )

    # ── Step 1: column selection ──────────────────────────────
    st.markdown("#### Step 1 — Select query string column")
    qsa_col = st.selectbox(
        "Column containing query strings",
        options=columns,
        key="qsa_col_select",
        help="Choose the column whose values look like: session_id=xxx&device_id=yyy&...",
    )

    # Source options
    qsa_source = st.radio(
        "Data source",
        [
            "Use Unique Values already scanned (Tab 2)  — fast, uses aggregated counts",
            "Read raw parquet rows directly  — slower, full row-level data",
        ],
        key="qsa_source",
    )

    use_cached_uv = "Unique Values" in qsa_source

    if use_cached_uv:
        cached_udf = st.session_state.get(f"udf_{qsa_col}")
        if cached_udf is None:
            st.info(
                f"No unique values cached for `{qsa_col}` yet. "
                "Go to **Tab 2 → Unique Values**, select this column, and click 'Count unique values' first. "
                "Or switch to 'Read raw parquet rows' above."
            )
            st.stop()

    if st.button("🔍 Parse & Analyze", type="primary", key="qsa_parse_btn"):
        _qbar = st.progress(0, text="Starting parse ...")
        _qph  = st.empty()
        _qbar.progress(10, text="📋 Reading cached values ...")
        if use_cached_uv:
            raw_df = st.session_state[f"udf_{qsa_col}"].rename(columns={"value": "_raw_val"})
            records = []
            total_qsa = len(raw_df)
            for qi, (_, row) in enumerate(raw_df.iterrows()):
                pct = 10 + int(qi / max(total_qsa, 1) * 70)
                if qi % 500 == 0:
                    _qbar.progress(pct, text=f"📋 Parsing query strings ... {qi:,} / {total_qsa:,}")
                raw_val = str(row["_raw_val"])
                cnt     = row.get("count", 1)
                try:
                    parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
                except Exception:
                    parsed = {}
                parsed["_raw"]   = raw_val
                parsed["_count"] = cnt
                records.append(parsed)
            _qbar.progress(85, text="🔧 Building DataFrame ...")
            parsed_df = pd.DataFrame(records).fillna("")
        else:
            _qbar.progress(20, text="📂 Reading raw parquet rows ...")
            parsed_df = load_qrystr_from_parquet(folder_key, qsa_col)

        _qbar.progress(90, text="🔍 Identifying keys ...")
        if parsed_df.empty:
            _qbar.empty()
            _qph.empty()
            st.warning("No data found. Check column selection.")
        else:
            meta_cols = {"_raw", "_count"}
            st.session_state.qsa_parsed_df = parsed_df
            st.session_state.qsa_column    = qsa_col
            st.session_state.qsa_keys = [c for c in parsed_df.columns if c not in meta_cols]
            _qbar.progress(100, text="✅ Parse complete!")
            time.sleep(0.3)
            _qbar.empty()
            _qph.empty()
            st.success(f"Parsed {len(parsed_df):,} unique values. Found keys: {', '.join(st.session_state.qsa_keys)}")

    # ── Results ──────────────────────────────────────────────
    parsed_df = st.session_state.get("qsa_parsed_df")
    qsa_keys  = st.session_state.get("qsa_keys", [])

    if parsed_df is not None and not parsed_df.empty:
        st.markdown("---")

        # ── Pure channel list from parsed query strings ──────────────────────────────
        if "channel" in parsed_df.columns:
            with st.expander("✅ Pure Channel List (cleaned from queryStr)", expanded=False):
                ch_src = parsed_df[parsed_df["channel"].astype(str).str.strip() != ""].copy()
                if not ch_src.empty:
                    if "platform" not in ch_src.columns:
                        ch_src["platform"] = ""
                    if "device" not in ch_src.columns:
                        ch_src["device"] = ""
                    ch_src["clean_channel"] = ch_src.apply(
                        lambda r: normalize_channel_name_smart(r.get("channel", ""), r.get("platform", ""), r.get("device", "")),
                        axis=1,
                    )
                    ch_master = (
                        ch_src.groupby(["clean_channel", "channel", "platform", "device"], dropna=False)["_count"]
                        .sum()
                        .reset_index()
                        .rename(columns={"channel": "raw_channel", "device": "device_name", "_count": "records"})
                        .sort_values("records", ascending=False)
                    )
                    ch_pure = (
                        ch_master.groupby("clean_channel", dropna=False)["records"]
                        .sum()
                        .reset_index()
                        .sort_values("records", ascending=False)
                    )
                    st.caption("Use this list for pure business channels. The normal Unique Values tab shows raw log values, so it will still show FireTV/AndroidTV suffixes.")
                    cpa, cpb = st.columns(2)
                    with cpa:
                        st.markdown("**Pure channels**")
                        st.dataframe(ch_pure, use_container_width=True, hide_index=True, height=300)
                        st.download_button(
                            "📥 Download pure channel list",
                            data=ch_pure.to_csv(index=False).encode("utf-8"),
                            file_name="pure_channel_list.csv",
                            mime="text/csv",
                            key="qsa_dl_pure_channels",
                        )
                    with cpb:
                        st.markdown("**Raw → clean mapping**")
                        st.dataframe(ch_master, use_container_width=True, hide_index=True, height=300)
                        st.download_button(
                            "📥 Download raw-to-clean mapping",
                            data=ch_master.to_csv(index=False).encode("utf-8"),
                            file_name="channel_raw_to_clean_mapping.csv",
                            mime="text/csv",
                            key="qsa_dl_channel_mapping",
                        )

        # ── Tabs within this tab ──────────────────────────────
        sub1, sub2, sub3, sub4 = st.tabs([
            "📊 Overview",
            "📱 Device ID Analysis",
            "🔗 Session ↔ Device Mapping",
            "🔍 Lookup by ID",
        ])

        # Helper: weighted unique count (respects _count)
        def weighted_nunique(df, col):
            if col not in df.columns:
                return 0
            sub = df[df[col].astype(str).str.strip() != ""]
            return sub[col].nunique()

        def weighted_total(df):
            return int(df["_count"].sum())

        # ── SUB-TAB 1: Overview ───────────────────────────────
        with sub1:
            st.markdown("#### Parsed Key Overview")
            st.caption("How many distinct non-empty values each key has across all records.")

            ov_rows = []
            for k in qsa_keys:
                non_empty = parsed_df[parsed_df[k].astype(str).str.strip() != ""]
                n_unique  = non_empty[k].nunique()
                n_filled  = int(non_empty["_count"].sum())
                n_total   = weighted_total(parsed_df)
                pct       = round(n_filled / n_total * 100, 1) if n_total else 0
                ov_rows.append({
                    "Key":          k,
                    "Unique values": n_unique,
                    "Filled rows":  n_filled,
                    "Fill %":       pct,
                })
            ov_df = pd.DataFrame(ov_rows).sort_values("Filled rows", ascending=False)
            st.dataframe(ov_df, use_container_width=True, hide_index=True)

            # Top values for any key
            st.markdown("---")
            st.markdown("#### Top values for a key")
            pick_key = st.selectbox("Select key", options=qsa_keys, key="ov_key_pick")
            if pick_key:
                top_df = (
                    parsed_df[parsed_df[pick_key].astype(str).str.strip() != ""]
                    .groupby(pick_key)["_count"]
                    .sum()
                    .reset_index()
                    .rename(columns={pick_key: "value", "_count": "count"})
                    .sort_values("count", ascending=False)
                    .head(50)
                )
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.dataframe(top_df, use_container_width=True, height=380, hide_index=True)
                with c2:
                    st.bar_chart(top_df.head(10).set_index("value")["count"])

        # ── SUB-TAB 2: Device ID Analysis ─────────────────────
        with sub2:
            st.markdown("#### Device ID Analysis")

            if "device_id" not in parsed_df.columns:
                st.warning("No `device_id` key found in parsed query strings.")
            else:
                dev_df = parsed_df[parsed_df["device_id"].astype(str).str.strip() != ""].copy()
                dev_df["device_id"] = dev_df["device_id"].astype(str)

                n_unique_devices = dev_df["device_id"].nunique()
                n_rows_with_dev  = int(dev_df["_count"].sum())
                n_total          = weighted_total(parsed_df)

                d1, d2, d3 = st.columns(3)
                d1.metric("Unique device IDs",   f"{n_unique_devices:,}")
                d2.metric("Records with device", f"{n_rows_with_dev:,}")
                d3.metric("Coverage",            f"{n_rows_with_dev/n_total*100:.1f}%" if n_total else "—")

                st.markdown("---")

                # Sessions per device
                if "session_id" in dev_df.columns:
                    dev_df["session_id"] = dev_df["session_id"].astype(str)
                    dev_sess = (
                        dev_df[dev_df["session_id"].str.strip() != ""]
                        .groupby("device_id")
                        .agg(
                            session_count  = ("session_id", "nunique"),
                            total_events   = ("_count", "sum"),
                        )
                        .reset_index()
                        .sort_values("session_count", ascending=False)
                    )

                    # Add platform/device_name if available
                    for extra_key in ["platform", "device"]:
                        if extra_key in dev_df.columns:
                            top_val = (
                                dev_df.groupby("device_id")[extra_key]
                                .agg(lambda x: x.mode()[0] if len(x) > 0 else "")
                            )
                            dev_sess[extra_key] = dev_sess["device_id"].map(top_val)

                    st.markdown("#### Devices ranked by session count")
                    st.caption("Each device_id, how many unique sessions it has, and total event weight.")

                    # Distribution
                    sess_bins = pd.cut(
                        dev_sess["session_count"],
                        bins=[0, 1, 5, 10, 25, 50, 100, dev_sess["session_count"].max() + 1],
                        labels=["1", "2–5", "6–10", "11–25", "26–50", "51–100", "100+"],
                    )
                    dist_df = sess_bins.value_counts().sort_index().reset_index()
                    dist_df.columns = ["Sessions per device", "Device count"]

                    c_left, c_right = st.columns([1, 2])
                    with c_left:
                        st.markdown("**Session count distribution**")
                        st.dataframe(dist_df, use_container_width=True, hide_index=True)
                    with c_right:
                        st.bar_chart(dist_df.set_index("Sessions per device")["Device count"])

                    st.markdown("---")
                    top_n_devs = st.slider("Show top N devices by session count", 10, 500, 50, key="top_n_devs")
                    st.dataframe(
                        dev_sess.head(top_n_devs),
                        use_container_width=True,
                        height=420,
                        hide_index=True,
                    )

                    st.download_button(
                        "📥 Export device summary CSV",
                        data=dev_sess.to_csv(index=False).encode(),
                        file_name="device_summary.csv",
                        mime="text/csv",
                        key="dl_dev_summary",
                    )
                else:
                    st.info("No `session_id` key found. Showing device ID frequency only.")
                    dev_freq = (
                        dev_df.groupby("device_id")["_count"]
                        .sum()
                        .reset_index()
                        .rename(columns={"_count": "event_count"})
                        .sort_values("event_count", ascending=False)
                    )
                    st.dataframe(dev_freq, use_container_width=True, height=420, hide_index=True)

        # ── SUB-TAB 3: Session ↔ Device Mapping ───────────────
        with sub3:
            st.markdown("#### Session ↔ Device Relationship")

            if "session_id" not in parsed_df.columns or "device_id" not in parsed_df.columns:
                st.warning("Both `session_id` and `device_id` keys are required for this analysis.")
            else:
                # Filter to rows with both filled
                both_df = parsed_df[
                    (parsed_df["session_id"].astype(str).str.strip() != "") &
                    (parsed_df["device_id"].astype(str).str.strip()  != "")
                ].copy()
                both_df["session_id"] = both_df["session_id"].astype(str)
                both_df["device_id"]  = both_df["device_id"].astype(str)

                b1, b2, b3 = st.columns(3)
                b1.metric("Records with both IDs", f"{int(both_df['_count'].sum()):,}")
                b2.metric("Unique sessions",        f"{both_df['session_id'].nunique():,}")
                b3.metric("Unique devices",         f"{both_df['device_id'].nunique():,}")

                st.markdown("---")

                # Sessions linked to multiple devices
                sess_dev_cnt = both_df.groupby("session_id")["device_id"].nunique().reset_index()
                sess_dev_cnt.columns = ["session_id", "n_devices"]
                multi_dev_sess = sess_dev_cnt[sess_dev_cnt["n_devices"] > 1].sort_values("n_devices", ascending=False)

                st.markdown("#### ⚠️ Sessions linked to multiple device IDs")
                st.caption("A session appearing on more than one device could indicate account sharing or data anomalies.")
                if multi_dev_sess.empty:
                    st.success("✅ No sessions are linked to more than one device ID.")
                else:
                    st.warning(f"{len(multi_dev_sess):,} sessions appear on multiple devices.")
                    st.dataframe(multi_dev_sess, use_container_width=True, height=300, hide_index=True)
                    st.download_button(
                        "📥 Export multi-device sessions CSV",
                        data=multi_dev_sess.to_csv(index=False).encode(),
                        file_name="multi_device_sessions.csv",
                        mime="text/csv",
                        key="dl_multi_dev",
                    )

                st.markdown("---")

                # Devices linked to multiple sessions
                dev_sess_cnt = both_df.groupby("device_id")["session_id"].nunique().reset_index()
                dev_sess_cnt.columns = ["device_id", "n_sessions"]
                multi_sess_dev = dev_sess_cnt[dev_sess_cnt["n_sessions"] > 1].sort_values("n_sessions", ascending=False)

                st.markdown("#### 📱 Devices linked to multiple sessions")
                st.caption("Normal — a device typically has many sessions over time.")
                if multi_sess_dev.empty:
                    st.info("No devices with multiple sessions found.")
                else:
                    st.info(f"{len(multi_sess_dev):,} devices have more than one session.")
                    top_n = st.slider("Show top N", 10, 500, 50, key="top_n_multi_sess")
                    st.dataframe(multi_sess_dev.head(top_n), use_container_width=True, height=300, hide_index=True)
                    st.download_button(
                        "📥 Export multi-session devices CSV",
                        data=multi_sess_dev.to_csv(index=False).encode(),
                        file_name="multi_session_devices.csv",
                        mime="text/csv",
                        key="dl_multi_sess",
                    )

                st.markdown("---")

                # Full mapping table
                st.markdown("#### Full session → device mapping")
                mapping = (
                    both_df.groupby(["session_id", "device_id"])["_count"]
                    .sum()
                    .reset_index()
                    .rename(columns={"_count": "event_count"})
                    .sort_values("event_count", ascending=False)
                )
                st.caption(f"{len(mapping):,} unique (session, device) pairs")
                st.dataframe(mapping.head(500), use_container_width=True, height=380, hide_index=True)
                st.download_button(
                    "📥 Export full mapping CSV",
                    data=mapping.to_csv(index=False).encode(),
                    file_name="session_device_mapping.csv",
                    mime="text/csv",
                    key="dl_full_mapping",
                )

        # ── SUB-TAB 4: Lookup ─────────────────────────────────
        with sub4:
            st.markdown("#### 🔍 Lookup by ID")
            st.caption("Enter a device ID or session ID to see all associated records.")

            lookup_mode = st.radio(
                "Lookup by",
                ["device_id", "session_id"] + [k for k in qsa_keys if k not in ("device_id", "session_id")],
                horizontal=True,
                key="lookup_mode",
            )

            lookup_val = st.text_input(
                f"Enter {lookup_mode} value",
                placeholder=f"paste a {lookup_mode} here ...",
                key="lookup_val",
            )

            if lookup_val.strip() and lookup_mode in parsed_df.columns:
                results_lk = parsed_df[
                    parsed_df[lookup_mode].astype(str).str.contains(
                        re.escape(lookup_val.strip()), case=False, na=False
                    )
                ].copy()

                if results_lk.empty:
                    st.warning(f"No records found for `{lookup_mode}` = `{lookup_val}`")
                else:
                    st.success(f"Found {len(results_lk):,} matching record(s)")

                    # Key metrics for this ID
                    if "device_id" in results_lk.columns:
                        st.markdown(f"**Unique device IDs:** {results_lk['device_id'].nunique()}")
                    if "session_id" in results_lk.columns:
                        st.markdown(f"**Unique session IDs:** {results_lk['session_id'].nunique()}")

                    # Show all parsed fields
                    display_cols = [c for c in results_lk.columns if c != "_raw"]
                    st.dataframe(
                        results_lk[display_cols],
                        use_container_width=True,
                        height=400,
                        hide_index=True,
                    )

                    st.download_button(
                        f"📥 Export parsed results for this {lookup_mode}",
                        data=results_lk[display_cols].to_csv(index=False).encode(),
                        file_name=f"lookup_{lookup_mode}_{lookup_val[:30]}.csv",
                        mime="text/csv",
                        key="dl_lookup",
                    )

                    # ── Fetch full parquet rows ──────────────────────
                    st.markdown("---")
                    st.markdown("#### 📂 Fetch full parquet rows for this lookup")
                    st.caption(
                        "This goes back to the original parquet files and returns **every column** "
                        f"for rows where `{qsa_col}` contains your lookup value — not just the parsed fields."
                    )

                    # Column selector for full row fetch
                    full_row_cols = st.multiselect(
                        "Columns to include  (leave empty = all columns)",
                        options=columns,
                        default=[],
                        key="full_row_cols",
                        help="Leave empty to get every column from the parquet file.",
                    )
                    out_full_cols = full_row_cols if full_row_cols else columns

                    # The filter: match the qrystr column value containing the lookup value
                    # We use the _raw query strings from the lookup results as the exact filter values
                    raw_values_to_match = results_lk["_raw"].astype(str).tolist()

                    fr1, fr2 = st.columns(2)
                    with fr1:
                        btn_preview_full = st.button(
                            "👀 Preview full rows (first 500)",
                            key="btn_preview_full_rows",
                            type="secondary",
                        )
                    with fr2:
                        btn_export_full = st.button(
                            "💾 Export all full rows to CSV",
                            key="btn_export_full_rows",
                            type="primary",
                        )

                    if btn_preview_full or btn_export_full:
                        max_r = 500 if btn_preview_full else None
                        df_full = run_query(
                            folder_key,
                            sel_cols=out_full_cols,
                            filters={qsa_col: raw_values_to_match},
                            dual={},
                            max_rows=max_r,
                            progress_label="Fetching full rows from parquet",
                        )

                        if df_full.empty:
                            st.warning("No matching rows found in parquet files.")
                        elif btn_preview_full:
                            st.caption(f"Preview — {len(df_full):,} rows × {len(df_full.columns)} columns")
                            st.dataframe(df_full, use_container_width=True, height=450, hide_index=True)
                        else:
                            csv_full = df_full.to_csv(index=False).encode()
                            fname_full = f"full_rows_{lookup_mode}_{lookup_val[:30]}.csv"
                            st.download_button(
                                label=f"📥 Download full rows CSV  ({len(df_full):,} rows × {len(df_full.columns)} cols)",
                                data=csv_full,
                                file_name=fname_full,
                                mime="text/csv",
                                key="dl_full_rows",
                            )
                            st.success(f"{len(df_full):,} full rows ready to download.")

            elif lookup_val.strip() and lookup_mode not in parsed_df.columns:
                st.warning(f"Key `{lookup_mode}` was not found in the parsed data.")


# ══════════════════════════════════════════════
# TAB 5 — User Behavior Dashboard
# ══════════════════════════════════════════════
with tab5:
    st.subheader("📺 User Behavior Dashboard")

    if not DUCKDB_OK:
        st.error("DuckDB not installed. Run: `pip install duckdb`")
        st.stop()
    if not PLOTLY_OK:
        st.error("Plotly not installed. Run: `pip install plotly`")
        st.stop()

    # ── Column mapping UI ─────────────────────────────────────
    with st.expander("⚙️ Column Mapping  (expand to configure)", expanded=False):
        st.caption(
            "Map the roles the dashboard needs to your actual parquet column names. "
            "Leave blank to skip optional columns."
        )
        col_map = st.session_state.ub_col_map
        none_opt = ["(not available)"] + columns

        def col_picker(role, label, required=True):
            opts   = columns if required else none_opt
            stored = col_map.get(role, role)
            try:
                idx = opts.index(stored)
            except ValueError:
                idx = 0
            chosen = st.selectbox(label, opts, index=idx, key=f"ub_cm_{role}")
            col_map[role] = chosen if chosen != "(not available)" else ""
            return col_map[role]

        r1, r2, r3 = st.columns(3)
        with r1:
            col_picker("queryStr",   "🔑 Query string column *",    required=True)
            col_picker("reqTimeSec", "🕐 Timestamp column (epoch) *", required=True)
            col_picker("reqPath",    "🛣️  Request path column *",    required=True)
        with r2:
            col_picker("UA",         "🖥️  User-Agent column",        required=False)
            col_picker("cliIP",      "🌐 Client IP column",          required=False)
            col_picker("asn",        "📡 ASN column",                required=False)
        with r3:
            col_picker("statusCode",      "✅ Status code column",     required=False)
            col_picker("transferTimeMSec","⚡ Transfer time (ms)",      required=False)
            col_picker("downloadTime",    "⬇️  Download time column",  required=False)

        st.session_state.ub_col_map = col_map
        if st.button("💾 Save column mapping", key="ub_save_map"):
            st.success("Column mapping saved.")

    st.markdown("---")

    # Validate required columns are mapped
    cm = st.session_state.ub_col_map
    qs_col  = cm.get("queryStr", "")
    ts_col  = cm.get("reqTimeSec", "")
    path_col= cm.get("reqPath", "")

    if not qs_col or not ts_col or not path_col:
        st.warning("Please configure at least the Query string, Timestamp, and Request path columns above.")
        st.stop()

    # Build parquet glob list for DuckDB
    pq_files = collect_files(valid_folders)
    if not pq_files:
        st.warning("No parquet files found in selected folders.")
        st.stop()

    pq_glob = [str(f) for f in pq_files]

    # ── Session gap setting ───────────────────────────────────
    session_gap = st.number_input(
        "Session gap (minutes) — gap larger than this starts a new session",
        min_value=5, max_value=180, value=20, step=5,
        key="ub_session_gap",
    )

    # ── Device ID selection ─────────────────────────────────────
    st.markdown("#### Step 1 — Select a device")

    qsa_df = st.session_state.get("qsa_parsed_df")
    qsa_device_ids = []
    if qsa_df is not None and not qsa_df.empty and "device_id" in qsa_df.columns:
        qsa_device_ids = sorted(
            qsa_df["device_id"]
            .dropna()
            .astype(str)
            .loc[lambda s: s.str.strip() != ""]
            .unique()
            .tolist()
        )

    if qsa_device_ids:
        st.session_state.ub_device_ids = qsa_device_ids

    ub_scan_col1, ub_scan_col2 = st.columns([3, 1])
    with ub_scan_col1:
        if st.session_state.ub_device_ids:
            source_label = "from QSA cache" if qsa_device_ids else "from parquet scan"
            st.caption(f"{len(st.session_state.ub_device_ids):,} device IDs ready ({source_label})")
    with ub_scan_col2:
        scan_devices_btn = st.button("🔄 Refresh from parquet", key="ub_scan_btn")

    if scan_devices_btn:
        _bar  = st.progress(0, text="Preparing scan ...")
        _ph   = st.empty()
        _stages = [
            (5,  "Opening parquet files"),
            (20, "Scanning queryStr column"),
            (50, "Extracting device_id values"),
            (75, "Deduplicating results"),
            (90, "Sorting device list"),
        ]
        ids = ub_staged_progress(_bar, _ph, _stages,
                                  lambda: ub_get_device_ids(pq_glob, qs_col))
        st.session_state.ub_device_ids = ids or []
        if not ids:
            st.warning("No device_id values found. Check your queryStr column mapping.")

    device_ids_list = st.session_state.ub_device_ids

    manual_device = st.text_input(
        "Or type a device_id manually",
        placeholder="paste device_id here ...",
        key="ub_manual_device",
    )

    if manual_device.strip():
        selected_device = manual_device.strip()
    elif device_ids_list:
        selected_device = st.selectbox("Select device_id", device_ids_list, key="ub_device_select")
    else:
        st.info("Parse query strings first in the Query String Analyzer, or refresh from parquet, or type a device_id manually.")
        st.stop()

    # ── Auto-load selected device once ────────────────────────
    if st.session_state.get("ub_loaded_device") != selected_device or st.session_state.get("ub_tmp_df") is None:
        min_d, max_d = ub_get_device_date_range(pq_glob, selected_device, cm)
        _bar2 = st.progress(0, text="Loading selected device ...")
        _ph2  = st.empty()
        _stages2 = [
            (5,  "Opening parquet files"),
            (25, "Filtering by device_id"),
            (55, "Reading matching rows"),
            (80, "Parsing query strings"),
            (92, "Preparing cached dataset"),
        ]
        _raw = ub_staged_progress(_bar2, _ph2, _stages2,
                                   lambda: ub_load_device(pq_glob, selected_device,
                                                          "1970-01-01", "2100-01-01", cm))
        _bar3 = st.progress(0, text="Enriching data ...")
        _ph3  = st.empty()
        _bar3.progress(40, text="Converting timestamps ...")
        tmp_df = ub_enrich(_raw)
        _bar3.progress(100, text="✅ Enriched!")
        time.sleep(0.2)
        _bar3.empty()
        _ph3.empty()
        if tmp_df.empty:
            st.warning("No rows found for this device.")
            st.stop()
        if min_d is None or max_d is None:
            min_d, max_d = tmp_df["event_date"].min(), tmp_df["event_date"].max()
        st.session_state["ub_tmp_df"] = tmp_df
        st.session_state["ub_loaded_device"] = selected_device
        st.session_state["ub_active_device"] = selected_device
        st.session_state["ub_device_min_date"] = min_d
        st.session_state["ub_device_max_date"] = max_d
        st.session_state["ub_sessionized_df"] = None
        st.session_state["ub_sessionized_key"] = None
        st.session_state["ub_date_range"] = (min_d, max_d)
        st.session_state["ub_start_time"] = dtime(0, 0)
        st.session_state["ub_end_time"] = dtime(23, 59)
        st.session_state["ub_min_req"] = 1

    tmp_df = st.session_state.get("ub_tmp_df")
    if tmp_df is None or tmp_df.empty:
        st.info("No rows loaded for this device yet.")
        st.stop()

    # ── Step 2 — Pick date range ─────────────────────────────
    st.markdown("#### Step 2 — Pick date range")
    all_dates = tmp_df["event_date"]
    min_allowed_date = st.session_state.get("ub_device_min_date", all_dates.min())
    max_allowed_date = st.session_state.get("ub_device_max_date", all_dates.max())

    if st.session_state.get("ub_active_device") != selected_device:
        st.session_state["ub_active_device"] = selected_device
        st.session_state["ub_date_range"] = (min_allowed_date, max_allowed_date)
        st.session_state["ub_start_time"] = dtime(0, 0)
        st.session_state["ub_end_time"] = dtime(23, 59)
        st.session_state["ub_min_req"] = 1

    date_range = st.date_input(
        "Date range",
        min_value=min_allowed_date,
        max_value=max_allowed_date,
        key="ub_date_range",
    )
    start_date, end_date = (date_range if isinstance(date_range, tuple) and len(date_range)==2
                            else (min_allowed_date, max_allowed_date))

    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        start_time = st.time_input("Start time", key="ub_start_time")
    with tc2:
        end_time   = st.time_input("End time", key="ub_end_time")
    with tc3:
        min_req_content = st.number_input(
            "Min requests per content", min_value=1, max_value=1000, step=1,
            key="ub_min_req",
        )

    st.caption("Changing the device or any filter above refreshes the dashboard automatically.")

    # ── Step 3 — Auto-run dashboard ──────────────────────────
    current_params = (
        selected_device,
        str(start_date),
        str(end_date),
        str(start_time),
        str(end_time),
        int(session_gap),
        int(min_req_content),
    )

    if st.session_state.get("ub_run_params") != current_params or st.session_state.get("ub_window_df") is None:
        _rbar = st.progress(0, text="Refreshing dashboard ...")
        _rph  = st.empty()

        sessionized_key = (selected_device, int(session_gap))
        sessionized_df = st.session_state.get("ub_sessionized_df")
        if st.session_state.get("ub_sessionized_key") != sessionized_key or sessionized_df is None:
            _rbar.progress(20, text=f"🔗 Building sessions once for {len(tmp_df):,} rows ...")
            _rph.caption("⏳ Applying session logic and watch-time estimation to the full selected device")
            sessionized_df = ub_build_sessions(tmp_df.copy(), gap_minutes=int(session_gap))
            sessionized_df = ub_estimate_watch_minutes(sessionized_df, cap_seconds=WATCH_GAP_CAP_SECONDS)
            st.session_state["ub_sessionized_df"] = sessionized_df
            st.session_state["ub_sessionized_key"] = sessionized_key
        else:
            _rbar.progress(20, text="⚡ Reusing cached sessionized device data ...")
            _rph.caption("⏳ Skipping session rebuild because the device and gap did not change")

        _rbar.progress(60, text="📅 Applying date range filter ...")
        device_df = sessionized_df[
            (sessionized_df["event_date"] >= start_date) &
            (sessionized_df["event_date"] <= end_date)
        ].copy()
        if device_df.empty:
            st.session_state["ub_window_df"] = pd.DataFrame()
            st.session_state["ub_sess_df"] = pd.DataFrame()
            st.session_state["ub_run_params"] = current_params
            _rbar.empty()
            _rph.empty()
            st.warning("No data found for this device in the selected date range.")
            st.stop()

        _rbar.progress(90, text="⏱️ Applying time window ...")
        window_df = device_df[
            (device_df["time_only"] >= start_time) &
            (device_df["time_only"] <= end_time)
        ].copy()

        if window_df.empty:
            st.session_state["ub_window_df"] = pd.DataFrame()
            st.session_state["ub_sess_df"] = pd.DataFrame()
            st.session_state["ub_run_params"] = current_params
            _rbar.empty()
            _rph.empty()
            st.warning("No activity in selected time window.")
            st.stop()

        st.session_state["ub_window_df"] = window_df
        st.session_state["ub_sess_df"] = ub_session_summary(window_df)
        st.session_state["ub_run_params"] = current_params

        _rbar.progress(100, text="✅ Dashboard refreshed")
        time.sleep(0.2)
        _rbar.empty()
        _rph.empty()

    window_df = st.session_state.get("ub_window_df")
    sess_df   = st.session_state.get("ub_sess_df", pd.DataFrame())

    if window_df is None or window_df.empty:
        st.stop()

    st.markdown("---")

    # ── KPIs ─────────────────────────────────────────────────
    first_seen     = window_df["event_time"].min()
    last_seen      = window_df["event_time"].max()
    n_sessions     = window_df["session_key"].nunique()
    est_watch_hrs  = round(window_df["watch_min_est"].sum() / 60, 2)
    top_content    = window_df["content_label"].mode().iloc[0] if not window_df["content_label"].mode().empty else "—"
    top_channel    = window_df["channel_name"].mode().iloc[0]  if not window_df["channel_name"].mode().empty  else "—"
    top_platform   = window_df["platform"].mode().iloc[0]       if not window_df["platform"].mode().empty      else "—"
    top_dev_type   = window_df["device_type"].mode().iloc[0]    if not window_df["device_type"].mode().empty   else "—"

    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("Rows",             f"{len(window_df):,}")
    k2.metric("Sessions",         f"{n_sessions:,}")
    k3.metric("Est. Watch Hrs",   str(est_watch_hrs))
    k4.metric("Top Channel",      str(top_channel)[:20])
    k5.metric("Top Content",      str(top_content)[:20])
    k6.metric("Device Type",      str(top_dev_type))

    with st.expander("📋 Window summary", expanded=True):
        # ── Geo/ISP lookup on unique IPs ──────────────────────
        _geo_data    = {}
        _top_ip      = ""
        _geo_summary = {}
        if "cliIP" in window_df.columns:
            _all_ips = window_df["cliIP"].dropna().astype(str).tolist()
            if _all_ips:
                _top_ip = window_df["cliIP"].mode().iloc[0] if not window_df["cliIP"].mode().empty else ""
                _unique_ips = window_df["cliIP"].dropna().unique().tolist()
                with st.spinner(f"🌍 Looking up geo info for {len(_unique_ips):,} unique IP(s) ..."):
                    _geo_data = ub_lookup_geo(_unique_ips)
                if _top_ip in _geo_data:
                    _geo_summary = ub_format_geo_summary(_geo_data[_top_ip])

        # ── Layout: 3 columns ─────────────────────────────────
        ws1, ws2, ws3 = st.columns(3)

        with ws1:
            st.markdown("**🖥️ Device**")
            st.write(f"**device_id:** `{selected_device}`")
            if "device_name_qs" in window_df.columns and not window_df["device_name_qs"].dropna().empty:
                dname = window_df["device_name_qs"].mode().iloc[0]
                if dname:
                    st.write(f"**Device name:** {dname}")
            st.write(f"**Device type:** {top_dev_type}")
            st.write(f"**Platform:** {top_platform}")
            st.markdown("**📅 Time window**")
            st.write(f"**From:** {start_date} {start_time}")
            st.write(f"**To:**   {end_date} {end_time}")
            st.write(f"**First event:** {first_seen}")
            st.write(f"**Last event:**  {last_seen}")

        with ws2:
            st.markdown("**🌍 Location (most common IP)**")
            if _geo_summary:
                loc = _geo_summary.get("location", "")
                if loc and loc != "Unknown":
                    st.write(f"**Location:** {loc}")
                if _geo_summary.get("country"):
                    st.write(f"**Country:** {_geo_summary['country']}")
                if _geo_summary.get("region"):
                    st.write(f"**State / Region:** {_geo_summary['region']}")
                if _geo_summary.get("city"):
                    st.write(f"**City:** {_geo_summary['city']}")
                if _geo_summary.get("timezone"):
                    st.write(f"**Timezone:** {_geo_summary['timezone']}")
                if _top_ip:
                    st.write(f"**IP (most common):** `{_top_ip}`")
            elif _top_ip:
                st.write(f"**IP (most common):** `{_top_ip}`")
                st.caption("_Geo lookup unavailable (offline or rate limited)_")
            else:
                st.caption("_No IP data available_")

            # All unique IPs seen
            if "cliIP" in window_df.columns:
                _n_ips = window_df["cliIP"].dropna().nunique()
                if _n_ips > 1:
                    st.caption(f"{_n_ips} unique IPs seen in this window")
                    with st.expander("Show all IPs"):
                        _ip_counts = (window_df["cliIP"].value_counts(dropna=True)
                                      .reset_index().rename(columns={"cliIP": "IP", "count": "requests"}))
                        # Add geo info per IP
                        _ip_counts["location"] = _ip_counts["IP"].map(
                            lambda ip: ub_format_geo_summary(_geo_data.get(str(ip), {})).get("location", "")
                        )
                        st.dataframe(_ip_counts, hide_index=True, use_container_width=True)

        with ws3:
            st.markdown("**📡 Network / ISP**")
            if _geo_summary:
                if _geo_summary.get("isp"):
                    st.write(f"**ISP:** {_geo_summary['isp']}")
                if _geo_summary.get("asn_name"):
                    st.write(f"**ASN:** {_geo_summary['asn_name']}")
            elif "asn" in window_df.columns and not window_df["asn"].dropna().empty:
                _top_asn = window_df["asn"].mode().iloc[0]
                st.write(f"**ASN (raw):** {_top_asn}")
                st.caption("_ISP name unavailable — geo lookup offline_")

            # ASN breakdown if multiple
            if "asn" in window_df.columns:
                _n_asns = window_df["asn"].dropna().nunique()
                if _n_asns > 1:
                    st.caption(f"{_n_asns} unique ASNs seen")
                    with st.expander("Show all ASNs"):
                        _asn_df = (window_df["asn"].value_counts(dropna=True)
                                   .reset_index().rename(columns={"asn": "ASN", "count": "requests"}))
                        # Add ISP name from geo data where possible
                        _ip_to_asn = {}
                        for _, _row in window_df[["cliIP","asn"]].dropna().iterrows():
                            _ip_to_asn[str(_row["asn"])] = _geo_data.get(str(_row["cliIP"]), {}).get("as", "")
                        _asn_df["ISP / Org"] = _asn_df["ASN"].astype(str).map(
                            lambda a: _ip_to_asn.get(str(a), "")
                        )
                        st.dataframe(_asn_df, hide_index=True, use_container_width=True)

            st.markdown("**📊 Activity**")
            st.write(f"**Total requests:** {len(window_df):,}")
            st.write(f"**Sessions:** {n_sessions:,}")
            st.write(f"**Est. watch hours:** {est_watch_hrs}")
            st.write(f"**Top channel:** {top_channel}")

    st.markdown("---")

    # ── Section 1: Watching timeline ─────────────────────────
    st.subheader("1) Watching history over time")
    hover_cols = [c for c in ["reqPath","content_title","quality","session_key","asn","platform"] if c in window_df.columns]
    fig1 = px.scatter(
        window_df, x="event_time", y="channel_name", color="content_label",
        hover_data=hover_cols, title="Watching history over time",
    )
    fig1.update_traces(marker=dict(opacity=0.85, size=9))
    fig1.update_layout(height=520)
    st.plotly_chart(fig1, use_container_width=True)

    # ── Section 1B: Date vs Time of day ──────────────────────
    st.subheader("1B) Date vs Time of Day (behavior pattern)")
    fig1b = px.scatter(
        window_df, x="watch_date", y="watch_time_min", color="channel_name",
        hover_data=[c for c in ["event_time","content_label","quality","session_key","asn","platform"] if c in window_df.columns],
        title="User behavior by date and time of day",
    )
    fig1b.update_traces(marker=dict(size=8, opacity=0.8))
    tick_vals = list(range(0, 1441, 60))
    tick_text = [f"{h:02d}:00" for h in range(25)]
    fig1b.update_yaxes(tickvals=tick_vals, ticktext=tick_text, range=[0, 1440])
    fig1b.update_layout(height=550, xaxis_title="Date", yaxis_title="Time of Day")
    st.plotly_chart(fig1b, use_container_width=True)

    # ── Section 2: Minute-by-minute ──────────────────────────
    st.subheader("2) Minute-by-minute activity")
    min_act = (
        window_df.set_index("event_time")
        .resample("1min").size()
        .rename("requests").reset_index()
    )
    fig2 = px.line(min_act, x="event_time", y="requests", title="Activity intensity")
    fig2.update_layout(height=350)
    st.plotly_chart(fig2, use_container_width=True)

    # ── Section 3: Content summary ───────────────────────────
    st.subheader("3) Content watched in this time range")
    content_window = (
        window_df.groupby(["content_label","channel_name"])
        .agg(
            requests      = ("reqPath",      "size"),
            est_watch_min = ("watch_min_est","sum"),
            first_seen    = ("event_time",   "min"),
            last_seen     = ("event_time",   "max"),
            sessions      = ("session_key",  "nunique"),
        )
        .reset_index()
        .sort_values(["est_watch_min","requests"], ascending=False)
    )
    content_window = content_window[content_window["requests"] >= min_req_content]
    st.dataframe(content_window, use_container_width=True, height=300, hide_index=True)

    fig3 = px.bar(
        content_window.head(15), x="est_watch_min", y="content_label",
        color="channel_name", orientation="h",
        title="Top content by estimated watch minutes",
    )
    fig3.update_layout(height=500, yaxis={"categoryorder":"total ascending"})
    st.plotly_chart(fig3, use_container_width=True)

    # ── Section 4: Paths / endpoints ─────────────────────────
    st.subheader("4) Request paths / endpoints")
    paths_window = (
        window_df.groupby("reqPath")
        .agg(
            requests      = ("reqPath",      "size"),
            first_seen    = ("event_time",   "min"),
            last_seen     = ("event_time",   "max"),
            est_watch_min = ("watch_min_est","sum"),
        )
        .reset_index()
        .sort_values(["requests","est_watch_min"], ascending=False)
        .head(100)
    )
    st.dataframe(paths_window, use_container_width=True, height=300, hide_index=True)

    # ── Section 5: Session drill-down ────────────────────────
    st.subheader("5) Session drill-down")
    session_opts = sorted(window_df["session_key"].unique().tolist())
    sel_session  = st.selectbox("Select session", session_opts, key="ub_sel_session")
    sess_win_df  = window_df[window_df["session_key"] == sel_session].copy()

    ss1,ss2,ss3,ss4 = st.columns(4)
    ss1.metric("Rows",          f"{len(sess_win_df):,}")
    dur = round((sess_win_df["event_time"].max() - sess_win_df["event_time"].min()).total_seconds() / 60, 2) if len(sess_win_df) > 1 else 0
    ss2.metric("Duration (min)", dur)
    ss3.metric("Unique Content", f"{sess_win_df['content_label'].nunique():,}")
    ss4.metric("Est. Watch Min", round(sess_win_df["watch_min_est"].sum(), 2))

    s_cols = [c for c in ["event_time","channel_name","content_label","reqPath","quality","platform","asn","statusCode","watch_min_est","session_key"] if c in sess_win_df.columns]
    st.dataframe(sess_win_df[s_cols].sort_values("event_time"), use_container_width=True, height=320, hide_index=True)

    # ── Section 6: Content switching ─────────────────────────
    st.subheader("6) Content switching moments")
    sw = window_df.sort_values("event_time").copy()
    sw["prev_content"] = sw["content_label"].shift(1)
    switches = sw[sw["content_label"] != sw["prev_content"]][
        [c for c in ["event_time","prev_content","content_label","channel_name","session_key"] if c in sw.columns]
    ].copy()
    st.dataframe(switches, use_container_width=True, height=250, hide_index=True)

    # ── Section 7: Watch starts ───────────────────────────────
    st.subheader("7) Likely watch starts (first event per session)")
    watch_starts = (
        window_df.sort_values("event_time")
        .groupby("session_key").first().reset_index()
        [[c for c in ["session_key","event_time","content_label","channel_name","platform"] if c in window_df.columns]]
    )
    st.dataframe(watch_starts, use_container_width=True, height=250, hide_index=True)

    # ── Section 8: Network / ASN ─────────────────────────────
    if "asn" in window_df.columns and not window_df["asn"].dropna().empty:
        st.subheader("8) Network usage (ASN)")
        asn_df = window_df["asn"].value_counts(dropna=False).reset_index()
        asn_df.columns = ["asn","requests"]
        c_asn1, c_asn2 = st.columns([2, 1])
        with c_asn1:
            st.dataframe(asn_df, use_container_width=True, hide_index=True)
        with c_asn2:
            st.bar_chart(asn_df.head(10).set_index("asn")["requests"])

    # ── Section 9: Raw events ─────────────────────────────────
    st.subheader("9) Raw events in selected window")
    raw_cols = [c for c in ["event_time","session_key","channel_name","content_label","content_title",
                             "platform","device_type","reqPath","quality","asn","cliIP",
                             "statusCode","transferTimeMSec","downloadTime","queryStr"] if c in window_df.columns]
    st.dataframe(window_df[raw_cols].sort_values("event_time"), use_container_width=True, height=420, hide_index=True)

    # ── Downloads ─────────────────────────────────────────────
    st.markdown("---")
    date_label = f"{start_date}_to_{end_date}"
    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        st.download_button(
            "📥 Raw events CSV",
            data=window_df.to_csv(index=False).encode("utf-8"),
            file_name=f"behavior_{selected_device}_{date_label}.csv",
            mime="text/csv", key="ub_dl_raw",
        )
    with dl2:
        if not sess_df.empty:
            st.download_button(
                "📥 Session summary CSV",
                data=sess_df.to_csv(index=False).encode("utf-8"),
                file_name=f"sessions_{selected_device}_{date_label}.csv",
                mime="text/csv", key="ub_dl_sess",
            )
    with dl3:
        st.download_button(
            "📥 Content summary CSV",
            data=content_window.to_csv(index=False).encode("utf-8"),
            file_name=f"content_{selected_device}_{date_label}.csv",
            mime="text/csv", key="ub_dl_content",
        )

    pdf_bytes = ub_build_pdf_report(
        window_df=window_df,
        sess_df=sess_df,
        content_window=content_window,
        selected_device=selected_device,
        start_date=start_date,
        end_date=end_date,
        top_channel=top_channel,
        top_content=top_content,
        est_watch_hrs=est_watch_hrs,
        n_sessions=n_sessions,
    )
    st.download_button(
        "📄 Download User Behavior Report (PDF)",
        data=pdf_bytes,
        file_name=f"user_behavior_report_{selected_device}_{date_label}.pdf",
        mime="application/pdf",
        key="ub_dl_pdf",
    )

    with st.expander("ℹ️ How to read this dashboard"):
        st.markdown("""
- **device_id** is extracted from the query string column via regex.
- **Sessions** are detected by time gaps, channel changes, IP/ASN shifts and platform changes.
- **Estimated watch minutes** = time gap between consecutive events, capped to avoid inflation.
- **Content switching** shows every moment the user changed what they were watching.
- **Watch starts** = first event in each session — likely when the user pressed play.
- This is behavioral estimation from CDN/access logs, not player telemetry.
        """)

# ══════════════════════════════════════════════
# TAB 6 — Global Behavior Dashboard
# ══════════════════════════════════════════════
with tab6:
    st.subheader("🌐 Global Behavior Dashboard")
    st.caption("This tab auto-runs for the selected date range. Behavior analytics use only rows with session metadata, while coverage shows how much of total traffic that represents.")

    if not DUCKDB_OK:
        st.error("DuckDB not installed. Run: `pip install duckdb`")
        st.stop()
    if not PLOTLY_OK:
        st.error("Plotly not installed. Run: `pip install plotly`")
        st.stop()

    gb_cm = st.session_state.ub_col_map
    gb_qs = gb_cm.get("queryStr", "")
    gb_ts = gb_cm.get("reqTimeSec", "")
    gb_path = gb_cm.get("reqPath", "")
    if not gb_qs or not gb_ts or not gb_path:
        st.warning("Please configure Query string, Timestamp, and Request path in the User Behavior column mapping first.")
        st.stop()

    gb_files = collect_files(valid_folders)
    if not gb_files:
        st.warning("No parquet files found in selected folders.")
        st.stop()
    gb_glob = [str(f) for f in gb_files]

    g1, g2, g3, g4 = st.columns([1, 1, 1, 1])
    with g1:
        gb_entity = st.radio("Focus", ["Content", "Channel"], horizontal=True, key="gb_entity")
    with g2:
        gb_top_n = st.selectbox("Top N", [5, 10, 15, 20], index=1, key="gb_top_n")
    with g3:
        gb_daypart_mode = st.selectbox("Day-part mode", ["Default", "TV Style"], index=1, key="gb_daypart_mode")
    with g4:
        gb_show_overall = st.checkbox("Show overall baseline", value=True, key="gb_show_overall")

    gb_channel_mode = "Clean"
    if gb_entity == "Channel":
        show_raw_channel = st.checkbox("Debug: use raw channel names instead of pure channels", value=False, key="gb_show_raw_channel")
        gb_channel_mode = "Raw" if show_raw_channel else "Clean"
        st.caption("Default is pure channel mode: platform/device suffixes after '_' are removed, while platform remains separate.")

    default_end = pd.to_datetime("today").date()
    default_start = default_end - pd.Timedelta(days=6)
    gb_dates = st.date_input("Date range", value=(default_start, default_end), key="gb_date_range")
    gb_start, gb_end = gb_dates if isinstance(gb_dates, tuple) and len(gb_dates) == 2 else (default_start, default_end)
    gb_start_s, gb_end_s = str(gb_start), str(gb_end)

    with st.spinner("Loading global behavior coverage ..."):
        cov = gb_get_coverage(gb_glob, gb_cm, gb_start_s, gb_end_s)
        overall_cov = gb_get_coverage(gb_glob, gb_cm, "1970-01-01", "2100-01-01") if gb_show_overall else None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows in range", f"{cov['total_rows']:,}")
    c2.metric("Behavior rows", f"{cov['behavior_rows']:,}")
    c3.metric("Coverage", f"{cov['coverage_pct']:.2f}%")
    c4.metric("Sessions", f"{cov['sessions']:,}")
    c5.metric("Devices", f"{cov['devices']:,}")

    if overall_cov is not None:
        st.caption(f"Overall baseline — {overall_cov['behavior_rows']:,} behavior rows out of {overall_cov['total_rows']:,} total rows ({overall_cov['coverage_pct']:.2f}% coverage).")

    if cov["behavior_rows"] == 0:
        st.warning("No behavior rows found in this date range. Try widening the range.")
        st.stop()

    st.markdown("---")
    with st.expander("✅ Pure Channel Master (business channel, platform kept separate)", expanded=True):
        st.caption("This is the correct channel view: platform/device suffixes are removed from channel names, while platform remains a separate dimension. Do not use raw Unique Values for business channel reporting.")
        with st.spinner("Building pure channel master ..."):
            ch_master_df = gb_get_channel_master_smart(gb_glob, gb_cm, gb_start_s, gb_end_s, top_n=10000)
        if ch_master_df.empty:
            st.info("No channel metadata found for this range.")
        else:
            pure_summary = (
                ch_master_df.groupby("clean_channel", dropna=False)
                .agg(requests=("requests", "sum"), sessions=("sessions", "sum"), devices=("devices", "sum"), raw_variants=("raw_channel", "nunique"))
                .reset_index()
                .sort_values(["requests", "sessions"], ascending=False)
            )
            pc1, pc2 = st.columns([1, 1])
            with pc1:
                st.markdown("**Pure channel summary**")
                st.dataframe(pure_summary, use_container_width=True, hide_index=True, height=340)
                st.download_button(
                    "📥 Download Pure Channel Summary CSV",
                    data=pure_summary.to_csv(index=False).encode("utf-8"),
                    file_name=f"pure_channel_summary_{gb_start_s}_to_{gb_end_s}.csv",
                    mime="text/csv",
                    key="gb_dl_pure_channel_summary",
                )
            with pc2:
                st.markdown("**Raw → clean audit mapping**")
                st.dataframe(ch_master_df, use_container_width=True, hide_index=True, height=340)
                st.download_button(
                    "📥 Download Raw-to-Clean Channel Mapping CSV",
                    data=ch_master_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"channel_raw_to_clean_{gb_start_s}_to_{gb_end_s}.csv",
                    mime="text/csv",
                    key="gb_dl_raw_clean_channel_mapping",
                )

    st.markdown("---")
    sec1, sec2, sec3, sec4 = st.tabs(["🌅 Day-part", "📌 Stickiness", "⏱️ Retention Proxy", "🔁 Switching"])

    with sec1:
        with st.spinner("Loading day-part analytics ..."):
            day_df = gb_get_daypart_top(gb_glob, gb_cm, gb_start_s, gb_end_s, gb_entity, int(gb_top_n), gb_daypart_mode, gb_channel_mode)
        if day_df.empty:
            st.info("No day-part data found.")
        else:
            st.dataframe(day_df, use_container_width=True, hide_index=True, height=340)
            fig_dp = px.bar(day_df, x="day_part", y="sessions", color="entity_name", title=f"Top {gb_top_n} {gb_entity.lower()} by day-part")
            fig_dp.update_layout(height=480)
            st.plotly_chart(fig_dp, use_container_width=True)

    with sec2:
        with st.spinner("Loading stickiness analytics ..."):
            sticky_df = gb_get_stickiness(gb_glob, gb_cm, gb_start_s, gb_end_s, gb_entity, max(int(gb_top_n) * 3, 15), gb_daypart_mode, gb_channel_mode)
        if sticky_df.empty:
            st.info("No stickiness data found.")
        else:
            st.dataframe(sticky_df, use_container_width=True, hide_index=True, height=360)
            fig_st = px.scatter(sticky_df, x="requests", y="avg_watch_per_request", size="sessions", color="label", hover_name="entity_name", title=f"{gb_entity} stickiness")
            fig_st.update_layout(height=520)
            st.plotly_chart(fig_st, use_container_width=True)

    with sec3:
        with st.spinner("Loading retention proxy analytics ..."):
            ret_df = gb_get_retention_buckets(gb_glob, gb_cm, gb_start_s, gb_end_s, gb_entity, int(gb_top_n), gb_daypart_mode, gb_channel_mode)
        if ret_df.empty:
            st.info("No retention proxy data found.")
        else:
            st.dataframe(ret_df, use_container_width=True, hide_index=True, height=320)
            fig_rt = px.bar(ret_df, x="entity_name", y="sessions", color="watch_bucket", title=f"{gb_entity} retention buckets")
            fig_rt.update_layout(height=520, xaxis_title=gb_entity)
            st.plotly_chart(fig_rt, use_container_width=True)

    with sec4:
        with st.spinner("Loading switching analytics ..."):
            trans_df, rate_df, sess_bucket_df = gb_get_switching(gb_glob, gb_cm, gb_start_s, gb_end_s, max(int(gb_top_n) * 2, 10), gb_daypart_mode, gb_channel_mode)
        s1, s2 = st.columns([2, 1])
        with s1:
            st.markdown("**Top channel transitions**")
            st.dataframe(trans_df, use_container_width=True, hide_index=True, height=300)
        with s2:
            st.markdown("**Switches per session**")
            st.dataframe(sess_bucket_df, use_container_width=True, hide_index=True)
        if not rate_df.empty:
            fig_sw = px.bar(rate_df.head(15), x="switch_away_pct", y="channel_name", orientation="h", title="Channels with highest switch-away rate")
            fig_sw.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_sw, use_container_width=True)
