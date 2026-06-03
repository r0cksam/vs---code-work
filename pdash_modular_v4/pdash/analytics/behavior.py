from pdash.common import *
from pdash.core.disk_cache import global_bundle_cache_key, load_global_bundle_from_disk, save_global_bundle_to_disk

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
    """Return canonical pure channel name for analytics.

    Removes trailing device/platform tokens after underscores and then
    case-folds the channel so IndiaTV and indiatv are treated as the same
    business channel. Raw values are still kept separately in master exports.
    """
    if pd.isna(channel_raw):
        return "unknown"
    raw = str(channel_raw).strip()
    if raw.lower() in {"", "nan", "none", "null", "(null)"}:
        return "unknown"
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

    return (clean or "unknown").casefold()

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
        except Exception as e:
            log_warning("Recoverable operation failed", e)  # offline or blocked — fail silently

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
def ub_get_conn():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    return con


@st.cache_data(show_spinner="Scanning device IDs ...", ttl=1800)
def ub_get_device_ids(parquet_glob: str, qs_col: str) -> list:
    con = ub_get_conn()
    query = f"""
    SELECT DISTINCT regexp_extract({qs_col}, '(?:^|&)device_id=([^&]+)', 1) AS device_id
    FROM read_parquet({parquet_glob!r}, union_by_name=true)
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
    con  = ub_get_conn()
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
        regexp_extract({qs}, '(?:^|&)channel_name=([^&]+)',   1) AS channel_name_param,
        regexp_extract({qs}, '(?:^|&)content_type=([^&]+)',   1) AS content_type,
        regexp_extract({qs}, '(?:^|&)content_title=([^&]+)',  1) AS content_title,
        regexp_extract({qs}, '(?:^|&)platform=([^&]+)',       1) AS platform,
        regexp_extract({qs}, '(?:^|&)device=([^&]+)',         1) AS device_name_qs,
        regexp_extract({qs}, '(?:^|&)category_name=([^&]+)',  1) AS category_name
    FROM read_parquet({parquet_glob!r}, union_by_name=true)
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

    for col in ["channel_param","channel_name_param","content_type","content_title","platform","device_name_qs","category_name","device_id","session_id"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).map(unquote_plus)

    if "reqPath" in out.columns:
        out["channel_from_path"] = ub_extract_channel_from_path(out["reqPath"])
        out["quality"]           = ub_extract_quality(out["reqPath"])
    else:
        out["channel_from_path"] = ""
        out["quality"]           = "unknown"

    out["channel_name_raw"] = (
        out.get("channel_param", pd.Series("", index=out.index)).replace("", pd.NA)
        .fillna(out.get("channel_name_param", pd.Series("", index=out.index)).replace("", pd.NA))
        .fillna(out.get("channel_from_path", pd.NA))
        .fillna("Unknown")
    )
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
    con = ub_get_conn()
    qs = _cm(col_map, "queryStr")
    ts = _cm(col_map, "reqTimeSec")
    query = f"""
    SELECT
        MIN(to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE) AS min_date,
        MAX(to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE) AS max_date
    FROM read_parquet({parquet_glob!r}, union_by_name=true)
    WHERE regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1) = ?
    """
    try:
        row = con.execute(query, [device_id]).fetchone()
        return row if row else (None, None)
    except Exception as e:
        log_warning("Could not get device date range", e)
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
    con = ub_get_conn()
    qs   = _cm(col_map, "queryStr")
    ts   = _cm(col_map, "reqTimeSec")
    query = f"""
    WITH base AS (
        SELECT {qs} AS queryStr, TRY_CAST({ts} AS BIGINT) AS req_ts
        FROM read_parquet({parquet_glob!r}, union_by_name=true)
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
    con = ub_get_conn()
    qs = _cm(col_map, "queryStr")
    ts = _cm(col_map, "reqTimeSec")
    path = _cm(col_map, "reqPath")
    query = f"""
    SELECT
        COALESCE(NULLIF(regexp_extract({qs}, '(?:^|&)channel=([^&]+)', 1), ''),
                 NULLIF(regexp_extract({qs}, '(?:^|&)channel_name=([^&]+)', 1), ''),
                 NULLIF(regexp_extract({path}, '(vglive-sk-\d+)', 1), ''),
                 'Unknown') AS raw_channel,
        regexp_extract({qs}, '(?:^|&)platform=([^&]+)', 1) AS platform,
        regexp_extract({qs}, '(?:^|&)device=([^&]+)', 1) AS device_name,
        COUNT(*) AS requests,
        COUNT(DISTINCT regexp_extract({qs}, '(?:^|&)session_id=([^&]+)', 1)) AS sessions,
        COUNT(DISTINCT regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1)) AS devices
    FROM read_parquet({parquet_glob!r}, union_by_name=true)
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
    df["pure_channel"] = df.apply(
        lambda r: normalize_channel_name_smart(r.get("raw_channel", ""), r.get("platform", ""), r.get("device_name", "")),
        axis=1,
    )
    return (
        df.groupby(["pure_channel", "raw_channel", "platform", "device_name"], dropna=False)
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
            regexp_extract({qs}, '(?:^|&)channel_name=([^&]+)', 1)   AS channel_name_qs,
            regexp_extract({qs}, '(?:^|&)content_title=([^&]+)', 1)  AS content_title_qs,
            regexp_extract({qs}, '(?:^|&)platform=([^&]+)', 1)       AS platform,
            regexp_extract({qs}, '(?:^|&)device=([^&]+)', 1)         AS device_name_qs,
            regexp_extract({qs}, '(?:^|&)category_name=([^&]+)', 1)  AS category_name
        FROM read_parquet({parquet_glob!r}, union_by_name=true)
        WHERE {qs} IS NOT NULL
          AND {qs} LIKE '%session_id=%'
          AND to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    ),
    enriched AS (
        SELECT
            *,
            COALESCE(NULLIF(channel_qs, ''), NULLIF(channel_name_qs, ''), NULLIF(regexp_extract(reqPath, '(vglive-sk-\d+)', 1), ''), 'Unknown') AS channel_name_raw,
            lower(regexp_replace(regexp_replace(COALESCE(NULLIF(channel_qs, ''), NULLIF(channel_name_qs, ''), NULLIF(regexp_extract(reqPath, '(vglive-sk-\d+)', 1), ''), 'Unknown'), '\\s*_\\s*', '_', 'g'), '_(firetv|firestick|fireos|androidtv|android|web|webos|lg|lgtv|apple|appletv|ios|iphone|ipad|samsung|samsungtv|tizen|roku|mi|mitv|xiaomi|sony|bravia|id|tv|mobile|phone|tablet)$', '', 'i')) AS channel_name,
            COALESCE(NULLIF(content_title_qs, ''), NULLIF(channel_qs, ''), NULLIF(channel_name_qs, ''), NULLIF(regexp_extract(reqPath, '(vglive-sk-\d+)', 1), ''), reqPath) AS content_label,
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
    con = ub_get_conn()
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
    con = ub_get_conn()
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
    con = ub_get_conn()
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
    con = ub_get_conn()
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


@st.cache_data(show_spinner="Finding global behavior date range ...", ttl=1800)
def gb_get_available_date_range(parquet_glob: list, col_map: dict):
    """Return first/last available behavior dates from the selected parquet records."""
    con = ub_get_conn()
    qs = _cm(col_map, "queryStr")
    ts = _cm(col_map, "reqTimeSec")
    query = f"""
    SELECT
        MIN(to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE) AS min_date,
        MAX(to_timestamp(TRY_CAST({ts} AS BIGINT))::DATE) AS max_date
    FROM read_parquet({parquet_glob!r}, union_by_name=true)
    WHERE TRY_CAST({ts} AS BIGINT) IS NOT NULL
      AND {qs} IS NOT NULL
      AND {qs} LIKE '%session_id=%'
    """
    try:
        row = con.execute(query).fetchone()
        return row if row else (None, None)
    except Exception as e:
        log_warning("Could not resolve global behavior date range", e)
        return (None, None)


@st.cache_data(show_spinner="Preloading Global Behavior dashboard ...", ttl=1800)
def gb_preload_global_bundle(parquet_glob: list, col_map: dict, start_date: str, end_date: str,
                             daypart_mode: str = "TV Style", preload_top_n: int = 50) -> dict:
    """Precompute Global Behavior once so UI option clicks only display cached data.

    This intentionally loads both Content and Channel views for the selected full
    date range. It is heavier once, but removes repeated reloads while exploring.
    v4 also persists the bundle to disk, so restarting Streamlit can reuse it.
    """
    disk_cache_key = global_bundle_cache_key(parquet_glob, col_map, start_date, end_date, daypart_mode, preload_top_n)
    if CONFIG.disk_cache_enabled:
        cached = load_global_bundle_from_disk(disk_cache_key, max_age_seconds=CONFIG.disk_cache_max_age_seconds)
        if cached is not None:
            return cached

    bundle = {
        "start_date": start_date,
        "end_date": end_date,
        "daypart_mode": daypart_mode,
        "preload_top_n": int(preload_top_n),
        "coverage": gb_get_coverage(parquet_glob, col_map, start_date, end_date),
        "content": {},
        "channel": {},
        "channel_master": pd.DataFrame(),
        "errors": [],
    }

    try:
        bundle["channel_master"] = gb_get_channel_master_smart(
            parquet_glob, col_map, start_date, end_date, top_n=max(int(preload_top_n) * 500, 10000)
        )
    except Exception as e:
        bundle["errors"].append(f"Channel master failed: {e}")
        log_warning("Channel master preload failed", e)

    jobs = [
        ("content", "Content", "Clean"),
        ("channel", "Channel", "Clean"),
    ]
    for key, entity, channel_mode in jobs:
        try:
            bundle[key]["daypart"] = gb_get_daypart_top(
                parquet_glob, col_map, start_date, end_date, entity, int(preload_top_n), daypart_mode, channel_mode
            )
        except Exception as e:
            bundle[key]["daypart"] = pd.DataFrame()
            bundle["errors"].append(f"{entity} day-part failed: {e}")
            log_warning(f"{entity} day-part preload failed", e)

        try:
            bundle[key]["stickiness"] = gb_get_stickiness(
                parquet_glob, col_map, start_date, end_date, entity, int(preload_top_n), daypart_mode, channel_mode
            )
        except Exception as e:
            bundle[key]["stickiness"] = pd.DataFrame()
            bundle["errors"].append(f"{entity} stickiness failed: {e}")
            log_warning(f"{entity} stickiness preload failed", e)

        try:
            bundle[key]["retention"] = gb_get_retention_buckets(
                parquet_glob, col_map, start_date, end_date, entity, int(preload_top_n), daypart_mode, channel_mode
            )
        except Exception as e:
            bundle[key]["retention"] = pd.DataFrame()
            bundle["errors"].append(f"{entity} retention failed: {e}")
            log_warning(f"{entity} retention preload failed", e)

    try:
        bundle["channel"]["switching"] = gb_get_switching(
            parquet_glob, col_map, start_date, end_date, int(preload_top_n), daypart_mode, "Clean"
        )
    except Exception as e:
        bundle["channel"]["switching"] = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        bundle["errors"].append(f"Channel switching failed: {e}")
        log_warning("Channel switching preload failed", e)

    bundle["_disk_cache_key"] = disk_cache_key
    if CONFIG.disk_cache_enabled:
        save_global_bundle_to_disk(disk_cache_key, bundle)
    return bundle
