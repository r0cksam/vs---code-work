from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote_plus

import pandas as pd
import streamlit as st

from vglive_core import (
    CHUNK_DURATION_HOURS,
    DEFAULT_LAKE_FOLDER,
    compute_metrics,
    get_available_dates,
    inspect_lake,
)


DEFAULT_PROFILE_FOLDER = Path(__file__).resolve().parent / "vglive_channel_profile" / "deep_profile_full"
DEFAULT_ASN_DECODED_CSV = Path(__file__).resolve().parent / "4.asn" / "asnDecoded.csv"

PROFILE_FILES = {
    "asn": "asn_top.csv",
    "cache": "cache_by_host.csv",
    "channel_daily": "channel_daily.csv",
    "channel_summary": "channel_summary.csv",
    "cmcd": "cmcd_presence.csv",
    "column_fill": "column_fill_rate.csv",
    "daily": "daily_volume.csv",
    "device": "device_type_by_channel.csv",
    "errors": "errors_by_host.csv",
    "extensions": "extensions.csv",
    "files": "file_inventory.csv",
    "geo": "geo_top.csv",
    "hosts": "hosts_overview.csv",
    "mapping_quality": "path_candidate_quality.csv",
    "performance": "performance_by_host_extension.csv",
    "query_params": "querystr_param_presence.csv",
    "query_channels": "querystr_channel_profile.csv",
    "schema": "schema.csv",
    "status": "status_codes.csv",
    "ua": "ua_top.csv",
    "unmapped": "unmapped_candidates.csv",
}


st.set_page_config(page_title="VgLive Watch Dashboard", layout="wide")

st.markdown(
    """
    <style>
    .block-container { padding-top: 1.25rem; padding-bottom: 2rem; }
    div[data-testid="stMetric"] {
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 0.8rem 0.9rem;
        background: #fbfcfe;
    }
    div[data-testid="stMetric"] label { color: #334155; }
    .stTabs [data-baseweb="tab-list"] { gap: 0.25rem; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=86400)
def cached_dates(lake_path: str):
    return get_available_dates(lake_path)


@st.cache_data(ttl=7200, show_spinner=False)
def cached_metrics(lake_path: str, start_date, end_date):
    return compute_metrics(lake_path, start_date, end_date)


@st.cache_data(ttl=1800, show_spinner=False)
def load_profile_csv(profile_path: str, filename: str) -> pd.DataFrame:
    path = Path(profile_path) / filename
    parquet_path = path.with_suffix(".parquet")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


@st.cache_data(ttl=1800, show_spinner=False)
def load_asn_decoded(asn_decoded_csv: str) -> pd.DataFrame:
    path = Path(asn_decoded_csv)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "asn" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["asn"] = df["asn"].map(normalize_asn)
    keep = [c for c in ["asn", "as_name", "as_country", "as_domain", "asn_type", "lookup_status"] if c in df.columns]
    return df[keep].drop_duplicates("asn")


def profile_csv(profile_path: str, key: str) -> pd.DataFrame:
    return load_profile_csv(profile_path, PROFILE_FILES[key])


def normalize_asn(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if text.startswith("AS"):
        text = text[2:].strip()
    if text.endswith(".0"):
        text = text[:-2]
    return "".join(ch for ch in text if ch.isdigit())


def enrich_asn(asn_df: pd.DataFrame, decoded_df: pd.DataFrame) -> pd.DataFrame:
    if asn_df.empty:
        return asn_df
    out = asn_df.copy()
    out["asn"] = out["asn"].map(normalize_asn)
    if not decoded_df.empty:
        out = out.merge(decoded_df, on="asn", how="left")
    for column in ["as_name", "as_country", "as_domain", "asn_type", "lookup_status"]:
        if column not in out.columns:
            out[column] = ""
        out[column] = out[column].fillna("")
    out["as_name"] = out.apply(
        lambda row: row["as_name"] if row["as_name"] else f"AS{row['asn']}",
        axis=1,
    )
    out["asn_display"] = out.apply(lambda row: f"AS{row['asn']} - {row['as_name']}", axis=1)
    return out


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def sum_col(df: pd.DataFrame, column: str) -> float:
    if df.empty or column not in df.columns:
        return 0.0
    return float(to_number(df[column]).fillna(0).sum())


def fmt_int(value: object) -> str:
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "0"


def fmt_float(value: object, decimals: int = 1) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "0"


def pct(part: float, total: float) -> float:
    return (part / total * 100.0) if total else 0.0


def existing_columns(df: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in df.columns]


def show_table(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    formats: dict[str, str] | None = None,
    height: int | None = None,
) -> None:
    if df.empty:
        st.info("No rows available.")
        return

    view = df.copy()
    if columns:
        view = view[existing_columns(view, columns)]

    fmt = {column: pattern for column, pattern in (formats or {}).items() if column in view.columns}
    dataframe_kwargs = {"width": "stretch", "hide_index": True}
    if height is not None:
        dataframe_kwargs["height"] = height

    if fmt:
        st.dataframe(view.style.format(fmt), **dataframe_kwargs)
    else:
        st.dataframe(view, **dataframe_kwargs)


def filter_by_date(df: pd.DataFrame, date_column: str, start_date, end_date) -> pd.DataFrame:
    if df.empty or date_column not in df.columns or start_date is None or end_date is None:
        return df.copy()

    out = df.copy()
    dates = pd.to_datetime(out[date_column], errors="coerce").dt.date
    return out[(dates >= start_date) & (dates <= end_date)].copy()


def selected_channel_profile(
    channel_summary: pd.DataFrame,
    channel_daily: pd.DataFrame,
    start_date,
    end_date,
) -> pd.DataFrame:
    filtered = filter_by_date(channel_daily, "log_date", start_date, end_date)
    if not filtered.empty:
        for column in ["raw_ts_chunks", "raw_watch_hours", "approx_unique_ips"]:
            if column in filtered.columns:
                filtered[column] = to_number(filtered[column]).fillna(0)

        grouped = (
            filtered.groupby("channel_name", as_index=False)
            .agg(
                raw_ts_chunks=("raw_ts_chunks", "sum"),
                raw_watch_hours=("raw_watch_hours", "sum"),
                approx_unique_ips=("approx_unique_ips", "max"),
            )
            .sort_values("raw_watch_hours", ascending=False)
        )
        total_hours = sum_col(grouped, "raw_watch_hours")
        grouped["share_pct"] = grouped["raw_watch_hours"].map(lambda value: pct(value, total_hours))
        return grouped.reset_index(drop=True)

    out = channel_summary.copy()
    if not out.empty:
        total_hours = sum_col(out, "raw_watch_hours")
        out["share_pct"] = to_number(out["raw_watch_hours"]).fillna(0).map(lambda value: pct(value, total_hours))
    return out


def selected_daily_profile(daily: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    out = filter_by_date(daily, "log_date", start_date, end_date)
    if not out.empty and "ts_rows" in out.columns:
        out["raw_watch_hours"] = to_number(out["ts_rows"]).fillna(0) * CHUNK_DURATION_HOURS
    return out


def add_watch_hours_from_ts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not out.empty and "ts_rows" in out.columns:
        out["watch_hours"] = to_number(out["ts_rows"]).fillna(0) * CHUNK_DURATION_HOURS
    return out


def prepare_deduped_channels(channel_df: pd.DataFrame) -> pd.DataFrame:
    out = channel_df.copy()
    if out.empty:
        return out
    total = sum_col(out, "watch_hours")
    out["share_pct"] = to_number(out["watch_hours"]).fillna(0).map(lambda value: pct(value, total))
    return out.sort_values("watch_hours", ascending=False).reset_index(drop=True)


def decode_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = out[column].fillna("").astype(str).map(unquote_plus)
    return out


def bar_chart(df: pd.DataFrame, label_column: str, value_column: str, top_n: int = 15) -> None:
    if df.empty or label_column not in df.columns or value_column not in df.columns:
        st.info("No chart data available.")
        return
    chart_df = df[[label_column, value_column]].copy()
    chart_df[value_column] = to_number(chart_df[value_column]).fillna(0)
    chart_df = chart_df.sort_values(value_column, ascending=False).head(top_n)
    st.bar_chart(chart_df.set_index(label_column), width="stretch")


def line_chart(df: pd.DataFrame, date_column: str, value_columns: list[str]) -> None:
    if df.empty or date_column not in df.columns:
        st.info("No trend data available.")
        return

    chart_df = df.copy()
    chart_df[date_column] = pd.to_datetime(chart_df[date_column], errors="coerce")
    chart_df = chart_df.dropna(subset=[date_column]).sort_values(date_column)
    columns = existing_columns(chart_df, value_columns)
    for column in columns:
        chart_df[column] = to_number(chart_df[column]).fillna(0)
    if columns:
        st.line_chart(chart_df.set_index(date_column)[columns], width="stretch")
    else:
        st.info("No trend data available.")


def status_rows(status_df: pd.DataFrame, code: str) -> float:
    if status_df.empty or "statusCode" not in status_df.columns:
        return 0.0
    code_mask = status_df["statusCode"].astype(str) == str(code)
    return sum_col(status_df[code_mask], "rows")


def cache_rollup(cache_df: pd.DataFrame) -> pd.DataFrame:
    if cache_df.empty:
        return cache_df

    out = cache_df.copy()
    out["rows"] = to_number(out["rows"]).fillna(0)
    out["cacheStatus"] = out["cacheStatus"].astype(str)
    grouped = out.groupby("reqHost", as_index=False).agg(rows=("rows", "sum"))
    hit_rows = (
        out[out["cacheStatus"] == "1"]
        .groupby("reqHost", as_index=False)
        .agg(cache_hit_rows=("rows", "sum"))
    )
    grouped = grouped.merge(hit_rows, on="reqHost", how="left").fillna({"cache_hit_rows": 0})
    grouped["cache_hit_pct"] = grouped.apply(lambda row: pct(row["cache_hit_rows"], row["rows"]), axis=1)
    return grouped.sort_values("rows", ascending=False)


def render_downloads(tables: dict[str, pd.DataFrame]) -> None:
    available = {name: df for name, df in tables.items() if not df.empty}
    if not available:
        return

    cols = st.columns(min(4, len(available)))
    for index, (name, df) in enumerate(available.items()):
        with cols[index % len(cols)]:
            st.download_button(
                name,
                df.to_csv(index=False).encode("utf-8"),
                f"{name.lower().replace(' ', '_')}.csv",
                "text/csv",
                width="stretch",
            )


with st.sidebar:
    st.header("Controls")
    lake_input = st.text_input("Lake Folder", value=str(DEFAULT_LAKE_FOLDER))
    profile_input = st.text_input("Profile Folder", value=str(DEFAULT_PROFILE_FOLDER))
    asn_decode_input = st.text_input("ASN Decode CSV", value=str(DEFAULT_ASN_DECODED_CSV))

    min_dt, max_dt = cached_dates(lake_input)
    if min_dt and max_dt:
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("From", value=min_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
        with c2:
            end_date = st.date_input("To", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    else:
        start_date = None
        end_date = None
        st.warning("No lake dates found.")

    if start_date and end_date and start_date > end_date:
        st.error("Start date cannot be after end date.")
        st.stop()

    run_button = st.button("Run Deduped Lake Scan", type="primary", width="stretch")
    inspect_button = st.button("Inspect Lake", width="stretch")
    show_raw_tables = st.checkbox("Show full audit rows", value=False)


if inspect_button:
    with st.spinner("Inspecting lake metadata..."):
        info = inspect_lake(lake_input)
    if "error" in info:
        st.error(info["error"])
    else:
        st.sidebar.success(f"Rows: {info['total_rows']:,}")
        st.sidebar.info(f"Successful .ts rows: {info['valid_ts_rows']:,}")


if run_button:
    with st.spinner("Scanning lake for deduped selected-range metrics..."):
        channel_df, user_df, unmapped_df, raw_channel_df, time_range = cached_metrics(
            lake_input,
            start_date,
            end_date,
        )
    st.session_state.metric_key = f"{lake_input}|{start_date}|{end_date}"
    st.session_state.channel_df = channel_df
    st.session_state.user_df = user_df
    st.session_state.unmapped_df = unmapped_df
    st.session_state.raw_channel_df = raw_channel_df
    st.session_state.time_range = time_range


metric_key = f"{lake_input}|{start_date}|{end_date}"
deduped_ready = st.session_state.get("metric_key") == metric_key and "channel_df" in st.session_state

profile_path = Path(profile_input)
if not profile_path.exists():
    st.error(f"Profile folder not found: {profile_path}")
    st.stop()

channel_summary = profile_csv(profile_input, "channel_summary")
channel_daily = profile_csv(profile_input, "channel_daily")
daily = selected_daily_profile(profile_csv(profile_input, "daily"), start_date, end_date)
status = profile_csv(profile_input, "status")
extensions = profile_csv(profile_input, "extensions")
hosts = profile_csv(profile_input, "hosts")
cache = profile_csv(profile_input, "cache")
errors = profile_csv(profile_input, "errors")
performance = profile_csv(profile_input, "performance")
geo = add_watch_hours_from_ts(decode_columns(profile_csv(profile_input, "geo"), ["state", "city"]))
asn = add_watch_hours_from_ts(enrich_asn(profile_csv(profile_input, "asn"), load_asn_decoded(asn_decode_input)))
ua = decode_columns(profile_csv(profile_input, "ua"), ["UA"])
query_params = profile_csv(profile_input, "query_params")
query_channels = decode_columns(profile_csv(profile_input, "query_channels"), ["raw_channel", "platform", "device_name"])
cmcd = profile_csv(profile_input, "cmcd")
column_fill = profile_csv(profile_input, "column_fill")
files = profile_csv(profile_input, "files")
schema = profile_csv(profile_input, "schema")
mapping_quality = profile_csv(profile_input, "mapping_quality")
unmapped_profile = profile_csv(profile_input, "unmapped")
device = add_watch_hours_from_ts(profile_csv(profile_input, "device"))
profile_channels = selected_channel_profile(channel_summary, channel_daily, start_date, end_date)

if deduped_ready:
    deduped_channels = prepare_deduped_channels(st.session_state.channel_df)
    user_df = st.session_state.user_df
    unmapped_df = st.session_state.unmapped_df
    raw_channel_df = st.session_state.raw_channel_df
else:
    deduped_channels = pd.DataFrame()
    user_df = pd.DataFrame()
    unmapped_df = pd.DataFrame()
    raw_channel_df = pd.DataFrame()


total_rows = sum_col(daily, "rows") or sum_col(status, "rows")
status_200 = sum_col(daily, "status_200_rows") or status_rows(status, "200")
non_200 = sum_col(daily, "non_200_rows") or max(total_rows - status_200, 0)
ts_rows = sum_col(daily, "ts_rows") or sum_col(profile_channels, "raw_ts_chunks")
m3u8_rows = sum_col(daily, "m3u8_rows")
raw_watch_hours = sum_col(profile_channels, "raw_watch_hours") or (ts_rows * CHUNK_DURATION_HOURS)
unmapped_ts_rows = sum_col(unmapped_profile, "raw_ts_chunks")
mapped_coverage = pct(max(ts_rows - unmapped_ts_rows, 0), ts_rows)
profile_start = daily["log_date"].min() if not daily.empty and "log_date" in daily.columns else ""
profile_end = daily["log_date"].max() if not daily.empty and "log_date" in daily.columns else ""


st.title("VgLive Watch Dashboard")
if start_date and end_date:
    st.caption(f"Selected range: {start_date} to {end_date}")
elif profile_start and profile_end:
    st.caption(f"Profile range: {profile_start} to {profile_end}")


overview_tab, channel_tab, mapping_tab, audience_tab, reliability_tab, quality_tab = st.tabs(
    ["Overview", "Channels", "Mapping QA", "Audience", "Reliability", "Data Quality"]
)


with overview_tab:
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric("Raw Watch Hours", fmt_float(raw_watch_hours, 1), help="Status 200 .ts rows multiplied by 6 seconds.")
    with m2:
        if not deduped_channels.empty:
            st.metric("Deduped Watch Hours", fmt_float(sum_col(deduped_channels, "watch_hours"), 1))
        else:
            st.metric("Deduped Watch Hours", "Not scanned")
    with m3:
        st.metric(".ts Chunks", fmt_int(ts_rows))
    with m4:
        st.metric("Mapped Coverage", f"{mapped_coverage:.2f}%")
    with m5:
        st.metric("Non-200 Rate", f"{pct(non_200, total_rows):.2f}%")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("Daily Volume")
        daily_chart = daily.copy()
        if not daily_chart.empty:
            for column in ["rows", "ts_rows", "m3u8_rows", "non_200_rows"]:
                if column in daily_chart.columns:
                    daily_chart[column] = to_number(daily_chart[column]).fillna(0)
        line_chart(daily_chart, "log_date", ["ts_rows", "m3u8_rows", "non_200_rows"])
    with c2:
        st.subheader("Request Mix")
        mix_df = pd.DataFrame(
            [
                {"type": ".ts", "rows": ts_rows},
                {"type": ".m3u8", "rows": m3u8_rows},
                {"type": "non-200", "rows": non_200},
            ]
        )
        bar_chart(mix_df, "type", "rows", top_n=10)

    st.subheader("Top Channels")
    bar_chart(profile_channels, "channel_name", "raw_watch_hours", top_n=12)
    show_table(
        profile_channels.head(15),
        ["channel_name", "raw_watch_hours", "raw_ts_chunks", "approx_unique_ips", "share_pct"],
        {
            "raw_watch_hours": "{:,.1f}",
            "raw_ts_chunks": "{:,.0f}",
            "approx_unique_ips": "{:,.0f}",
            "share_pct": "{:.2f}%",
        },
    )


with channel_tab:
    source_options = ["Precomputed raw profile"]
    if not deduped_channels.empty:
        source_options.insert(0, "Deduped selected range")
    metric_source = st.radio("Channel Metric Source", source_options, horizontal=True)

    if metric_source == "Deduped selected range":
        channel_view = deduped_channels.rename(
            columns={
                "watch_hours": "watch_hours",
                "total_chunks": "chunks",
                "unique_viewers": "viewers",
            }
        )
        value_column = "watch_hours"
        chunk_column = "chunks"
        viewer_column = "viewers"
    else:
        channel_view = profile_channels.rename(
            columns={
                "raw_watch_hours": "watch_hours",
                "raw_ts_chunks": "chunks",
                "approx_unique_ips": "viewers",
            }
        )
        value_column = "watch_hours"
        chunk_column = "chunks"
        viewer_column = "viewers"

    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("Channel Ranking")
        bar_chart(channel_view, "channel_name", value_column, top_n=20)
    with c2:
        st.subheader("Channel Metrics")
        show_table(
            channel_view.head(20),
            ["channel_name", value_column, chunk_column, viewer_column, "share_pct", "avg_watch_hours_per_viewer"],
            {
                value_column: "{:,.1f}",
                chunk_column: "{:,.0f}",
                viewer_column: "{:,.0f}",
                "share_pct": "{:.2f}%",
                "avg_watch_hours_per_viewer": "{:,.2f}",
            },
            height=560,
        )

    if not channel_daily.empty:
        st.subheader("Channel Daily Trend")
        selected_channel = st.selectbox("Channel", profile_channels["channel_name"].tolist())
        trend_df = filter_by_date(channel_daily[channel_daily["channel_name"] == selected_channel], "log_date", start_date, end_date)
        line_chart(trend_df, "log_date", ["raw_watch_hours", "raw_ts_chunks"])

    if not user_df.empty:
        st.subheader("Top Viewers")
        selected_user_channel = st.selectbox("Viewer Channel", deduped_channels["channel_name"].tolist())
        top_users = user_df[user_df["channel_name"] == selected_user_channel].head(50)
        show_table(
            top_users,
            ["cliIP", "watch_hours", "chunks_watched"],
            {"watch_hours": "{:,.2f}", "chunks_watched": "{:,.0f}"},
        )


with mapping_tab:
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Unmapped .ts Candidates", fmt_int(len(unmapped_profile)))
    with m2:
        st.metric("Unmapped Raw Hours", fmt_float(sum_col(unmapped_profile, "raw_watch_hours"), 2))
    with m3:
        mismatch_rows = query_channels[query_channels.get("review_status", pd.Series(dtype=str)) == "query_mapping_mismatch"]
        st.metric("Query Mismatch Rows", fmt_int(len(mismatch_rows)))
    with m4:
        ok_requests = sum_col(query_channels[query_channels.get("review_status", pd.Series(dtype=str)) == "ok"], "requests")
        all_query_requests = sum_col(query_channels, "requests")
        st.metric("Query Match Request Share", f"{pct(ok_requests, all_query_requests):.2f}%")

    st.subheader("QueryStr Review")
    if not query_channels.empty:
        review = query_channels.copy()
        review["requests"] = to_number(review["requests"]).fillna(0)
        review_summary = (
            review.groupby("review_status", as_index=False)
            .agg(
                rows=("review_status", "size"),
                requests=("requests", "sum"),
                sessions=("sessions", "sum"),
                devices=("devices", "sum"),
            )
            .sort_values("requests", ascending=False)
        )
        show_table(
            review_summary,
            ["review_status", "rows", "requests", "sessions", "devices"],
            {"rows": "{:,.0f}", "requests": "{:,.0f}", "sessions": "{:,.0f}", "devices": "{:,.0f}"},
        )

        top_review = review[review["review_status"] != "ok"].head(50)
        show_table(
            top_review,
            [
                "review_status",
                "pure_channel",
                "raw_channel",
                "mapped_channel",
                "reqHost",
                "candidate_id",
                "requests",
                "sessions",
                "sample_reqPath",
            ],
            {"requests": "{:,.0f}", "sessions": "{:,.0f}"},
            height=420,
        )

    st.subheader("Unmapped Candidates")
    combined_unmapped = unmapped_df if not unmapped_df.empty else unmapped_profile
    show_table(
        combined_unmapped,
        ["reqHost", "candidate_id", "raw_ts_chunks", "raw_watch_hours", "raw_chunks", "watch_hours", "unique_viewers", "sample_reqPath"],
        {
            "raw_ts_chunks": "{:,.0f}",
            "raw_watch_hours": "{:,.2f}",
            "raw_chunks": "{:,.0f}",
            "watch_hours": "{:,.2f}",
            "unique_viewers": "{:,.0f}",
        },
    )

    st.subheader("Path And Quality Evidence")
    show_table(
        mapping_quality.head(100 if show_raw_tables else 35),
        ["reqHost", "candidate_id", "channel_name", "quality_bucket", "raw_watch_hours", "raw_ts_chunks", "approx_unique_ips", "sample_reqPath"],
        {"raw_watch_hours": "{:,.1f}", "raw_ts_chunks": "{:,.0f}", "approx_unique_ips": "{:,.0f}"},
        height=520,
    )

    if not raw_channel_df.empty:
        st.subheader("Deduped Mapping Audit")
        audit_df = (
            raw_channel_df.groupby("channel_name", as_index=False)
            .agg(
                raw_ids_mapped=("channel_id", lambda s: ", ".join(sorted(set(map(str, s))))),
                host_count=("reqHost", "nunique"),
                raw_id_count=("channel_id", "nunique"),
                watch_hours=("watch_hours", "sum"),
                deduped_chunks=("total_chunks", "sum"),
                raw_chunks=("raw_chunks", "sum"),
            )
            .sort_values("watch_hours", ascending=False)
        )
        show_table(
            audit_df,
            ["channel_name", "raw_ids_mapped", "host_count", "raw_id_count", "watch_hours", "deduped_chunks", "raw_chunks"],
            {"watch_hours": "{:,.1f}", "deduped_chunks": "{:,.0f}", "raw_chunks": "{:,.0f}"},
            height=420,
        )


with audience_tab:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top Geographies")
        show_table(
            geo.sort_values("watch_hours", ascending=False).head(50),
            ["country", "state", "city", "watch_hours", "approx_unique_ips", "distinct_hosts"],
            {"watch_hours": "{:,.1f}", "approx_unique_ips": "{:,.0f}", "distinct_hosts": "{:,.0f}"},
            height=520,
        )
    with c2:
        st.subheader("Device Mix")
        if not device.empty:
            device_channels = ["All"] + sorted(device["channel_name"].dropna().unique().tolist())
            selected_device_channel = st.selectbox("Device Channel", device_channels)
            device_view = device if selected_device_channel == "All" else device[device["channel_name"] == selected_device_channel]
            show_table(
                device_view.sort_values("watch_hours", ascending=False).head(50),
                ["channel_name", "device_type", "watch_hours", "approx_unique_ips"],
                {"watch_hours": "{:,.1f}", "approx_unique_ips": "{:,.0f}"},
                height=520,
            )

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Top ASNs")
        if not asn.empty and "as_name" in asn.columns:
            decoded_count = int((asn["as_name"].fillna("") != asn["asn"].map(lambda value: f"AS{value}")).sum())
            st.caption(f"Decoded ASN names available for {decoded_count:,} of {len(asn):,} profiled ASN rows.")
        show_table(
            asn.sort_values("watch_hours", ascending=False).head(40),
            [
                "asn",
                "as_name",
                "asn_type",
                "as_country",
                "watch_hours",
                "approx_unique_ips",
                "distinct_hosts",
                "sample_reqHost",
            ],
            {"watch_hours": "{:,.1f}", "approx_unique_ips": "{:,.0f}", "distinct_hosts": "{:,.0f}"},
            height=520,
        )
    with c4:
        st.subheader("Top User Agents")
        show_table(
            ua.head(40),
            ["UA", "rows", "approx_unique_ips", "distinct_hosts"],
            {"rows": "{:,.0f}", "approx_unique_ips": "{:,.0f}", "distinct_hosts": "{:,.0f}"},
            height=520,
        )


with reliability_tab:
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("Status Codes")
        status_view = status.copy()
        if not status_view.empty:
            total_status_rows = sum_col(status_view, "rows")
            status_view["row_pct"] = to_number(status_view["rows"]).fillna(0).map(lambda value: pct(value, total_status_rows))
        show_table(
            status_view.head(20),
            ["statusCode", "rows", "row_pct", "approx_unique_ips", "sample_reqPath"],
            {"rows": "{:,.0f}", "row_pct": "{:.2f}%", "approx_unique_ips": "{:,.0f}"},
            height=460,
        )
    with c2:
        st.subheader("Errors By Host")
        show_table(
            errors.head(50),
            ["reqHost", "statusCode", "errorCode", "startupError", "rows", "approx_unique_ips", "sample_reqPath"],
            {"rows": "{:,.0f}", "approx_unique_ips": "{:,.0f}"},
            height=460,
        )

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Cache By Host")
        cache_view = cache_rollup(cache)
        show_table(
            cache_view,
            ["reqHost", "rows", "cache_hit_rows", "cache_hit_pct"],
            {"rows": "{:,.0f}", "cache_hit_rows": "{:,.0f}", "cache_hit_pct": "{:.2f}%"},
            height=460,
        )
    with c4:
        st.subheader("Host Overview")
        host_view = hosts.copy()
        if not host_view.empty:
            host_view["non_200_pct"] = host_view.apply(
                lambda row: pct(float(row.get("non_200_rows", 0)), float(row.get("rows", 0))),
                axis=1,
            )
        show_table(
            host_view.head(40),
            ["reqHost", "rows", "status_200_rows", "non_200_rows", "non_200_pct", "ts_rows", "approx_unique_ips"],
            {
                "rows": "{:,.0f}",
                "status_200_rows": "{:,.0f}",
                "non_200_rows": "{:,.0f}",
                "non_200_pct": "{:.2f}%",
                "ts_rows": "{:,.0f}",
                "approx_unique_ips": "{:,.0f}",
            },
            height=460,
        )

    st.subheader("Performance By Host And Extension")
    perf_view = performance.copy()
    if not perf_view.empty:
        perf_view["rows"] = to_number(perf_view["rows"]).fillna(0)
        perf_view["ttfb_p95_ms"] = to_number(perf_view["ttfb_p95_ms"]).fillna(0)
        perf_view = perf_view.sort_values(["extension", "ttfb_p95_ms"], ascending=[True, False])
    show_table(
        perf_view.head(60),
        [
            "reqHost",
            "extension",
            "rows",
            "ttfb_p50_ms",
            "ttfb_p95_ms",
            "transfer_p50_ms",
            "transfer_p95_ms",
            "throughput_p50",
            "throughput_p05",
        ],
        {
            "rows": "{:,.0f}",
            "ttfb_p50_ms": "{:,.1f}",
            "ttfb_p95_ms": "{:,.1f}",
            "transfer_p50_ms": "{:,.1f}",
            "transfer_p95_ms": "{:,.1f}",
            "throughput_p50": "{:,.1f}",
            "throughput_p05": "{:,.1f}",
        },
        height=520,
    )


with quality_tab:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Column Fill Rate")
        fill_view = column_fill.copy()
        if not fill_view.empty:
            fill_view["non_empty_pct"] = to_number(fill_view["non_empty_pct"]).fillna(0)
            fill_view = fill_view.sort_values("non_empty_pct")
        show_table(
            fill_view,
            ["column_name", "total_rows", "non_empty_rows", "empty_rows", "non_empty_pct"],
            {
                "total_rows": "{:,.0f}",
                "non_empty_rows": "{:,.0f}",
                "empty_rows": "{:,.0f}",
                "non_empty_pct": "{:.4f}%",
            },
            height=560,
        )
    with c2:
        st.subheader("Extensions")
        show_table(
            extensions,
            ["extension", "rows", "approx_unique_ips", "sample_reqPath"],
            {"rows": "{:,.0f}", "approx_unique_ips": "{:,.0f}"},
            height=260,
        )

        st.subheader("QueryStr Coverage")
        show_table(
            query_params,
            [
                "rows_with_querystr",
                "channel_rows",
                "channel_name_rows",
                "session_rows",
                "device_id_rows",
                "platform_rows",
                "device_rows",
                "content_title_rows",
            ],
            {
                "rows_with_querystr": "{:,.0f}",
                "channel_rows": "{:,.0f}",
                "channel_name_rows": "{:,.0f}",
                "session_rows": "{:,.0f}",
                "device_id_rows": "{:,.0f}",
                "platform_rows": "{:,.0f}",
                "device_rows": "{:,.0f}",
                "content_title_rows": "{:,.0f}",
            },
            height=170,
        )

        st.subheader("CMCD Coverage")
        show_table(
            cmcd,
            ["rows_with_cmcd", "br_rows", "duration_rows", "measured_throughput_rows", "object_type_rows", "session_id_rows"],
            {
                "rows_with_cmcd": "{:,.0f}",
                "br_rows": "{:,.0f}",
                "duration_rows": "{:,.0f}",
                "measured_throughput_rows": "{:,.0f}",
                "object_type_rows": "{:,.0f}",
                "session_id_rows": "{:,.0f}",
            },
            height=150,
        )

    st.subheader("Lake Files And Schema")
    file_tab, schema_tab = st.tabs(["Files", "Schema"])
    with file_tab:
        show_table(
            files.head(200 if show_raw_tables else 50),
            ["date", "file", "size_mb"],
            {"size_mb": "{:,.3f}"},
            height=520,
        )
    with schema_tab:
        show_table(schema, height=520)

    st.subheader("Exports")
    render_downloads(
        {
            "Channel Summary": profile_channels,
            "Query Review": query_channels,
            "Mapping Quality": mapping_quality,
            "Status Codes": status,
            "Errors": errors,
        }
    )
