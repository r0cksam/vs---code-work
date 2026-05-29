#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote_plus
from zoneinfo import ZoneInfo

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEFAULT_PROFILE_DIR = ROOT / "vglive_channel_profile" / "deep_profile_full"
DEFAULT_OUT = Path(r"Y:\Veto Logs Backup\Dashboards\Watch Hours\veto_watch_hours.html")
DEFAULT_ASN_DECODED_CSV = ROOT / "4.asn" / "asnDecoded.csv"
CHARTJS_CACHE = HERE / ".chartjs_cache.js"
CHUNK_DURATION_HOURS = 6 / 3600.0
IST = ZoneInfo("Asia/Kolkata")

try:
    from vglive_core import DEFAULT_LAKE_FOLDER
except Exception:
    DEFAULT_LAKE_FOLDER = Path(r"D:\Veto Logs Backup\veto Stream logs\lake")


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
    "status": "status_codes.csv",
    "unmapped": "unmapped_candidates.csv",
    "ua": "ua_top.csv",
}

COUNTRY_LABELS = {
    "AE": "United Arab Emirates",
    "AU": "Australia",
    "BD": "Bangladesh",
    "BH": "Bahrain",
    "BR": "Brazil",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "GB": "United Kingdom",
    "IN": "India",
    "JP": "Japan",
    "NL": "Netherlands",
    "NP": "Nepal",
    "OM": "Oman",
    "SG": "Singapore",
    "US": "United States",
}

STATUS_CODE_MEANINGS = {
    "200": "OK: request succeeded.",
    "206": "Partial Content: byte-range response, commonly used for media segment delivery.",
    "301": "Moved Permanently: client should use the redirected URL.",
    "302": "Found: temporary redirect.",
    "304": "Not Modified: cache validation response; payload not sent again.",
    "400": "Bad Request: malformed or invalid request.",
    "401": "Unauthorized: authentication is required or failed.",
    "403": "Forbidden: server understood the request but refused access.",
    "404": "Not Found: requested object was not available.",
    "408": "Request Timeout: client did not complete the request in time.",
    "429": "Too Many Requests: rate limit or throttling response.",
    "500": "Internal Server Error: origin/server-side failure.",
    "501": "Not Implemented: method or feature not supported by server.",
    "502": "Bad Gateway: upstream server returned an invalid response.",
    "503": "Service Unavailable: server or upstream temporarily unavailable.",
    "504": "Gateway Timeout: upstream did not respond in time.",
}


def read_csv(profile_dir: Path, key: str) -> pd.DataFrame:
    path = profile_dir / PROFILE_FILES[key]
    parquet_path = path.with_suffix(".parquet")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def read_store_table(profile_dir: Path, table_name: str) -> pd.DataFrame:
    path = profile_dir.parent / "etl_store" / "tables" / f"{table_name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def lake_partition_files(lake: Path, date_text: str) -> list[Path]:
    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError:
        return []
    candidates = [
        lake / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}",
        lake / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}",
    ]
    files: list[Path] = []
    for folder in candidates:
        if folder.exists():
            files.extend(sorted(folder.glob("*.parquet")))
    return files


def resolve_inventory_file(path_text: object, lake: Path = DEFAULT_LAKE_FOLDER) -> Path:
    path = Path(str(path_text))
    if path.exists():
        return path
    normalized = str(path_text).replace("\\", "/")
    marker = "/year="
    if marker in normalized:
        rel = normalized.split(marker, 1)[1]
        return lake / ("year=" + rel.replace("/", "\\"))
    return path


def inventory_files_for_date(files: pd.DataFrame, date_text: str) -> list[Path]:
    if files.empty or "date" not in files.columns or "file" not in files.columns:
        return []
    day_files = files[files["date"].astype(str) == date_text]["file"].tolist()
    return [resolve_inventory_file(path) for path in day_files]


def min_max_req_epoch(files: list[Path]) -> tuple[float | None, float | None]:
    if not files:
        return None, None
    try:
        import pyarrow.parquet as pq
    except Exception:
        return None, None
    lo: float | None = None
    hi: float | None = None
    for file in files:
        try:
            pf = pq.ParquetFile(file)
            if "reqTimeSec" not in pf.schema_arrow.names:
                continue
            for batch in pf.iter_batches(columns=["reqTimeSec"], batch_size=1_000_000):
                values = pd.to_numeric(batch.column(0).to_pandas(), errors="coerce").dropna()
                if values.empty:
                    continue
                b_min = float(values.min())
                b_max = float(values.max())
                lo = b_min if lo is None else min(lo, b_min)
                hi = b_max if hi is None else max(hi, b_max)
        except Exception:
            continue
    return lo, hi


def format_epoch_ist(epoch: float | None) -> str:
    if epoch is None:
        return ""
    return (datetime.fromtimestamp(epoch, IST) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")


def true_data_range(file_dates: list[str], files: pd.DataFrame, lake: Path = DEFAULT_LAKE_FOLDER) -> dict[str, str]:
    if not file_dates:
        return {"first": "", "last": ""}
    first_files = inventory_files_for_date(files, file_dates[0]) or lake_partition_files(lake, file_dates[0])
    last_files = inventory_files_for_date(files, file_dates[-1]) or lake_partition_files(lake, file_dates[-1])
    first_min, _ = min_max_req_epoch(first_files)
    _, last_max = min_max_req_epoch(last_files)
    return {
        "first": format_epoch_ist(first_min) or f"{file_dates[0]} 00:00:00",
        "last": format_epoch_ist(last_max) or f"{file_dates[-1]} 23:59:59",
    }


def numeric(df: pd.DataFrame, column: str) -> pd.Series:
    if df.empty or column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def total(df: pd.DataFrame, column: str) -> float:
    if df.empty or column not in df.columns:
        return 0.0
    return float(numeric(df, column).sum())


def pct(part: float, whole: float) -> float:
    return float(part / whole * 100.0) if whole else 0.0


def decode_cols(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = out[column].fillna("").astype(str).map(unquote_plus)
    return out


def normalize_asn(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if text.startswith("AS"):
        text = text[2:].strip()
    if text.endswith(".0"):
        text = text[:-2]
    return "".join(ch for ch in text if ch.isdigit())


def load_asn_decoded(path: Path = DEFAULT_ASN_DECODED_CSV) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "asn" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["asn"] = df["asn"].map(normalize_asn)
    keep = [c for c in ["asn", "as_name", "as_country", "as_domain", "asn_type", "lookup_status"] if c in df.columns]
    return df[keep].drop_duplicates("asn")


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


def records(df: pd.DataFrame, columns: list[str] | None = None, limit: int | None = None) -> list[dict]:
    if df.empty:
        return []
    out = df.copy()
    if columns:
        out = out[[col for col in columns if col in out.columns]]
    if limit:
        out = out.head(limit)
    return json.loads(out.to_json(orient="records", date_format="iso"))


def one_row(df: pd.DataFrame) -> dict:
    rows = records(df, limit=1)
    return rows[0] if rows else {}


def with_numbers(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = numeric(out, column)
    return out


def add_watch_hours_from_ts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not out.empty and "ts_rows" in out.columns:
        out["watch_hours"] = numeric(out, "ts_rows") * CHUNK_DURATION_HOURS
    return out


def add_status_meanings(status_df: pd.DataFrame) -> pd.DataFrame:
    if status_df.empty or "statusCode" not in status_df.columns:
        return status_df
    out = status_df.copy()
    out["statusCode"] = out["statusCode"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    out["meaning"] = out["statusCode"].map(lambda code: STATUS_CODE_MEANINGS.get(code, "Status code was present in logs but is not in the built-in reference list."))
    return out


def country_label(value: object) -> str:
    code = "" if pd.isna(value) else str(value).strip().upper()
    if not code:
        return "Unknown"
    name = COUNTRY_LABELS.get(code)
    if code == "IN":
        return "India"
    return f"{name} ({code})" if name else code


def geo_text(value: object, fallback: str) -> str:
    if pd.isna(value):
        return fallback
    text = str(value).strip()
    return text if text else fallback


def aggregate_geo_rows(df: pd.DataFrame, group_cols: list[str], total_watch_hours: float, limit: int | None = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(group_cols, as_index=False, dropna=False)
        .agg(watch_hours=("watch_hours", "sum"), approx_unique_ips=("approx_unique_ips", "sum"))
        .sort_values("watch_hours", ascending=False)
    )
    grouped["share_pct"] = grouped["watch_hours"].map(lambda v: pct(v, total_watch_hours))
    return grouped.head(limit) if limit else grouped


def build_geo_breakdown(geo: pd.DataFrame) -> dict:
    if geo.empty:
        return {
            "country_split": [],
            "india_regions": [],
            "india_cities": [],
            "other_regions": [],
            "other_cities": [],
        }

    out = geo.copy()
    for column in ["country", "state", "city", "watch_hours", "approx_unique_ips"]:
        if column not in out.columns:
            out[column] = 0 if column in ["watch_hours", "approx_unique_ips"] else ""
    out["country_code"] = out["country"].fillna("").astype(str).str.strip().str.upper()
    out["country_label"] = out["country_code"].map(country_label)
    out["state_region"] = out["state"].map(lambda value: geo_text(value, "Unknown Region"))
    out["city_name"] = out["city"].map(lambda value: geo_text(value, ""))

    total_hours = total(out, "watch_hours")
    india = out[out["country_code"] == "IN"].copy()
    other = out[out["country_code"] != "IN"].copy()

    country_split = pd.DataFrame(
        [
            {
                "segment": "India",
                "watch_hours": total(india, "watch_hours"),
                "approx_unique_ips": total(india, "approx_unique_ips"),
            },
            {
                "segment": "Other Countries",
                "watch_hours": total(other, "watch_hours"),
                "approx_unique_ips": total(other, "approx_unique_ips"),
            },
        ]
    )
    country_split["share_pct"] = country_split["watch_hours"].map(lambda v: pct(v, total_hours))

    india_regions = aggregate_geo_rows(india, ["state_region"], total(india, "watch_hours"), 30)
    india_cities = aggregate_geo_rows(india[india["city_name"] != ""], ["city_name", "state_region"], total(india, "watch_hours"), 30)
    other_regions = aggregate_geo_rows(other, ["country_label", "state_region"], total(other, "watch_hours"), 40)
    other_cities = aggregate_geo_rows(other[other["city_name"] != ""], ["country_label", "city_name", "state_region"], total(other, "watch_hours"), 40)

    return {
        "country_split": records(country_split.sort_values("watch_hours", ascending=False), ["segment", "watch_hours", "share_pct", "approx_unique_ips"]),
        "india_regions": records(india_regions, ["state_region", "watch_hours", "share_pct", "approx_unique_ips"]),
        "india_cities": records(india_cities, ["city_name", "state_region", "watch_hours", "share_pct", "approx_unique_ips"]),
        "other_regions": records(other_regions, ["country_label", "state_region", "watch_hours", "share_pct", "approx_unique_ips"]),
        "other_cities": records(other_cities, ["country_label", "city_name", "state_region", "watch_hours", "share_pct", "approx_unique_ips"]),
    }


def build_channel_daily_series(channel_daily: pd.DataFrame, top_channels: list[str]) -> list[dict]:
    if channel_daily.empty or not top_channels:
        return []
    out = with_numbers(channel_daily, ["raw_watch_hours", "raw_ts_chunks"])
    out = out[out["channel_name"].isin(top_channels)].copy()
    out["log_date"] = pd.to_datetime(out["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["log_date"])
    series = []
    for channel_name in top_channels:
        rows = out[out["channel_name"] == channel_name].sort_values("log_date")
        series.append(
            {
                "channel": channel_name,
                "points": records(rows, ["log_date", "raw_watch_hours"], limit=None),
            }
        )
    return series


def build_cache_rollup(cache: pd.DataFrame) -> pd.DataFrame:
    if cache.empty:
        return cache
    out = with_numbers(cache, ["rows", "approx_unique_ips"])
    out["cacheStatus"] = out["cacheStatus"].astype(str)
    grouped = out.groupby("reqHost", as_index=False).agg(rows=("rows", "sum"))
    hits = (
        out[out["cacheStatus"] == "1"]
        .groupby("reqHost", as_index=False)
        .agg(cache_hit_rows=("rows", "sum"))
    )
    grouped = grouped.merge(hits, on="reqHost", how="left").fillna({"cache_hit_rows": 0})
    grouped["cache_hit_pct"] = grouped.apply(lambda r: pct(r["cache_hit_rows"], r["rows"]), axis=1)
    return grouped.sort_values("rows", ascending=False)


def build_query_review(query_channels: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    if query_channels.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    out = with_numbers(query_channels, ["requests", "sessions", "devices", "unique_viewers"])
    summary = (
        out.groupby("review_status", as_index=False)
        .agg(
            rows=("review_status", "size"),
            requests=("requests", "sum"),
            sessions=("sessions", "sum"),
            devices=("devices", "sum"),
        )
        .sort_values("requests", ascending=False)
    )
    non_ok = out[out["review_status"] != "ok"].sort_values("requests", ascending=False)
    ok_requests = total(out[out["review_status"] == "ok"], "requests")
    all_requests = total(out, "requests")
    stats = {
        "review_rows": int(len(out)),
        "query_review_requests": all_requests,
        "query_ok_requests": ok_requests,
        "query_ok_request_pct": pct(ok_requests, all_requests),
        "non_ok_rows": int(len(non_ok)),
    }
    return summary, non_ok, stats


def build_report_data(profile_dir: Path, title: str) -> dict:
    channel_summary = with_numbers(
        read_csv(profile_dir, "channel_summary"),
        ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "approx_unique_ips", "approx_distinct_segments", "distinct_hosts"],
    ).sort_values("raw_watch_hours", ascending=False)
    channel_daily = read_csv(profile_dir, "channel_daily")
    daily = with_numbers(
        read_csv(profile_dir, "daily"),
        ["rows", "status_200_rows", "non_200_rows", "raw_ts_rows", "status_200_ts_rows", "ts_rows", "m3u8_rows", "approx_unique_ips"],
    )
    status = add_status_meanings(with_numbers(read_csv(profile_dir, "status"), ["rows", "approx_unique_ips"]))
    extensions = with_numbers(read_csv(profile_dir, "extensions"), ["rows", "approx_unique_ips"])
    hosts = with_numbers(read_csv(profile_dir, "hosts"), ["rows", "status_200_rows", "non_200_rows", "ts_rows", "approx_unique_ips"])
    cache = read_csv(profile_dir, "cache")
    errors = with_numbers(read_csv(profile_dir, "errors"), ["rows", "approx_unique_ips"])
    performance = with_numbers(
        read_csv(profile_dir, "performance"),
        ["rows", "ttfb_p50_ms", "ttfb_p95_ms", "transfer_p50_ms", "transfer_p95_ms", "throughput_p50", "throughput_p05"],
    )
    geo = add_watch_hours_from_ts(decode_cols(with_numbers(read_csv(profile_dir, "geo"), ["ts_rows", "approx_unique_ips", "distinct_hosts"]), ["state", "city"]))
    device = add_watch_hours_from_ts(with_numbers(read_csv(profile_dir, "device"), ["ts_rows", "approx_unique_ips"]))
    asn = add_watch_hours_from_ts(enrich_asn(
        with_numbers(read_csv(profile_dir, "asn"), ["ts_rows", "approx_unique_ips", "distinct_hosts"]),
        load_asn_decoded(),
    ))
    ua = decode_cols(with_numbers(read_csv(profile_dir, "ua"), ["rows", "approx_unique_ips", "distinct_hosts"]), ["UA"])
    query_params = read_csv(profile_dir, "query_params")
    query_channels = decode_cols(read_csv(profile_dir, "query_channels"), ["raw_channel", "platform", "device_name"])
    cmcd = read_csv(profile_dir, "cmcd")
    column_fill = with_numbers(read_csv(profile_dir, "column_fill"), ["total_rows", "non_empty_rows", "empty_rows", "non_empty_pct"])
    mapping_quality = with_numbers(read_csv(profile_dir, "mapping_quality"), ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "approx_unique_ips"])
    unmapped = with_numbers(read_csv(profile_dir, "unmapped"), ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "approx_unique_ips"])
    files = with_numbers(read_csv(profile_dir, "files"), ["size_mb"])

    total_rows = total(daily, "rows") or total(status, "rows")
    status_200_rows = total(daily, "status_200_rows")
    if not status_200_rows and not status.empty:
        status_200_rows = total(status[status["statusCode"].astype(str) == "200"], "rows")
    non_200_rows = total(daily, "non_200_rows") or max(total_rows - status_200_rows, 0)
    if not daily.empty:
        if "raw_ts_rows" not in daily.columns:
            daily["raw_ts_rows"] = numeric(daily, "ts_rows")
        if "status_200_ts_rows" not in daily.columns:
            daily["status_200_ts_rows"] = numeric(daily, "ts_rows")
    if not channel_summary.empty:
        if "status_200_ts_chunks" not in channel_summary.columns:
            channel_summary["status_200_ts_chunks"] = numeric(channel_summary, "raw_ts_chunks")
        if "status_200_watch_hours" not in channel_summary.columns:
            channel_summary["status_200_watch_hours"] = numeric(channel_summary, "raw_watch_hours")

    raw_ts_rows = total(daily, "raw_ts_rows") or total(channel_summary, "raw_ts_chunks")
    status_200_ts_rows = total(daily, "status_200_ts_rows") or total(channel_summary, "status_200_ts_chunks")
    ts_rows = raw_ts_rows
    m3u8_rows = total(daily, "m3u8_rows")
    raw_watch_hours = total(channel_summary, "raw_watch_hours") or (raw_ts_rows * CHUNK_DURATION_HOURS)
    status_200_watch_hours = total(channel_summary, "status_200_watch_hours") or (status_200_ts_rows * CHUNK_DURATION_HOURS)
    unmapped_ts_rows = total(unmapped, "raw_ts_chunks")
    mapped_coverage = pct(max(ts_rows - unmapped_ts_rows, 0), ts_rows)
    approx_unique_ips = 0.0
    if not status.empty and "statusCode" in status.columns:
        approx_unique_ips = total(status[status["statusCode"].astype(str) == "200"], "approx_unique_ips")
    if not approx_unique_ips:
        approx_unique_ips = numeric(daily, "approx_unique_ips").max() if not daily.empty else 0

    if not channel_summary.empty:
        channel_summary["share_pct"] = channel_summary["raw_watch_hours"].map(lambda v: pct(v, raw_watch_hours))
    if not daily.empty:
        daily["raw_watch_hours"] = daily["raw_ts_rows"] * CHUNK_DURATION_HOURS
        daily["status_200_watch_hours"] = daily["status_200_ts_rows"] * CHUNK_DURATION_HOURS
        daily["non_200_pct"] = daily.apply(lambda r: pct(r["non_200_rows"], r["rows"]), axis=1)
        daily["log_date"] = pd.to_datetime(daily["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        daily = daily.dropna(subset=["log_date"])
    if not channel_daily.empty:
        channel_daily = with_numbers(channel_daily, ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "approx_unique_ips"])
        if "status_200_ts_chunks" not in channel_daily.columns:
            channel_daily["status_200_ts_chunks"] = numeric(channel_daily, "raw_ts_chunks")
        if "status_200_watch_hours" not in channel_daily.columns:
            channel_daily["status_200_watch_hours"] = numeric(channel_daily, "raw_watch_hours")
        channel_daily["log_date"] = pd.to_datetime(channel_daily["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        channel_daily = channel_daily.dropna(subset=["log_date"])
    if not status.empty:
        status["row_pct"] = status["rows"].map(lambda v: pct(v, total_rows))
    if not hosts.empty:
        hosts["non_200_pct"] = hosts.apply(lambda r: pct(r["non_200_rows"], r["rows"]), axis=1)

    cache_rollup = build_cache_rollup(cache)
    query_summary, query_non_ok, query_stats = build_query_review(query_channels)
    for frame in [mapping_quality, unmapped]:
        if not frame.empty:
            if "status_200_ts_chunks" not in frame.columns:
                frame["status_200_ts_chunks"] = numeric(frame, "raw_ts_chunks")
            if "status_200_watch_hours" not in frame.columns:
                frame["status_200_watch_hours"] = numeric(frame, "raw_watch_hours")

    top_channels = channel_summary["channel_name"].head(8).tolist() if not channel_summary.empty else []
    channel_series = build_channel_daily_series(channel_daily, top_channels)
    low_fill = column_fill.sort_values("non_empty_pct").head(18) if not column_fill.empty else pd.DataFrame()
    perf_ts = performance[performance["extension"].astype(str).str.lower() == "ts"] if not performance.empty and "extension" in performance.columns else performance
    perf_slowest = (
        perf_ts.sort_values("ttfb_p95_ms", ascending=False)
        if not perf_ts.empty and "ttfb_p95_ms" in perf_ts.columns
        else pd.DataFrame()
    )

    query_param_row = one_row(query_params)
    cmcd_row = one_row(cmcd)
    file_dates = sorted(files["date"].dropna().astype(str).unique().tolist()) if not files.empty and "date" in files.columns else []
    true_range = true_data_range(file_dates, files)
    daily_table_names = [
        "status_codes_daily",
        "extensions_daily",
        "hosts_daily",
        "geo_daily",
        "asn_daily",
        "cache_daily",
        "errors_daily",
        "query_params_daily",
        "cmcd_daily",
        "user_agents_daily",
        "device_type_by_channel_daily",
        "mapping_quality_daily",
        "unmapped_candidates_daily",
    ]
    daily_tables = {
        name: records(read_store_table(profile_dir, name))
        for name in daily_table_names
    }

    return {
        "title": title,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "profile_dir": str(profile_dir),
        "date_range": {
            "first": file_dates[0] if file_dates else "",
            "last": file_dates[-1] if file_dates else "",
        },
        "true_data_range": true_range,
        "status_meanings": STATUS_CODE_MEANINGS,
        "country_labels": COUNTRY_LABELS,
        "definitions": [
            "Raw watch hours = all .ts segment rows multiplied by 6 seconds, independent of HTTP status.",
            "Status 200 watch hours = HTTP 200 .ts segment rows multiplied by 6 seconds; this is shown separately from raw hours.",
            ".m3u8 rows are playlist and evidence rows; they are not counted as watch hours.",
            "HTTP status code meanings are listed in Reliability > Status Codes.",
            "queryStr channel values are used as mapping QA evidence, not as the primary watch-hour source.",
            "Approx unique IP metrics use pre-aggregated profile counts and can overlap across channels or days.",
        ],
        "summary": {
            "total_rows": total_rows,
            "status_200_rows": status_200_rows,
            "non_200_rows": non_200_rows,
            "non_200_pct": pct(non_200_rows, total_rows),
            "ts_rows": ts_rows,
            "raw_ts_rows": raw_ts_rows,
            "status_200_ts_rows": status_200_ts_rows,
            "m3u8_rows": m3u8_rows,
            "raw_watch_hours": raw_watch_hours,
            "status_200_watch_hours": status_200_watch_hours,
            "active_channels": int(len(channel_summary[channel_summary["channel_name"] != "Other"])) if not channel_summary.empty else 0,
            "approx_unique_ips": approx_unique_ips,
            "mapped_coverage_pct": mapped_coverage,
            "unmapped_candidates": int(len(unmapped)),
            "query_ok_request_pct": query_stats.get("query_ok_request_pct", 0.0),
            "rows_with_querystr": float(query_param_row.get("rows_with_querystr", 0) or 0),
            "rows_with_cmcd": float(cmcd_row.get("rows_with_cmcd", 0) or 0),
        },
        "daily": records(daily, ["log_date", "rows", "status_200_rows", "non_200_rows", "raw_ts_rows", "status_200_ts_rows", "ts_rows", "m3u8_rows", "approx_unique_ips", "raw_watch_hours", "status_200_watch_hours", "non_200_pct"]),
        "daily_tables": daily_tables,
        "channels": records(channel_summary, ["channel_name", "raw_watch_hours", "status_200_watch_hours", "approx_unique_ips", "share_pct", "first_seen", "last_seen"]),
        "channel_daily_all": records(channel_daily, ["log_date", "channel_name", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips"]),
        "channel_series": channel_series,
        "status": records(status.sort_values("rows", ascending=False), ["statusCode", "meaning", "rows", "row_pct", "approx_unique_ips", "sample_reqPath"], 20),
        "extensions": records(extensions.sort_values("rows", ascending=False), ["extension", "rows", "approx_unique_ips", "sample_reqPath"], 20),
        "hosts": records(hosts.sort_values("rows", ascending=False), ["reqHost", "rows", "status_200_rows", "non_200_rows", "non_200_pct", "ts_rows", "approx_unique_ips"], 25),
        "cache": records(cache_rollup, ["reqHost", "rows", "cache_hit_rows", "cache_hit_pct"], 25),
        "errors": records(errors.sort_values("rows", ascending=False), ["reqHost", "statusCode", "errorCode", "startupError", "rows", "approx_unique_ips", "sample_reqPath"], 30),
        "performance": records(perf_slowest, ["reqHost", "extension", "rows", "ttfb_p50_ms", "ttfb_p95_ms", "transfer_p95_ms", "throughput_p50", "throughput_p05"], 30),
        "geo": build_geo_breakdown(geo),
        "device": records(device.sort_values("watch_hours", ascending=False), ["channel_name", "device_type", "watch_hours", "approx_unique_ips"], 30),
        "asn": records(
            asn.sort_values("watch_hours", ascending=False),
            ["asn", "as_name", "as_country", "asn_type", "watch_hours", "approx_unique_ips", "distinct_hosts", "sample_reqHost"],
            25,
        ),
        "ua": records(ua.sort_values("rows", ascending=False), ["UA", "rows", "approx_unique_ips", "distinct_hosts"], 20),
        "query_summary": records(query_summary, ["review_status", "rows", "requests", "sessions", "devices"]),
        "query_non_ok": records(query_non_ok, ["review_status", "pure_channel", "raw_channel", "mapped_channel", "reqHost", "candidate_id", "requests", "sessions", "sample_reqPath"], 30),
        "unmapped": records(unmapped, ["reqHost", "candidate_id", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips", "sample_reqPath"], 50),
        "mapping_quality": records(mapping_quality.sort_values("raw_watch_hours", ascending=False), ["reqHost", "candidate_id", "channel_name", "quality_bucket", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips", "sample_reqPath"], 40),
        "column_fill": records(low_fill, ["column_name", "total_rows", "non_empty_rows", "empty_rows", "non_empty_pct"]),
        "query_params": query_param_row,
        "cmcd": cmcd_row,
    }


def load_chartjs() -> str:
    if CHARTJS_CACHE.exists():
        return CHARTJS_CACHE.read_text(encoding="utf-8")
    return "window.Chart=null;"


def render_html(data: dict) -> str:
    chartjs = load_chartjs()
    data_blob = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{data["title"]}</title>
<script>{chartjs}</script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{--ink:#172033;--muted:#667085;--line:#d8e1ee;--surface:#eef3f8;--panel:#fff;--brand:#173b5c;--teal:#0f766e;--gold:#b58517;--red:#b42318;--green:#2f6b2f;--violet:#5b55a3}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;background:var(--surface);color:var(--ink);min-height:100vh;-webkit-text-size-adjust:100%}}
.topbar{{background:var(--brand);color:#fff;border-bottom:4px solid var(--gold);padding:18px 28px;display:flex;align-items:center;justify-content:space-between;gap:18px;flex-wrap:wrap}}
.topbar h1{{font-size:21px;font-weight:800;letter-spacing:0}}
.topbar .sub{{font-size:12px;color:#dbe7f2;display:flex;flex-direction:column;gap:3px;text-align:right;max-width:min(980px,72vw);line-height:1.35}}
.wrap{{max-width:1760px;margin:0 auto;padding:22px 28px 34px}}
.note{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:11px 14px;color:#475569;font-size:12px;line-height:1.5;margin-bottom:16px}}
.note-bottom{{margin:18px 0 0}}
.filter-bar{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:12px 14px;margin-bottom:16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;box-shadow:0 1px 2px rgba(15,23,42,.05);min-width:0}}
.date-filter{{padding:11px 12px;border-top:3px solid var(--gold);gap:8px;overflow-x:auto;white-space:nowrap;flex-wrap:nowrap}}
.filter-title{{font-size:11px;font-weight:850;text-transform:uppercase;color:#475569;letter-spacing:.04em;margin-right:4px}}
.filter-btn{{border:1px solid #cbd5e1;background:#fff;color:#334155;border-radius:5px;padding:5px 10px;font-size:11px;font-weight:800;cursor:pointer;transition:all .2s}}
.filter-btn:hover{{background:#edf2f7;color:var(--brand)}}
.filter-btn.active{{background:var(--brand);border-color:var(--brand);color:#fff}}
.filter-bar input[type="date"]{{border:1px solid #cbd5e1;border-radius:4px;padding:5px 6px;font-size:11px;color:var(--brand);font-family:inherit;font-weight:600;min-width:116px;outline:none;cursor:pointer}}
.channel-select{{border:1px solid #cbd5e1;background:#fff;color:var(--brand);border-radius:5px;padding:5px 8px;font-size:11px;font-weight:800;outline:none;cursor:pointer;min-width:190px}}
.channel-select.compare-count{{min-width:112px;display:none}}
.channel-select.compare-select{{min-width:150px;display:none}}
.channel-select:hover{{background:#edf2f7;border-color:#94a3b8}}
.channel-select:focus{{border-color:var(--brand);box-shadow:0 0 0 2px rgba(23,59,92,.12)}}
.filter-sep{{width:1px;height:24px;background:#d8e1ee;flex:0 0 auto}}
.filter-range{{margin-left:auto;font-size:12px;color:#64748b;font-weight:700}}
.cards{{display:flex;flex-wrap:wrap;justify-content:center;gap:10px;margin-bottom:18px}}
.card{{flex:0 1 calc((100% - 60px)/7);min-width:0;background:var(--panel);border:1px solid var(--line);border-top:3px solid var(--brand);border-radius:8px;padding:12px 14px;text-align:center;box-shadow:0 1px 2px rgba(15,23,42,.05)}}
.card.teal{{border-top-color:var(--teal)}}.card.gold{{border-top-color:var(--gold)}}.card.red{{border-top-color:var(--red)}}.card.green{{border-top-color:var(--green)}}.card.violet{{border-top-color:var(--violet)}}
.label{{font-size:10px;color:var(--muted);font-weight:800;text-transform:uppercase;letter-spacing:.04em;margin-bottom:7px;line-height:1.15;min-height:23px;text-align:center}}
.value{{font-size:25px;font-weight:850;line-height:1.05;color:#172033}}
.small{{font-size:11px;color:var(--muted);margin-top:5px}}
.tabs{{display:flex;gap:6px;flex-wrap:wrap;margin:12px 0 18px}}
.tab-btn{{border:1px solid #cbd5e1;background:#fff;color:#334155;border-radius:7px;padding:8px 12px;font-size:12px;font-weight:750;cursor:pointer}}
.tab-btn.active{{background:var(--brand);color:#fff;border-color:var(--brand)}}
.tab{{display:none}}.tab.active{{display:block}}
.grid-2{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,430px),1fr));gap:16px;margin-bottom:16px}}
.grid-3{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,330px),1fr));gap:16px;margin-bottom:16px}}
.panel{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:15px 17px;box-shadow:0 1px 2px rgba(15,23,42,.05);min-width:0}}
.panel h2{{font-size:14px;margin-bottom:11px;color:#24324a;display:flex;align-items:center;justify-content:space-between;gap:10px}}
.panel h2 span{{font-size:11px;color:#94a3b8;font-weight:600}}
.count-chip{{margin-left:auto;border:1px solid #d8e1ee;background:#f8fafc;color:#475569!important;border-radius:999px;padding:3px 8px;font-size:10px!important;font-weight:850!important;white-space:nowrap}}
.chart-actions{{display:flex;align-items:center;gap:8px;flex-shrink:0}}
.chart-sub{{font-size:11px;color:#94a3b8;font-weight:600}}
.expand-btn{{border:1px solid #cbd5e1;background:#fff;color:var(--brand);border-radius:5px;padding:3px 8px;font-size:10px;font-weight:800;cursor:pointer}}
.expand-btn:hover{{background:#edf2f7;border-color:#94a3b8}}
.chart-box{{height:clamp(240px,30vh,360px)}}.chart-box.channel-trend{{height:clamp(280px,36vh,440px)}}.chart-box.tall{{height:clamp(320px,42vh,500px)}}.chart-box.channel-rank{{height:clamp(440px,58vh,720px)}}
canvas{{width:100%!important;height:100%!important}}
.table-wrap{{overflow:auto;max-height:520px;border:1px solid #e2e8f0;border-radius:7px;background:#fff}}
.chart-box + .table-wrap{{margin-top:12px;max-height:260px}}
table{{width:100%;border-collapse:separate;border-spacing:0;font-size:11px;min-width:760px}}
th{{position:sticky;top:0;z-index:2;background:#f8fafc;color:#475569;font-size:10px;text-align:left;padding:8px;border-bottom:1px solid #dbe3ef;white-space:nowrap}}
td{{padding:7px 8px;border-bottom:1px solid #eef2f7;vertical-align:top;color:#1f2937;line-height:1.25}}
tr:hover td{{background:#f6f9ff}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.pill{{display:inline-block;border-radius:999px;border:1px solid #cbd5e1;padding:3px 8px;font-size:10px;font-weight:800;background:#fff;color:#475569}}
.pill.good{{border-color:#8fce9f;color:#166534;background:#f0fdf4}}.pill.warn{{border-color:#fdba74;color:#9a3412;background:#fff7ed}}.pill.bad{{border-color:#fca5a5;color:#991b1b;background:#fef2f2}}
.metric-strip{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin-bottom:12px}}
.mini{{border:1px solid #e2e8f0;border-radius:7px;padding:8px 10px;background:#fbfcfe}}
.mini b{{display:block;font-size:10px;text-transform:uppercase;color:#64748b;margin-bottom:3px}}.mini span{{font-size:17px;font-weight:850}}
.footer{{font-size:11px;color:#94a3b8;text-align:center;padding:20px 0 4px}}
.modal-backdrop{{position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:50;display:none;align-items:center;justify-content:center;padding:22px}}
.modal-backdrop.open{{display:flex}}
.chart-modal{{width:min(1180px,96vw);height:min(760px,92vh);background:#fff;border-radius:10px;border:1px solid #d8e1ee;box-shadow:0 24px 80px rgba(15,23,42,.28);display:flex;flex-direction:column;overflow:hidden}}
.modal-top{{padding:14px 18px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;gap:14px;background:#f8fafc}}
.modal-title{{font-size:16px;font-weight:800;color:var(--brand)}}
.modal-close{{border:1px solid #cbd5e1;background:#fff;color:#334155;border-radius:6px;padding:5px 10px;font-weight:800;cursor:pointer}}
.modal-body{{padding:14px 18px 18px;display:flex;flex-direction:column;gap:10px;min-height:0;flex:1}}
.modal-controls{{display:flex;align-items:center;gap:8px;flex-wrap:wrap}}
.modal-controls input[type="date"]{{border:1px solid #cbd5e1;border-radius:6px;padding:6px 8px;font-size:12px;color:#334155;font-family:inherit}}
.modal-range{{font-size:11px;color:#64748b;font-weight:700}}
.modal-stats{{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:8px}}
.modal-chart-wrap{{min-height:0;flex:1;border:1px solid #e2e8f0;border-radius:8px;padding:12px;background:#fff}}
.modal-chart-wrap canvas{{width:100%!important;height:100%!important}}
@media(max-width:1450px){{.wrap{{padding:18px 20px 30px}}.topbar{{padding:16px 22px}}.cards{{gap:8px}}.card{{flex-basis:calc((100% - 24px)/4);padding:10px 9px}}.label{{font-size:9.5px}}.value{{font-size:21px}}.grid-2,.grid-3{{gap:12px}}.panel{{padding:13px 14px}}.chart-box{{height:clamp(235px,30vh,330px)}}.chart-box.channel-trend{{height:clamp(270px,35vh,400px)}}.chart-box.tall{{height:clamp(300px,40vh,460px)}}.chart-box.channel-rank{{height:clamp(420px,56vh,640px)}}}}
@media(max-width:1180px){{.card{{flex-basis:calc((100% - 24px)/4)}}.topbar .sub{{max-width:100%;text-align:left}}}}
@media(min-width:1900px){{.wrap{{max-width:1880px}}.chart-box{{height:clamp(300px,31vh,390px)}}.chart-box.channel-trend{{height:clamp(340px,38vh,480px)}}.chart-box.tall{{height:clamp(380px,44vh,540px)}}.chart-box.channel-rank{{height:clamp(560px,62vh,780px)}}.table-wrap{{max-height:620px}}}}
@media(min-width:2200px){{.wrap{{max-width:2200px}}.chart-box{{height:clamp(330px,32vh,440px)}}.chart-box.channel-trend{{height:clamp(390px,40vh,540px)}}.chart-box.tall{{height:clamp(430px,46vh,610px)}}.chart-box.channel-rank{{height:clamp(640px,64vh,860px)}}.chart-modal{{width:min(1500px,94vw);height:min(900px,92vh)}}}}
@media(max-width:900px){{.wrap{{padding:16px}}.card{{flex-basis:calc((100% - 16px)/3)}}.grid-2,.grid-3{{grid-template-columns:1fr}}.metric-strip,.modal-stats{{grid-template-columns:repeat(2,minmax(0,1fr))}}.topbar .sub{{text-align:left;max-width:100%}}.filter-range{{margin-left:0;width:100%}}.date-filter{{width:100%;overflow-x:visible;white-space:normal;flex-wrap:wrap}}.chart-box{{height:240px}}.chart-box.channel-rank{{height:520px}}.chart-actions{{gap:6px}}}}
@media(max-width:680px){{.card{{flex-basis:calc((100% - 8px)/2)}}}}
@media print{{.tabs,.tab-btn{{display:none}}.tab{{display:block}}body{{background:#fff}}.panel,.card,.note{{box-shadow:none}}.table-wrap{{max-height:none;overflow:visible}}}}
</style>
</head>
<body>
<div class="topbar">
  <h1 id="reportTitle"></h1>
  <div class="sub"><span id="rangeText"></span><span id="genText"></span></div>
</div>
<div class="wrap">
  <div class="filter-bar date-filter">
    <span class="filter-title">Date Range</span>
    <button class="filter-btn active" id="presetAll" onclick="setDatePreset('all')">ALL</button>
    <button class="filter-btn" id="preset30" onclick="setDatePreset(30)">30D</button>
    <button class="filter-btn" id="preset7" onclick="setDatePreset(7)">7D</button>
    <span class="filter-sep"></span>
    <input type="date" id="dateStart" onchange="applyCustomDateRange()" />
    <span style="font-size:12px;color:#64748b;font-weight:700">to</span>
    <input type="date" id="dateEnd" onchange="applyCustomDateRange()" />
    <span class="filter-range" id="selectedRange"></span>
  </div>
  <div class="cards" id="summaryCards"></div>
  <div class="tabs" id="tabs"></div>

  <section class="tab active" id="overview">
    <div class="filter-bar">
      <span class="filter-title">Channel Charts</span>
      <button class="filter-btn active" id="channelAll" onclick="setChannelLimit('all')">All</button>
      <button class="filter-btn" id="channelTop15" onclick="setChannelLimit(15)">Top 15</button>
      <button class="filter-btn" id="channelTop10" onclick="setChannelLimit(10)">Top 10</button>
      <span class="filter-sep"></span>
      <span class="filter-title">Trend Focus</span>
      <select class="channel-select" id="trendFocus" onchange="setTrendFocus(this.value)"></select>
      <button class="filter-btn" id="trendCompareBtn" onclick="setTrendFocus('compare')">Compare</button>
      <select class="channel-select compare-count" id="compareCount" onchange="setCompareCount(this.value)"></select>
      <select class="channel-select compare-select" id="compareCh0" onchange="setCompareChannel(0, this.value)"></select>
      <select class="channel-select compare-select" id="compareCh1" onchange="setCompareChannel(1, this.value)"></select>
      <select class="channel-select compare-select" id="compareCh2" onchange="setCompareChannel(2, this.value)"></select>
      <select class="channel-select compare-select" id="compareCh3" onchange="setCompareChannel(3, this.value)"></select>
      <select class="channel-select compare-select" id="compareCh4" onchange="setCompareChannel(4, this.value)"></select>
      <span class="filter-range" id="channelLimitText"></span>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>Daily Volume <span class="chart-actions"><span class="chart-sub">rows, .ts, .m3u8, errors</span><button class="expand-btn" onclick="openChartModal('dailyChart')">Expand</button></span></h2><div class="chart-box"><canvas id="dailyChart"></canvas></div></div>
      <div class="panel"><h2>Channel Trend <span class="chart-actions"><span class="chart-sub">all channel watch hours</span><button class="expand-btn" onclick="openChartModal('channelTrend')">Expand</button></span></h2><div class="chart-box channel-trend"><canvas id="channelTrend"></canvas></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>All Channels <span class="chart-actions"><span class="chart-sub">long-tail watch hours</span><button class="expand-btn" onclick="openChartModal('channelBar')">Expand</button></span></h2><div class="chart-box tall channel-rank" id="channelBarWrap"><canvas id="channelBar"></canvas></div></div>
      <div class="panel"><h2>Channel Summary <span class="count-chip" id="channelCount"></span></h2><div class="table-wrap" id="channelTable"></div></div>
    </div>
  </section>

  <section class="tab" id="mapping">
    <div class="grid-2">
      <div class="panel"><h2>Mapping QA Summary <span class="count-chip" id="querySummaryCount"></span></h2><div class="metric-strip" id="mappingMetrics"></div><div class="table-wrap" id="querySummaryTable"></div></div>
      <div class="panel"><h2>QueryStr Mismatches And Review Items <span class="count-chip" id="queryNonOkCount"></span></h2><div class="table-wrap" id="queryNonOkTable"></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>Unmapped Candidates <span class="count-chip" id="unmappedCount"></span></h2><div class="table-wrap" id="unmappedTable"></div></div>
      <div class="panel"><h2>Path And Quality Evidence <span class="count-chip" id="mappingQualityCount"></span></h2><div class="table-wrap" id="mappingQualityTable"></div></div>
    </div>
  </section>

  <section class="tab" id="audience">
    <div class="grid-2">
      <div class="panel"><h2>Country Split <span class="count-chip" id="geoCountryCount"></span></h2><div class="table-wrap" id="geoCountryTable"></div></div>
      <div class="panel"><h2>Device Mix <span class="count-chip" id="deviceCount"></span></h2><div class="table-wrap" id="deviceTable"></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>India State / Region <span class="count-chip" id="geoIndiaRegionCount"></span></h2><div class="table-wrap" id="geoIndiaRegionTable"></div></div>
      <div class="panel"><h2>India City <span class="count-chip" id="geoIndiaCityCount"></span></h2><div class="table-wrap" id="geoIndiaCityTable"></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>Other Countries State / Region <span class="count-chip" id="geoOtherRegionCount"></span></h2><div class="table-wrap" id="geoOtherRegionTable"></div></div>
      <div class="panel"><h2>Other Countries City <span class="count-chip" id="geoOtherCityCount"></span></h2><div class="table-wrap" id="geoOtherCityTable"></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>Top ASNs <span class="count-chip" id="asnCount"></span></h2><div class="table-wrap" id="asnTable"></div></div>
      <div class="panel"><h2>Top User Agents <span class="count-chip" id="uaCount"></span></h2><div class="table-wrap" id="uaTable"></div></div>
    </div>
  </section>

  <section class="tab" id="reliability">
    <div class="grid-3">
      <div class="panel"><h2>Status Codes <span class="count-chip" id="statusCount"></span></h2><div class="chart-box"><canvas id="statusChart"></canvas></div><div class="table-wrap" id="statusTable"></div></div>
      <div class="panel"><h2>Cache By Host <span class="count-chip" id="cacheCount"></span></h2><div class="table-wrap" id="cacheTable"></div></div>
      <div class="panel"><h2>Host Overview <span class="count-chip" id="hostCount"></span></h2><div class="table-wrap" id="hostTable"></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>Top Errors <span class="count-chip" id="errorCount"></span></h2><div class="table-wrap" id="errorTable"></div></div>
      <div class="panel"><h2>Slowest .ts Hosts By p95 TTFB <span class="count-chip" id="performanceCount"></span></h2><div class="table-wrap" id="performanceTable"></div></div>
    </div>
  </section>

  <section class="tab" id="quality">
    <div class="grid-2">
      <div class="panel"><h2>Lowest Column Fill Rates <span class="count-chip" id="fillCount"></span></h2><div class="table-wrap" id="fillTable"></div></div>
      <div class="panel"><h2>Extension Mix <span class="count-chip" id="extensionCount"></span></h2><div class="table-wrap" id="extensionTable"></div></div>
    </div>
    <div class="grid-2">
      <div class="panel"><h2>QueryStr Coverage <span class="count-chip" id="queryParamCount"></span></h2><div class="table-wrap" id="queryParamTable"></div></div>
      <div class="panel"><h2>CMCD Coverage <span class="count-chip" id="cmcdCount"></span></h2><div class="table-wrap" id="cmcdTable"></div></div>
    </div>
  </section>

  <div class="note note-bottom" id="definitionNote"></div>
  <div class="footer">Generated as a standalone HTML file from the VgLive deep profile CSVs.</div>
</div>
<div class="modal-backdrop" id="chartModal">
  <div class="chart-modal">
    <div class="modal-top">
      <div class="modal-title" id="modalTitle">Chart</div>
      <button class="modal-close" onclick="closeChartModal()">Close</button>
    </div>
    <div class="modal-body">
      <div class="modal-controls">
        <button class="filter-btn" id="modalDashboard" onclick="setModalPreset('dashboard')">Dashboard Range</button>
        <button class="filter-btn" id="modalAll" onclick="setModalPreset('all')">ALL</button>
        <button class="filter-btn" id="modal30" onclick="setModalPreset(30)">30D</button>
        <button class="filter-btn" id="modal7" onclick="setModalPreset(7)">7D</button>
        <select class="channel-select" id="modalTrendFocus" onchange="setTrendFocus(this.value)"></select>
        <button class="filter-btn" id="modalTrendCompareBtn" onclick="setTrendFocus('compare')">Compare</button>
        <select class="channel-select compare-count" id="modalCompareCount" onchange="setCompareCount(this.value)"></select>
        <select class="channel-select compare-select" id="modalCompareCh0" onchange="setCompareChannel(0, this.value)"></select>
        <select class="channel-select compare-select" id="modalCompareCh1" onchange="setCompareChannel(1, this.value)"></select>
        <select class="channel-select compare-select" id="modalCompareCh2" onchange="setCompareChannel(2, this.value)"></select>
        <select class="channel-select compare-select" id="modalCompareCh3" onchange="setCompareChannel(3, this.value)"></select>
        <select class="channel-select compare-select" id="modalCompareCh4" onchange="setCompareChannel(4, this.value)"></select>
        <input type="date" id="modalStart" onchange="setModalCustom()" />
        <span style="font-size:12px;color:#64748b;font-weight:700">to</span>
        <input type="date" id="modalEnd" onchange="setModalCustom()" />
      </div>
      <div class="modal-range" id="modalRange"></div>
      <div class="modal-stats" id="modalStats"></div>
      <div class="modal-chart-wrap"><canvas id="modalChart"></canvas></div>
    </div>
  </div>
</div>
<script>
const D = {data_blob};
const COLORS = ['#173b5c','#0f766e','#b58517','#5b55a3','#2f6b2f','#b42318','#1f6f8b','#7c5800','#6b4ab6','#64748b'];
const ALL_DAILY_RAW = (D.daily || []).slice().sort((a,b) => String(a.log_date).localeCompare(String(b.log_date)));
const ALL_DAILY = ALL_DAILY_RAW.length > 2 ? ALL_DAILY_RAW.slice(1, -1) : ALL_DAILY_RAW.slice();
const USABLE_DATES = new Set(ALL_DAILY.map(r => String(r.log_date)));
const ALL_CHANNEL_DAILY_RAW = (D.channel_daily_all || []).slice().sort((a,b) => String(a.log_date).localeCompare(String(b.log_date)));
const ALL_CHANNEL_DAILY = ALL_CHANNEL_DAILY_RAW.filter(r => USABLE_DATES.has(String(r.log_date)));
const DAILY_TABLES_RAW = D.daily_tables || {{}};
const DAILY_TABLES = Object.fromEntries(Object.entries(DAILY_TABLES_RAW).map(([key, rows]) => [
  key,
  (rows || []).filter(r => !r.log_date || USABLE_DATES.has(String(r.log_date)))
]));
const CHUNK_HOURS = 6 / 3600;
let viewDaily = ALL_DAILY.slice();
let viewChannels = [];
let chartChannelLimit = 'all';
let trendFocus = 'all';
let compareCount = 2;
let compareChannels = [];
let modalChartSourceId = null;
let modalDaily = [];
const chartInstances = {{}};
const fmt = (n, d=0) => n == null || isNaN(n) ? '-' : Number(n).toLocaleString('en-IN', {{minimumFractionDigits:d, maximumFractionDigits:d}});
const fmtPct = n => n == null || isNaN(n) ? '-' : Number(n).toFixed(2) + '%';
const fmtH = n => fmt(n, 1);
const compactAxis = v => v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':fmt(v, v < 10 && v > 0 ? 1 : 0);
const longTailValue = v => Math.log10(Math.max(0, Number(v || 0)) + 1);
const longTailRaw = v => Math.max(0, Math.pow(10, Number(v || 0)) - 1);
const longTailAxis = v => compactAxis(longTailRaw(v));
const chartColor = (i, total=10) => `hsl(${{Math.round((i * 360) / Math.max(total, 1))}}, 62%, 38%)`;
function longTailTooltip(ctx) {{
  const ds = ctx.dataset || {{}};
  const raw = ds.originalData ? ds.originalData[ctx.dataIndex] : 0;
  return `${{ds.label || 'Value'}}: ${{fmtH(raw)}}`;
}}
const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));

function fmtDateTime12(value) {{
  const m = String(value || '').trim().match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})(?:[ T](\\d{{2}}):(\\d{{2}})(?::(\\d{{2}}))?)?/);
  if(!m) return String(value || '');
  let hh = Number(m[4] || 0);
  const ap = hh >= 12 ? 'PM' : 'AM';
  hh = hh % 12 || 12;
  return `${{m[3]}}-${{m[2]}}-${{String(m[1]).slice(2)}} ${{String(hh).padStart(2,'0')}}:${{m[5] || '00'}}:${{m[6] || '00'}} ${{ap}}`;
}}

function metric(label, value, sub, cls='') {{
  return `<div class="card ${{cls}}"><div class="label">${{label}}</div><div class="value">${{value}}</div><div class="small">${{sub || ''}}</div></div>`;
}}

function mini(label, value) {{
  return `<div class="mini"><b>${{label}}</b><span>${{value}}</span></div>`;
}}

const sum = (rows, key) => rows.reduce((acc, r) => acc + Number(r[key] || 0), 0);
const max = (rows, key) => rows.reduce((acc, r) => Math.max(acc, Number(r[key] || 0)), 0);
const percent = (part, whole) => whole ? (part / whole * 100) : 0;

function selectedDateSet(dailyRows=viewDaily) {{
  return new Set((dailyRows || []).map(r => String(r.log_date)));
}}

function rowsForDates(rows, dailyRows=viewDaily) {{
  const dates = selectedDateSet(dailyRows);
  return (rows || []).filter(r => dates.has(String(r.log_date)));
}}

function groupRows(rows, keys, sumKeys=[], maxKeys=[], firstKeys=[]) {{
  const buckets = new Map();
  (rows || []).forEach(row => {{
    const id = keys.map(k => String(row[k] ?? '')).join('||');
    if(!buckets.has(id)) {{
      const item = {{}};
      keys.forEach(k => item[k] = row[k] ?? '');
      sumKeys.forEach(k => item[k] = 0);
      maxKeys.forEach(k => item[k] = 0);
      firstKeys.forEach(k => item[k] = row[k] ?? '');
      buckets.set(id, item);
    }}
    const item = buckets.get(id);
    sumKeys.forEach(k => item[k] += Number(row[k] || 0));
    maxKeys.forEach(k => item[k] = Math.max(Number(item[k] || 0), Number(row[k] || 0)));
    firstKeys.forEach(k => {{
      if((item[k] == null || item[k] === '') && row[k] != null && row[k] !== '') item[k] = row[k];
    }});
  }});
  return [...buckets.values()];
}}

function topRows(rows, sortKey, limit=30) {{
  return (rows || []).slice().sort((a,b) => Number(b[sortKey] || 0) - Number(a[sortKey] || 0)).slice(0, limit);
}}

function oneDailyRow(rows, sumKeys=[], firstKeys=[], dailyRows=viewDaily) {{
  const selected = rowsForDates(rows, dailyRows);
  const out = {{}};
  sumKeys.forEach(k => out[k] = sum(selected, k));
  firstKeys.forEach(k => out[k] = selected.find(r => r[k] != null && r[k] !== '')?.[k] || '');
  return out;
}}

function countryLabel(value) {{
  const code = String(value || '').trim().toUpperCase();
  if(!code) return 'Unknown';
  const name = (D.country_labels || {{}})[code];
  if(code === 'IN') return 'India';
  return name ? `${{name}} (${{code}})` : code;
}}

function geoText(value, fallback) {{
  const text = String(value ?? '').trim();
  return text || fallback;
}}

function statusMeaning(value) {{
  const code = String(value ?? '').replace(/\\.0$/, '');
  return (D.status_meanings || {{}})[code] || 'Status code was present in logs but is not in the built-in reference list.';
}}

function aggregateChannels(dailyRows) {{
  if(!ALL_CHANNEL_DAILY.length) return (D.channels || []).slice();
  const dates = new Set(dailyRows.map(r => r.log_date));
  const buckets = new Map();
  ALL_CHANNEL_DAILY.forEach(r => {{
    if(!dates.has(r.log_date)) return;
    const key = r.channel_name || 'Unknown';
    const item = buckets.get(key) || {{channel_name:key, raw_watch_hours:0, status_200_watch_hours:0, raw_ts_chunks:0, status_200_ts_chunks:0, approx_unique_ips:0}};
    item.raw_watch_hours += Number(r.raw_watch_hours || 0);
    item.status_200_watch_hours += Number(r.status_200_watch_hours || 0);
    item.raw_ts_chunks += Number(r.raw_ts_chunks || 0);
    item.status_200_ts_chunks += Number(r.status_200_ts_chunks || 0);
    item.approx_unique_ips = Math.max(item.approx_unique_ips, Number(r.approx_unique_ips || 0));
    buckets.set(key, item);
  }});
  const rows = [...buckets.values()].sort((a,b) => b.raw_watch_hours - a.raw_watch_hours);
  const totalHours = sum(rows, 'raw_watch_hours');
  rows.forEach(r => r.share_pct = percent(r.raw_watch_hours, totalHours));
  return rows;
}}

function selectedSummary() {{
  const totalRows = sum(viewDaily, 'rows');
  const non200Rows = sum(viewDaily, 'non_200_rows');
  const rawHours = sum(viewDaily, 'raw_watch_hours') || sum(viewChannels, 'raw_watch_hours');
  const status200Hours = sum(viewDaily, 'status_200_watch_hours') || sum(viewChannels, 'status_200_watch_hours');
  const rawTsRows = sum(viewDaily, 'raw_ts_rows') || sum(viewDaily, 'ts_rows');
  const unmappedRows = aggregateUnmapped();
  const unmappedTsRows = sum(unmappedRows, 'raw_ts_chunks');
  const queryParams = aggregateQueryParams();
  return {{
    raw_watch_hours: rawHours,
    status_200_watch_hours: status200Hours,
    active_channels: viewChannels.filter(c => c.channel_name !== 'Other' && Number(c.raw_watch_hours || 0) > 0).length,
    approx_unique_ips: max(viewDaily, 'approx_unique_ips'),
    non_200_rows: non200Rows,
    non_200_pct: percent(non200Rows, totalRows),
    mapped_coverage_pct: percent(Math.max(rawTsRows - unmappedTsRows, 0), rawTsRows),
    query_ok_request_pct: D.summary.query_ok_request_pct,
    unmapped_candidates: unmappedRows.length,
    rows_with_querystr: queryParams.rows_with_querystr || 0
  }};
}}

function setActivePreset(id) {{
  ['presetAll','preset30','preset7'].forEach(btnId => {{
    const el = document.getElementById(btnId);
    if(el) el.classList.toggle('active', btnId === id);
  }});
}}

function setActiveChannelLimit(id) {{
  ['channelTop10','channelTop15','channelAll'].forEach(btnId => {{
    const el = document.getElementById(btnId);
    if(el) el.classList.toggle('active', btnId === id);
  }});
}}

function chartChannels() {{
  return chartChannelsFrom(viewChannels || []);
}}

function chartChannelsFrom(rows) {{
  const source = rows || [];
  if(chartChannelLimit === 'all') return source;
  return source.slice(0, Number(chartChannelLimit));
}}

function selectableChannels(rows) {{
  return (rows || viewChannels || []).filter(c => c.channel_name && c.channel_name !== 'Other');
}}

function ensureCompareDefaults(rows) {{
  const channels = selectableChannels(rows);
  if(!channels.length) return;
  compareCount = Math.max(2, Math.min(5, Number(compareCount) || 2, Math.max(2, channels.length)));
  const available = channels.map(c => c.channel_name);
  const chosen = [];
  for(let i = 0; i < compareCount; i++) {{
    let current = compareChannels[i];
    if(!current || !available.includes(current) || chosen.includes(current)) {{
      current = available.find(name => !chosen.includes(name)) || '';
    }}
    compareChannels[i] = current;
    if(current) chosen.push(current);
  }}
  compareChannels = compareChannels.slice(0, 5);
}}

function selectedCompareNames(rows) {{
  ensureCompareDefaults(rows);
  return compareChannels.slice(0, compareCount).filter(Boolean);
}}

function trendChannelsFrom(rows) {{
  const source = rows || [];
  if(trendFocus === 'all') return source;
  if(trendFocus === 'compare') {{
    const names = selectedCompareNames(source);
    return names.map(name => source.find(c => c.channel_name === name)).filter(Boolean);
  }}
  if(String(trendFocus).startsWith('channel:')) {{
    const channelName = decodeURIComponent(String(trendFocus).slice(8));
    return source.filter(c => c.channel_name === channelName);
  }}
  const limit = Number(String(trendFocus).replace('top', '')) || 10;
  return source.slice(0, limit);
}}

function trendFocusText() {{
  if(trendFocus === 'all') return 'Trend: All channels';
  if(trendFocus === 'compare') {{
    const names = selectedCompareNames(viewChannels);
    return `Compare ${{names.length}}: ${{names.join(' vs ')}}`;
  }}
  if(String(trendFocus).startsWith('channel:')) return 'Trend: ' + decodeURIComponent(String(trendFocus).slice(8));
  return 'Trend: Top ' + (Number(String(trendFocus).replace('top', '')) || 10);
}}

function renderTrendFocusOptions() {{
  if(trendFocus === 'compare') ensureCompareDefaults(viewChannels);
  const baseOptions = [
    ['all', 'All channels'],
    ['compare', 'Compare channels'],
    ['top15', 'Top 15'],
    ['top10', 'Top 10']
  ];
  const channels = selectableChannels(viewChannels);
  const channelOptions = channels.map(c => ['channel:' + encodeURIComponent(c.channel_name), c.channel_name]);
  const html = baseOptions.concat(channelOptions)
    .map(([value, label]) => `<option value="${{escapeHtml(value)}}"${{value === trendFocus ? ' selected' : ''}}>${{escapeHtml(label)}}</option>`)
    .join('');
  ['trendFocus','modalTrendFocus'].forEach(id => {{
    const el = document.getElementById(id);
    if(el) el.innerHTML = html;
  }});
  ['trendCompareBtn','modalTrendCompareBtn'].forEach(id => {{
    const el = document.getElementById(id);
    if(el) el.classList.toggle('active', trendFocus === 'compare');
  }});
  const maxCompare = Math.min(5, Math.max(2, channels.length || 2));
  const countHtml = Array.from({{length:maxCompare - 1}}, (_, i) => i + 2)
    .map(n => `<option value="${{n}}"${{n === compareCount ? ' selected' : ''}}>${{n}} channels</option>`)
    .join('');
  ['compareCount','modalCompareCount'].forEach(id => {{
    const el = document.getElementById(id);
    if(el) el.innerHTML = countHtml;
  }});
  const compareHtml = (current, label) => [`<option value="">${{label}}</option>`].concat(
    channels.map(c => `<option value="${{escapeHtml(c.channel_name)}}"${{c.channel_name === current ? ' selected' : ''}}>${{escapeHtml(c.channel_name)}}</option>`)
  ).join('');
  for(let i = 0; i < 5; i++) {{
    [['compareCh' + i, compareChannels[i], 'Channel ' + (i + 1)],
     ['modalCompareCh' + i, compareChannels[i], 'Channel ' + (i + 1)]].forEach(([id, current, label]) => {{
      const el = document.getElementById(id);
      if(el) el.innerHTML = compareHtml(current, label);
    }});
  }}
  const showCompare = trendFocus === 'compare';
  ['compareCount'].forEach(id => {{
    const el = document.getElementById(id);
    if(el) el.style.display = showCompare ? '' : 'none';
  }});
  for(let i = 0; i < 5; i++) {{
    const el = document.getElementById('compareCh' + i);
    if(el) el.style.display = showCompare && i < compareCount ? '' : 'none';
  }}
  const showModalCompare = showCompare && modalChartSourceId === 'channelTrend';
  ['modalCompareCount'].forEach(id => {{
    const el = document.getElementById(id);
    if(el) el.style.display = showModalCompare ? '' : 'none';
  }});
  for(let i = 0; i < 5; i++) {{
    const el = document.getElementById('modalCompareCh' + i);
    if(el) el.style.display = showModalCompare && i < compareCount ? '' : 'none';
  }}
}}

function updateChannelLimitText() {{
  const total = (viewChannels || []).length;
  const shown = chartChannelLimit === 'all' ? total : Math.min(Number(chartChannelLimit), total);
  const label = chartChannelLimit === 'all' ? 'All' : `Top ${{chartChannelLimit}}`;
  const el = document.getElementById('channelLimitText');
  if(el) el.textContent = `${{label}}: ${{shown}} of ${{total}} in ranking | ${{trendFocusText()}}`;
}}

function setChannelLimit(limit) {{
  chartChannelLimit = limit;
  setActiveChannelLimit(limit === 'all' ? 'channelAll' : limit === 10 ? 'channelTop10' : 'channelTop15');
  renderCharts();
  updateChannelLimitText();
}}

function setTrendFocus(value) {{
  trendFocus = value || 'all';
  if(trendFocus === 'compare') ensureCompareDefaults(viewChannels);
  renderTrendFocusOptions();
  renderCharts();
  updateChannelLimitText();
  if(modalChartSourceId === 'channelTrend') renderModalChart();
}}

function setCompareCount(value) {{
  compareCount = Math.max(2, Math.min(5, Number(value) || 2));
  trendFocus = 'compare';
  ensureCompareDefaults(viewChannels);
  renderTrendFocusOptions();
  renderCharts();
  updateChannelLimitText();
  if(modalChartSourceId === 'channelTrend') renderModalChart();
}}

function setCompareChannel(index, value) {{
  compareChannels[index] = value;
  trendFocus = 'compare';
  ensureCompareDefaults(viewChannels);
  renderTrendFocusOptions();
  renderCharts();
  updateChannelLimitText();
  if(modalChartSourceId === 'channelTrend') renderModalChart();
}}

function syncDateInputs() {{
  document.getElementById('dateStart').value = viewDaily.length ? viewDaily[0].log_date : '';
  document.getElementById('dateEnd').value = viewDaily.length ? viewDaily[viewDaily.length - 1].log_date : '';
  document.getElementById('selectedRange').textContent = viewDaily.length
    ? `${{viewDaily[0].log_date}} to ${{viewDaily[viewDaily.length - 1].log_date}} | ${{viewDaily.length}} days`
    : 'No data in selected range';
}}

function currentRangeText() {{
  return document.getElementById('selectedRange')?.textContent || '';
}}

function setActiveModalPreset(id) {{
  ['modalDashboard','modalAll','modal30','modal7'].forEach(btnId => {{
    const el = document.getElementById(btnId);
    if(el) el.classList.toggle('active', btnId === id);
  }});
}}

function setModalInputBounds() {{
  if(!ALL_DAILY.length) return;
  ['modalStart','modalEnd'].forEach(id => {{
    const el = document.getElementById(id);
    if(!el) return;
    el.min = ALL_DAILY[0].log_date;
    el.max = ALL_DAILY[ALL_DAILY.length - 1].log_date;
  }});
}}

function syncModalInputs() {{
  document.getElementById('modalStart').value = modalDaily.length ? modalDaily[0].log_date : '';
  document.getElementById('modalEnd').value = modalDaily.length ? modalDaily[modalDaily.length - 1].log_date : '';
  document.getElementById('modalRange').textContent = modalDaily.length
    ? `${{modalDaily[0].log_date}} to ${{modalDaily[modalDaily.length - 1].log_date}} | ${{modalDaily.length}} days`
    : 'No data in selected modal range';
}}

function renderModalStats() {{
  const daily = modalDaily || [];
  const channels = aggregateChannels(daily);
  const totalRows = sum(daily, 'rows');
  const non200Rows = sum(daily, 'non_200_rows');
  const rawHours = sum(daily, 'raw_watch_hours') || sum(channels, 'raw_watch_hours');
  const status200Hours = sum(daily, 'status_200_watch_hours') || sum(channels, 'status_200_watch_hours');
  const activeChannels = channels.filter(c => c.channel_name !== 'Other' && Number(c.raw_watch_hours || 0) > 0).length;
  const focusChannels = modalChartSourceId === 'channelTrend' ? trendChannelsFrom(channels) : channels;
  const topChannel = focusChannels[0] || channels[0] || {{}};
  let cards;
  if(modalChartSourceId === 'dailyChart') {{
    cards = [
      mini('Raw Watch Hours', fmtH(rawHours)),
      mini('Status 200 Hours', fmtH(status200Hours)),
      mini('Non-200 Rate', fmtPct(percent(non200Rows, totalRows))),
      mini('Max Approx IPs', fmt(max(daily, 'approx_unique_ips')))
    ];
  }} else {{
    cards = [
      mini('Top Channel', escapeHtml(topChannel.channel_name || '-')),
      mini('Top Channel Hours', fmtH(topChannel.raw_watch_hours || 0)),
      mini('Active Channels', fmt(activeChannels)),
      mini('Range Raw Hours', fmtH(rawHours))
    ];
  }}
  document.getElementById('modalStats').innerHTML = cards.join('');
}}

function setDatePreset(mode) {{
  if(mode === 'all') {{
    viewDaily = ALL_DAILY.slice();
    setActivePreset('presetAll');
  }} else {{
    viewDaily = ALL_DAILY.slice(-Number(mode));
    setActivePreset(mode === 30 ? 'preset30' : 'preset7');
  }}
  applyDateView();
}}

function applyCustomDateRange() {{
  setActivePreset(null);
  const start = document.getElementById('dateStart').value || '0000-00-00';
  const end = document.getElementById('dateEnd').value || '9999-99-99';
  viewDaily = ALL_DAILY.filter(r => String(r.log_date) >= start && String(r.log_date) <= end);
  applyDateView(false);
}}

function applyDateView(updateInputs=true) {{
  viewChannels = aggregateChannels(viewDaily);
  if(updateInputs) syncDateInputs();
  else document.getElementById('selectedRange').textContent = viewDaily.length
    ? `${{viewDaily[0].log_date}} to ${{viewDaily[viewDaily.length - 1].log_date}} | ${{viewDaily.length}} days`
    : 'No data in selected range';
  renderTrendFocusOptions();
  renderCards(selectedSummary());
  renderCharts();
  renderTables();
  updateChannelLimitText();
}}

function table(rows, columns, formatters={{}}) {{
  if(!rows || !rows.length) return '<div style="padding:14px;color:#64748b;font-size:12px">No rows available.</div>';
  const head = `<thead><tr>${{columns.map(c => `<th${{formatters[c.key]?.num ? ' class="num"' : ''}}>${{c.label}}</th>`).join('')}}</tr></thead>`;
  const body = rows.map(r => `<tr>${{columns.map(c => {{
    const f = formatters[c.key];
    let v = r[c.key];
    let out = f && f.fn ? f.fn(v, r) : escapeHtml(v);
    return `<td${{f?.num ? ' class="num"' : ''}}>${{out}}</td>`;
  }}).join('')}}</tr>`).join('');
  return `<table>${{head}}<tbody>${{body}}</tbody></table>`;
}}

function setCount(id, rows, label='rows') {{
  const el = document.getElementById(id);
  if(!el) return;
  const n = Array.isArray(rows) ? rows.length : Number(rows || 0);
  el.textContent = `${{fmt(n)}} ${{label}}`;
}}

function oneRowTable(obj, labels) {{
  const rows = Object.entries(labels).map(([key, label]) => ({{metric:label, value:obj?.[key] ?? 0}}));
  return table(rows, [{{key:'metric', label:'Metric'}}, {{key:'value', label:'Value'}}], {{value:{{fn:v=>fmt(v), num:true}}}});
}}

function aggregateStatus(dailyRows=viewDaily) {{
  const rows = groupRows(rowsForDates(DAILY_TABLES.status_codes_daily, dailyRows), ['statusCode'],
    ['rows','raw_ts_rows','status_200_ts_rows','raw_watch_hours','status_200_watch_hours'], ['approx_unique_ips'], ['sample_reqPath']);
  const totalRows = sum(rows, 'rows');
  rows.forEach(r => {{
    r.statusCode = String(r.statusCode ?? '').replace(/\\.0$/, '');
    r.meaning = statusMeaning(r.statusCode);
    r.row_pct = percent(r.rows, totalRows);
  }});
  return topRows(rows, 'rows', 20);
}}

function aggregateExtensions(dailyRows=viewDaily) {{
  return topRows(groupRows(rowsForDates(DAILY_TABLES.extensions_daily, dailyRows), ['extension'],
    ['rows','status_200_rows'], ['approx_unique_ips'], ['sample_reqPath']), 'rows', 20);
}}

function aggregateHosts(dailyRows=viewDaily) {{
  const rows = groupRows(rowsForDates(DAILY_TABLES.hosts_daily, dailyRows), ['reqHost'],
    ['rows','status_200_rows','non_200_rows','raw_ts_rows','status_200_ts_rows','raw_watch_hours','status_200_watch_hours'], ['approx_unique_ips']);
  rows.forEach(r => {{
    r.non_200_pct = percent(r.non_200_rows, r.rows);
    r.ts_rows = r.raw_ts_rows;
  }});
  return topRows(rows, 'rows', 25);
}}

function aggregateCache(dailyRows=viewDaily) {{
  const rows = groupRows(rowsForDates(DAILY_TABLES.cache_daily, dailyRows), ['reqHost','cacheStatus','cacheable'],
    ['rows','raw_ts_rows','status_200_ts_rows'], ['approx_unique_ips']);
  const byHost = new Map();
  rows.forEach(r => {{
    const key = r.reqHost || '';
    const item = byHost.get(key) || {{reqHost:key, rows:0, cache_hit_rows:0}};
    item.rows += Number(r.rows || 0);
    if(String(r.cacheStatus) === '1') item.cache_hit_rows += Number(r.rows || 0);
    byHost.set(key, item);
  }});
  const out = [...byHost.values()];
  out.forEach(r => r.cache_hit_pct = percent(r.cache_hit_rows, r.rows));
  return topRows(out, 'rows', 25);
}}

function aggregateErrors(dailyRows=viewDaily) {{
  return topRows(groupRows(rowsForDates(DAILY_TABLES.errors_daily, dailyRows), ['reqHost','statusCode','errorCode','startupError'],
    ['rows','raw_ts_rows'], ['approx_unique_ips'], ['sample_reqPath']), 'rows', 30);
}}

function buildGeoBreakdown(dailyRows=viewDaily) {{
  const base = rowsForDates(DAILY_TABLES.geo_daily, dailyRows).map(r => ({{
    ...r,
    country_code:String(r.country || '').trim().toUpperCase(),
    country_label:countryLabel(r.country),
    state_region:geoText(r.state, 'Unknown Region'),
    city_name:geoText(r.city, '')
  }}));
  const totalHours = sum(base, 'raw_watch_hours');
  const india = base.filter(r => r.country_code === 'IN');
  const other = base.filter(r => r.country_code !== 'IN');
  const finish = (rows, total, limit) => topRows(rows, 'raw_watch_hours', limit).map(r => ({{
    ...r,
    watch_hours:r.raw_watch_hours,
    share_pct:percent(r.raw_watch_hours, total)
  }}));
  const countrySplit = [
    {{segment:'India', watch_hours:sum(india, 'raw_watch_hours'), approx_unique_ips:max(india, 'approx_unique_ips')}},
    {{segment:'Other Countries', watch_hours:sum(other, 'raw_watch_hours'), approx_unique_ips:max(other, 'approx_unique_ips')}}
  ].map(r => ({{...r, share_pct:percent(r.watch_hours, totalHours)}})).sort((a,b) => b.watch_hours - a.watch_hours);
  return {{
    country_split:countrySplit,
    india_regions:finish(groupRows(india, ['state_region'], ['raw_watch_hours'], ['approx_unique_ips']), sum(india, 'raw_watch_hours'), 30),
    india_cities:finish(groupRows(india.filter(r => r.city_name), ['city_name','state_region'], ['raw_watch_hours'], ['approx_unique_ips']), sum(india, 'raw_watch_hours'), 30),
    other_regions:finish(groupRows(other, ['country_label','state_region'], ['raw_watch_hours'], ['approx_unique_ips']), sum(other, 'raw_watch_hours'), 40),
    other_cities:finish(groupRows(other.filter(r => r.city_name), ['country_label','city_name','state_region'], ['raw_watch_hours'], ['approx_unique_ips']), sum(other, 'raw_watch_hours'), 40)
  }};
}}

function aggregateDevice(dailyRows=viewDaily) {{
  const rows = groupRows(rowsForDates(DAILY_TABLES.device_type_by_channel_daily, dailyRows), ['channel_name','device_type'],
    ['raw_ts_rows','status_200_ts_rows'], ['approx_unique_ips']);
  rows.forEach(r => {{
    r.watch_hours = Number(r.raw_ts_rows || 0) * CHUNK_HOURS;
    r.status_200_watch_hours = Number(r.status_200_ts_rows || 0) * CHUNK_HOURS;
  }});
  return topRows(rows, 'watch_hours', 30);
}}

function aggregateAsn(dailyRows=viewDaily) {{
  const meta = new Map((D.asn || []).map(r => [String(r.asn), r]));
  const rows = groupRows(rowsForDates(DAILY_TABLES.asn_daily, dailyRows), ['asn'],
    ['raw_ts_rows','status_200_ts_rows','raw_watch_hours','status_200_watch_hours'], ['approx_unique_ips','distinct_hosts'], ['sample_reqHost']);
  rows.forEach(r => {{
    const m = meta.get(String(r.asn)) || {{}};
    r.as_name = m.as_name || ('AS' + (r.asn || ''));
    r.as_country = m.as_country || '';
    r.asn_type = m.asn_type || '';
    r.watch_hours = r.raw_watch_hours;
  }});
  return topRows(rows, 'watch_hours', 25);
}}

function aggregateUa(dailyRows=viewDaily) {{
  const rows = groupRows(rowsForDates(DAILY_TABLES.user_agents_daily, dailyRows), ['userAgent'],
    ['rows','raw_ts_rows','status_200_ts_rows'], ['approx_unique_ips','distinct_hosts']);
  rows.forEach(r => r.UA = r.userAgent);
  return topRows(rows, 'rows', 20);
}}

function aggregateMappingQuality(dailyRows=viewDaily) {{
  return topRows(groupRows(rowsForDates(DAILY_TABLES.mapping_quality_daily, dailyRows), ['reqHost','candidate_id','channel_name','quality_bucket'],
    ['rows','raw_ts_chunks','status_200_ts_chunks','raw_watch_hours','status_200_watch_hours'], ['approx_unique_ips'], ['sample_reqPath']), 'raw_watch_hours', 40);
}}

function aggregateUnmapped(dailyRows=viewDaily) {{
  return topRows(groupRows(rowsForDates(DAILY_TABLES.unmapped_candidates_daily, dailyRows), ['reqHost','candidate_id'],
    ['rows','raw_ts_chunks','status_200_ts_chunks','raw_watch_hours','status_200_watch_hours'], ['approx_unique_ips'], ['sample_reqPath']), 'raw_ts_chunks', 50);
}}

function aggregateQueryParams(dailyRows=viewDaily) {{
  return oneDailyRow(DAILY_TABLES.query_params_daily, [
    'rows_with_querystr','channel_rows','channel_name_rows','session_rows','device_id_rows','platform_rows','device_rows','content_title_rows','category_name_rows'
  ], ['sample_queryStr'], dailyRows);
}}

function aggregateCmcd(dailyRows=viewDaily) {{
  return oneDailyRow(DAILY_TABLES.cmcd_daily, [
    'rows_with_cmcd','br_rows','duration_rows','measured_throughput_rows','object_type_rows','streaming_format_rows','session_id_rows','stream_type_rows','top_bitrate_rows'
  ], ['sample_cmcd'], dailyRows);
}}

function initTabs() {{
  const tabs = [
    ['overview','Overview'], ['mapping','Mapping QA'], ['audience','Audience'],
    ['reliability','Reliability'], ['quality','Data Quality']
  ];
  document.getElementById('tabs').innerHTML = tabs.map(([id,label], i) =>
    `<button class="tab-btn ${{i===0?'active':''}}" onclick="showTab('${{id}}', this)">${{label}}</button>`
  ).join('');
}}

function showTab(id, btn) {{
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
}}

function renderHeader() {{
  document.getElementById('reportTitle').textContent = D.title;
  const trueStart = D.true_data_range?.first || (ALL_DAILY_RAW.length ? `${{ALL_DAILY_RAW[0].log_date}} 00:00:00` : '');
  const trueEnd = D.true_data_range?.last || (ALL_DAILY_RAW.length ? `${{ALL_DAILY_RAW[ALL_DAILY_RAW.length - 1].log_date}} 23:59:59` : '');
  const usedStart = ALL_DAILY.length ? fmtDateTime12(`${{ALL_DAILY[0].log_date}} 00:00:00`) : '';
  const usedEnd = ALL_DAILY.length ? fmtDateTime12(ALL_DAILY[ALL_DAILY.length - 1].log_date + ' 23:59:59') + ' IST' : '';
  document.getElementById('rangeText').textContent = trueStart && trueEnd
    ? `True data range: ${{fmtDateTime12(trueStart)}} \u2192 ${{fmtDateTime12(trueEnd)}} | Used range: ${{usedStart}} to ${{usedEnd}}`
    : 'Profile date range unavailable';
  document.getElementById('genText').textContent = `Last updated ${{fmtDateTime12(D.generated_at)}}`;
  document.getElementById('definitionNote').innerHTML = D.definitions.map(x => `<div>${{escapeHtml(x)}}</div>`).join('');
  if(ALL_DAILY.length) {{
    document.getElementById('dateStart').min = ALL_DAILY[0].log_date;
    document.getElementById('dateStart').max = ALL_DAILY[ALL_DAILY.length - 1].log_date;
    document.getElementById('dateEnd').min = ALL_DAILY[0].log_date;
    document.getElementById('dateEnd').max = ALL_DAILY[ALL_DAILY.length - 1].log_date;
  }}
}}

function renderCards(summary) {{
  const s = summary || D.summary;
  document.getElementById('summaryCards').innerHTML = [
    metric('Raw Watch Hours', fmtH(s.raw_watch_hours), 'all .ts statuses', 'teal'),
    metric('Status 200 Watch Hours', fmtH(s.status_200_watch_hours), 'HTTP 200 .ts only', 'green'),
    metric('Active Channels', fmt(s.active_channels), 'channels with .ts hours', 'gold'),
    metric('Mapped Coverage', fmtPct(s.mapped_coverage_pct), 'selected range mapping QA', 'green'),
    metric('Approx Unique IPs', fmt(s.approx_unique_ips), 'selected range daily max', 'violet'),
    metric('Non-200 Rate', fmtPct(s.non_200_pct), fmt(s.non_200_rows) + ' selected rows', 'red'),
    metric('Rows With QueryStr', fmt(s.rows_with_querystr), 'selected range queryStr rows', 'teal'),
  ].join('');
}}

function resetChart(id) {{
  if(chartInstances[id]) {{
    chartInstances[id].destroy();
    delete chartInstances[id];
  }}
}}

function lineTooltipLabel(ctx) {{
  const ds = ctx.dataset || {{}};
  const raw = ds.originalData ? ds.originalData[ctx.dataIndex] : ctx.parsed.y;
  return `${{ds.label || 'Value'}}: ${{fmtH(raw)}}`;
}}

function focusDataset(chart, hoveredIndex, expanded=false) {{
  if(chart.$hoveredDatasetIndex === hoveredIndex) return;
  chart.$hoveredDatasetIndex = hoveredIndex;
  chart.data.datasets.forEach((ds, i) => {{
    const baseColor = ds._baseBorderColor || ds.borderColor;
    const baseWidth = ds._baseBorderWidth || ds.borderWidth || 1.4;
    if(hoveredIndex == null) {{
      ds.borderColor = baseColor;
      ds.borderWidth = baseWidth;
    }} else if(i === hoveredIndex) {{
      ds.borderColor = baseColor;
      ds.borderWidth = Math.max(baseWidth + .8, expanded ? 2.7 : 2.2);
    }} else {{
      ds.borderColor = baseColor;
      ds.borderWidth = baseWidth;
    }}
  }});
  chart.update('none');
}}

function makeLineChart(id, labels, datasets, expanded=false, longTail=false, nearestOnly=true) {{
  if(typeof Chart === 'undefined' || !document.getElementById(id)) return;
  resetChart(id);
  const chartSets = longTail ? datasets.map(ds => {{
    const rawData = (ds.data || []).map(v => Number(v || 0));
    return {{...ds, originalData:rawData, data:rawData.map(longTailValue)}};
  }}) : datasets;
  chartSets.forEach(ds => {{
    ds.borderWidth = ds.borderWidth ?? (expanded ? 1.8 : 1.25);
    ds.pointRadius = ds.pointRadius ?? 0;
    ds.pointHoverRadius = ds.pointHoverRadius ?? 5;
    ds.tension = ds.tension ?? .32;
    ds.spanGaps = true;
    ds._baseBorderColor = ds.borderColor;
    ds._baseBorderWidth = ds.borderWidth;
  }});
  const showLegend = expanded || datasets.length <= 8;
  const plugins = {{
    legend:{{display:showLegend, position:'bottom', labels:{{usePointStyle:true, pointStyle:'line', font:{{size:expanded ? 11 : 9.5}}, boxWidth:12, padding:expanded ? 10 : 7}}}},
    tooltip:{{backgroundColor:'#172033', titleFont:{{size:11, weight:'700'}}, bodyFont:{{size:11, weight:'600'}}, padding:9, displayColors:true, callbacks:{{label:longTail ? longTailTooltip : lineTooltipLabel}}}}
  }};
  chartInstances[id] = new Chart(document.getElementById(id), {{
    type:'line',
    data:{{labels, datasets:chartSets}},
    options:{{responsive:true, maintainAspectRatio:false, normalized:true, interaction:{{mode:nearestOnly ? 'nearest' : 'index', axis:'xy', intersect:false}},
      onHover:(event, active, chart) => focusDataset(chart, active.length ? active[0].datasetIndex : null, expanded),
      plugins, elements:{{line:{{borderJoinStyle:'round', borderCapStyle:'round'}}, point:{{hitRadius:8}}}},
      scales:{{x:{{ticks:{{font:{{size:expanded ? 11 : 9}}, maxTicksLimit:expanded ? 18 : 12, autoSkip:true}}, grid:{{color:'#f5f7fb'}}}},
              y:{{ticks:{{font:{{size:expanded ? 11 : 9}}, callback:v=>longTail ? longTailAxis(v) : compactAxis(v)}}, grid:{{color:'#f5f7fb'}}}}}}}}
  }});
  return chartInstances[id];
}}

function makeBarChart(id, labels, values, expanded=false, longTail=false) {{
  if(typeof Chart === 'undefined' || !document.getElementById(id)) return;
  resetChart(id);
  const rawValues = values.map(v => Number(v || 0));
  const chartValues = longTail ? rawValues.map(longTailValue) : rawValues;
  const plugins = {{legend:{{display:false}}}};
  if(longTail) plugins.tooltip = {{callbacks:{{label:longTailTooltip}}}};
  chartInstances[id] = new Chart(document.getElementById(id), {{
    type:'bar',
    data:{{labels, datasets:[{{label:'Raw watch hours', data:chartValues, originalData:rawValues, backgroundColor:labels.map((_,i)=>chartColor(i, labels.length))}}]}},
    options:{{indexAxis:'y', responsive:true, maintainAspectRatio:false, plugins,
      scales:{{x:{{ticks:{{callback:v=>longTail ? longTailAxis(v) : compactAxis(v)}}}}, y:{{ticks:{{autoSkip:false, font:{{size:expanded ? 11 : 10}}}}}}}}}}
  }});
  return chartInstances[id];
}}

function makeDoughnut(id, labels, values) {{
  if(typeof Chart === 'undefined' || !document.getElementById(id)) return;
  resetChart(id);
  chartInstances[id] = new Chart(document.getElementById(id), {{
    type:'doughnut',
    data:{{labels, datasets:[{{data:values, backgroundColor:labels.map((_,i)=>COLORS[i%COLORS.length])}}]}},
    options:{{responsive:true, maintainAspectRatio:false, plugins:{{legend:{{position:'bottom'}}}}}}
  }});
}}

function syncChannelBarHeight(count) {{
  const wrap = document.getElementById('channelBarWrap');
  if(wrap) wrap.style.height = Math.min(820, Math.max(460, count * 22 + 96)) + 'px';
}}

function renderDailyVolume(targetId='dailyChart', expanded=false, dailyRows=null) {{
  const daily = dailyRows || viewDaily || [];
  return makeLineChart(targetId, daily.map(r=>r.log_date), [
    {{label:'raw .ts rows', data:daily.map(r=>r.raw_ts_rows ?? r.ts_rows), borderColor:'#0f766e', backgroundColor:'rgba(15,118,110,.08)', fill:true, tension:.3, pointRadius:1}},
    {{label:'status 200 .ts rows', data:daily.map(r=>r.status_200_ts_rows ?? r.ts_rows), borderColor:'#2f6b2f', backgroundColor:'transparent', fill:false, tension:.3, pointRadius:1}},
    {{label:'.m3u8 rows', data:daily.map(r=>r.m3u8_rows), borderColor:'#173b5c', backgroundColor:'rgba(23,59,92,.06)', fill:true, tension:.3, pointRadius:1}},
    {{label:'non-200 rows', data:daily.map(r=>r.non_200_rows), borderColor:'#b42318', backgroundColor:'transparent', fill:false, tension:.3, pointRadius:1}}
  ], expanded);
}}

function renderChannelBar(targetId='channelBar', expanded=false, channelRows=null) {{
  const ch = channelRows ? chartChannelsFrom(channelRows) : chartChannels();
  if(targetId === 'channelBar') syncChannelBarHeight(ch.length);
  return makeBarChart(targetId, ch.map(r=>r.channel_name), ch.map(r=>r.raw_watch_hours), expanded, true);
}}

function renderChannelTrend(targetId='channelTrend', expanded=false, dailyRows=null, channelRows=null) {{
  const daily = dailyRows || viewDaily || [];
  const sourceChannels = channelRows || viewChannels || [];
  const ch = trendChannelsFrom(sourceChannels);
  const trendTop = ch.map(c => c.channel_name);
  const trendDates = daily.map(r => r.log_date);
  const trendDateSet = new Set(trendDates);
  const single = trendTop.length === 1;
  const compare = trendFocus === 'compare';
  return makeLineChart(targetId, trendDates, trendTop.map((channelName, i) => {{
    const byDate = new Map(ALL_CHANNEL_DAILY
      .filter(p => p.channel_name === channelName && trendDateSet.has(p.log_date))
      .map(p => [p.log_date, p.raw_watch_hours]));
    return {{
      label:channelName, data:trendDates.map(d => byDate.get(d) || 0), borderColor:chartColor(i, trendTop.length), backgroundColor:'transparent', fill:false, tension:.25, pointRadius:0, pointHoverRadius:5, borderWidth:expanded || single || compare ? 2 : 1.25
    }};
  }}), expanded, true, true);
}}

function renderSingleChart(sourceId, targetId, expanded=false, dailyRows=null) {{
  const rowsForChart = dailyRows || viewDaily || [];
  const channelsForChart = aggregateChannels(rowsForChart);
  if(sourceId === 'dailyChart') return renderDailyVolume(targetId, expanded, rowsForChart);
  if(sourceId === 'channelTrend') return renderChannelTrend(targetId, expanded, rowsForChart, channelsForChart);
  if(sourceId === 'channelBar') return renderChannelBar(targetId, expanded, channelsForChart);
}}

function renderCharts() {{
  renderDailyVolume();
  renderChannelTrend();
  renderChannelBar();
  const st = (D.status || []).slice(0, 8);
  makeDoughnut('statusChart', st.map(r=>String(r.statusCode)), st.map(r=>r.rows));
}}

function openChartModal(id) {{
  if(typeof Chart === 'undefined') return;
  const titles = {{dailyChart:'Daily Volume', channelTrend:'Channel Trend', channelBar:'All Channels By Watch Hours'}};
  modalChartSourceId = id;
  setModalInputBounds();
  document.getElementById('modalTitle').textContent = titles[id] || 'Chart';
  const trendSelect = document.getElementById('modalTrendFocus');
  if(trendSelect) trendSelect.style.display = id === 'channelTrend' ? '' : 'none';
  const trendCompareBtn = document.getElementById('modalTrendCompareBtn');
  if(trendCompareBtn) trendCompareBtn.style.display = id === 'channelTrend' ? '' : 'none';
  ['modalCompareCount','modalCompareCh0','modalCompareCh1','modalCompareCh2','modalCompareCh3','modalCompareCh4'].forEach(ctrlId => {{
    const el = document.getElementById(ctrlId);
    const isSlot = ctrlId.includes('Ch');
    const slot = isSlot ? Number(ctrlId.slice(-1)) : -1;
    if(el) el.style.display = id === 'channelTrend' && trendFocus === 'compare' && (!isSlot || slot < compareCount) ? '' : 'none';
  }});
  document.getElementById('chartModal').classList.add('open');
  setModalPreset('dashboard');
}}

function closeChartModal() {{
  document.getElementById('chartModal').classList.remove('open');
  resetChart('modalChart');
  modalChartSourceId = null;
  modalDaily = [];
}}

function renderModalChart() {{
  if(!modalChartSourceId) return;
  syncModalInputs();
  renderModalStats();
  renderSingleChart(modalChartSourceId, 'modalChart', true, modalDaily);
}}

function setModalPreset(mode) {{
  if(!modalChartSourceId) return;
  if(mode === 'dashboard') {{
    modalDaily = (viewDaily || []).slice();
    setActiveModalPreset('modalDashboard');
  }} else if(mode === 'all') {{
    modalDaily = ALL_DAILY.slice();
    setActiveModalPreset('modalAll');
  }} else {{
    modalDaily = ALL_DAILY.slice(-Number(mode));
    setActiveModalPreset(mode === 30 ? 'modal30' : 'modal7');
  }}
  renderModalChart();
}}

function setModalCustom() {{
  if(!modalChartSourceId) return;
  setActiveModalPreset(null);
  const start = document.getElementById('modalStart').value || '0000-00-00';
  const end = document.getElementById('modalEnd').value || '9999-99-99';
  modalDaily = ALL_DAILY.filter(r => String(r.log_date) >= start && String(r.log_date) <= end);
  renderModalChart();
}}

document.getElementById('chartModal').addEventListener('click', e => {{
  if(e.target.id === 'chartModal') closeChartModal();
}});
document.addEventListener('keydown', e => {{
  if(e.key === 'Escape' && document.getElementById('chartModal').classList.contains('open')) closeChartModal();
}});

function renderTables() {{
  const s = selectedSummary();
  const unmappedRows = aggregateUnmapped();
  const mappingQualityRows = aggregateMappingQuality();
  const geo = buildGeoBreakdown();
  const deviceRows = aggregateDevice();
  const asnRows = aggregateAsn();
  const uaRows = aggregateUa();
  const statusRows = aggregateStatus();
  const cacheRows = aggregateCache();
  const hostRows = aggregateHosts();
  const errorRows = aggregateErrors();
  const extensionRows = aggregateExtensions();
  const queryParams = aggregateQueryParams();
  const cmcd = aggregateCmcd();

  document.getElementById('channelTable').innerHTML = table(viewChannels || [], [
    {{key:'channel_name', label:'Channel'}}, {{key:'raw_watch_hours', label:'Raw Hours'}}, {{key:'status_200_watch_hours', label:'Status 200 Hours'}}, {{key:'share_pct', label:'Share'}},
    {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], {{raw_watch_hours:{{fn:v=>fmtH(v), num:true}}, status_200_watch_hours:{{fn:v=>fmtH(v), num:true}}, share_pct:{{fn:v=>fmtPct(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});

  document.getElementById('mappingMetrics').innerHTML = [
    mini('Unmapped Candidates', fmt(s.unmapped_candidates)),
    mini('Mapped Coverage', fmtPct(s.mapped_coverage_pct)),
    mini('Rows With QueryStr', fmt(queryParams.rows_with_querystr || 0)),
    mini('Rows With CMCD', fmt(cmcd.rows_with_cmcd || 0))
  ].join('');
  document.getElementById('querySummaryTable').innerHTML = table(D.query_summary, [
    {{key:'review_status', label:'Status'}}, {{key:'rows', label:'Rows'}}, {{key:'requests', label:'Requests'}}, {{key:'sessions', label:'Sessions'}}, {{key:'devices', label:'Devices'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, requests:{{fn:v=>fmt(v), num:true}}, sessions:{{fn:v=>fmt(v), num:true}}, devices:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('queryNonOkTable').innerHTML = table(D.query_non_ok, [
    {{key:'review_status', label:'Status'}}, {{key:'pure_channel', label:'Pure Channel'}}, {{key:'mapped_channel', label:'Mapped Channel'}},
    {{key:'reqHost', label:'Host'}}, {{key:'candidate_id', label:'Candidate'}}, {{key:'requests', label:'Requests'}}, {{key:'sample_reqPath', label:'Sample Path'}}
  ], {{requests:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('unmappedTable').innerHTML = table(unmappedRows, [
    {{key:'reqHost', label:'Host'}}, {{key:'candidate_id', label:'Candidate'}}, {{key:'raw_watch_hours', label:'Raw Hours'}}, {{key:'status_200_watch_hours', label:'Status 200 Hours'}}, {{key:'sample_reqPath', label:'Sample Path'}}
  ], {{raw_watch_hours:{{fn:v=>fmtH(v), num:true}}, status_200_watch_hours:{{fn:v=>fmtH(v), num:true}}}});
  document.getElementById('mappingQualityTable').innerHTML = table(mappingQualityRows, [
    {{key:'channel_name', label:'Channel'}}, {{key:'candidate_id', label:'Candidate'}}, {{key:'quality_bucket', label:'Quality'}},
    {{key:'raw_watch_hours', label:'Raw Hours'}}, {{key:'status_200_watch_hours', label:'Status 200 Hours'}}, {{key:'reqHost', label:'Host'}}
  ], {{raw_watch_hours:{{fn:v=>fmtH(v), num:true}}, status_200_watch_hours:{{fn:v=>fmtH(v), num:true}}}});

  const geoFmt = {{watch_hours:{{fn:v=>fmtH(v), num:true}}, share_pct:{{fn:v=>fmtPct(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}};
  document.getElementById('geoCountryTable').innerHTML = table(geo.country_split || [], [
    {{key:'segment', label:'Country Group'}}, {{key:'watch_hours', label:'Watch Hours'}}, {{key:'share_pct', label:'Share'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], geoFmt);
  document.getElementById('geoIndiaRegionTable').innerHTML = table(geo.india_regions || [], [
    {{key:'state_region', label:'State / Region'}}, {{key:'watch_hours', label:'Watch Hours'}}, {{key:'share_pct', label:'India Share'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], geoFmt);
  document.getElementById('geoIndiaCityTable').innerHTML = table(geo.india_cities || [], [
    {{key:'city_name', label:'City'}}, {{key:'state_region', label:'State / Region'}}, {{key:'watch_hours', label:'Watch Hours'}}, {{key:'share_pct', label:'India Share'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], geoFmt);
  document.getElementById('geoOtherRegionTable').innerHTML = table(geo.other_regions || [], [
    {{key:'country_label', label:'Country'}}, {{key:'state_region', label:'State / Region'}}, {{key:'watch_hours', label:'Watch Hours'}}, {{key:'share_pct', label:'Other Share'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], geoFmt);
  document.getElementById('geoOtherCityTable').innerHTML = table(geo.other_cities || [], [
    {{key:'country_label', label:'Country'}}, {{key:'city_name', label:'City'}}, {{key:'state_region', label:'State / Region'}}, {{key:'watch_hours', label:'Watch Hours'}}, {{key:'share_pct', label:'Other Share'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], geoFmt);
  document.getElementById('deviceTable').innerHTML = table(deviceRows, [
    {{key:'channel_name', label:'Channel'}}, {{key:'device_type', label:'Device'}}, {{key:'watch_hours', label:'Watch Hours'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], {{watch_hours:{{fn:v=>fmtH(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('asnTable').innerHTML = table(asnRows, [
    {{key:'asn', label:'ASN'}}, {{key:'as_name', label:'Network'}}, {{key:'asn_type', label:'Type'}}, {{key:'as_country', label:'Country'}},
    {{key:'watch_hours', label:'Watch Hours'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}, {{key:'sample_reqHost', label:'Sample Host'}}
  ], {{watch_hours:{{fn:v=>fmtH(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('uaTable').innerHTML = table(uaRows, [
    {{key:'UA', label:'User Agent'}}, {{key:'rows', label:'Rows'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});

  document.getElementById('statusTable').innerHTML = table(statusRows, [
    {{key:'statusCode', label:'Status'}}, {{key:'meaning', label:'Meaning'}}, {{key:'rows', label:'Rows'}}, {{key:'row_pct', label:'Share'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}, {{key:'sample_reqPath', label:'Sample Path'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, row_pct:{{fn:v=>fmtPct(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('cacheTable').innerHTML = table(cacheRows, [
    {{key:'reqHost', label:'Host'}}, {{key:'rows', label:'Rows'}}, {{key:'cache_hit_rows', label:'Hit Rows'}}, {{key:'cache_hit_pct', label:'Hit %'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, cache_hit_rows:{{fn:v=>fmt(v), num:true}}, cache_hit_pct:{{fn:v=>fmtPct(v), num:true}}}});
  document.getElementById('hostTable').innerHTML = table(hostRows, [
    {{key:'reqHost', label:'Host'}}, {{key:'rows', label:'Rows'}}, {{key:'non_200_pct', label:'Non-200 %'}}, {{key:'ts_rows', label:'.ts Rows'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, non_200_pct:{{fn:v=>fmtPct(v), num:true}}, ts_rows:{{fn:v=>fmt(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('errorTable').innerHTML = table(errorRows, [
    {{key:'reqHost', label:'Host'}}, {{key:'statusCode', label:'Status'}}, {{key:'errorCode', label:'Error'}}, {{key:'rows', label:'Rows'}}, {{key:'sample_reqPath', label:'Sample Path'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('performanceTable').innerHTML = table(D.performance, [
    {{key:'reqHost', label:'Host'}}, {{key:'extension', label:'Ext'}}, {{key:'rows', label:'Rows'}}, {{key:'ttfb_p50_ms', label:'TTFB p50'}}, {{key:'ttfb_p95_ms', label:'TTFB p95'}}, {{key:'throughput_p05', label:'Throughput p05'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, ttfb_p50_ms:{{fn:v=>fmt(v,1), num:true}}, ttfb_p95_ms:{{fn:v=>fmt(v,1), num:true}}, throughput_p05:{{fn:v=>fmt(v,1), num:true}}}});

  document.getElementById('fillTable').innerHTML = table(D.column_fill, [
    {{key:'column_name', label:'Column'}}, {{key:'non_empty_pct', label:'Fill %'}}, {{key:'empty_rows', label:'Empty Rows'}}, {{key:'non_empty_rows', label:'Non-empty Rows'}}
  ], {{non_empty_pct:{{fn:v=>fmtPct(v), num:true}}, empty_rows:{{fn:v=>fmt(v), num:true}}, non_empty_rows:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('extensionTable').innerHTML = table(extensionRows, [
    {{key:'extension', label:'Extension'}}, {{key:'rows', label:'Rows'}}, {{key:'approx_unique_ips', label:'Approx IPs'}}, {{key:'sample_reqPath', label:'Sample Path'}}
  ], {{rows:{{fn:v=>fmt(v), num:true}}, approx_unique_ips:{{fn:v=>fmt(v), num:true}}}});
  document.getElementById('queryParamTable').innerHTML = oneRowTable(queryParams, {{
    rows_with_querystr:'Rows with queryStr', channel_rows:'channel rows', channel_name_rows:'channel_name rows',
    session_rows:'session rows', device_id_rows:'device_id rows', platform_rows:'platform rows'
  }});
  document.getElementById('cmcdTable').innerHTML = oneRowTable(cmcd, {{
    rows_with_cmcd:'Rows with CMCD', br_rows:'Bitrate rows', duration_rows:'Duration rows',
    measured_throughput_rows:'Measured throughput rows', session_id_rows:'Session ID rows'
  }});
  setCount('channelCount', viewChannels);
  setCount('querySummaryCount', D.query_summary);
  setCount('queryNonOkCount', D.query_non_ok);
  setCount('unmappedCount', unmappedRows);
  setCount('mappingQualityCount', mappingQualityRows);
  setCount('geoCountryCount', geo.country_split);
  setCount('deviceCount', deviceRows);
  setCount('geoIndiaRegionCount', geo.india_regions);
  setCount('geoIndiaCityCount', geo.india_cities);
  setCount('geoOtherRegionCount', geo.other_regions);
  setCount('geoOtherCityCount', geo.other_cities);
  setCount('asnCount', asnRows);
  setCount('uaCount', uaRows);
  setCount('statusCount', statusRows);
  setCount('cacheCount', cacheRows);
  setCount('hostCount', hostRows);
  setCount('errorCount', errorRows);
  setCount('performanceCount', D.performance);
  setCount('fillCount', D.column_fill);
  setCount('extensionCount', extensionRows);
  setCount('queryParamCount', 6, 'metrics');
  setCount('cmcdCount', 5, 'metrics');
}}

initTabs();
renderHeader();
setDatePreset('all');
</script>
</body>
</html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a standalone VgLive stakeholder HTML report.")
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE_DIR), help="Folder containing deep profile CSV files.")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Output HTML file.")
    parser.add_argument("--title", default="Veto Watch Hours", help="HTML report title.")
    args = parser.parse_args()

    profile_dir = Path(args.profile).resolve()
    out_path = Path(args.out).resolve()
    if not profile_dir.exists():
        raise SystemExit(f"Profile folder not found: {profile_dir}")

    data = build_report_data(profile_dir, args.title)
    html = render_html(data)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Report written: {out_path}")
    print(f"Size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
