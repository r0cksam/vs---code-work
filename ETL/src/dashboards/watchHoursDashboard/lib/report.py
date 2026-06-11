"""
lib/report.py — Orchestrates data loading and assembles the full report dict.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote_plus

import pandas as pd

from .aggregations import (
    add_status_meanings,
    build_cache_rollup,
    build_channel_daily_series,
    build_geo_breakdown,
    build_query_review,
)
from .constants import CHUNK_DURATION_HOURS, REPORT_DEFINITIONS, STATUS_CODE_MEANINGS, COUNTRY_LABELS
from .readers import (
    DEFAULT_LAKE_FOLDER,
    enrich_asn,
    load_asn_decoded,
    read_all_daily_tables,
    read_csv,
    true_data_range,
)
from .utils import (
    add_watch_hours_from_ts,
    decode_cols,
    numeric,
    one_row,
    pct,
    records,
    total,
    with_numbers,
)

DEFAULT_ASN_DECODED_CSV = Path(
    os.getenv(
        "VG_ASN_DECODED_CSV",
        str(Path(__file__).resolve().parents[4] / "data" / "asn" / "asnDecoded.csv"),
    )
)

DEFAULT_DEVICE_DECODE_DIR = Path(
    os.getenv(
        "VG_DEVICE_DECODE_DIR",
        str(Path(__file__).resolve().parents[4] / "output" / "device_decode"),
    )
)

DEFAULT_CONCURRENCY_DIR = Path(
    os.getenv(
        "VG_CONCURRENCY_DIR",
        str(Path(__file__).resolve().parents[4] / "output" / "watch_hours" / "concurrency"),
    )
)


def _latest_file(folder: Path, pattern: str) -> Path | None:
    if not folder.exists():
        return None
    files = [p for p in folder.glob(pattern) if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _read_parquet_safe(path: Path | None) -> pd.DataFrame:
    if not path or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError, ImportError):
        return pd.DataFrame()


def _read_csv_safe(path: Path | None) -> pd.DataFrame:
    if not path or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, ValueError):
        return pd.DataFrame()


def _read_json_safe(path: Path | None) -> dict:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _clean_decode_samples(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["log_date", "first_date", "last_date"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["sample_UA", "sample_reqPath", "sample_reqHost", "UA"]:
        if col in out.columns:
            out[col] = out[col].map(_clean_encoded_value)
    return out


def _clean_concurrency_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["log_date", "minute_utc", "minute_ist", "peak_unique_viewers_minute_ist", "peak_segment_minute_ist"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    for col in ["source", "reqHost", "platform_key", "platform_name", "candidate_id", "channel_name"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    if not out.empty:
        def text_col(name: str) -> pd.Series:
            if name in out.columns:
                return out[name].astype(str)
            return pd.Series([""] * len(out), index=out.index)

        out["pair_key"] = (
            text_col("source")
            + "|"
            + text_col("platform_key")
            + "|"
            + text_col("candidate_id")
            + "|"
            + text_col("channel_name")
        )
    return out


def load_concurrency_outputs(folder: Path = DEFAULT_CONCURRENCY_DIR) -> dict:
    minute_path = folder / "concurrency_minute.parquet"
    summary_path = folder / "concurrency_summary.parquet"
    manifest_path = folder / "concurrency_manifest.json"
    if not minute_path.exists():
        minute_path = _latest_file(folder, "concurrency_minute*.parquet") or minute_path
    if not summary_path.exists():
        summary_path = _latest_file(folder, "concurrency_summary*.parquet") or summary_path

    minute_numeric = [
        "raw_ts_rows",
        "status_200_ts_rows",
        "distinct_hosts",
        "unique_viewers",
        "segment_viewers_estimate",
        "status_200_segment_viewers_estimate",
    ]
    summary_numeric = [
        "minute_count",
        "raw_ts_rows",
        "status_200_ts_rows",
        "distinct_hosts",
        "avg_unique_viewers",
        "peak_unique_viewers",
        "p95_unique_viewers",
        "avg_segment_viewers_estimate",
        "peak_segment_viewers_estimate",
        "avg_status_200_segment_viewers_estimate",
        "peak_status_200_segment_viewers_estimate",
    ]
    minute = _clean_concurrency_frame(with_numbers(_read_parquet_safe(minute_path), minute_numeric))
    summary = _clean_concurrency_frame(with_numbers(_read_parquet_safe(summary_path), summary_numeric))

    top_pairs_limit = max(1, int(os.getenv("VG_CONCURRENCY_TOP_PAIRS", "80")))
    selected_pairs: set[str] = set()
    pair_rank = pd.DataFrame()
    if not summary.empty and "pair_key" in summary.columns:
        pair_rank = (
            summary.groupby(
                ["pair_key", "source", "platform_key", "platform_name", "candidate_id", "channel_name", "reqHost"],
                dropna=False,
                as_index=False,
            )
            .agg(
                raw_ts_rows=("raw_ts_rows", "sum"),
                peak_unique_viewers=("peak_unique_viewers", "max"),
                peak_segment_viewers_estimate=("peak_segment_viewers_estimate", "max"),
                days=("log_date", "nunique"),
            )
            .sort_values(["peak_unique_viewers", "raw_ts_rows"], ascending=False)
        )
        selected_pairs = set(pair_rank.head(top_pairs_limit)["pair_key"].astype(str))

    total_minute_rows = int(len(minute))
    if selected_pairs and "pair_key" in minute.columns:
        minute = minute[minute["pair_key"].astype(str).isin(selected_pairs)]
    if selected_pairs and "pair_key" in summary.columns:
        summary = summary[summary["pair_key"].astype(str).isin(selected_pairs)]

    if not minute.empty:
        minute = minute.sort_values(["log_date", "platform_name", "channel_name", "candidate_id", "minute_ist"])
    if not summary.empty:
        summary = summary.sort_values(["peak_unique_viewers", "raw_ts_rows"], ascending=False)

    return {
        "available": bool(minute_path.exists() and summary_path.exists() and not minute.empty),
        "source_dir": str(folder),
        "top_pairs_limit": top_pairs_limit,
        "embedded_minute_rows": int(len(minute)),
        "total_minute_rows": total_minute_rows,
        "files": {
            "minute": str(minute_path) if minute_path.exists() else "",
            "summary": str(summary_path) if summary_path.exists() else "",
            "manifest": str(manifest_path) if manifest_path.exists() else "",
        },
        "manifest": _read_json_safe(manifest_path),
        "pairs": records(
            pair_rank,
            [
                "source", "platform_key", "platform_name", "candidate_id", "channel_name", "reqHost",
                "raw_ts_rows", "peak_unique_viewers", "peak_segment_viewers_estimate", "days",
            ],
            top_pairs_limit,
        ),
        "summary": records(
            summary,
            [
                "log_date", "source", "reqHost", "platform_key", "platform_name", "candidate_id", "channel_name",
                "distinct_hosts", "minute_count", "raw_ts_rows", "status_200_ts_rows", "avg_unique_viewers",
                "peak_unique_viewers", "peak_unique_viewers_minute_ist", "p95_unique_viewers",
                "avg_segment_viewers_estimate", "peak_segment_viewers_estimate",
                "peak_segment_minute_ist", "avg_status_200_segment_viewers_estimate",
                "peak_status_200_segment_viewers_estimate", "pair_key",
            ],
        ),
        "minute": records(
            minute,
            [
                "log_date", "source", "minute_utc", "minute_ist", "reqHost", "platform_key",
                "platform_name", "candidate_id", "channel_name", "raw_ts_rows",
                "status_200_ts_rows", "distinct_hosts", "unique_viewers", "segment_viewers_estimate",
                "status_200_segment_viewers_estimate", "pair_key",
            ],
        ),
    }


def load_device_decode_outputs(folder: Path = DEFAULT_DEVICE_DECODE_DIR) -> dict:
    summary_path = _latest_file(folder, "device_decode_summary_*.parquet")
    unknown_path = _latest_file(folder, "unknown_device_codes_*.csv")
    ua_path = _latest_file(folder, "top_user_agents_*.parquet")
    ua_cache_path = _latest_file(folder, "ua_decode_enriched_*.parquet")
    manifest_path = _latest_file(folder, "device_decode_manifest_*.json")

    summary = with_numbers(
        _clean_decode_samples(_read_parquet_safe(summary_path)),
        ["rows", "ts_rows", "status_200_rows", "approx_ips", "distinct_device_ids"],
    )
    unknown = with_numbers(
        _clean_decode_samples(_read_csv_safe(unknown_path)),
        ["rows", "ts_rows", "approx_ips"],
    )
    top_ua = with_numbers(
        _clean_decode_samples(_read_parquet_safe(ua_path)),
        ["rows", "approx_ips"],
    )
    if not top_ua.empty:
        top_ua = top_ua.sort_values("rows", ascending=False).head(500)
    ua_cache = with_numbers(
        _clean_decode_samples(_read_parquet_safe(ua_cache_path)),
        ["rows", "ts_rows", "status_200_ts_rows", "watch_hours", "status_200_watch_hours", "approx_ips"],
    )
    if not ua_cache.empty:
        ua_cache = ua_cache.sort_values("rows", ascending=False).head(500)

    return {
        "available": bool(summary_path and not summary.empty),
        "source_dir": str(folder),
        "files": {
            "summary": str(summary_path) if summary_path else "",
            "unknown_codes": str(unknown_path) if unknown_path else "",
            "top_user_agents": str(ua_path) if ua_path else "",
            "ua_cache": str(ua_cache_path) if ua_cache_path else "",
            "manifest": str(manifest_path) if manifest_path else "",
        },
        "manifest": _read_json_safe(manifest_path),
        "summary": records(
            summary,
            [
                "log_date", "source", "decode_status", "signal_source", "vendor",
                "device_code", "decoded_device_name", "product_family", "generation",
                "model_number", "release_year", "mapping_status", "mapping_notes",
                "platform", "query_device", "rows", "ts_rows", "status_200_rows",
                "approx_ips", "distinct_device_ids", "sample_UA", "sample_reqHost",
                "sample_reqPath",
            ],
        ),
        "unknown_codes": records(
            unknown,
            ["device_code", "rows", "ts_rows", "approx_ips", "first_date", "last_date", "sample_UA", "sample_reqHost"],
            250,
        ),
        "top_user_agents": records(
            top_ua,
            ["source", "decode_status", "signal_source", "device_code", "decoded_device_name", "UA", "rows", "approx_ips", "first_date", "last_date"],
        ),
        "ua_cache": records(
            ua_cache,
            [
                "source", "ua_hash", "ua_norm_key", "ua_sample", "rows", "ts_rows",
                "status_200_ts_rows", "watch_hours", "status_200_watch_hours",
                "approx_ips", "first_date", "last_date", "decoder", "decode_status",
                "device_type", "brand", "model", "os_name", "os_version", "os_family",
                "browser_name", "browser_version", "browser_engine", "bot_name",
            ],
        ),
    }


def _clean_encoded_value(value: object) -> object:
    if value is None:
        return value
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return ""
    for _ in range(3):
        decoded = unquote_plus(text)
        if decoded == text:
            break
        text = decoded
    return text


def build_report_data(profile_dir: Path, title: str) -> dict:
    """
    Load all profile CSVs, aggregate them, and return a serialisable report dict.
    """

    # ── Load all source frames ────────────────────────────────────────────────
    channel_summary = with_numbers(
        read_csv(profile_dir, "channel_summary"),
        ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours",
         "approx_unique_ips", "approx_distinct_segments", "distinct_hosts"],
    ).sort_values("raw_watch_hours", ascending=False)

    channel_daily = read_csv(profile_dir, "channel_daily")

    daily = with_numbers(
        read_csv(profile_dir, "daily"),
        ["rows", "status_200_rows", "non_200_rows", "raw_ts_rows", "status_200_ts_rows",
         "ts_rows", "m3u8_rows", "approx_unique_ips"],
    )

    status     = add_status_meanings(with_numbers(read_csv(profile_dir, "status"), ["rows", "approx_unique_ips"]))
    extensions = with_numbers(read_csv(profile_dir, "extensions"), ["rows", "approx_unique_ips"])
    hosts      = with_numbers(read_csv(profile_dir, "hosts"), ["rows", "status_200_rows", "non_200_rows", "ts_rows", "approx_unique_ips"])
    cache      = read_csv(profile_dir, "cache")
    errors     = with_numbers(read_csv(profile_dir, "errors"), ["rows", "approx_unique_ips"])
    performance = with_numbers(
        read_csv(profile_dir, "performance"),
        ["rows", "ttfb_p50_ms", "ttfb_p95_ms", "transfer_p50_ms", "transfer_p95_ms",
         "throughput_p50", "throughput_p05"],
    )
    geo = add_watch_hours_from_ts(
        decode_cols(
            with_numbers(read_csv(profile_dir, "geo"), ["ts_rows", "approx_unique_ips", "distinct_hosts"]),
            ["state", "city"],
        )
    )
    device = add_watch_hours_from_ts(with_numbers(read_csv(profile_dir, "device"), ["ts_rows", "approx_unique_ips"]))
    asn = add_watch_hours_from_ts(
        enrich_asn(
            with_numbers(read_csv(profile_dir, "asn"), ["ts_rows", "approx_unique_ips", "distinct_hosts"]),
            load_asn_decoded(DEFAULT_ASN_DECODED_CSV),
        )
    )
    ua             = decode_cols(with_numbers(read_csv(profile_dir, "ua"), ["rows", "approx_unique_ips", "distinct_hosts"]), ["UA"])
    query_params   = read_csv(profile_dir, "query_params")
    query_channels = decode_cols(read_csv(profile_dir, "query_channels"), ["raw_channel", "platform", "device_name"])
    cmcd           = read_csv(profile_dir, "cmcd")
    column_fill    = with_numbers(read_csv(profile_dir, "column_fill"), ["total_rows", "non_empty_rows", "empty_rows", "non_empty_pct"])
    mapping_quality = with_numbers(
        read_csv(profile_dir, "mapping_quality"),
        ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "approx_unique_ips"],
    )
    unmapped = with_numbers(
        read_csv(profile_dir, "unmapped"),
        ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "approx_unique_ips"],
    )
    files = with_numbers(read_csv(profile_dir, "files"), ["size_mb"])

    # ── Derived summary scalars ───────────────────────────────────────────────
    total_rows     = total(daily, "rows") or total(status, "rows")
    status_200_rows = total(daily, "status_200_rows")
    if not status_200_rows and not status.empty:
        status_200_rows = total(status[status["statusCode"].astype(str) == "200"], "rows")
    non_200_rows = total(daily, "non_200_rows") or max(total_rows - status_200_rows, 0)

    # Backfill missing columns
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

    raw_ts_rows         = total(daily, "raw_ts_rows") or total(channel_summary, "raw_ts_chunks")
    status_200_ts_rows  = total(daily, "status_200_ts_rows") or total(channel_summary, "status_200_ts_chunks")
    ts_rows             = raw_ts_rows
    m3u8_rows           = total(daily, "m3u8_rows")
    raw_watch_hours     = total(channel_summary, "raw_watch_hours") or (raw_ts_rows * CHUNK_DURATION_HOURS)
    status_200_watch_hours = total(channel_summary, "status_200_watch_hours") or (status_200_ts_rows * CHUNK_DURATION_HOURS)
    unmapped_ts_rows    = total(unmapped, "raw_ts_chunks")
    mapped_coverage     = pct(max(ts_rows - unmapped_ts_rows, 0), ts_rows)

    approx_unique_ips = 0.0
    if not status.empty and "statusCode" in status.columns:
        approx_unique_ips = total(status[status["statusCode"].astype(str) == "200"], "approx_unique_ips")
    if not approx_unique_ips:
        approx_unique_ips = numeric(daily, "approx_unique_ips").max() if not daily.empty else 0

    # ── Enrich main frames ────────────────────────────────────────────────────
    if not channel_summary.empty:
        channel_summary["share_pct"] = channel_summary["raw_watch_hours"].map(
            lambda v: pct(v, raw_watch_hours)
        )

    if not daily.empty:
        daily["raw_watch_hours"]        = daily["raw_ts_rows"] * CHUNK_DURATION_HOURS
        daily["status_200_watch_hours"] = daily["status_200_ts_rows"] * CHUNK_DURATION_HOURS
        daily["non_200_pct"]            = daily.apply(lambda r: pct(r["non_200_rows"], r["rows"]), axis=1)
        daily["log_date"]               = pd.to_datetime(daily["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        daily                           = daily.dropna(subset=["log_date"])

    if not channel_daily.empty:
        channel_daily = with_numbers(
            channel_daily,
            ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "m3u8_rows", "approx_unique_ips"],
        )
        if "status_200_ts_chunks" not in channel_daily.columns:
            channel_daily["status_200_ts_chunks"] = numeric(channel_daily, "raw_ts_chunks")
        if "status_200_watch_hours" not in channel_daily.columns:
            channel_daily["status_200_watch_hours"] = numeric(channel_daily, "raw_watch_hours")
        if "m3u8_rows" not in channel_daily.columns:
            channel_daily["m3u8_rows"] = 0
        channel_daily["log_date"] = pd.to_datetime(channel_daily["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        channel_daily             = channel_daily.dropna(subset=["log_date"])

    if not status.empty:
        status["row_pct"] = status["rows"].map(lambda v: pct(v, total_rows))

    if not hosts.empty:
        hosts["non_200_pct"] = hosts.apply(lambda r: pct(r["non_200_rows"], r["rows"]), axis=1)

    for frame in [mapping_quality, unmapped]:
        if not frame.empty:
            if "status_200_ts_chunks" not in frame.columns:
                frame["status_200_ts_chunks"] = numeric(frame, "raw_ts_chunks")
            if "status_200_watch_hours" not in frame.columns:
                frame["status_200_watch_hours"] = numeric(frame, "raw_watch_hours")

    # ── Domain aggregations ───────────────────────────────────────────────────
    cache_rollup                         = build_cache_rollup(cache)
    query_summary, query_non_ok, query_stats = build_query_review(query_channels)
    top_channels                         = channel_summary["channel_name"].head(8).tolist() if not channel_summary.empty else []
    channel_series                       = build_channel_daily_series(channel_daily, top_channels)

    low_fill   = column_fill.sort_values("non_empty_pct").head(18) if not column_fill.empty else pd.DataFrame()
    perf_ts    = (
        performance[performance["extension"].astype(str).str.lower() == "ts"]
        if not performance.empty and "extension" in performance.columns
        else performance
    )
    perf_slowest = (
        perf_ts.sort_values("ttfb_p95_ms", ascending=False)
        if not perf_ts.empty and "ttfb_p95_ms" in perf_ts.columns
        else pd.DataFrame()
    )

    query_param_row = one_row(query_params)
    cmcd_row        = one_row(cmcd)
    file_dates      = (
        sorted(files["date"].dropna().astype(str).unique().tolist())
        if not files.empty and "date" in files.columns
        else []
    )
    true_range  = true_data_range(file_dates, files)
    daily_tables = read_all_daily_tables(profile_dir)
    device_decode = load_device_decode_outputs()
    encoded_display_fields = {"state", "city", "sample_reqPath", "sample_value", "userAgent", "UA"}
    for rows in daily_tables.values():
        for row in rows:
            for key in encoded_display_fields:
                if key in row:
                    row[key] = _clean_encoded_value(row.get(key))
    for row in daily_tables.get("query_params_daily", []):
        if str(row.get("sample_queryStr", "")).strip():
            row["sample_queryStr"] = "[queryStr sample hidden]"

    # ── Assemble output dict ──────────────────────────────────────────────────
    return {
        "title":          title,
        "generated_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "profile_dir":    str(profile_dir),
        "date_range":     {
            "first": file_dates[0] if file_dates else "",
            "last":  file_dates[-1] if file_dates else "",
        },
        "true_data_range":  true_range,
        "status_meanings":  STATUS_CODE_MEANINGS,
        "country_labels":   COUNTRY_LABELS,
        "definitions":      REPORT_DEFINITIONS,
        "summary": {
            "total_rows":           total_rows,
            "status_200_rows":      status_200_rows,
            "non_200_rows":         non_200_rows,
            "non_200_pct":          pct(non_200_rows, total_rows),
            "ts_rows":              ts_rows,
            "raw_ts_rows":          raw_ts_rows,
            "status_200_ts_rows":   status_200_ts_rows,
            "m3u8_rows":            m3u8_rows,
            "raw_watch_hours":      raw_watch_hours,
            "status_200_watch_hours": status_200_watch_hours,
            "active_channels":      int(len(channel_summary[channel_summary["channel_name"] != "Other"])) if not channel_summary.empty else 0,
            "approx_unique_ips":    approx_unique_ips,
            "mapped_coverage_pct":  mapped_coverage,
            "unmapped_candidates":  int(len(unmapped)),
            "query_ok_request_pct": query_stats.get("query_ok_request_pct", 0.0),
            "rows_with_querystr":   float(query_param_row.get("rows_with_querystr", 0) or 0),
            "rows_with_cmcd":       float(cmcd_row.get("rows_with_cmcd", 0) or 0),
        },
        "daily": records(
            daily,
            ["log_date", "rows", "status_200_rows", "non_200_rows", "raw_ts_rows",
             "status_200_ts_rows", "ts_rows", "m3u8_rows", "approx_unique_ips",
             "raw_watch_hours", "status_200_watch_hours", "non_200_pct"],
        ),
        "daily_tables":       daily_tables,
        "sources":            sorted({
            str(r.get("source", "stream")).lower()
            for rows in daily_tables.values()
            for r in rows
            if str(r.get("source", "stream")).strip()
        }),
        "channels":           records(channel_summary, ["channel_name", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips", "share_pct", "first_seen", "last_seen"]),
        "channel_daily_all":  records(channel_daily,   ["log_date", "source", "channel_name", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "m3u8_rows", "approx_unique_ips"]),
        "channel_series":     channel_series,
        "status":             records(status.sort_values("rows", ascending=False),              ["statusCode", "meaning", "rows", "row_pct", "approx_unique_ips", "sample_reqPath"], 20),
        "extensions":         records(extensions.sort_values("rows", ascending=False),          ["extension", "rows", "approx_unique_ips", "sample_reqPath"], 20),
        "hosts":              records(hosts.sort_values("rows", ascending=False),               ["reqHost", "rows", "status_200_rows", "non_200_rows", "non_200_pct", "ts_rows", "approx_unique_ips"], 25),
        "cache":              records(cache_rollup,                                              ["reqHost", "rows", "cache_hit_rows", "cache_hit_pct"], 25),
        "errors":             records(errors.sort_values("rows", ascending=False),              ["reqHost", "statusCode", "errorCode", "startupError", "rows", "approx_unique_ips", "sample_reqPath"], 30),
        "performance":        records(perf_slowest,                                             ["reqHost", "extension", "rows", "ttfb_p50_ms", "ttfb_p95_ms", "transfer_p95_ms", "throughput_p50", "throughput_p05"], 30),
        "geo":                build_geo_breakdown(geo),
        "device":             records(device.sort_values("watch_hours", ascending=False),       ["channel_name", "device_type", "watch_hours", "approx_unique_ips"], 30),
        "device_decode":      device_decode,
        "asn":                records(asn.sort_values("watch_hours", ascending=False),          ["asn", "as_name", "as_country", "asn_type", "watch_hours", "approx_unique_ips", "distinct_hosts", "sample_reqHost"], 25),
        "ua":                 records(ua.sort_values("rows", ascending=False),                  ["UA", "rows", "approx_unique_ips", "distinct_hosts"], 20),
        "query_summary":      records(query_summary,                                            ["review_status", "rows", "requests", "sessions", "devices"]),
        "query_non_ok":       records(query_non_ok,                                             ["review_status", "pure_channel", "raw_channel", "mapped_channel", "reqHost", "candidate_id", "requests", "sessions", "sample_reqPath"], 30),
        "unmapped":           records(unmapped,                                                 ["reqHost", "candidate_id", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips", "sample_reqPath"], 50),
        "mapping_quality":    records(mapping_quality.sort_values("raw_watch_hours", ascending=False), ["reqHost", "candidate_id", "channel_name", "quality_bucket", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips", "sample_reqPath"], 40),
        "column_fill":        records(low_fill,                                                 ["column_name", "total_rows", "non_empty_rows", "empty_rows", "non_empty_pct"]),
        "query_params":       query_param_row,
        "cmcd":               cmcd_row,
    }
