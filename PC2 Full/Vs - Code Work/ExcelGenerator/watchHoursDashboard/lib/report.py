"""
lib/report.py — Orchestrates data loading and assembles the full report dict.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

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

DEFAULT_ASN_DECODED_CSV = Path(__file__).resolve().parent.parent.parent / "4.asn" / "asnDecoded.csv"


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
        "channels":           records(channel_summary, ["channel_name", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "approx_unique_ips", "share_pct", "first_seen", "last_seen"]),
        "channel_daily_all":  records(channel_daily,   ["log_date", "channel_name", "raw_watch_hours", "status_200_watch_hours", "raw_ts_chunks", "status_200_ts_chunks", "m3u8_rows", "approx_unique_ips"]),
        "channel_series":     channel_series,
        "status":             records(status.sort_values("rows", ascending=False),              ["statusCode", "meaning", "rows", "row_pct", "approx_unique_ips", "sample_reqPath"], 20),
        "extensions":         records(extensions.sort_values("rows", ascending=False),          ["extension", "rows", "approx_unique_ips", "sample_reqPath"], 20),
        "hosts":              records(hosts.sort_values("rows", ascending=False),               ["reqHost", "rows", "status_200_rows", "non_200_rows", "non_200_pct", "ts_rows", "approx_unique_ips"], 25),
        "cache":              records(cache_rollup,                                              ["reqHost", "rows", "cache_hit_rows", "cache_hit_pct"], 25),
        "errors":             records(errors.sort_values("rows", ascending=False),              ["reqHost", "statusCode", "errorCode", "startupError", "rows", "approx_unique_ips", "sample_reqPath"], 30),
        "performance":        records(perf_slowest,                                             ["reqHost", "extension", "rows", "ttfb_p50_ms", "ttfb_p95_ms", "transfer_p95_ms", "throughput_p50", "throughput_p05"], 30),
        "geo":                build_geo_breakdown(geo),
        "device":             records(device.sort_values("watch_hours", ascending=False),       ["channel_name", "device_type", "watch_hours", "approx_unique_ips"], 30),
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
