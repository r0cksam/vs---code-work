#!/usr/bin/env python3
"""Generate the Veto Audience Operations static HTML dashboard."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import duckdb
except Exception:  # pragma: no cover - dashboard can fall back to date ranges
    duckdb = None


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
DEFAULT_OUTPUT_ROOT = ETL_ROOT / "output"
DEFAULT_OUT = DEFAULT_OUTPUT_ROOT / "audience_ops" / "veto_audience_operations.html"
CHARTJS_CACHE = DEFAULT_OUTPUT_ROOT / "cache" / "chartjs" / "chart.umd.min.js"

for path in [SRC_ROOT, ETL_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common.chartjs import load_chartjs  # noqa: E402
from common.render import chartjs_script, json_blob, render_template  # noqa: E402


CHUNK_DURATION_HOURS = 6 / 3600
IST_OFFSET_SECONDS = 5 * 60 * 60 + 30 * 60


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        print(f"[warn] Could not read {path}: {exc}")
        return pd.DataFrame()


def latest_file(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    for file in files:
        if ".tmp." not in file.name and file.suffix.lower() == ".parquet":
            return file
    return None


def records(df: pd.DataFrame, limit: int | None = None, tail: bool = False) -> list[dict[str, Any]]:
    if df.empty:
        return []
    if limit is not None:
        df = df.tail(limit) if tail else df.head(limit)
    clean = df.copy()
    for col in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[col]):
            clean[col] = clean[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return json.loads(clean.to_json(orient="records", date_format="iso"))


def numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    return out


def date_string(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def add_watch_hours(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = numeric(df, ["raw_ts_rows", "status_200_ts_rows", "ts_rows", "m3u8_rows", "rows", "approx_unique_ips"])
    if "request_watch_hours" not in out.columns:
        out["request_watch_hours"] = out.get("raw_ts_rows", 0) * CHUNK_DURATION_HOURS
    if "status_200_request_hours" not in out.columns:
        out["status_200_request_hours"] = out.get("status_200_ts_rows", 0) * CHUNK_DURATION_HOURS
    if "non_200_rows" in out.columns and "rows" in out.columns:
        out["non_200_pct"] = out.apply(lambda r: (r["non_200_rows"] * 100 / r["rows"]) if r["rows"] else 0, axis=1)
    return out


def range_info(df: pd.DataFrame, date_col: str = "log_date") -> dict[str, Any]:
    if df.empty or date_col not in df.columns:
        return {"min_date": "", "max_date": "", "rows": 0, "sources": []}
    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    sources = sorted(str(v) for v in df.get("source", pd.Series(dtype=object)).dropna().unique())
    return {
        "min_date": dates.min().strftime("%Y-%m-%d") if not dates.empty else "",
        "max_date": dates.max().strftime("%Y-%m-%d") if not dates.empty else "",
        "rows": int(len(df)),
        "sources": sources,
    }


def source_ranges(df: pd.DataFrame, date_col: str = "log_date") -> list[dict[str, Any]]:
    if df.empty or "source" not in df.columns:
        return []
    out = []
    for source, part in df.groupby("source", dropna=False):
        info = range_info(part, date_col)
        info["source"] = str(source)
        out.append(info)
    return out


def sql_path(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def resolve_lake_root() -> Path:
    env = os.getenv("VG_ETL_LAKE_ROOT")
    if env:
        return Path(env).expanduser()
    return ETL_ROOT / "data" / "lake"


def format_epoch_ist(epoch: float | None) -> str:
    if epoch is None:
        return ""
    return datetime.fromtimestamp(float(epoch) + IST_OFFSET_SECONDS, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def day_partition_folder(lake_root: Path, source: str, date_text: str) -> Path | None:
    try:
        dt = datetime.strptime(str(date_text)[:10], "%Y-%m-%d")
    except ValueError:
        return None
    folder = lake_root / f"source={source.lower()}" / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
    return folder if folder.exists() else None


def reqtime_bounds(folder: Path | None) -> tuple[float | None, float | None]:
    if duckdb is None or folder is None:
        return None, None
    if not any(folder.glob("*.parquet")):
        return None, None
    glob = sql_path(folder / "*.parquet")
    try:
        con = duckdb.connect()
        row = con.execute(
            f"""
            SELECT
                MIN(TRY_CAST(reqTimeSec AS DOUBLE)) AS first_epoch,
                MAX(TRY_CAST(reqTimeSec AS DOUBLE)) AS last_epoch
            FROM read_parquet('{glob}', hive_partitioning=1, union_by_name=1)
            WHERE TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
            """
        ).fetchone()
        con.close()
    except Exception as exc:
        print(f"[warn] Could not scan true time range for {folder}: {exc}")
        return None, None
    if not row:
        return None, None
    return row[0], row[1]


def source_true_ranges_from_lake(df: pd.DataFrame, lake_root: Path | None = None) -> list[dict[str, Any]]:
    ranges = source_ranges(df)
    if not ranges:
        return []
    lake_root = lake_root or resolve_lake_root()
    out: list[dict[str, Any]] = []
    for item in ranges:
        source = str(item.get("source", "")).lower()
        min_date = str(item.get("min_date", "") or "")[:10]
        max_date = str(item.get("max_date", "") or "")[:10]
        first_epoch, _ = reqtime_bounds(day_partition_folder(lake_root, source, min_date))
        _, last_epoch = reqtime_bounds(day_partition_folder(lake_root, source, max_date))
        enriched = dict(item)
        enriched.update(
            {
                "first_ist": format_epoch_ist(first_epoch) or (f"{min_date} 00:00:00" if min_date else ""),
                "last_ist": format_epoch_ist(last_epoch) or (f"{max_date} 23:59:59" if max_date else ""),
                "first_epoch": first_epoch,
                "last_epoch": last_epoch,
                "basis": "reqTimeSec min/max from first and last lake partitions",
            }
        )
        out.append(enriched)
    return out


def safe_group_sum(df: pd.DataFrame, keys: list[str], metrics: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=keys + metrics)
    cols = list(dict.fromkeys(c for c in keys + metrics if c in df.columns))
    metric_cols = list(dict.fromkeys(m for m in metrics if m in cols))
    out = numeric(df[cols].copy(), metric_cols)
    return out.groupby([k for k in keys if k in out.columns], dropna=False, as_index=False).sum(numeric_only=True)


def filter_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty or source == "all" or "source" not in df.columns:
        return df
    return df[df["source"].astype(str).str.lower().eq(source)].copy()


def filter_date_window(df: pd.DataFrame, min_date: Any, max_date: Any, date_col: str = "log_date") -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    dates = pd.to_datetime(df[date_col], errors="coerce")
    return df[dates.dt.date.ge(min_date) & dates.dt.date.le(max_date)].copy()


def build_daily(watch_dir: Path) -> pd.DataFrame:
    daily = add_watch_hours(read_parquet(watch_dir / "daily_volume.parquet"))
    if daily.empty:
        return daily
    daily["log_date"] = date_string(daily["log_date"])
    keep = [
        "log_date",
        "source",
        "rows",
        "status_200_rows",
        "non_200_rows",
        "raw_ts_rows",
        "status_200_ts_rows",
        "ts_rows",
        "m3u8_rows",
        "approx_unique_ips",
        "request_watch_hours",
        "status_200_request_hours",
        "non_200_pct",
    ]
    return daily[[c for c in keep if c in daily.columns]].sort_values(["log_date", "source"])


def build_overview_daily(output_root: Path) -> pd.DataFrame:
    path = output_root / "overview" / "overview_source_daily.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        overview = pd.read_csv(path)
    except Exception as exc:
        print(f"[warn] Could not read {path}: {exc}")
        return pd.DataFrame()
    if overview.empty or "date" not in overview.columns:
        return pd.DataFrame()
    overview["date"] = date_string(overview["date"])
    metrics = [
        "bytes",
        "rows",
        "ip_rows",
        "dist_ip",
        "dist_ipua",
        "dist_dev",
        "dist_sess",
        "dist_ip_r2",
        "dist_ipua_r2",
        "sess_avail",
        "sess_na",
        "sess_none",
    ]
    overview = numeric(overview, metrics)
    keep = ["date", "source"] + metrics
    return overview[[c for c in keep if c in overview.columns]].sort_values(["date", "source"])


def build_channel(watch_dir: Path) -> pd.DataFrame:
    channel = read_parquet(watch_dir / "channel_audience_daily.parquet")
    if channel.empty:
        return channel
    channel = numeric(
        channel,
        [
            "raw_watch_hours",
            "status_200_watch_hours",
            "raw_ts_chunks",
            "status_200_ts_chunks",
            "approx_unique_ips",
            "approx_sessions",
            "approx_devices",
        ],
    )
    channel["log_date"] = date_string(channel["log_date"])
    return channel.sort_values(["log_date", "source", "channel_name"])


def build_geo(watch_dir: Path) -> pd.DataFrame:
    geo = read_parquet(watch_dir / "geo_daily.parquet")
    if geo.empty:
        return geo
    geo = numeric(geo, ["raw_watch_hours", "status_200_watch_hours", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"])
    geo["log_date"] = date_string(geo["log_date"])
    return geo.sort_values(["log_date", "source", "raw_watch_hours"], ascending=[True, True, False])


def ua_label(value: Any) -> str:
    text = str(value or "").replace("%20", " ").replace("+", " ").strip()
    return text


def derive_ua_platform(ua_value: Any) -> str:
    ua = ua_label(ua_value)
    low = ua.lower()
    if "aft" in low or "fire tv" in low:
        return "Amazon Fire TV"
    if "bravia" in low or "sony" in low:
        return "Sony BRAVIA"
    if "tizen" in low or "stvplus" in low or "samsung" in low:
        return "Samsung / Tizen TV"
    if "webos" in low or "web0s" in low or "lg smart" in low:
        return "LG Smart TV"
    if "xiaomi" in low or "mitv" in low or "mi tv" in low:
        return "Xiaomi Mi TV"
    if "sharp" in low:
        return "Sharp TV"
    if "hisense" in low:
        return "Hisense Smart TV"
    if "haier" in low:
        return "Haier MatrixTV"
    if "vu tv" in low or "vutv" in low:
        return "VU TV"
    if "ai pont" in low or "aipont" in low:
        return "AI PONT TV"
    if "iphone" in low or "ipad" in low:
        return "iPhone/iPad"
    if "android" in low or "okhttp" in low or "exoplayer" in low or "dalvik" in low:
        return "ExoPlayer-Android"
    if "smart-tv" in low or "smart tv" in low:
        return "Smart TV / Other"
    if "mac os" in low:
        return "macOS"
    if "windows" in low:
        return "Windows"
    return "Others"


def derive_ua_os(ua_value: Any) -> str:
    ua = ua_label(ua_value)
    low = ua.lower()
    if "aft" in low or "fire tv" in low:
        return "FireOS"
    if "android" in low or "okhttp" in low or "dalvik" in low or "exoplayer" in low:
        return "Android"
    if "webos" in low or "web0s" in low:
        return "webOS"
    if "tizen" in low:
        return "Tizen"
    if "appletv" in low or "apple tv" in low:
        return "Apple TV"
    if "iphone" in low or "ipad" in low or "cpu os" in low:
        return "iOS"
    if "mac os" in low:
        return "macOS"
    if "windows" in low:
        return "Windows"
    if "linux" in low:
        return "Linux"
    return "Others"


def build_ua_playtime(watch_dir: Path) -> dict[str, pd.DataFrame]:
    ua = read_parquet(watch_dir / "user_agents_daily.parquet")
    if ua.empty:
        return {"platform": pd.DataFrame(), "os": pd.DataFrame()}
    ua = numeric(ua, ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"])
    ua["log_date"] = date_string(ua["log_date"])
    ua["playtime_hours"] = ua["raw_ts_rows"] * CHUNK_DURATION_HOURS
    ua["status_200_playtime_hours"] = ua["status_200_ts_rows"] * CHUNK_DURATION_HOURS
    ua["ua_platform"] = ua["userAgent"].map(derive_ua_platform)
    ua["ua_os"] = ua["userAgent"].map(derive_ua_os)
    metrics = ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips", "playtime_hours", "status_200_playtime_hours"]
    platform = safe_group_sum(ua, ["log_date", "source", "ua_platform"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    os = safe_group_sum(ua, ["log_date", "source", "ua_os"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    return {"platform": platform, "os": os}


def build_latency(latency_dir: Path) -> dict[str, pd.DataFrame]:
    daily = read_parquet(latency_dir / "daily.parquet")
    hourly = read_parquet(latency_dir / "hourly.parquet")
    channel = read_parquet(latency_dir / "channel_daily.parquet")
    host = read_parquet(latency_dir / "host_daily.parquet")
    status = read_parquet(latency_dir / "status_daily.parquet")
    cache = read_parquet(latency_dir / "cache_daily.parquet")
    metric_cols = [
        "rows",
        "status_200_rows",
        "non_200_rows",
        "status_5xx_rows",
        "approx_unique_ips",
        "cache_hit_rows",
        "ttfb_rows",
        "ttfb_p50_ms",
        "ttfb_p95_ms",
        "ttfb_p99_ms",
        "turnaround_p95_ms",
        "transfer_p95_ms",
        "throughput_p05",
        "avg_total_bytes",
    ]
    out = {}
    for name, frame in {
        "daily": daily,
        "hourly": hourly,
        "channel": channel,
        "host": host,
        "status": status,
        "cache": cache,
    }.items():
        frame = numeric(frame, metric_cols)
        if not frame.empty and "log_date" in frame.columns:
            frame["log_date"] = date_string(frame["log_date"])
        if not frame.empty and "hour_ist" in frame.columns:
            frame["hour_ist"] = pd.to_datetime(frame["hour_ist"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        if not frame.empty and "rows" in frame.columns:
            frame["cache_hit_pct"] = frame.apply(lambda r: (r.get("cache_hit_rows", 0) * 100 / r["rows"]) if r["rows"] else 0, axis=1)
            frame["error_5xx_pct"] = frame.apply(lambda r: (r.get("status_5xx_rows", 0) * 100 / r["rows"]) if r["rows"] else 0, axis=1)
            frame["non_200_pct"] = frame.apply(lambda r: (r.get("non_200_rows", 0) * 100 / r["rows"]) if r["rows"] else 0, axis=1)
        out[name] = frame
    return out


def build_concurrency(concurrency_dir: Path) -> dict[str, pd.DataFrame]:
    minute = read_parquet(concurrency_dir / "concurrency_minute.parquet")
    summary = read_parquet(concurrency_dir / "concurrency_summary.parquet")
    status = read_parquet(concurrency_dir / "concurrency_status_minute.parquet")
    if not minute.empty:
        minute = numeric(
            minute,
            [
                "raw_ts_rows",
                "status_200_ts_rows",
                "unique_viewers",
                "unique_ua_viewers",
                "segment_viewers_estimate",
                "status_200_segment_viewers_estimate",
            ],
        )
        minute["log_date"] = date_string(minute["log_date"])
        minute["minute_ist"] = pd.to_datetime(minute["minute_ist"], errors="coerce")
        minute["bucket_5min"] = minute["minute_ist"].dt.floor("5min").dt.strftime("%Y-%m-%d %H:%M:%S")
        minute5 = (
            minute.groupby(["log_date", "source", "bucket_5min", "platform_name", "channel_name"], dropna=False, as_index=False)
            .agg(
                avg_unique_viewers=("unique_viewers", "mean"),
                peak_unique_viewers=("unique_viewers", "max"),
                avg_unique_ua_viewers=("unique_ua_viewers", "mean"),
                raw_ts_rows=("raw_ts_rows", "sum"),
                status_200_ts_rows=("status_200_ts_rows", "sum"),
            )
            .sort_values(["log_date", "bucket_5min"])
        )
    else:
        minute5 = pd.DataFrame()
    if not summary.empty:
        summary = numeric(
            summary,
            [
                "minute_count",
                "raw_ts_rows",
                "status_200_ts_rows",
                "avg_unique_viewers",
                "peak_unique_viewers",
                "p95_unique_viewers",
                "avg_unique_ua_viewers",
                "peak_unique_ua_viewers",
            ],
        )
        summary["log_date"] = date_string(summary["log_date"])
    if not status.empty:
        status = numeric(status, ["status_ts_rows", "status_segment_viewers_estimate"])
        status["log_date"] = date_string(status["log_date"])
    return {"minute5": minute5, "summary": summary, "status": status}


def build_device_decode(device_dir: Path) -> dict[str, pd.DataFrame | str]:
    summary_path = latest_file(device_dir, "device_decode_summary_*.parquet")
    ua_path = latest_file(device_dir, "ua_decode_enriched_*.parquet")
    top_ua_path = latest_file(device_dir, "top_user_agents_*.parquet")
    summary = read_parquet(summary_path) if summary_path else pd.DataFrame()
    ua = read_parquet(ua_path) if ua_path else pd.DataFrame()
    top_ua = read_parquet(top_ua_path) if top_ua_path else pd.DataFrame()
    for frame in [summary, ua, top_ua]:
        if not frame.empty:
            for col in ["rows", "ts_rows", "status_200_rows", "status_200_ts_rows", "watch_hours", "status_200_watch_hours", "approx_ips"]:
                if col in frame.columns:
                    frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0)
    return {
        "summary": summary,
        "ua": ua,
        "top_ua": top_ua,
        "summary_path": str(summary_path or ""),
        "ua_path": str(ua_path or ""),
        "top_ua_path": str(top_ua_path or ""),
    }


def build_identity(identity_dir: Path) -> dict[str, pd.DataFrame]:
    metric_cols = [
        "total_devices",
        "total_sessions",
        "total_ipua_sessions",
        "new_devices",
        "returning_devices",
        "device_identity_rows",
        "session_identity_rows",
        "ipua_identity_rows",
        "device_distinct_ip_day_sum",
        "device_distinct_ipua_day_sum",
    ]
    tables = {
        "daily": read_parquet(identity_dir / "identity_daily.parquet"),
        "channel": read_parquet(identity_dir / "identity_channel_daily.parquet"),
        "platform": read_parquet(identity_dir / "identity_platform_daily.parquet"),
        "platform_channel": read_parquet(identity_dir / "identity_platform_channel_daily.parquet"),
    }
    for name, frame in list(tables.items()):
        if frame.empty:
            tables[name] = frame
            continue
        frame = numeric(frame, metric_cols)
        if "log_date" in frame.columns:
            frame["log_date"] = date_string(frame["log_date"])
        tables[name] = frame.sort_values([c for c in ["log_date", "source", "platform_name", "channel_name"] if c in frame.columns])
    return tables


def build_content(content_dir: Path) -> pd.DataFrame:
    content = read_parquet(content_dir / "content_daily.parquet")
    if content.empty:
        return content
    content = numeric(
        content,
        [
            "rows",
            "raw_ts_rows",
            "status_200_ts_rows",
            "m3u8_rows",
            "status_200_m3u8_rows",
            "non_200_rows",
            "raw_watch_hours",
            "status_200_watch_hours",
            "approx_unique_ips",
            "approx_sessions",
            "approx_devices",
        ],
    )
    if "log_date" in content.columns:
        content["log_date"] = date_string(content["log_date"])
    return content.sort_values(["log_date", "source", "m3u8_rows"], ascending=[True, True, False])


def top_table(df: pd.DataFrame, group_cols: list[str], metric: str, limit: int = 25) -> pd.DataFrame:
    if df.empty or metric not in df.columns:
        return pd.DataFrame()
    keys = [c for c in group_cols if c in df.columns]
    if not keys:
        return pd.DataFrame()
    grouped = safe_group_sum(df, keys, [metric, "approx_unique_ips", "raw_watch_hours", "status_200_watch_hours", "rows"])
    return grouped.sort_values(metric, ascending=False).head(limit)


def build_data(args: argparse.Namespace) -> dict[str, Any]:
    output_root = args.output_root.expanduser().resolve()
    watch_dir = output_root / "watch_hours" / "daily_tables"
    concurrency_dir = output_root / "watch_hours" / "concurrency"
    latency_dir = output_root / "latency" / "profile"
    device_dir = output_root / "device_decode"
    identity_dir = output_root / "identity"
    content_dir = output_root / "content"

    daily = build_daily(watch_dir)
    overview_daily = build_overview_daily(output_root)
    channel = build_channel(watch_dir)
    geo = build_geo(watch_dir)
    ua_playtime = build_ua_playtime(watch_dir)
    latency = build_latency(latency_dir)
    concurrency = build_concurrency(concurrency_dir)
    devices = build_device_decode(device_dir)
    identity = build_identity(identity_dir)
    content = build_content(content_dir)
    source = args.source.lower()

    daily = filter_source(daily, source)
    channel = filter_source(channel, source)
    geo = filter_source(geo, source)
    ua_playtime = {name: filter_source(frame, source) for name, frame in ua_playtime.items()}
    latency = {name: filter_source(frame, source) for name, frame in latency.items()}
    concurrency = {name: filter_source(frame, source) for name, frame in concurrency.items()}
    identity = {name: filter_source(frame, source) for name, frame in identity.items()}
    content = filter_source(content, source)
    if isinstance(devices.get("summary"), pd.DataFrame):
        devices["summary"] = filter_source(devices["summary"], source)
    if isinstance(devices.get("ua"), pd.DataFrame):
        devices["ua"] = filter_source(devices["ua"], source)
    if isinstance(devices.get("top_ua"), pd.DataFrame):
        devices["top_ua"] = filter_source(devices["top_ua"], source)

    source_true_ranges = source_true_ranges_from_lake(daily)

    latest_possible = datetime.now().date() - timedelta(days=1)
    max_dates = [
        pd.to_datetime(frame["log_date"], errors="coerce").max()
        for frame in [daily, channel, geo, latency["daily"], concurrency["summary"], identity["daily"]]
        if not frame.empty and "log_date" in frame.columns
    ]
    max_date = min([d.date() for d in max_dates if pd.notna(d)] + [latest_possible])
    min_dates = [
        pd.to_datetime(frame["log_date"], errors="coerce").min()
        for frame in [daily, channel, geo, latency["daily"], concurrency["summary"], identity["daily"]]
        if not frame.empty and "log_date" in frame.columns
    ]
    min_date = min([d.date() for d in min_dates if pd.notna(d)] + [max_date])

    daily = filter_date_window(daily, min_date, max_date)
    channel = filter_date_window(channel, min_date, max_date)
    geo = filter_date_window(geo, min_date, max_date)
    ua_playtime = {name: filter_date_window(frame, min_date, max_date) for name, frame in ua_playtime.items()}
    latency = {name: filter_date_window(frame, min_date, max_date) for name, frame in latency.items()}
    concurrency = {name: filter_date_window(frame, min_date, max_date) for name, frame in concurrency.items()}
    identity = {name: filter_date_window(frame, min_date, max_date) for name, frame in identity.items()}
    content = filter_date_window(content, min_date, max_date)
    if isinstance(devices.get("summary"), pd.DataFrame):
        devices["summary"] = filter_date_window(devices["summary"], min_date, max_date)
    if isinstance(devices.get("ua"), pd.DataFrame):
        devices["ua"] = filter_date_window(devices["ua"], min_date, max_date, "first_date")
    if isinstance(devices.get("top_ua"), pd.DataFrame):
        devices["top_ua"] = filter_date_window(devices["top_ua"], min_date, max_date, "first_date")

    channel_top = top_table(channel, ["source", "channel_name"], "raw_watch_hours", 60)
    geo_top = top_table(geo, ["source", "country", "state", "city"], "raw_watch_hours", 80)
    platform_top = top_table(concurrency["summary"], ["source", "platform_name"], "raw_ts_rows", 40)
    content_top = top_table(content, ["source", "channel_name", "platform_name", "content_title", "category_name"], "m3u8_rows", 120)
    status_top = top_table(latency["status"], ["source", "extension", "status_code"], "rows", 80)
    host_latency = top_table(latency["host"], ["source", "platform_name", "reqHost"], "rows", 80)
    if not host_latency.empty and not latency["host"].empty:
        cols = ["source", "platform_name", "reqHost", "rows", "ttfb_p95_ms", "cache_hit_pct", "error_5xx_pct", "non_200_pct"]
        host_latency = latency["host"][[c for c in cols if c in latency["host"].columns]].sort_values(
            ["ttfb_p95_ms", "rows"], ascending=[False, False]
        ).head(80)

    device_summary = devices["summary"]
    device_by_name = pd.DataFrame()
    if isinstance(device_summary, pd.DataFrame) and not device_summary.empty:
        key_cols = [c for c in ["source", "decoded_device_name", "platform", "decode_status"] if c in device_summary.columns]
        device_by_name = safe_group_sum(device_summary, key_cols, ["rows", "ts_rows", "status_200_rows", "approx_ips"]).sort_values(
            "rows", ascending=False
        ).head(80)

    availability = [
        {
            "area": "Watch and channel trends",
            "status": "Available",
            "basis": "watch_hours/daily_tables",
            "note": "Request-hour metrics are production-ready. Dedup playback mart is WIP.",
        },
        {
            "area": "FAST concurrency",
            "status": "Available",
            "basis": "watch_hours/concurrency",
            "note": "Minute and 5-minute viewer estimates are available for FAST. STREAM concurrency mart is not available yet.",
        },
        {
            "area": "Latency and CDN reliability",
            "status": "Available",
            "basis": "latency/profile",
            "note": "TTFB, turnaround, transfer, throughput, cache, and status metrics exist.",
        },
        {
            "area": "Device, OS, UA decode",
            "status": "Partially available",
            "basis": "device_decode",
            "note": "UA/device decode exists but needs a normalized platform/OS/device mart.",
        },
        {
            "area": "App device/session IDs",
            "status": "Available for STREAM",
            "basis": "identity mart from queryStr session_id/device_id",
            "note": "STREAM has queryStr device/session evidence. FAST currently does not expose these fields in the CDN mart.",
        },
        {
            "area": "Content-title analytics",
            "status": "Partially available",
            "basis": "content/content_daily.parquet",
            "note": "STREAM has content_title manifest-view analytics. FAST currently has no content_title parameter. True content watch-hours need session attribution beyond CDN manifest rows.",
        },
        {
            "area": "New vs returning users",
            "status": "Available for STREAM",
            "basis": "identity mart first-seen device_id logic",
            "note": "STREAM new/returning is scoped to the selected identity dimension. FAST remains Not Available until device_id appears.",
        },
    ]

    return {
        "meta": {
            "title": args.title,
            "source": source.upper(),
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "min_date": min_date.isoformat(),
            "max_date": max_date.isoformat(),
            "source_true_ranges": source_true_ranges,
            "output_root": str(output_root),
            "notes": [
                "Request Hours = all .ts request rows * 6 seconds.",
                "Playback Hours = dedup unique segment-path estimate. Shared mart is WIP.",
                "This dashboard supports FAST and STREAM source filters.",
                "WIP sections are intentionally shown so stakeholders know which telemetry is not production-ready.",
            ],
        },
        "ranges": {
            "watch_daily": range_info(daily),
            "watch_by_source": source_ranges(daily),
            "channel": range_info(channel),
            "geo": range_info(geo),
            "ua_platform": range_info(ua_playtime["platform"]),
            "ua_os": range_info(ua_playtime["os"]),
            "content": range_info(content),
            "concurrency": range_info(concurrency["summary"]),
            "latency": range_info(latency["daily"]),
            "identity": range_info(identity["daily"]),
            "overview_daily": range_info(overview_daily, "date"),
        },
        "availability": availability,
        "data": {
            "daily": records(daily, 5000),
            "overview_daily": records(overview_daily, 5000),
            "channel_daily": records(channel, 6000),
            "geo_daily": records(geo, 25000),
            "channel_top": records(channel_top, 80),
            "geo_top": records(geo_top, 100),
            "ua_platform_daily": records(ua_playtime["platform"], 5000),
            "ua_os_daily": records(ua_playtime["os"], 5000),
            "concurrency_5min": records(concurrency["minute5"], 24000, tail=True),
            "concurrency_summary": records(concurrency["summary"], 2000),
            "platform_top": records(platform_top, 80),
            "content_daily": records(content, 50000),
            "content_top": records(content_top, 120),
            "latency_daily": records(latency["daily"], 3000),
            "latency_hourly": records(latency["hourly"], 12000, tail=True),
            "latency_channel": records(latency["channel"], 6000),
            "latency_host_daily": records(latency["host"], 5000),
            "latency_host_top": records(host_latency, 100),
            "latency_status": records(status_top, 120),
            "device_summary": records(device_by_name, 100),
            "identity_daily": records(identity["daily"], 5000),
            "identity_channel_daily": records(identity["channel"], 12000),
            "identity_platform_daily": records(identity["platform"], 8000),
            "identity_platform_channel_daily": records(identity["platform_channel"], 16000),
        },
        "source_files": {
            "watch_daily_tables": str(watch_dir),
            "concurrency": str(concurrency_dir),
            "latency": str(latency_dir),
            "device_decode": str(device_dir),
            "identity": str(identity_dir),
            "content": str(content_dir),
            "device_summary": devices["summary_path"],
            "ua_decode": devices["ua_path"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the Veto Audience Operations dashboard.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--title", default="Veto Audience Operations")
    parser.add_argument("--source", default="all", choices=["fast", "stream", "all"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data = build_data(args)
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        CHARTJS_TAG=chartjs_script(chartjs),
        DATA_BLOB=json_blob(data),
    )

    if args.dry_run:
        print(f"[dry-run] Audience ops HTML chars: {len(html):,}")
        print(f"Would write: {args.out}")
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"Audience operations dashboard written: {args.out.resolve()}")
    print(f"Size: {args.out.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
