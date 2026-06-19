#!/usr/bin/env python3
"""Near-live Veto log worker.

The worker downloads recent raw log folders, processes only stable new .gz
files, writes normalized detail parquet, and rebuilds exact minute aggregates.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

try:
    import orjson
except Exception:  # pragma: no cover - fallback for minimal envs
    orjson = None

from clickhouse_sink import clickhouse_enabled, insert_detail_rows, ping
from live_common import (
    DEFAULT_CONFIG,
    DEFAULT_STATE,
    IST_OFFSET_SECONDS,
    LIVE_ROOT,
    SEGMENTS_PER_MINUTE,
    SourceConfig,
    atomic_write_json,
    atomic_write_text,
    default_rclone_exe,
    get_sources,
    is_ts_path,
    load_config,
    load_json,
    normalize_host,
    normalize_status,
    normalize_ua,
    now_utc_iso,
    platform_for_host,
    resolve_live_channel,
    resolve_live_path,
    setup_rclone_env,
)


def log(message: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run near-live Veto microbatch processing.")
    parser.add_argument("--config", type=Path, default=Path(os.getenv("VETO_LIVE_CONFIG", str(DEFAULT_CONFIG))))
    parser.add_argument("--state", type=Path, default=Path(os.getenv("VETO_LIVE_STATE", str(DEFAULT_STATE))))
    parser.add_argument("--source", choices=["all", "stream", "fast"], default="all")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument("--loop", action="store_true", help="Run forever.")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-clickhouse", action="store_true")
    parser.add_argument("--max-files", type=int, default=None, help="Limit files processed per cycle. 0 means no limit.")
    parser.add_argument("--date", default=None, help="IST date to aggregate, YYYY-MM-DD. Default is current IST date.")
    parser.add_argument(
        "--remote-lookback-days",
        type=int,
        default=None,
        help="Override config remote_lookback_days for this run.",
    )
    parser.add_argument(
        "--download-max-age",
        default=None,
        help="Optional rclone --max-age duration for first controlled runs, for example 2h or 30m.",
    )
    return parser.parse_args()


def parse_json_line(raw: bytes) -> dict | None:
    try:
        if orjson is not None:
            obj = orjson.loads(raw)
        else:
            obj = json.loads(raw)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ist_now() -> datetime:
    return utc_now() + timedelta(seconds=IST_OFFSET_SECONDS)


def floor_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def target_remote_dates(config: dict, override_lookback_days: int | None = None) -> list[date]:
    lookback = max(1, int(override_lookback_days or config.get("remote_lookback_days") or 2))
    today_utc = utc_now().date()
    return [today_utc - timedelta(days=offset) for offset in range(lookback)]


def raw_day_path(source: SourceConfig, day_value: date) -> Path:
    return source.local_root / f"year={day_value:%Y}" / f"month={day_value:%m}" / f"day={day_value:%d}"


def remote_day_path(source: SourceConfig, day_value: date) -> str:
    return f"{source.remote_root}/{day_value:%m}/{day_value:%d}"


def remote_day_locations(source: SourceConfig, day_value: date) -> list[tuple[str, Path]]:
    remote_base = remote_day_path(source, day_value)
    local_base = raw_day_path(source, day_value)
    if not source.remote_children:
        return [(remote_base, local_base)]
    return [
        (f"{remote_base}/{child}", local_base / child.replace("/", "_"))
        for child in source.remote_children
    ]


def parse_duration_seconds(value: str | None) -> int | None:
    if not value:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    match = re.fullmatch(r"(\d+)\s*([smhd]?)", raw)
    if not match:
        raise SystemExit(f"Invalid duration: {value}. Use examples like 30m, 2h, 1d.")
    amount = int(match.group(1))
    unit = match.group(2) or "s"
    factors = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return amount * factors[unit]


def filename_epoch(name: str) -> int | None:
    match = re.search(r"-(1[0-9]{9})-", name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def rclone_lsf_files(remote_dir: str) -> list[str]:
    rclone = default_rclone_exe()
    args = [rclone, "lsf", remote_dir, "--files-only", "--max-depth", "1"]
    started = time.time()
    result = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=setup_rclone_env(),
    )
    files = [line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()]
    log(f"listed {len(files):,} remote files from {remote_dir} in {time.time() - started:.1f}s")
    return files


def copy_selected_remote_files(remote_dir: str, local_dir: Path, names: list[str]) -> None:
    if not names:
        return
    local_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = LIVE_ROOT / "state" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    files_from = tmp_dir / f"rclone_files_{int(time.time() * 1000)}.txt"
    files_from.write_text("\n".join(names) + "\n", encoding="utf-8")
    try:
        rclone = default_rclone_exe()
        args = [
            rclone,
            "copy",
            remote_dir,
            str(local_dir),
            "--files-from",
            str(files_from),
            "--no-traverse",
            "--size-only",
            "--transfers",
            "8",
            "--checkers",
            "16",
            "--retries",
            "3",
            "--low-level-retries",
            "10",
            "--stats",
            "30s",
        ]
        log(f"copying {len(names):,} selected files from {remote_dir} -> {local_dir}")
        subprocess.run(args, check=True, env=setup_rclone_env())
    finally:
        try:
            files_from.unlink()
        except FileNotFoundError:
            pass


def download_recent_files(
    config: dict,
    source: SourceConfig,
    dates: Iterable[date],
    state: dict,
    args: argparse.Namespace,
) -> list[Path]:
    min_age = int(os.getenv("VETO_LIVE_MIN_FILE_AGE_SECONDS") or config.get("min_file_age_seconds") or 120)
    max_age = parse_duration_seconds(args.download_max_age or (config.get("download") or {}).get("max_age"))
    max_files = args.max_files
    if max_files is None:
        max_files = int(config.get("max_files_per_cycle") or 0)

    now_epoch = int(time.time())
    processed = state.setdefault("processed_files", {})
    candidates: list[dict] = []

    for day_value in dates:
        for remote_dir, local_dir in remote_day_locations(source, day_value):
            for name in rclone_lsf_files(remote_dir):
                if not name.lower().endswith(".gz"):
                    continue
                epoch_value = filename_epoch(name)
                if epoch_value is None:
                    continue
                age = now_epoch - epoch_value
                if age < min_age:
                    continue
                if max_age is not None and age > max_age:
                    continue
                local_path = local_dir / name
                if str(local_path.resolve()) in processed:
                    continue
                candidates.append(
                    {
                        "epoch": epoch_value,
                        "remote_dir": remote_dir,
                        "local_dir": local_dir,
                        "name": name,
                        "local_path": local_path,
                    }
                )

    candidates.sort(key=lambda item: item["epoch"], reverse=True)
    if max_files and max_files > 0:
        candidates = candidates[:max_files]

    grouped: dict[tuple[str, Path], list[str]] = {}
    local_paths: list[Path] = []
    for item in candidates:
        local_paths.append(item["local_path"])
        if item["local_path"].exists():
            continue
        grouped.setdefault((item["remote_dir"], item["local_dir"]), []).append(item["name"])

    for (remote_dir, local_dir), names in grouped.items():
        copy_selected_remote_files(remote_dir, local_dir, names)

    log(f"{source.name}: selected recent remote files={len(candidates):,}")
    return local_paths


def run_rclone_copy(config: dict, source: SourceConfig, day_value: date, download_max_age: str | None = None) -> None:
    download = config.get("download") or {}
    rclone = default_rclone_exe()
    remote = remote_day_path(source, day_value)
    local = raw_day_path(source, day_value)
    local.mkdir(parents=True, exist_ok=True)

    args = [
        rclone,
        "copy",
        remote,
        str(local),
        "--size-only",
        "--transfers",
        str(download.get("transfers", 8)),
        "--checkers",
        str(download.get("checkers", 16)),
        "--buffer-size",
        str(download.get("buffer_size", "16M")),
        "--retries",
        str(download.get("retries", 3)),
        "--low-level-retries",
        str(download.get("low_level_retries", 10)),
        "--stats",
        "30s",
    ]
    max_age = download_max_age or download.get("max_age")
    if max_age:
        args.extend(["--max-age", str(max_age)])
    log(f"{source.name}: rclone copy {remote} -> {local}")
    subprocess.run(args, check=True, env=setup_rclone_env())


def file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def stable_gz_files(source: SourceConfig, dates: Iterable[date], min_age_seconds: int) -> list[Path]:
    cutoff = time.time() - max(0, min_age_seconds)
    files: list[Path] = []
    for day_value in dates:
        folder = raw_day_path(source, day_value)
        if not folder.exists():
            continue
        for path in folder.rglob("*.gz"):
            try:
                if path.stat().st_mtime <= cutoff:
                    files.append(path)
            except OSError:
                continue
    return sorted(files, key=lambda item: str(item).lower())


def read_state(path: Path) -> dict:
    state = load_json(path, {})
    if not isinstance(state, dict):
        return {}
    state.setdefault("processed_files", {})
    state.setdefault("changed_after_processed", {})
    state.setdefault("batches", [])
    return state


def choose_new_files(files: list[Path], state: dict, max_files: int) -> tuple[list[Path], int]:
    processed = state.setdefault("processed_files", {})
    changed = state.setdefault("changed_after_processed", {})
    selected: list[Path] = []
    skipped_changed = 0

    for path in files:
        key = str(path.resolve())
        sig = file_signature(path)
        old_sig = processed.get(key, {}).get("signature")
        if old_sig == sig:
            continue
        if old_sig and old_sig != sig:
            changed[key] = {
                "old_signature": old_sig,
                "new_signature": sig,
                "seen_at_utc": now_utc_iso(),
            }
            skipped_changed += 1
            continue
        selected.append(path)
        if max_files and max_files > 0 and len(selected) >= max_files:
            break
    return selected, skipped_changed


def epoch_to_minutes(req_time_sec: object) -> tuple[str, str, str] | None:
    try:
        sec = float(req_time_sec)
    except (TypeError, ValueError):
        return None
    if sec <= 0:
        return None

    utc_minute = floor_minute(datetime.fromtimestamp(sec, tz=timezone.utc))
    ist_minute = floor_minute(datetime.fromtimestamp(sec + IST_OFFSET_SECONDS, tz=timezone.utc))
    return (
        utc_minute.strftime("%Y-%m-%d %H:%M:%S"),
        ist_minute.strftime("%Y-%m-%d %H:%M:%S"),
        ist_minute.strftime("%Y-%m-%d"),
    )


def detail_rows_from_file(path: Path, source_name: str) -> tuple[list[dict], Counter]:
    rows: list[dict] = []
    counters: Counter = Counter()
    source_hash = hashlib.blake2b(str(path.resolve()).encode("utf-8"), digest_size=8).hexdigest()

    try:
        handle = gzip.open(path, "rb")
    except OSError as exc:
        counters[f"open_error:{type(exc).__name__}"] += 1
        return rows, counters

    with handle:
        for raw in handle:
            counters["lines"] += 1
            raw = raw.strip()
            if not raw:
                continue
            obj = parse_json_line(raw)
            if not obj:
                counters["bad_json"] += 1
                continue

            req_path = obj.get("reqPath") or obj.get("reqpath") or ""
            if not is_ts_path(req_path):
                counters["non_ts"] += 1
                continue

            minute_parts = epoch_to_minutes(obj.get("reqTimeSec") or obj.get("req_time_sec"))
            if minute_parts is None:
                counters["bad_time"] += 1
                continue

            minute_utc, minute_ist, log_date = minute_parts
            req_host = normalize_host(obj.get("reqHost") or obj.get("reqhost") or "")
            candidate_id, channel_name = resolve_live_channel(req_host, req_path)
            platform_key, platform_name = platform_for_host(source_name, req_host)

            rows.append(
                {
                    "log_date": log_date,
                    "source": source_name,
                    "minute_utc": minute_utc,
                    "minute_ist": minute_ist,
                    "reqHost": req_host,
                    "platform_key": platform_key,
                    "platform_name": platform_name,
                    "candidate_id": candidate_id,
                    "channel_name": channel_name,
                    "status_code": normalize_status(obj.get("statusCode") or obj.get("status") or ""),
                    "cliIP": str(obj.get("cliIP") or obj.get("clientIP") or ""),
                    "UA": normalize_ua(obj.get("UA") or obj.get("userAgent") or ""),
                    "source_file_hash": source_hash,
                }
            )
            counters["ts_rows"] += 1

    return rows, counters


def write_detail_batch(config: dict, rows: list[dict], source_name: str, dry_run: bool) -> list[Path]:
    if not rows:
        return []
    detail_root = resolve_live_path((config.get("output") or {}).get("detail_root", "data/detail"))
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    frame = pd.DataFrame(rows)
    written: list[Path] = []
    for log_date, part in frame.groupby("log_date", sort=True):
        out_dir = detail_root / f"source={source_name}" / f"log_date={log_date}"
        out_path = out_dir / f"batch_{batch_id}.parquet"
        written.append(out_path)
        if dry_run:
            log(f"{source_name}: dry-run would write {len(part):,} detail rows -> {out_path}")
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        part.to_parquet(out_path, index=False, compression="zstd")
        log(f"{source_name}: wrote {len(part):,} detail rows -> {out_path}")
    return written


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def build_live_outputs(config: dict, aggregate_date: str, dry_run: bool) -> dict:
    output_cfg = config.get("output") or {}
    detail_root = resolve_live_path(output_cfg.get("detail_root", "data/detail"))
    minute_path = resolve_live_path(output_cfg.get("minute_parquet", "output/live_minute_aggregate.parquet"))
    status_path = resolve_live_path(output_cfg.get("status_parquet", "output/live_status_minute.parquet"))
    health_path = resolve_live_path(output_cfg.get("health_json", "output/live_health.json"))
    metrics_path = resolve_live_path(output_cfg.get("metrics_prom", "output/live_metrics.prom"))

    files = list(detail_root.glob(f"source=*/log_date={aggregate_date}/*.parquet"))
    if not files:
        health = {
            "updated_at_utc": now_utc_iso(),
            "aggregate_date": aggregate_date,
            "detail_files": 0,
            "minute_rows": 0,
            "status_rows": 0,
            "message": "No live detail parquet files found for aggregate date.",
        }
        if not dry_run:
            atomic_write_json(health_path, health)
            atomic_write_text(metrics_path, prometheus_metrics([], health))
        return health

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    glob_sql = q(detail_root / f"source=*/log_date={aggregate_date}/*.parquet")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE detail AS
        SELECT * FROM read_parquet('{glob_sql}', hive_partitioning=1, union_by_name=1)
        WHERE log_date = '{aggregate_date}'
        """
    )
    minute = con.execute(
        f"""
        SELECT
            log_date,
            source,
            minute_utc,
            minute_ist,
            platform_key,
            platform_name,
            candidate_id,
            channel_name,
            any_value(reqHost ORDER BY reqHost) AS reqHost,
            COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
            COUNT(*)::BIGINT AS raw_ts_rows,
            COUNT(*) FILTER (WHERE status_code = '200')::BIGINT AS status_200_ts_rows,
            COUNT(DISTINCT NULLIF(cliIP, ''))::BIGINT AS unique_viewers,
            COUNT(DISTINCT NULLIF(UA, ''))::BIGINT AS unique_ua_viewers,
            ROUND(COUNT(*) / {SEGMENTS_PER_MINUTE}, 3) AS segment_viewers_estimate,
            ROUND(COUNT(*) FILTER (WHERE status_code = '200') / {SEGMENTS_PER_MINUTE}, 3)
                AS status_200_segment_viewers_estimate
        FROM detail
        GROUP BY 1,2,3,4,5,6,7,8
        ORDER BY minute_ist, source, platform_name, channel_name
        """
    ).fetchdf()
    status = con.execute(
        f"""
        SELECT
            log_date,
            source,
            minute_utc,
            minute_ist,
            platform_key,
            platform_name,
            candidate_id,
            channel_name,
            status_code,
            COUNT(*)::BIGINT AS status_ts_rows,
            ROUND(COUNT(*) / {SEGMENTS_PER_MINUTE}, 3) AS status_segment_viewers_estimate
        FROM detail
        GROUP BY 1,2,3,4,5,6,7,8,9
        ORDER BY minute_ist, source, platform_name, channel_name, status_code
        """
    ).fetchdf()

    health = {
        "updated_at_utc": now_utc_iso(),
        "aggregate_date": aggregate_date,
        "detail_files": len(files),
        "minute_rows": int(len(minute)),
        "status_rows": int(len(status)),
        "raw_ts_rows": int(minute["raw_ts_rows"].sum()) if not minute.empty else 0,
        "status_200_ts_rows": int(minute["status_200_ts_rows"].sum()) if not minute.empty else 0,
        "latest_minute_ist": str(minute["minute_ist"].max()) if not minute.empty else "",
    }
    if not minute.empty:
        source_summary = (
            minute.groupby("source", as_index=False)
            .agg(
                minute_rows=("minute_ist", "count"),
                raw_ts_rows=("raw_ts_rows", "sum"),
                status_200_ts_rows=("status_200_ts_rows", "sum"),
                latest_minute_ist=("minute_ist", "max"),
            )
            .sort_values("source")
        )
        health["aggregate_sources"] = source_summary.to_dict("records")
    else:
        health["aggregate_sources"] = []

    if dry_run:
        log(f"dry-run would write aggregates: minute_rows={len(minute):,}, status_rows={len(status):,}")
        return health

    minute_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    minute.to_parquet(minute_path, index=False, compression="zstd")
    status.to_parquet(status_path, index=False, compression="zstd")
    atomic_write_json(health_path, health)
    atomic_write_text(metrics_path, prometheus_metrics(minute.to_dict("records"), health))
    log(f"wrote live aggregates -> {minute_path}")
    return health


def prometheus_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", " ").replace('"', '\\"')


def prometheus_metrics(minute_rows: list[dict], health: dict) -> str:
    lines = [
        "# HELP veto_live_updated_timestamp_seconds Unix timestamp when live metrics were generated.",
        "# TYPE veto_live_updated_timestamp_seconds gauge",
        f"veto_live_updated_timestamp_seconds {int(time.time())}",
        "# HELP veto_live_raw_ts_rows_total Current aggregate raw .ts rows.",
        "# TYPE veto_live_raw_ts_rows_total gauge",
        f"veto_live_raw_ts_rows_total {int(health.get('raw_ts_rows') or 0)}",
        "# HELP veto_live_status_200_ts_rows_total Current aggregate HTTP 200 .ts rows.",
        "# TYPE veto_live_status_200_ts_rows_total gauge",
        f"veto_live_status_200_ts_rows_total {int(health.get('status_200_ts_rows') or 0)}",
        "# HELP veto_live_estimated_viewers Latest-minute estimated viewers by source/platform/channel.",
        "# TYPE veto_live_estimated_viewers gauge",
        "# HELP veto_live_estimated_viewers_http_200 Latest-minute estimated HTTP 200 viewers by source/platform/channel.",
        "# TYPE veto_live_estimated_viewers_http_200 gauge",
    ]

    if minute_rows:
        latest_by_series: dict[tuple[str, str, str], dict] = {}
        for row in minute_rows:
            key = (
                str(row.get("source") or ""),
                str(row.get("platform_name") or ""),
                str(row.get("channel_name") or ""),
            )
            if key not in latest_by_series or str(row.get("minute_ist") or "") > str(latest_by_series[key].get("minute_ist") or ""):
                latest_by_series[key] = row
        for row in list(latest_by_series.values())[:500]:
            labels = {
                "source": row.get("source", ""),
                "platform": row.get("platform_name", ""),
                "channel": row.get("channel_name", ""),
            }
            label_text = ",".join(f'{key}="{prometheus_label(value)}"' for key, value in labels.items())
            lines.append(f"veto_live_estimated_viewers{{{label_text}}} {float(row.get('segment_viewers_estimate') or 0)}")
            lines.append(
                f"veto_live_estimated_viewers_http_200{{{label_text}}} "
                f"{float(row.get('status_200_segment_viewers_estimate') or 0)}"
            )

    return "\n".join(lines) + "\n"


def process_source(config: dict, source: SourceConfig, state: dict, args: argparse.Namespace) -> dict:
    dates = target_remote_dates(config, args.remote_lookback_days)
    download_enabled = bool((config.get("download") or {}).get("enabled", True))
    downloaded_candidates: list[Path] = []
    if download_enabled and not args.skip_download:
        downloaded_candidates = download_recent_files(config, source, dates, state, args)

    env_min_age = os.getenv("VETO_LIVE_MIN_FILE_AGE_SECONDS")
    min_age = int(env_min_age or config.get("min_file_age_seconds") or 120)
    max_files = args.max_files
    if max_files is None:
        max_files = int(config.get("max_files_per_cycle") or 0)

    local_stable_files = stable_gz_files(source, dates, min_age)
    files = list(dict.fromkeys(downloaded_candidates + local_stable_files))
    selected, skipped_changed = choose_new_files(files, state, max_files)
    log(f"{source.name}: stable files={len(files):,}, new files={len(selected):,}, changed-skipped={skipped_changed:,}")

    all_rows: list[dict] = []
    counters: Counter = Counter()
    for index, path in enumerate(selected, start=1):
        rows, file_counts = detail_rows_from_file(path, source.name)
        counters.update(file_counts)
        all_rows.extend(rows)
        log(f"{source.name}: processed {index}/{len(selected)} {path.name}: ts_rows={len(rows):,}")

    batch_paths = write_detail_batch(config, all_rows, source.name, args.dry_run)
    clickhouse_rows = 0
    if all_rows and clickhouse_enabled(config) and not args.skip_clickhouse and not args.dry_run:
        clickhouse_rows = insert_detail_rows(config, all_rows)
        log(f"{source.name}: inserted {clickhouse_rows:,} detail rows into ClickHouse")

    if not args.dry_run:
        processed = state.setdefault("processed_files", {})
        for path in selected:
            key = str(path.resolve())
            processed[key] = {
                "source": source.name,
                "signature": file_signature(path),
                "processed_at_utc": now_utc_iso(),
            }
        for batch_path in batch_paths:
            state.setdefault("batches", []).append(
                {
                    "source": source.name,
                    "path": str(batch_path),
                    "created_at_utc": now_utc_iso(),
                }
            )

    return {
        "source": source.name,
        "stable_files": len(files),
        "new_files": len(selected),
        "changed_skipped": skipped_changed,
        "detail_rows": len(all_rows),
        "clickhouse_rows": clickhouse_rows,
        "counters": dict(counters),
    }


def run_cycle(config: dict, state_path: Path, args: argparse.Namespace) -> dict:
    state = read_state(state_path)
    sources = get_sources(config, args.source)
    if not sources:
        raise SystemExit("No enabled sources selected.")

    summaries = []
    for source in sources:
        summaries.append(process_source(config, source, state, args))

    aggregate_date = args.date or ist_now().strftime("%Y-%m-%d")
    health = build_live_outputs(config, aggregate_date, args.dry_run)
    health["cycle_sources"] = summaries
    health["cycle_finished_at_utc"] = now_utc_iso()

    if not args.dry_run:
        atomic_write_json(state_path, state)
        output_cfg = config.get("output") or {}
        health_path = resolve_live_path(output_cfg.get("health_json", "output/live_health.json"))
        atomic_write_json(health_path, health)

    return health


def main() -> None:
    args = parse_args()
    if not args.once and not args.loop:
        args.once = True

    config = load_config(args.config)
    state_path = args.state.expanduser()
    if not state_path.is_absolute():
        state_path = (Path(__file__).resolve().parents[1] / state_path).resolve()

    if clickhouse_enabled(config) and not args.skip_clickhouse:
        log(f"ClickHouse ping: {'ok' if ping(config) else 'failed'}")

    env_poll = os.getenv("VETO_LIVE_POLL_SECONDS")
    interval = max(10, int(env_poll or config.get("poll_interval_seconds") or 60))
    while True:
        try:
            health = run_cycle(config, state_path, args)
            log(f"cycle complete: aggregate_date={health.get('aggregate_date')} minute_rows={health.get('minute_rows')}")
        except subprocess.CalledProcessError as exc:
            log(f"rclone/process command failed with exit code {exc.returncode}")
            if args.once:
                raise
        except KeyboardInterrupt:
            log("stopped by user")
            return
        except Exception as exc:
            log(f"cycle failed: {exc}")
            if args.once:
                raise

        if args.once:
            return
        time.sleep(interval)


if __name__ == "__main__":
    main()
