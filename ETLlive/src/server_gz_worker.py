#!/usr/bin/env python3
"""Server-side near-live Veto worker.

Run this on the server or VM that can see the raw .gz log folders directly.
It avoids rclone/download work, processes only stable new .gz files, writes
rolling parquet/Prometheus outputs for the preview stack, and can insert detail
rows into ClickHouse for network-accessible Grafana history.
"""

from __future__ import annotations

import argparse
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from clickhouse_sink import clickhouse_enabled, insert_detail_rows, ping
from live_common import (
    DEFAULT_CONFIG,
    IST_OFFSET_SECONDS,
    LIVE_ROOT,
    atomic_write_json,
    load_config,
    load_json,
    now_utc_iso,
    resolve_live_path,
)
from live_worker import (
    build_live_outputs,
    choose_new_files,
    detail_rows_from_file,
    file_signature,
    log,
    write_detail_batch,
)


DEFAULT_SERVER_CONFIG = LIVE_ROOT / "config" / "server_live_config.json"
DEFAULT_SERVER_STATE = LIVE_ROOT / "state" / "server_gz_state.json"


@dataclass(frozen=True)
class ServerSource:
    name: str
    root: Path
    path_template: str
    children: tuple[str, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process server-local Veto .gz logs.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(str(DEFAULT_SERVER_CONFIG)),
        help="Server live config JSON. Defaults to config/server_live_config.json.",
    )
    parser.add_argument("--state", type=Path, default=Path(str(DEFAULT_SERVER_STATE)))
    parser.add_argument("--source", choices=["all", "stream", "fast"], default="all")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-clickhouse", action="store_true")
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument(
        "--date",
        default=None,
        help="IST date to rebuild preview aggregate, YYYY-MM-DD. Default is current IST date.",
    )
    return parser.parse_args()


def read_state(path: Path) -> dict:
    state = load_json(path, {})
    if not isinstance(state, dict):
        state = {}
    state.setdefault("processed_files", {})
    state.setdefault("changed_after_processed", {})
    state.setdefault("batches", [])
    return state


def resolve_state_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return (LIVE_ROOT / path).resolve()


def target_utc_dates(config: dict, override: int | None) -> list[date]:
    lookback = max(
        1,
        int(
            override
            or config.get("server_lookback_days")
            or config.get("remote_lookback_days")
            or 2
        ),
    )
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=offset) for offset in range(lookback)]


def ist_today() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=IST_OFFSET_SECONDS)).strftime(
        "%Y-%m-%d"
    )


def format_day_path(template: str, day_value: date) -> str:
    return template.format(
        year=f"{day_value:%Y}",
        yyyy=f"{day_value:%Y}",
        yy=f"{day_value:%y}",
        month=f"{day_value:%m}",
        mm=f"{day_value:%m}",
        day=f"{day_value:%d}",
        dd=f"{day_value:%d}",
    )


def get_server_sources(config: dict, only_source: str) -> list[ServerSource]:
    raw_sources = config.get("server_sources") or {}
    if not isinstance(raw_sources, dict):
        raise SystemExit("config.server_sources must be an object")

    sources: list[ServerSource] = []
    for name, item in raw_sources.items():
        if only_source != "all" and name != only_source:
            continue
        if not isinstance(item, dict) or not bool(item.get("enabled", True)):
            continue

        root_value = item.get("root") or item.get("server_root") or item.get("input_root")
        if not root_value:
            raise SystemExit(f"Missing server source root for {name!r}")
        root = Path(str(root_value)).expanduser()
        if not root.is_absolute():
            root = resolve_live_path(root)

        children = tuple(
            str(child).strip("/\\")
            for child in (item.get("children") or item.get("remote_children") or [])
            if str(child).strip("/\\")
        )
        sources.append(
            ServerSource(
                name=name,
                root=root,
                path_template=str(item.get("path_template") or "{month}/{day}"),
                children=children,
            )
        )

    if not sources:
        raise SystemExit("No enabled server_sources selected.")
    return sources


def source_day_dirs(source: ServerSource, dates: Iterable[date]) -> list[Path]:
    folders: list[Path] = []
    for day_value in dates:
        base = source.root / format_day_path(source.path_template, day_value)
        if source.children:
            folders.extend(base / child for child in source.children)
        else:
            folders.append(base)
    return folders


def stable_gz_files(source: ServerSource, dates: Iterable[date], min_age_seconds: int) -> list[Path]:
    cutoff = time.time() - max(0, min_age_seconds)
    files: list[Path] = []
    missing: list[Path] = []
    for folder in source_day_dirs(source, dates):
        if not folder.exists():
            missing.append(folder)
            continue
        for path in folder.rglob("*.gz"):
            try:
                if path.stat().st_mtime <= cutoff:
                    files.append(path)
            except OSError:
                continue
    if missing:
        log(f"{source.name}: missing source folder(s): {len(missing)}")
    return sorted(files, key=lambda item: str(item).lower())


def process_source(config: dict, source: ServerSource, state: dict, args: argparse.Namespace) -> dict:
    min_age = int(config.get("min_file_age_seconds") or 120)
    max_files = args.max_files
    if max_files is None:
        max_files = int(config.get("max_files_per_cycle") or 0)

    dates = target_utc_dates(config, args.lookback_days)
    files = stable_gz_files(source, dates, min_age)
    selected, skipped_changed = choose_new_files(files, state, max_files)
    log(
        f"{source.name}: server gz files={len(files):,}, "
        f"new={len(selected):,}, changed-skipped={skipped_changed:,}"
    )

    rows: list[dict] = []
    counters: Counter = Counter()
    for index, path in enumerate(selected, start=1):
        file_rows, file_counts = detail_rows_from_file(path, source.name)
        rows.extend(file_rows)
        counters.update(file_counts)
        log(f"{source.name}: processed {index}/{len(selected)} {path.name}: ts_rows={len(file_rows):,}")

    batch_paths = write_detail_batch(config, rows, source.name, args.dry_run)
    clickhouse_rows = 0
    if rows and clickhouse_enabled(config) and not args.skip_clickhouse and not args.dry_run:
        clickhouse_rows = insert_detail_rows(config, rows)
        log(f"{source.name}: inserted {clickhouse_rows:,} detail rows into ClickHouse")

    if not args.dry_run:
        processed = state.setdefault("processed_files", {})
        for path in selected:
            processed[str(path.resolve())] = {
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
        "server_files": len(files),
        "new_files": len(selected),
        "changed_skipped": skipped_changed,
        "detail_rows": len(rows),
        "clickhouse_rows": clickhouse_rows,
        "counters": dict(counters),
    }


def run_cycle(config: dict, state_path: Path, args: argparse.Namespace) -> dict:
    state = read_state(state_path)
    summaries = []
    for source in get_server_sources(config, args.source):
        summaries.append(process_source(config, source, state, args))

    aggregate_date = args.date or ist_today()
    health = build_live_outputs(config, aggregate_date, args.dry_run)
    health["cycle_sources"] = summaries
    health["cycle_finished_at_utc"] = now_utc_iso()
    health["ingest_mode"] = "server_local_gz"

    if not args.dry_run:
        atomic_write_json(state_path, state)
        output_cfg = config.get("output") or {}
        health_path = resolve_live_path(output_cfg.get("health_json", "output/live_health.json"))
        atomic_write_json(health_path, health)
    return health


def main() -> None:
    args = parse_args()
    if not args.config.exists():
        fallback = DEFAULT_CONFIG
        raise SystemExit(
            f"Server config not found: {args.config}\n"
            f"Create it from ETLlive/config/server_live_config.example.json.\n"
            f"For local rclone preview, use {fallback} with live_worker.py instead."
        )
    if not args.once and not args.loop:
        args.once = True

    config = load_config(args.config)
    state_path = resolve_state_path(args.state)
    interval = max(10, int(config.get("poll_interval_seconds") or 60))

    if clickhouse_enabled(config) and not args.skip_clickhouse:
        log(f"ClickHouse ping: {'ok' if ping(config) else 'failed'}")

    while True:
        try:
            health = run_cycle(config, state_path, args)
            log(
                "cycle complete: "
                f"aggregate_date={health.get('aggregate_date')} "
                f"minute_rows={health.get('minute_rows')}"
            )
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
