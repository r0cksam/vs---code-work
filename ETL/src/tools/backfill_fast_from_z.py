#!/usr/bin/env python3
"""Backfill historical FAST raw folders from a backup drive into the ETL lake."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
import subprocess
import sys


ETL_ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ETL_ROOT / "src" / "orchestrator" / "run_pipeline.py"
DEFAULT_BASE = ETL_ROOT / "data"
DEFAULT_RAW_ROOT = Path(r"Z:\Veto Logs Backup")
DEFAULT_FAST_NAME = "veto fast logs"
DEFAULT_LOG_DIR = ETL_ROOT / "output" / "logs" / "fast_backfill"


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; use YYYY-MM-DD") from exc


def date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit("--end must be on or after --start")
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def final_clean_path(base: Path, target: date) -> Path:
    return (
        base
        / "stage"
        / "final_clean"
        / "source=fast"
        / f"year={target:%Y}"
        / f"month={target:%m}"
        / f"day={target:%d}"
        / f"fast_{target:%Y_%m_%d}_final_clean.parquet"
    )


def raw_day_path(raw_root: Path, fast_name: str, target: date) -> Path:
    return raw_root / fast_name / f"{target:%m}" / f"{target:%d}"


def run_date(args: argparse.Namespace, target: date, python: Path, log_dir: Path) -> tuple[str, int]:
    raw_dir = raw_day_path(args.raw_root, args.fast_name, target)
    final_clean = final_clean_path(args.base, target)
    if not raw_dir.is_dir():
        return ("missing_raw", 0)
    if args.skip_existing and final_clean.exists():
        return ("already_done", 0)

    cmd = [
        str(python),
        str(PIPELINE),
        "--base",
        str(args.base),
        "--etl1-daily-date",
        target.isoformat(),
        "--etl1-daily-raw-root",
        str(args.raw_root),
        "--etl1-sources",
        "fast",
        "--etl1-fast-name",
        args.fast_name,
        "--skip-watch",
        "--skip-overview",
        "--skip-deep-profile",
        "--skip-device-snapshot",
        "--etl1-workers",
        str(args.workers),
        "--etl1-compression",
        args.compression,
    ]

    log_path = log_dir / f"fast_backfill_{target:%Y_%m_%d}.log"
    with log_path.open("w", encoding="utf-8", newline="") as log:
        log.write(f"Started: {datetime.now().isoformat()}\n")
        log.write(f"Raw dir: {raw_dir}\n")
        log.write("Command: " + " ".join(cmd) + "\n\n")
        log.flush()
        proc = subprocess.run(
            cmd,
            cwd=str(ETL_ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        log.write(f"\nFinished: {datetime.now().isoformat()}\n")
        log.write(f"Exit code: {proc.returncode}\n")

    return ("ok" if proc.returncode == 0 else "failed", proc.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical FAST raw dates from Z: into ETL lake.")
    parser.add_argument("--start", type=parse_date, required=True)
    parser.add_argument("--end", type=parse_date, required=True)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--fast-name", default=DEFAULT_FAST_NAME)
    parser.add_argument("--base", type=Path, default=DEFAULT_BASE)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "lz4", "gzip", "brotli", "none"])
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    args = parser.parse_args()

    args.raw_root = args.raw_root.expanduser().resolve()
    args.base = args.base.expanduser().resolve()
    args.log_dir = args.log_dir.expanduser().resolve()
    args.log_dir.mkdir(parents=True, exist_ok=True)

    python = Path(sys.executable).resolve()
    summary_path = args.log_dir / f"fast_backfill_{args.start:%Y_%m_%d}_to_{args.end:%Y_%m_%d}_summary.log"
    with summary_path.open("a", encoding="utf-8", newline="") as summary:
        summary.write(f"\nBackfill started: {datetime.now().isoformat()}\n")
        summary.write(f"Range: {args.start.isoformat()} to {args.end.isoformat()}\n")
        summary.write(f"Raw root: {args.raw_root}\n")
        summary.write(f"Base: {args.base}\n")
        summary.flush()

        for target in date_range(args.start, args.end):
            print(f"[{datetime.now().isoformat()}] {target.isoformat()} starting", flush=True)
            status, code = run_date(args, target, python, args.log_dir)
            line = f"{datetime.now().isoformat()} {target.isoformat()} {status} exit={code}\n"
            summary.write(line)
            summary.flush()
            print(line.strip(), flush=True)

        summary.write(f"Backfill finished: {datetime.now().isoformat()}\n")

    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
