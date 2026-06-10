#!/usr/bin/env python3
"""Run the production ETL + dashboard pipeline from one place."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional


def _resolve_default_base(project_root: Path) -> Path:
    env_base = os.getenv("VG_ETL_BASE")
    if env_base:
        return Path(env_base).expanduser().resolve()

    script_dir = Path(__file__).resolve().parent
    candidates = [
        project_root / "data",
        project_root,
    ]
    candidates = [c.resolve() for c in candidates]
    for candidate in candidates:
        if (candidate / "lake").exists():
            return candidate
    for candidate in candidates:
        if candidate.exists():
            return candidate
    # Fallback to project-relative convention used by the legacy setup
    return candidates[0]


def _python_exec(project_root: Path) -> str:
    venv_python = project_root / "venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _local_script(etl_root: Path, local_rel: str) -> Path:
    script = (etl_root / local_rel).resolve()
    if not script.exists():
        raise SystemExit(f"Required ETL script not found: {script}")
    return script


def _safe_log_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "step"


def _print_subprocess_line(line: str) -> None:
    """Echo child-process output without failing on legacy Windows consoles."""
    try:
        sys.stdout.write(line)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        safe = line.encode(encoding, errors="replace").decode(encoding, errors="replace")
        try:
            sys.stdout.write(safe)
        except OSError:
            return
    except OSError:
        return
    try:
        sys.stdout.flush()
    except OSError:
        return


def run(
    command: list[str],
    env: dict[str, str],
    cwd: Optional[Path] = None,
    step_name: str = "command",
    log_dir: Optional[Path] = None,
) -> None:
    nice = " ".join(f'"{c}"' if " " in c else c for c in command)
    print(f"\n[run] {nice}")

    if log_dir is None:
        subprocess.run(command, check=True, cwd=str(cwd) if cwd else None, env=env)
        return

    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_path = log_dir / f"{stamp}_{_safe_log_name(step_name)}.log"
    print(f"[log] {log_path}")

    start = datetime.now()
    with log_path.open("w", encoding="utf-8", errors="replace") as log:
        log.write(f"step={step_name}\n")
        log.write(f"cwd={cwd or Path.cwd()}\n")
        log.write(f"command={nice}\n")
        log.write(f"started_at={start.isoformat(timespec='seconds')}\n\n")
        log.flush()

        process = subprocess.Popen(
            command,
            cwd=str(cwd) if cwd else None,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert process.stdout is not None
        for line in process.stdout:
            _print_subprocess_line(line)
            log.write(line)
        return_code = process.wait()
        finished = datetime.now()
        log.write(f"\nfinished_at={finished.isoformat(timespec='seconds')}\n")
        log.write(f"duration_seconds={(finished - start).total_seconds():.2f}\n")
        log.write(f"exit_code={return_code}\n")

    if return_code:
        raise SystemExit(f"Step failed: {step_name} (exit {return_code}). Log: {log_path}")


def _latest_lake_day(lake_root: Path) -> Optional[date]:
    if not lake_root.exists():
        return None
    days = []
    for d in lake_root.glob("**/day=*"):
        parts = d.parts
        try:
            day_part = next((p for p in parts if p.startswith("day=")), None)
            month_part = next((p for p in parts if p.startswith("month=")), None)
            year_part = next((p for p in parts if p.startswith("year=")), None)
            if not (day_part and month_part and year_part):
                continue
            ddd = day_part.split("=", 1)[1]
            mmm = month_part.split("=", 1)[1]
            yyy = year_part.split("=", 1)[1]
            days.append(date(int(yyy), int(mmm), int(ddd)))
        except Exception:
            continue
    return max(days) if days else None


def _build_profile_range(lake_root: Path, lookback_days: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    if not lookback_days or lookback_days <= 0:
        return None, None
    latest = _latest_lake_day(lake_root)
    if latest is None:
        return None, None
    start = latest - timedelta(days=lookback_days - 1)
    return start.isoformat(), latest.isoformat()


def _lake_day_exists(lake_root: Path, day_value: date) -> bool:
    year = day_value.strftime("%Y")
    month = day_value.strftime("%m")
    day = day_value.strftime("%d")
    candidates = [
        lake_root / f"year={year}" / f"month={month}" / f"day={day}",
    ]
    candidates.extend(lake_root.glob(f"source=*/year={year}/month={month}/day={day}"))
    return any(path.exists() and any(path.glob("*.parquet")) for path in candidates)


def _profile_ready(profile_dir: Path) -> bool:
    required_profile = [
        profile_dir / "daily_volume.parquet",
        profile_dir / "channel_daily.parquet",
        profile_dir / "channel_summary.parquet",
    ]
    daily_dir = profile_dir.parent / "daily_tables"
    required_daily = [
        daily_dir / "status_codes_daily.parquet",
        daily_dir / "geo_daily.parquet",
        daily_dir / "channel_geo_daily.parquet",
        daily_dir / "asn_daily.parquet",
        daily_dir / "mapping_quality_daily.parquet",
    ]
    return all(path.exists() and path.stat().st_size > 0 for path in required_profile + required_daily)


def _daily_profile_dates(lake_root: Path, target_date: date) -> list[date]:
    # A UTC raw day can write into two IST lake partitions: D and D+1.
    candidates = [target_date, target_date + timedelta(days=1)]
    return [day for day in candidates if _lake_day_exists(lake_root, day)]


def main() -> None:
    etl_root = Path(__file__).resolve().parents[2]
    workspace = etl_root
    default_base = _resolve_default_base(etl_root)
    default_output_root = workspace / "output"

    parser = argparse.ArgumentParser(
        description="Run Veto watch-hours/overview pipeline from one command"
    )
    parser.add_argument(
        "--base",
        default=str(default_base),
        help="Base folder for ETL output/input (contains *_parquet, *_final_clean and lake/).",
    )
    parser.add_argument(
        "--output-root",
        default=str(default_output_root),
        help="Reusable output root for dashboard artifacts.",
    )

    parser.add_argument("--skip-etl", action="store_true", help="Skip 001/02/03.")
    parser.add_argument("--skip-watch", action="store_true", help="Skip watch-hours dashboard.")
    parser.add_argument(
        "--skip-overview",
        action="store_true",
        help="Skip overview data + overview dashboard (watch-hours can still run).",
    )
    parser.add_argument(
        "--skip-deep-profile",
        action="store_true",
        help="Skip generating vglive deep profile outputs.",
    )
    parser.add_argument(
        "--skip-device-snapshot",
        action="store_true",
        help="Skip device_snapshot/device_daily generation.",
    )
    parser.add_argument(
        "--skip-concurrency",
        action="store_true",
        help="Skip FAST minute-level concurrency aggregate generation.",
    )
    parser.add_argument(
        "--skip-latency",
        action="store_true",
        help="Skip Veto latency dashboard generation.",
    )

    # 001.py controls
    parser.add_argument(
        "--etl1-mode",
        choices=["master", "single"],
        default="master",
        help="Run 001.py non-interactive in master or single mode.",
    )
    parser.add_argument("--etl1-master-dir", default=None, help="Master folder for 001.py master mode.")
    parser.add_argument("--etl1-input-dir", default=None, help="Input folder for 001.py single mode.")
    parser.add_argument("--etl1-output-dir", default=None, help="Output folder for 001.py single mode.")
    parser.add_argument("--etl1-output-root", default=None, help="Output root for 001.py master mode.")
    parser.add_argument("--etl1-prefs-file", default=None, help="Column preference JSON for 001.py.")
    parser.add_argument("--etl1-workers", type=int, default=None, help="001.py worker count.")
    parser.add_argument("--etl1-batch-size", type=int, default=None, help="001.py files-per-batch.")
    parser.add_argument(
        "--etl1-compression",
        choices=["zstd", "snappy", "lz4", "gzip", "brotli", "none"],
        default=None,
        help="001.py parquet compression.",
    )
    parser.add_argument("--etl1-add-meta", action="store_true", help="Add _src_file in 001.py output.")
    parser.add_argument(
        "--etl1-daily-date",
        default=None,
        help="Process only one downloaded raw day (YYYY-MM-DD) into source/date parquet folders.",
    )
    parser.add_argument(
        "--etl1-daily-raw-root",
        default=None,
        help="Raw root containing stream/fast source folders for --etl1-daily-date.",
    )
    parser.add_argument(
        "--etl1-stream-name",
        default="Veto Stream Backup",
        help="Stream raw folder name under the daily raw root.",
    )
    parser.add_argument(
        "--etl1-fast-name",
        default="Veto fast Backup",
        help="Fast raw folder name under the daily raw root.",
    )
    parser.add_argument(
        "--etl1-sources",
        choices=["both", "stream", "fast"],
        default="both",
        help="Daily source folders to process when --etl1-daily-date is set.",
    )

    # Deep profile controls
    parser.add_argument(
        "--deep-profile-window-days",
        type=int,
        default=None,
        help="Optional rolling window for deep profile --start/--end.",
    )
    parser.add_argument(
        "--deep-profile-mode",
        choices=["auto", "full", "incremental"],
        default="auto",
        help="auto uses incremental profile merge for --etl1-daily-date when an existing profile is present.",
    )
    parser.add_argument(
        "--deep-profile-threads",
        type=int,
        default=8,
        help="Threads passed to vglive_deep_profile.py.",
    )
    parser.add_argument(
        "--deep-profile-memory",
        default="20GB",
        help="Memory passed to vglive_deep_profile.py.",
    )
    parser.add_argument(
        "--deep-profile-output-format",
        choices=["parquet", "csv"],
        default="parquet",
    )
    parser.add_argument(
        "--deep-profile-top-n",
        type=int,
        default=1000,
        help="Top-N limit for large profile tables.",
    )
    parser.add_argument(
        "--deep-profile-querystr-profile",
        choices=["reuse", "refresh", "skip"],
        default="skip",
        help="queryStr profile freshness for deep profile step.",
    )
    parser.add_argument(
        "--deep-profile-top-values",
        choices=["reuse", "refresh", "skip"],
        default="skip",
        help="querystr/cmcd top-value table freshness.",
    )
    parser.add_argument(
        "--deep-profile-column-fill",
        choices=["reuse", "refresh", "skip"],
        default="reuse",
        help="Column fill csv freshness.",
    )

    # Dashboard/control locations
    parser.add_argument(
        "--watch-profile",
        default=None,
        help="Folder with deep_profile files for watch-hours dashboard.",
    )
    parser.add_argument(
        "--watch-out",
        default=None,
        help="Watch-hours dashboard html output path.",
    )
    parser.add_argument(
        "--watch-title",
        default="Veto Watch Hours",
        help="Watch-hours dashboard title.",
    )
    parser.add_argument(
        "--overview-data-dir",
        default=None,
        help="Folder with overview_report.xlsx and device_* csv files.",
    )
    parser.add_argument(
        "--overview-html",
        default=None,
        help="Overview dashboard output html path.",
    )
    parser.add_argument(
        "--overview-year",
        default=None,
        help="Optional year filter for overview_report.xlsx regeneration (YYYY).",
    )
    parser.add_argument(
        "--overview-month",
        default=None,
        help="Optional month filter for overview_report.xlsx regeneration (01-12).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run dashboards in validation mode where supported.")

    # UA decode cache controls
    parser.add_argument(
        "--run-ua-profile",
        action="store_true",
        help="Build distinct User-Agent profile and local decode cache before dashboards.",
    )
    parser.add_argument(
        "--ua-profile-window-days",
        type=int,
        default=None,
        help="Rolling lake window for UA profile. Defaults to 7 days when --run-ua-profile has no explicit range.",
    )
    parser.add_argument("--ua-profile-start", default=None, help="UA profile IST start date YYYY-MM-DD.")
    parser.add_argument("--ua-profile-end", default=None, help="UA profile IST end date YYYY-MM-DD.")
    parser.add_argument("--ua-profile-source", choices=["stream", "fast"], default=None)
    parser.add_argument("--ua-api-limit", type=int, default=0, help="Optional whatmyuseragent.com API decode count.")
    parser.add_argument("--ua-min-rows-for-api", type=int, default=1)

    # FAST concurrency controls
    parser.add_argument(
        "--concurrency-window-days",
        type=int,
        default=14,
        help="Recent lake window for concurrency when no daily ETL date is supplied.",
    )
    parser.add_argument("--concurrency-start", default=None, help="Concurrency IST start date YYYY-MM-DD.")
    parser.add_argument("--concurrency-end", default=None, help="Concurrency IST end date YYYY-MM-DD.")
    parser.add_argument("--concurrency-threads", type=int, default=6)
    parser.add_argument("--concurrency-memory", default="16GB")
    parser.add_argument(
        "--concurrency-html",
        default=None,
        help="Standalone FAST concurrency dashboard html output path.",
    )

    # Latency dashboard controls
    parser.add_argument("--latency-start", default=None, help="Latency IST start date YYYY-MM-DD.")
    parser.add_argument("--latency-end", default=None, help="Latency IST end date YYYY-MM-DD.")
    parser.add_argument(
        "--latency-source",
        choices=["fast", "stream"],
        default="fast",
        help="Source to process for incremental latency profile. Start with fast, then run stream later.",
    )
    parser.add_argument(
        "--latency-window-days",
        type=int,
        default=1,
        help="Recent lake window for latency dashboard when no explicit latency dates are supplied. Use 0 for all.",
    )
    parser.add_argument("--latency-threads", type=int, default=6)
    parser.add_argument("--latency-memory", default="16GB")
    parser.add_argument(
        "--latency-html",
        default=None,
        help="Standalone Veto latency dashboard html output path.",
    )
    parser.add_argument(
        "--latency-profile",
        default=None,
        help="Reusable latency aggregate/profile output folder.",
    )

    args = parser.parse_args()

    base_root = Path(args.base).resolve()
    output_root = Path(args.output_root).resolve()
    lake_root = base_root / "lake"
    output_root.mkdir(parents=True, exist_ok=True)
    log_dir = output_root / "logs"

    src_root = workspace / "src"
    pipeline_dir = src_root / "pipeline"
    watch_dir = src_root / "dashboards" / "watchHoursDashboard"
    concurrency_dashboard_dir = src_root / "dashboards" / "concurrencyDashboard"
    overview_dashboard_dir = src_root / "dashboards" / "overViewDashboard"
    profile_dir = Path(args.watch_profile).resolve() if args.watch_profile else output_root / "watch_hours" / "profile"
    watch_out = Path(args.watch_out).resolve() if args.watch_out else output_root / "watch_hours" / "veto_watch_hours.html"
    concurrency_out = (
        Path(args.concurrency_html).resolve()
        if args.concurrency_html
        else output_root / "watch_hours" / "concurrency" / "veto_concurrency.html"
    )
    latency_out = (
        Path(args.latency_html).resolve()
        if args.latency_html
        else output_root / "latency" / "veto_latency.html"
    )
    latency_profile = (
        Path(args.latency_profile).resolve()
        if args.latency_profile
        else output_root / "latency" / "profile"
    )
    overview_data_dir = Path(args.overview_data_dir).resolve() if args.overview_data_dir else output_root / "overview"
    overview_html = Path(args.overview_html).resolve() if args.overview_html else overview_data_dir / "overview_dashboard.html"

    if not base_root.exists():
        raise SystemExit(f"Base folder not found: {base_root}")

    if (
        args.skip_etl
        and args.skip_watch
        and args.skip_overview
        and args.skip_deep_profile
        and args.skip_device_snapshot
        and (args.skip_watch or args.skip_concurrency)
        and args.skip_latency
        and not args.run_ua_profile
    ):
        raise SystemExit("Nothing to run. Remove one skip flag.")

    profile_dir.mkdir(parents=True, exist_ok=True)
    watch_out.parent.mkdir(parents=True, exist_ok=True)
    concurrency_out.parent.mkdir(parents=True, exist_ok=True)
    latency_out.parent.mkdir(parents=True, exist_ok=True)
    latency_profile.mkdir(parents=True, exist_ok=True)
    overview_data_dir.mkdir(parents=True, exist_ok=True)
    overview_html.parent.mkdir(parents=True, exist_ok=True)

    needs_lake = (
        (not args.skip_deep_profile)
        or (not args.skip_device_snapshot)
        or (not args.skip_overview)
        or (not args.skip_latency)
        or ((not args.skip_watch) and (not args.skip_concurrency))
        or args.run_ua_profile
    )
    if args.skip_etl and needs_lake and not lake_root.exists():
        raise SystemExit(f"Lake folder not found: {lake_root}. Run 03.py first or re-check --base.")

    env = os.environ.copy()
    env.update(
        {
            "VG_ETL_BASE": str(base_root),
            "VG_DASH_PROFILE_DIR": str(profile_dir),
            "VG_DASH_WATCH_OUT": str(watch_out),
            "VG_DASH_OVERVIEW_BASE": str(overview_data_dir),
            "VG_CONCURRENCY_DIR": str(output_root / "watch_hours" / "concurrency"),
            "VG_CONCURRENCY_HTML": str(concurrency_out),
            "VG_LATENCY_HTML": str(latency_out),
            "VG_LATENCY_PROFILE_DIR": str(latency_profile),
            "VG_ETL_LAKE_ROOT": str(lake_root),
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUTF8": "1",
        }
    )

    # Non-interactive 001.py config
    mode = args.etl1_mode
    env["VG_ETL_001_MODE"] = mode
    default_raw_master = base_root / "raw" / "Veto Logs Backup"
    if args.etl1_master_dir:
        env["VG_ETL_001_MASTER_DIR"] = args.etl1_master_dir
    elif mode == "master" and default_raw_master.exists():
        env["VG_ETL_001_MASTER_DIR"] = str(default_raw_master)
        if not args.etl1_output_root:
            env["VG_ETL_001_OUTPUT_ROOT"] = str(base_root)
    else:
        env["VG_ETL_001_MASTER_DIR"] = str(base_root)
    if args.etl1_input_dir:
        env["VG_ETL_001_INPUT_DIR"] = args.etl1_input_dir
    if args.etl1_output_dir:
        env["VG_ETL_001_OUTPUT_DIR"] = args.etl1_output_dir
    if args.etl1_output_root:
        env["VG_ETL_001_OUTPUT_ROOT"] = args.etl1_output_root
    if args.etl1_prefs_file:
        env["VG_ETL_001_PREFS_FILE"] = args.etl1_prefs_file
    if args.etl1_workers:
        env["VG_ETL_001_WORKERS"] = str(args.etl1_workers)
    if args.etl1_batch_size:
        env["VG_ETL_001_BATCH_SIZE"] = str(args.etl1_batch_size)
    if args.etl1_compression:
        env["VG_ETL_001_COMPRESSION"] = args.etl1_compression
    if args.etl1_add_meta:
        env["VG_ETL_001_ADD_META"] = "1"

    python = _python_exec(etl_root)

    deep_profile_script = _local_script(
        etl_root,
        str(Path("src") / "profile" / "vglive_deep_profile.py"),
    )
    profile_merge_script = _local_script(
        etl_root,
        str(Path("src") / "profile" / "merge_watch_profile_delta.py"),
    )
    overview_generator_script = _local_script(
        etl_root,
        str(Path("src") / "overview" / "overViewGenerator.py"),
    )
    snapshot_generator_script = _local_script(
        etl_root,
        str(Path("src") / "overview" / "deviceSnapshotGenerator.py"),
    )
    ua_profile_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "profile_user_agents.py"),
    )
    concurrency_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_concurrency.py"),
    )
    concurrency_dashboard_script = _local_script(
        etl_root,
        str(Path("src") / "dashboards" / "concurrencyDashboard" / "generate_concurrency.py"),
    )
    latency_dashboard_script = _local_script(
        etl_root,
        str(Path("src") / "dashboards" / "latencyDashboard" / "generate_latency.py"),
    )
    latency_incremental_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_latency_profile_incremental.py"),
    )

    if not args.skip_etl:
        if args.etl1_daily_date:
            try:
                target_date = date.fromisoformat(args.etl1_daily_date)
            except ValueError as exc:
                raise SystemExit("--etl1-daily-date must be YYYY-MM-DD") from exc

            daily_raw_root = (
                Path(args.etl1_daily_raw_root).expanduser().resolve()
                if args.etl1_daily_raw_root
                else default_raw_master
            )
            month = target_date.strftime("%m")
            day = target_date.strftime("%d")
            active_source_ids: list[str] = []
            stage_jobs: list[dict[str, str]] = []
            daily_sources = []
            if args.etl1_sources in ("both", "stream"):
                daily_sources.append(("stream", args.etl1_stream_name))
            if args.etl1_sources in ("both", "fast"):
                daily_sources.append(("fast", args.etl1_fast_name))

            for source_key, source_folder_name in daily_sources:
                input_dir = daily_raw_root / source_folder_name / month / day
                if not input_dir.exists() or not input_dir.is_dir():
                    raise SystemExit(f"Daily raw folder not found for {source_key}: {input_dir}")

                source_id = f"{source_key}_{target_date.strftime('%Y_%m_%d')}"
                output_dir = (
                    base_root
                    / "stage"
                    / "parquet"
                    / f"source={source_key}"
                    / f"year={target_date.strftime('%Y')}"
                    / f"month={month}"
                    / f"day={day}"
                )
                final_clean_file = (
                    base_root
                    / "stage"
                    / "final_clean"
                    / f"source={source_key}"
                    / f"year={target_date.strftime('%Y')}"
                    / f"month={month}"
                    / f"day={day}"
                    / f"{source_id}_final_clean.parquet"
                )
                active_source_ids.append(source_id)
                stage_jobs.append(
                    {
                        "source_id": source_id,
                        "source_key": source_key,
                        "parquet_dir": str(output_dir),
                        "final_clean_file": str(final_clean_file),
                    }
                )

                step_env = env.copy()
                step_env["VG_ETL_001_MODE"] = "single"
                step_env["VG_ETL_001_INPUT_DIR"] = str(input_dir)
                step_env["VG_ETL_001_OUTPUT_DIR"] = str(output_dir)
                step_env["VG_ETL_001_STREAMING_BATCHES"] = "1"
                step_env.pop("VG_ETL_001_MASTER_DIR", None)
                step_env.pop("VG_ETL_001_OUTPUT_ROOT", None)

                run(
                    [python, "001.py"],
                    cwd=pipeline_dir,
                    env=step_env,
                    step_name=f"etl_001_{source_id}_raw_to_parquet",
                    log_dir=log_dir,
                )

            env["VG_ETL_PROCESS_SOURCES"] = ",".join(active_source_ids)
            env["VG_ETL_STAGE_JOBS"] = json.dumps(stage_jobs)
            env["VG_ETL_REPLACE_DATES"] = target_date.isoformat()
            print(f"\n[scope] ETL daily source IDs: {env['VG_ETL_PROCESS_SOURCES']}")
        else:
            run([python, "001.py"], cwd=pipeline_dir, env=env, step_name="etl_001_raw_to_parquet", log_dir=log_dir)
        run([python, "02.py"], cwd=pipeline_dir, env=env, step_name="etl_02_dedupe_final_clean", log_dir=log_dir)
        run([python, "03.py"], cwd=pipeline_dir, env=env, step_name="etl_03_partition_lake", log_dir=log_dir)
    else:
        print("\n[skip] ETL stages skipped.")

    if needs_lake and not lake_root.exists():
        raise SystemExit(f"Lake folder not found after ETL stage: {lake_root}. Check ETL logs in {log_dir}.")

    if not args.skip_deep_profile:
        daily_profile_dates: list[date] = []
        if args.etl1_daily_date:
            daily_profile_dates = _daily_profile_dates(lake_root, date.fromisoformat(args.etl1_daily_date))

        use_incremental_profile = (
            args.deep_profile_mode == "incremental"
            or (
                args.deep_profile_mode == "auto"
                and bool(daily_profile_dates)
                and _profile_ready(profile_dir)
            )
        )
        if use_incremental_profile and not daily_profile_dates:
            if args.deep_profile_mode == "incremental":
                raise SystemExit("Incremental deep profile needs --etl1-daily-date and matching lake date partitions.")
            use_incremental_profile = False
        if use_incremental_profile and not _profile_ready(profile_dir):
            if args.deep_profile_mode == "incremental":
                raise SystemExit(f"Base profile is not ready for incremental merge: {profile_dir}")
            use_incremental_profile = False

        base_profile_cmd = [
            python,
            str(deep_profile_script),
            "--lake",
            str(lake_root),
            "--threads",
            str(args.deep_profile_threads),
            "--memory-limit",
            args.deep_profile_memory,
            "--top-n",
            str(args.deep_profile_top_n),
            "--output-format",
            args.deep_profile_output_format,
            "--column-fill",
            args.deep_profile_column_fill,
            "--querystr-profile",
            args.deep_profile_querystr_profile,
            "--top-values",
            args.deep_profile_top_values,
        ]
        if use_incremental_profile:
            start_dt = min(daily_profile_dates).isoformat()
            end_dt = max(daily_profile_dates).isoformat()
            delta_root = output_root / "temp" / "watch_profile_delta" / date.fromisoformat(args.etl1_daily_date).strftime("%Y_%m_%d")
            delta_profile = delta_root / "profile"
            if delta_root.exists():
                shutil.rmtree(delta_root)
            delta_profile.mkdir(parents=True, exist_ok=True)

            profile_cmd = base_profile_cmd + [
                "--out",
                str(delta_profile),
                "--start",
                start_dt,
                "--end",
                end_dt,
            ]
            run(profile_cmd, cwd=etl_root, env=env, step_name="watch_hours_profile_delta", log_dir=log_dir)

            merge_cmd = [
                python,
                str(profile_merge_script),
                "--base-profile",
                str(profile_dir),
                "--delta-profile",
                str(delta_profile),
                "--lake",
                str(lake_root),
                "--top-n",
                str(args.deep_profile_top_n),
                "--dates",
            ] + [day.isoformat() for day in daily_profile_dates]
            run(merge_cmd, cwd=etl_root, env=env, step_name="watch_hours_profile_merge", log_dir=log_dir)
            shutil.rmtree(delta_root, ignore_errors=True)
        else:
            start_dt, end_dt = _build_profile_range(lake_root, args.deep_profile_window_days)
            profile_cmd = base_profile_cmd + [
                "--out",
                str(profile_dir),
            ]
            if start_dt and end_dt:
                profile_cmd.extend(["--start", start_dt, "--end", end_dt])
            run(profile_cmd, cwd=etl_root, env=env, step_name="watch_hours_profile", log_dir=log_dir)
    else:
        print("\n[skip] deep profile step skipped.")

    if not args.skip_device_snapshot:
        run(
            [
                python,
                str(snapshot_generator_script),
                "--lake",
                str(lake_root),
                "--snapshot-csv",
                str(overview_data_dir / "device_snapshot.csv"),
                "--daily-csv",
                str(overview_data_dir / "device_daily.csv"),
            ],
            cwd=etl_root,
            env=env,
            step_name="overview_device_snapshot",
            log_dir=log_dir,
        )
    else:
        print("\n[skip] device snapshot step skipped.")

    if args.run_ua_profile:
        ua_start = args.ua_profile_start
        ua_end = args.ua_profile_end
        if not (ua_start and ua_end):
            if args.etl1_daily_date:
                daily_dates = _daily_profile_dates(lake_root, date.fromisoformat(args.etl1_daily_date))
                if daily_dates:
                    ua_start = min(daily_dates).isoformat()
                    ua_end = max(daily_dates).isoformat()
                else:
                    ua_start = args.etl1_daily_date
                    ua_end = args.etl1_daily_date
            else:
                ua_start, ua_end = _build_profile_range(lake_root, args.ua_profile_window_days or 7)

        ua_cmd = [
            python,
            str(ua_profile_script),
            "--lake",
            str(lake_root),
            "--out-dir",
            str(output_root / "device_decode"),
            "--cache",
            str(base_root / "cache" / "device_decode" / "ua_decode_cache.parquet"),
            "--threads",
            str(max(1, min(args.deep_profile_threads, 4))),
            "--memory-limit",
            "12GB",
            "--api-limit",
            str(args.ua_api_limit),
            "--min-rows-for-api",
            str(args.ua_min_rows_for_api),
        ]
        if ua_start and ua_end:
            ua_cmd.extend(["--start", ua_start, "--end", ua_end])
        if args.ua_profile_source:
            ua_cmd.extend(["--source", args.ua_profile_source])
        if args.dry_run:
            ua_cmd.append("--dry-run")
        run(ua_cmd, cwd=etl_root, env=env, step_name="ua_distinct_profile_decode_cache", log_dir=log_dir)
    else:
        print("\n[skip] UA profile/cache step skipped.")

    if not args.skip_overview:
        run(
            [
                python,
                str(overview_generator_script),
                str(lake_root),
                "--out-dir",
                str(overview_data_dir),
                "--year",
                args.overview_year or "",
                "--month",
                args.overview_month or "",
                "--yes",
                "--auto",
            ],
            cwd=etl_root,
            env=env,
            step_name="overview_report_xlsx",
            log_dir=log_dir,
        )

        overview_cmd = [
            python,
            str(overview_dashboard_dir / "generate_dashboard.py"),
            "--data-dir",
            str(overview_data_dir),
            str(overview_data_dir / "overview_report.xlsx"),
            str(overview_html),
        ]
        if args.dry_run:
            overview_cmd.append("--dry-run")
        run(overview_cmd, cwd=overview_dashboard_dir, env=env, step_name="overview_dashboard_html", log_dir=log_dir)
    else:
        print("\n[skip] overview step skipped.")

    if not args.skip_watch and not args.skip_concurrency:
        fast_lake = lake_root / "source=fast"
        if args.dry_run:
            print("\n[skip] FAST concurrency skipped in dry-run mode.")
        elif not fast_lake.exists():
            print(f"\n[skip] FAST concurrency skipped because FAST lake folder is missing: {fast_lake}")
        else:
            concurrency_start = args.concurrency_start
            concurrency_end = args.concurrency_end
            if not (concurrency_start and concurrency_end):
                if args.etl1_daily_date:
                    daily_dates = _daily_profile_dates(lake_root, date.fromisoformat(args.etl1_daily_date))
                    if daily_dates:
                        concurrency_start = min(daily_dates).isoformat()
                        concurrency_end = max(daily_dates).isoformat()
                    else:
                        concurrency_start = args.etl1_daily_date
                        concurrency_end = args.etl1_daily_date
                else:
                    latest_fast = _latest_lake_day(fast_lake) or _latest_lake_day(lake_root)
                    if latest_fast and args.concurrency_window_days and args.concurrency_window_days > 0:
                        start_fast = latest_fast - timedelta(days=args.concurrency_window_days - 1)
                        concurrency_start = start_fast.isoformat()
                        concurrency_end = latest_fast.isoformat()

            concurrency_cmd = [
                python,
                str(concurrency_script),
                "--lake",
                str(lake_root),
                "--out-dir",
                str(output_root / "watch_hours" / "concurrency"),
                "--threads",
                str(max(1, int(args.concurrency_threads))),
                "--memory-limit",
                args.concurrency_memory,
            ]
            if concurrency_start and concurrency_end:
                concurrency_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            run(concurrency_cmd, cwd=etl_root, env=env, step_name="watch_hours_fast_concurrency", log_dir=log_dir)

            concurrency_html_cmd = [
                python,
                str(concurrency_dashboard_script),
                "--data-dir",
                str(output_root / "watch_hours" / "concurrency"),
                "--out",
                str(concurrency_out),
                "--title",
                "Veto Concurrency",
            ]
            run(
                concurrency_html_cmd,
                cwd=concurrency_dashboard_dir,
                env=env,
                step_name="concurrency_dashboard_html",
                log_dir=log_dir,
            )
    else:
        print("\n[skip] FAST concurrency step skipped.")

    if not args.skip_latency:
        latency_start = args.latency_start
        latency_end = args.latency_end
        if not (latency_start and latency_end) and args.etl1_daily_date:
            daily_dates = _daily_profile_dates(lake_root, date.fromisoformat(args.etl1_daily_date))
            if daily_dates:
                latency_start = min(daily_dates).isoformat()
                latency_end = max(daily_dates).isoformat()
            else:
                latency_start = args.etl1_daily_date
                latency_end = args.etl1_daily_date

        latency_cmd = [
            python,
            str(latency_incremental_script),
            "--lake",
            str(lake_root),
            "--source",
            args.latency_source,
            "--out-dir",
            str(latency_profile),
            "--parts-dir",
            str(output_root / "latency" / "parts"),
            "--state",
            str(output_root / "latency" / "latency_incremental_state.json"),
            "--html-out",
            str(latency_out),
            "--title",
            "Veto Latency",
            "--threads",
            str(max(1, int(args.latency_threads))),
            "--memory-limit",
            args.latency_memory,
        ]
        if latency_start and latency_end:
            latency_cmd.extend(["--start", latency_start, "--end", latency_end])
        elif args.latency_window_days and args.latency_window_days > 0:
            latest_latency = _latest_lake_day(lake_root / f"source={args.latency_source}") or _latest_lake_day(lake_root)
            if latest_latency:
                latency_start_date = latest_latency - timedelta(days=args.latency_window_days - 1)
                latency_cmd.extend(["--start", latency_start_date.isoformat(), "--end", latest_latency.isoformat()])
        if args.dry_run:
            latency_cmd.append("--dry-run")
        run(latency_cmd, cwd=etl_root, env=env, step_name="latency_dashboard_html", log_dir=log_dir)
    else:
        print("\n[skip] latency dashboard skipped.")

    if not args.skip_watch:
        watch_cmd = [
            python,
            str(watch_dir / "generate_watch_hours.py"),
            "--profile",
            str(profile_dir),
            "--out",
            str(watch_out),
            "--title",
            args.watch_title,
        ]
        if args.dry_run:
            watch_cmd.append("--dry-run")
        run(watch_cmd, cwd=watch_dir, env=env, step_name="watch_hours_dashboard_html", log_dir=log_dir)
    else:
        print("\n[skip] watch-hours dashboard skipped.")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
