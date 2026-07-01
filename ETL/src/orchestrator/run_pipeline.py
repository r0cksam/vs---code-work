#!/usr/bin/env python3
"""Run the production ETL + dashboard pipeline from one place."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional


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


def _default_duckdb_temp_dir() -> Path:
    env_dir = os.getenv("VG_DUCKDB_TEMP_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    base = Path(os.getenv("LOCALAPPDATA") or tempfile.gettempdir())
    return (base / "VetoETL" / "duckdb_temp" / "deep_profile").resolve()


def _local_script(etl_root: Path, local_rel: str) -> Path:
    script = (etl_root / local_rel).resolve()
    if not script.exists():
        raise SystemExit(f"Required ETL script not found: {script}")
    return script


def _safe_log_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "step"


def _safe_state_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_")
    if not cleaned:
        raise SystemExit("--state-name must contain at least one letter, digit, dot, underscore, or dash.")
    return cleaned


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


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _tail_file(path: Path, max_chars: int = 4000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_chars:]


def _classify_failure(text: str) -> dict[str, str]:
    lower = text.lower()
    checks = [
        (
            "memory_or_temp_spill",
            [
                "out of memory",
                "failed to offload",
                "max_temp_directory_size",
                "memoryerror",
                "cannot allocate memory",
                "could not allocate",
            ],
            "Memory/temp spill. Rerun that step with fewer threads, smaller date range, or a larger DuckDB temp limit.",
        ),
        (
            "disk_space",
            ["no space left on device", "not enough space", "disk full", "there is not enough space"],
            "Disk is full or temp output cannot be written. Free space on the ETL drive and rerun the failed step.",
        ),
        (
            "missing_input",
            ["not found", "no such file", "no lake days found", "folder not found", "required etl script not found"],
            "Input file/folder is missing. Check raw download, lake partitions, and configured paths.",
        ),
        (
            "permission",
            ["permission denied", "access is denied", "unauthorized"],
            "Permission issue. Check file locks, credentials, or whether another process is using the output.",
        ),
        (
            "data_empty",
            ["no fast .ts rows", "zero rows", "remote file count is zero", "empty output"],
            "Selected range/source produced no usable rows. Validate source/date filters and upstream data.",
        ),
        (
            "python_exception",
            ["traceback", "exception"],
            "Python exception. Open the step log for the traceback and rerun after fixing the code/data issue.",
        ),
    ]
    for error_class, tokens, hint in checks:
        if any(token in lower for token in tokens):
            return {"error_class": error_class, "hint": hint}
    return {
        "error_class": "unknown",
        "hint": "Open the step log and inspect the last traceback/error lines.",
    }


def _safe_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _lower_parallelism(command: list[str]) -> tuple[Optional[list[str]], list[str]]:
    retry = list(command)
    changes: list[str] = []
    parallel_flags = {
        "--threads",
        "--workers",
        "--etl1-workers",
        "--deep-profile-threads",
        "--device-decode-threads",
        "--concurrency-threads",
        "--latency-threads",
    }
    for i, token in enumerate(retry[:-1]):
        if token not in parallel_flags:
            continue
        try:
            current = int(retry[i + 1])
        except ValueError:
            continue
        lowered = max(1, current // 2)
        if lowered < current:
            retry[i + 1] = str(lowered)
            changes.append(f"{token} {current}->{lowered}")
    return (retry, changes) if changes else (None, [])


class RunRecorder:
    def __init__(self, output_root: Path, args: argparse.Namespace, base_root: Path, lake_root: Path) -> None:
        self.output_root = output_root
        self.state_dir = output_root / "state"
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_name = getattr(args, "state_name", "pipeline") or "pipeline"
        if getattr(args, "dry_run", False) and state_name == "pipeline":
            # Dry-runs are health/validation probes. Keep them out of the real
            # daily-run state so a probe cannot hide the last production run.
            state_name = "pipeline_health"
        self.state_name = _safe_state_name(state_name)
        self.last_path = self.state_dir / f"{self.state_name}_last_run.json"
        self.run_path = self.state_dir / f"{self.state_name}_run_{self.run_id}.json"
        self.steps_csv = self.state_dir / f"{self.state_name}_last_run_steps.csv"
        self.data: dict[str, Any] = {
            "schema_version": 1,
            "run_id": self.run_id,
            "state_name": self.state_name,
            "status": "running",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": "",
            "base_root": str(base_root),
            "lake_root": str(lake_root),
            "output_root": str(output_root),
            "target_date": getattr(args, "etl1_daily_date", "") or "",
            "continue_on_error": bool(getattr(args, "continue_on_error", False)),
            "args": _jsonable(vars(args)),
            "steps": [],
        }
        self.write()

    def record_step(self, entry: dict[str, Any]) -> None:
        self.data.setdefault("steps", []).append(_jsonable(entry))
        self.write()

    def record_skip(self, step_name: str, reason: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.record_step(
            {
                "step": step_name,
                "status": "skipped",
                "allow_failure": True,
                "started_at": now,
                "finished_at": now,
                "duration_seconds": 0,
                "reason": reason,
            }
        )

    def finish(self, status: str) -> None:
        steps = self.data.get("steps", [])
        hard_failures = [s for s in steps if s.get("status") == "failed" and not s.get("allow_failure")]
        warnings = [s for s in steps if s.get("status") == "failed" and s.get("allow_failure")]
        if hard_failures:
            final_status = "failed"
        elif warnings and status == "complete":
            final_status = "complete_with_warnings"
        else:
            final_status = status
        self.data["status"] = final_status
        self.data["finished_at"] = datetime.now().isoformat(timespec="seconds")
        self.data["warning_count"] = len(warnings)
        self.data["failure_count"] = len(hard_failures)
        self.write()
        self.write_csv()
        print(
            f"\n[summary] status={final_status} warnings={len(warnings)} "
            f"failures={len(hard_failures)}"
        )
        print(f"[summary] json={self.last_path}")
        print(f"[summary] csv={self.steps_csv}")

    def write(self) -> None:
        _safe_write_json(self.last_path, self.data)
        _safe_write_json(self.run_path, self.data)

    def write_csv(self) -> None:
        self.steps_csv.parent.mkdir(parents=True, exist_ok=True)
        columns = [
            "step",
            "status",
            "attempt",
            "allow_failure",
            "exit_code",
            "error_class",
            "hint",
            "log_path",
            "duration_seconds",
            "started_at",
            "finished_at",
        ]
        with self.steps_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for step in self.data.get("steps", []):
                writer.writerow(step)


RUN_RECORDER: Optional[RunRecorder] = None


def record_skip(step_name: str, reason: str) -> None:
    if RUN_RECORDER is not None:
        RUN_RECORDER.record_skip(step_name, reason)


def run(
    command: list[str],
    env: dict[str, str],
    cwd: Optional[Path] = None,
    step_name: str = "command",
    log_dir: Optional[Path] = None,
    allow_failure: bool = False,
    retry_on_memory: bool = False,
) -> bool:
    attempt = 1
    current_command = list(command)
    while True:
        nice = " ".join(f'"{c}"' if " " in c else c for c in current_command)
        print(f"\n[run] {nice}")

        if log_dir is None:
            start = datetime.now()
            result = subprocess.run(
                current_command,
                check=False,
                cwd=str(cwd) if cwd else None,
                env=env,
            )
            finished = datetime.now()
            entry = {
                "step": step_name,
                "status": "ok" if result.returncode == 0 else "failed",
                "attempt": attempt,
                "allow_failure": allow_failure,
                "exit_code": result.returncode,
                "started_at": start.isoformat(timespec="seconds"),
                "finished_at": finished.isoformat(timespec="seconds"),
                "duration_seconds": round((finished - start).total_seconds(), 2),
                "command": nice,
            }
            if result.returncode and RUN_RECORDER is not None:
                entry.update(_classify_failure(f"exit code {result.returncode}"))
            if RUN_RECORDER is not None:
                RUN_RECORDER.record_step(entry)
            if result.returncode and not allow_failure:
                if RUN_RECORDER is not None:
                    RUN_RECORDER.finish("failed")
                raise SystemExit(f"Step failed: {step_name} (exit {result.returncode}).")
            return result.returncode == 0

        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        suffix = f"{_safe_log_name(step_name)}" if attempt == 1 else f"{_safe_log_name(step_name)}_retry{attempt}"
        log_path = log_dir / f"{stamp}_{suffix}.log"
        print(f"[log] {log_path}")

        start = datetime.now()
        with log_path.open("w", encoding="utf-8", errors="replace") as log:
            log.write(f"step={step_name}\n")
            log.write(f"attempt={attempt}\n")
            log.write(f"allow_failure={allow_failure}\n")
            log.write(f"cwd={cwd or Path.cwd()}\n")
            log.write(f"command={nice}\n")
            log.write(f"started_at={start.isoformat(timespec='seconds')}\n\n")
            log.flush()

            process = subprocess.Popen(
                current_command,
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

        entry = {
            "step": step_name,
            "attempt": attempt,
            "allow_failure": allow_failure,
            "status": "ok" if return_code == 0 else "failed",
            "exit_code": return_code,
            "started_at": start.isoformat(timespec="seconds"),
            "finished_at": finished.isoformat(timespec="seconds"),
            "duration_seconds": round((finished - start).total_seconds(), 2),
            "command": nice,
            "log_path": str(log_path),
        }
        if return_code:
            tail = _tail_file(log_path)
            entry.update(_classify_failure(tail))
            entry["log_tail"] = tail
            retry_command, retry_changes = _lower_parallelism(current_command)
            if (
                retry_on_memory
                and attempt == 1
                and entry.get("error_class") == "memory_or_temp_spill"
                and retry_command is not None
            ):
                entry["status"] = "retrying"
                entry["retry_reason"] = "memory_or_temp_spill"
                entry["retry_changes"] = retry_changes
                if RUN_RECORDER is not None:
                    RUN_RECORDER.record_step(entry)
                print(
                    f"[retry] {step_name} hit memory/temp pressure. "
                    f"Retrying once with: {', '.join(retry_changes)}"
                )
                current_command = retry_command
                attempt += 1
                continue

        if RUN_RECORDER is not None:
            RUN_RECORDER.record_step(entry)

        if return_code:
            if allow_failure:
                print(
                    f"[warn] Optional step failed: {step_name} (exit {return_code}). "
                    f"Class={entry.get('error_class', 'unknown')}. Log: {log_path}"
                )
                print(f"[warn] Suggested action: {entry.get('hint', 'Open the step log.')}")
                return False
            if RUN_RECORDER is not None:
                RUN_RECORDER.finish("failed")
            raise SystemExit(f"Step failed: {step_name} (exit {return_code}). Log: {log_path}")
        return True


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


def _latest_matching(root: Path, pattern: str) -> Optional[Path]:
    matches = [path for path in root.glob(pattern) if path.is_file() and path.stat().st_size > 0]
    return max(matches, key=lambda path: path.stat().st_mtime_ns) if matches else None


def _ua_distinct_profile_path(output_root: Path, source: str, start: Optional[str], end: Optional[str]) -> Optional[Path]:
    suffix = f"{source}_sources" if source != "both" else "all_sources"
    out_dir = output_root / "device_decode"
    if start and end:
        return out_dir / f"ua_distinct_profile_{suffix}_{start}_to_{end}.parquet"
    return _latest_matching(out_dir, f"ua_distinct_profile_{suffix}_*_to_*.parquet")


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
        "--skip-device-decode-profile",
        action="store_true",
        help="Skip UA model-code device decode profile generation.",
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
    parser.add_argument(
        "--skip-identity-mart",
        action="store_true",
        help="Skip reusable queryStr identity mart generation.",
    )
    parser.add_argument(
        "--skip-content-mart",
        action="store_true",
        help="Skip reusable content_title manifest-view mart generation.",
    )
    parser.add_argument(
        "--skip-audience",
        action="store_true",
        help="Skip Veto Audience Operations dashboard generation.",
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
        "--deep-profile-temp-dir",
        default=None,
        help="DuckDB spill/temp directory for vglive_deep_profile.py. Defaults to a user temp folder outside D:.",
    )
    parser.add_argument(
        "--deep-profile-max-temp-size",
        default=os.getenv("VG_DUCKDB_MAX_TEMP_SIZE", "40GB"),
        help="DuckDB max_temp_directory_size passed to vglive_deep_profile.py.",
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
    parser.add_argument(
        "--state-name",
        default="pipeline",
        help=(
            "State file prefix under output/state. Default writes pipeline_last_run.*; "
            "dry-run automatically uses pipeline_health_last_run.* unless this is overridden."
        ),
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help=(
            "Continue after recoverable enrichment/dashboard failures and record them in "
            "the selected output/state summary. Core raw/stage/lake/watch-profile steps remain strict."
        ),
    )

    # UA decode cache controls
    parser.add_argument(
        "--run-ua-profile",
        action="store_true",
        help="Build incremental distinct User-Agent profile, API-fill new UAs, and rebuild the production lookup before dashboards.",
    )
    parser.add_argument(
        "--ua-profile-window-days",
        type=int,
        default=None,
        help="Rolling lake window for UA profile. Defaults to 7 days when --run-ua-profile has no explicit range.",
    )
    parser.add_argument("--ua-profile-start", default=None, help="UA profile IST start date YYYY-MM-DD.")
    parser.add_argument("--ua-profile-end", default=None, help="UA profile IST end date YYYY-MM-DD.")
    parser.add_argument("--ua-profile-source", choices=["both", "stream", "fast"], default="both")
    parser.add_argument("--ua-api-limit", type=int, default=0, help="Optional whatmyuseragent.com API decode count. Use -1 for all candidates.")
    parser.add_argument(
        "--ua-api-include-malformed",
        action="store_true",
        help="Also send locally suspicious/malformed UA strings to the API when filling the cache.",
    )
    parser.add_argument("--ua-min-rows-for-api", type=int, default=1)

    # UA model-code device decode controls
    parser.add_argument(
        "--device-decode-window-days",
        type=int,
        default=None,
        help="Rolling lake window for UA model-code device decode. Daily ETL uses --etl1-daily-date when omitted.",
    )
    parser.add_argument("--device-decode-start", default=None, help="Device decode IST start date YYYY-MM-DD.")
    parser.add_argument("--device-decode-end", default=None, help="Device decode IST end date YYYY-MM-DD.")
    parser.add_argument("--device-decode-source", choices=["stream", "fast"], default=None)
    parser.add_argument("--device-decode-threads", type=int, default=2)
    parser.add_argument("--device-decode-memory", default="12GB")
    parser.add_argument(
        "--device-decode-max-temp-size",
        default="80GB",
        help="DuckDB max_temp_directory_size for UA model-code device decode.",
    )
    parser.add_argument(
        "--strict-device-decode-profile",
        action="store_true",
        help="Fail the full pipeline if UA model-code device decode fails. Default is best-effort.",
    )

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
        choices=["fast", "stream", "both"],
        default="both",
        help="Source to process for incremental latency profile. Default keeps both FAST and STREAM marts current.",
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

    # Audience identity mart controls
    parser.add_argument("--identity-start", default=None, help="Identity mart IST start date YYYY-MM-DD.")
    parser.add_argument("--identity-end", default=None, help="Identity mart IST end date YYYY-MM-DD.")
    parser.add_argument(
        "--identity-source",
        choices=["fast", "stream"],
        default="stream",
        help="Source filter for identity mart. STREAM has queryStr identity today; run fast explicitly if that changes.",
    )
    parser.add_argument("--identity-threads", type=int, default=6)
    parser.add_argument("--identity-memory", default="16GB")
    parser.add_argument("--content-start", default=None, help="Content mart IST start date YYYY-MM-DD.")
    parser.add_argument("--content-end", default=None, help="Content mart IST end date YYYY-MM-DD.")
    parser.add_argument(
        "--content-source",
        choices=["fast", "stream", "all"],
        default="stream",
        help="Source filter for content mart. STREAM has content_title today; FAST currently has none.",
    )
    parser.add_argument("--content-threads", type=int, default=6)
    parser.add_argument("--content-memory", default="16GB")
    parser.add_argument(
        "--audience-html",
        default=None,
        help="Standalone Veto Audience Operations dashboard html output path.",
    )

    args = parser.parse_args()

    base_root = Path(args.base).resolve()
    output_root = Path(args.output_root).resolve()
    deep_profile_temp_dir = (
        Path(args.deep_profile_temp_dir).expanduser().resolve()
        if args.deep_profile_temp_dir
        else _default_duckdb_temp_dir()
    )
    lake_root = base_root / "lake"
    output_root.mkdir(parents=True, exist_ok=True)
    log_dir = output_root / "logs"
    global RUN_RECORDER
    RUN_RECORDER = RunRecorder(output_root, args, base_root, lake_root)

    src_root = workspace / "src"
    pipeline_dir = src_root / "pipeline"
    watch_dir = src_root / "dashboards" / "watchHoursDashboard"
    concurrency_dashboard_dir = src_root / "dashboards" / "concurrencyDashboard"
    overview_dashboard_dir = src_root / "dashboards" / "overViewDashboard"
    audience_dashboard_dir = src_root / "dashboards" / "audienceOpsDashboard"
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
    audience_out = (
        Path(args.audience_html).resolve()
        if args.audience_html
        else output_root / "audience_ops" / "veto_audience_operations.html"
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
        and args.skip_device_decode_profile
        and (args.skip_watch or args.skip_concurrency)
        and args.skip_latency
        and args.skip_identity_mart
        and args.skip_content_mart
        and args.skip_audience
        and not args.run_ua_profile
    ):
        raise SystemExit("Nothing to run. Remove one skip flag.")

    profile_dir.mkdir(parents=True, exist_ok=True)
    watch_out.parent.mkdir(parents=True, exist_ok=True)
    concurrency_out.parent.mkdir(parents=True, exist_ok=True)
    latency_out.parent.mkdir(parents=True, exist_ok=True)
    latency_profile.mkdir(parents=True, exist_ok=True)
    audience_out.parent.mkdir(parents=True, exist_ok=True)
    overview_data_dir.mkdir(parents=True, exist_ok=True)
    overview_html.parent.mkdir(parents=True, exist_ok=True)

    needs_lake = (
        (not args.skip_deep_profile)
        or (not args.skip_device_snapshot)
        or (not args.skip_device_decode_profile)
        or (not args.skip_overview)
        or (not args.skip_latency)
        or (not args.skip_identity_mart)
        or (not args.skip_content_mart)
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
            "VG_AUDIENCE_HTML": str(audience_out),
            "VG_ETL_LAKE_ROOT": str(lake_root),
            "VG_DUCKDB_TEMP_DIR": str(deep_profile_temp_dir),
            "VG_DUCKDB_MAX_TEMP_SIZE": str(args.deep_profile_max_temp_size),
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
        str(Path("src") / "tools" / "build_ua_profile_incremental.py"),
    )
    ua_reference_sync_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "sync_distinct_ua_reference.py"),
    )
    ua_api_fill_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "decode_all_distinct_ua_api.py"),
    )
    ua_lookup_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "decode_distinct_ua_lookup.py"),
    )
    device_decode_profile_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "profile_device_decode.py"),
    )
    concurrency_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_concurrency.py"),
    )
    fast_platform_channel_identity_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_fast_platform_channel_identity.py"),
    )
    fast_platform_channel_geo_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_fast_platform_channel_geo.py"),
    )
    fast_platform_channel_ua_device_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_fast_platform_channel_ua_device.py"),
    )
    fast_platform_channel_manifest_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_fast_platform_channel_manifest.py"),
    )
    manifest_minute_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_manifest_minute.py"),
    )
    identity_minute_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_identity_minute.py"),
    )
    fast_platform_channel_bandwidth_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_fast_platform_channel_bandwidth.py"),
    )
    fast_platform_channel_cmcd_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_fast_platform_channel_cmcd.py"),
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
    identity_mart_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_identity_mart.py"),
    )
    content_mart_script = _local_script(
        etl_root,
        str(Path("src") / "tools" / "build_content_mart.py"),
    )
    audience_dashboard_script = _local_script(
        etl_root,
        str(Path("src") / "dashboards" / "audienceOpsDashboard" / "generate_audience_ops.py"),
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
            "--temp-dir",
            str(deep_profile_temp_dir),
            "--max-temp-size",
            str(args.deep_profile_max_temp_size),
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
            run(
                profile_cmd,
                cwd=etl_root,
                env=env,
                step_name="watch_hours_profile_delta",
                log_dir=log_dir,
                retry_on_memory=True,
            )

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
            run(
                profile_cmd,
                cwd=etl_root,
                env=env,
                step_name="watch_hours_profile",
                log_dir=log_dir,
                retry_on_memory=True,
            )
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
            allow_failure=args.continue_on_error,
            retry_on_memory=True,
        )
    else:
        print("\n[skip] device snapshot step skipped.")

    if args.run_ua_profile:
        ua_start = args.ua_profile_start
        ua_end = args.ua_profile_end
        ua_source = args.ua_profile_source or "both"
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
            "--parts-dir",
            str(output_root / "device_decode" / "ua_profile_parts"),
            "--source",
            ua_source,
            "--threads",
            str(max(1, min(args.deep_profile_threads, 4))),
            "--memory-limit",
            "12GB",
            "--temp-dir",
            str(output_root / "cache" / "duckdb_temp"),
        ]
        if ua_start and ua_end:
            ua_cmd.extend(["--start", ua_start, "--end", ua_end])
        if args.dry_run:
            ua_cmd.append("--dry-run")
        ua_profile_ok = run(
            ua_cmd,
            cwd=etl_root,
            env=env,
            step_name="ua_distinct_profile_incremental",
            log_dir=log_dir,
            allow_failure=args.continue_on_error,
            retry_on_memory=True,
        )

        ua_profile_input = _ua_distinct_profile_path(output_root, ua_source, ua_start, ua_end)
        if args.dry_run:
            record_skip("ua_distinct_reference_sync", "dry-run mode; incremental UA profile was not written")
            record_skip("ua_api_cache_fill", "dry-run mode; external API cache was not changed")
            record_skip("ua_decode_lookup_rebuild", "dry-run mode; production lookup was not rebuilt")
        elif not ua_profile_ok:
            reason = "incremental UA profile failed; skipped reference/API/lookup refresh to avoid using stale input"
            print(f"\n[skip] ua_distinct_reference_sync: {reason}")
            record_skip("ua_distinct_reference_sync", reason)
            record_skip("ua_api_cache_fill", reason)
            record_skip("ua_decode_lookup_rebuild", reason)
        elif ua_profile_input is None or not ua_profile_input.exists():
            reason = "incremental UA profile output was not found after build"
            print(f"\n[skip] ua_distinct_reference_sync: {reason}")
            record_skip("ua_distinct_reference_sync", reason)
            record_skip("ua_api_cache_fill", reason)
            record_skip("ua_decode_lookup_rebuild", reason)
            if not args.continue_on_error:
                raise SystemExit(reason)
        else:
            distinct_reference = etl_root / "distinct_UA_Both_All.csv"
            api_cache = base_root / "cache" / "device_decode" / "whatmyuseragent_all_distinct_ua_cache.parquet"
            sync_ok = run(
                [
                    python,
                    str(ua_reference_sync_script),
                    "--reference",
                    str(distinct_reference),
                    "--profile",
                    str(ua_profile_input),
                    "--out-parquet",
                    str(output_root / "device_decode" / "distinct_UA_Both_All.parquet"),
                    "--manifest",
                    str(output_root / "device_decode" / "distinct_UA_Both_All_manifest.json"),
                ],
                cwd=etl_root,
                env=env,
                step_name="ua_distinct_reference_sync",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
            )

            api_ok = True
            if args.ua_api_limit != 0 and sync_ok:
                api_fill_cmd = [
                    python,
                    str(ua_api_fill_script),
                    "--input",
                    str(distinct_reference),
                    "--api-cache",
                    str(api_cache),
                    "--out-dir",
                    str(output_root / "device_decode"),
                    "--api-limit",
                    str(args.ua_api_limit),
                ]
                if args.ua_api_include_malformed:
                    api_fill_cmd.append("--include-malformed")
                api_ok = run(
                    api_fill_cmd,
                    cwd=etl_root,
                    env=env,
                    step_name="ua_api_cache_fill",
                    log_dir=log_dir,
                    allow_failure=args.continue_on_error,
                )
            elif sync_ok:
                record_skip("ua_api_cache_fill", "--ua-api-limit is 0; production lookup will use existing API cache")

            if sync_ok and (api_ok or args.continue_on_error):
                run(
                    [
                        python,
                        str(ua_lookup_script),
                        "--input",
                        str(distinct_reference),
                        "--api-cache",
                        str(api_cache),
                        "--out-dir",
                        str(output_root / "device_decode"),
                        "--output-prefix",
                        "ua_decode_lookup_both_all",
                        "--api-limit",
                        "0",
                    ],
                    cwd=etl_root,
                    env=env,
                    step_name="ua_decode_lookup_rebuild",
                    log_dir=log_dir,
                    allow_failure=args.continue_on_error,
                )
            else:
                reason = "UA reference sync or API fill failed; skipped production lookup rebuild"
                print(f"\n[skip] ua_decode_lookup_rebuild: {reason}")
                record_skip("ua_decode_lookup_rebuild", reason)
    else:
        print("\n[skip] UA profile/cache step skipped.")

    if not args.skip_device_decode_profile:
        device_decode_start = args.device_decode_start
        device_decode_end = args.device_decode_end
        if not (device_decode_start and device_decode_end):
            if args.etl1_daily_date and args.device_decode_window_days is None:
                device_decode_start = args.etl1_daily_date
                device_decode_end = args.etl1_daily_date
            else:
                device_decode_start, device_decode_end = _build_profile_range(
                    lake_root,
                    args.device_decode_window_days or 7,
                )

        device_decode_cmd = [
            python,
            str(device_decode_profile_script),
            "--lake",
            str(lake_root),
            "--out-dir",
            str(output_root / "device_decode"),
            "--threads",
            str(max(1, int(args.device_decode_threads))),
            "--memory-limit",
            str(args.device_decode_memory),
            "--temp-dir",
            str(output_root / "cache" / "duckdb_temp"),
            "--max-temp-size",
            str(args.device_decode_max_temp_size),
        ]
        if device_decode_start and device_decode_end:
            device_decode_cmd.extend(["--start", device_decode_start, "--end", device_decode_end])
        if args.device_decode_source:
            device_decode_cmd.extend(["--source", args.device_decode_source])
        run(
            device_decode_cmd,
            cwd=etl_root,
            env=env,
            step_name="ua_model_code_device_decode_profile",
            log_dir=log_dir,
            allow_failure=not args.strict_device_decode_profile,
            retry_on_memory=True,
        )
    else:
        print("\n[skip] UA model-code device decode profile step skipped.")

    overview_report_ok = False
    if not args.skip_overview:
        overview_report_ok = run(
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
            allow_failure=args.continue_on_error,
            retry_on_memory=True,
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
        if overview_report_ok:
            run(
                overview_cmd,
                cwd=overview_dashboard_dir,
                env=env,
                step_name="overview_dashboard_html",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
            )
        else:
            reason = "overview_report_xlsx failed; skipped HTML refresh to avoid publishing stale overview data"
            print(f"\n[skip] overview_dashboard_html: {reason}")
            record_skip("overview_dashboard_html", reason)
    else:
        print("\n[skip] overview step skipped.")

    if not args.skip_watch and not args.skip_concurrency:
        fast_lake = lake_root / "source=fast"
        stream_lake = lake_root / "source=stream"
        if args.dry_run:
            print("\n[skip] FAST/STREAM concurrency skipped in dry-run mode.")
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
                "--source",
                "fast",
                "--threads",
                str(max(1, int(args.concurrency_threads))),
                "--memory-limit",
                args.concurrency_memory,
            ]
            if concurrency_start and concurrency_end:
                concurrency_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            concurrency_ok = run(
                concurrency_cmd,
                cwd=etl_root,
                env=env,
                step_name="watch_hours_fast_concurrency",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            stream_concurrency_ok = True
            if stream_lake.exists():
                stream_concurrency_cmd = [
                    python,
                    str(concurrency_script),
                    "--lake",
                    str(lake_root),
                    "--out-dir",
                    str(output_root / "watch_hours" / "concurrency"),
                    "--source",
                    "stream",
                    "--threads",
                    str(max(1, int(args.concurrency_threads))),
                    "--memory-limit",
                    args.concurrency_memory,
                ]
                if concurrency_start and concurrency_end:
                    stream_concurrency_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
                stream_concurrency_ok = run(
                    stream_concurrency_cmd,
                    cwd=etl_root,
                    env=env,
                    step_name="watch_hours_stream_concurrency",
                    log_dir=log_dir,
                    allow_failure=args.continue_on_error,
                    retry_on_memory=True,
                )
            else:
                print(f"\n[skip] STREAM concurrency skipped because STREAM lake folder is missing: {stream_lake}")

            manifest_minute_fast_cmd = [
                python,
                str(manifest_minute_script),
                "--lake",
                str(lake_root),
                "--out-dir",
                str(output_root / "watch_hours" / "concurrency"),
                "--source",
                "fast",
                "--threads",
                str(max(1, int(args.concurrency_threads))),
                "--memory-limit",
                args.concurrency_memory,
            ]
            if concurrency_start and concurrency_end:
                manifest_minute_fast_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            run(
                manifest_minute_fast_cmd,
                cwd=etl_root,
                env=env,
                step_name="manifest_minute_fast",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            if stream_lake.exists():
                manifest_minute_stream_cmd = [
                    python,
                    str(manifest_minute_script),
                    "--lake",
                    str(lake_root),
                    "--out-dir",
                    str(output_root / "watch_hours" / "concurrency"),
                    "--source",
                    "stream",
                    "--threads",
                    str(max(1, int(args.concurrency_threads))),
                    "--memory-limit",
                    args.concurrency_memory,
                ]
                if concurrency_start and concurrency_end:
                    manifest_minute_stream_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
                run(
                    manifest_minute_stream_cmd,
                    cwd=etl_root,
                    env=env,
                    step_name="manifest_minute_stream",
                    log_dir=log_dir,
                    allow_failure=args.continue_on_error,
                    retry_on_memory=True,
                )

            identity_minute_fast_cmd = [
                python,
                str(identity_minute_script),
                "--lake",
                str(lake_root),
                "--out-dir",
                str(output_root / "watch_hours" / "concurrency"),
                "--source",
                "fast",
                "--threads",
                str(max(1, int(args.concurrency_threads))),
                "--memory-limit",
                args.concurrency_memory,
            ]
            if concurrency_start and concurrency_end:
                identity_minute_fast_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            run(
                identity_minute_fast_cmd,
                cwd=etl_root,
                env=env,
                step_name="identity_minute_fast",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            if stream_lake.exists():
                identity_minute_stream_cmd = [
                    python,
                    str(identity_minute_script),
                    "--lake",
                    str(lake_root),
                    "--out-dir",
                    str(output_root / "watch_hours" / "concurrency"),
                    "--source",
                    "stream",
                    "--threads",
                    str(max(1, int(args.concurrency_threads))),
                    "--memory-limit",
                    args.concurrency_memory,
                ]
                if concurrency_start and concurrency_end:
                    identity_minute_stream_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
                run(
                    identity_minute_stream_cmd,
                    cwd=etl_root,
                    env=env,
                    step_name="identity_minute_stream",
                    log_dir=log_dir,
                    allow_failure=args.continue_on_error,
                    retry_on_memory=True,
                )

            fast_identity_cmd = [
                python,
                str(fast_platform_channel_identity_script),
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
                fast_identity_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            fast_identity_ok = run(
                fast_identity_cmd,
                cwd=etl_root,
                env=env,
                step_name="fast_platform_channel_identity",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            fast_geo_cmd = [
                python,
                str(fast_platform_channel_geo_script),
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
                fast_geo_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            fast_geo_ok = run(
                fast_geo_cmd,
                cwd=etl_root,
                env=env,
                step_name="fast_platform_channel_geo",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            fast_ua_device_cmd = [
                python,
                str(fast_platform_channel_ua_device_script),
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
                fast_ua_device_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            fast_ua_device_ok = run(
                fast_ua_device_cmd,
                cwd=etl_root,
                env=env,
                step_name="fast_platform_channel_ua_device",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            fast_manifest_cmd = [
                python,
                str(fast_platform_channel_manifest_script),
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
                fast_manifest_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            fast_manifest_ok = run(
                fast_manifest_cmd,
                cwd=etl_root,
                env=env,
                step_name="fast_platform_channel_manifest",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            fast_bandwidth_cmd = [
                python,
                str(fast_platform_channel_bandwidth_script),
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
                fast_bandwidth_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            fast_bandwidth_ok = run(
                fast_bandwidth_cmd,
                cwd=etl_root,
                env=env,
                step_name="fast_platform_channel_bandwidth",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

            fast_cmcd_cmd = [
                python,
                str(fast_platform_channel_cmcd_script),
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
                fast_cmcd_cmd.extend(["--start", concurrency_start, "--end", concurrency_end])
            fast_cmcd_ok = run(
                fast_cmcd_cmd,
                cwd=etl_root,
                env=env,
                step_name="fast_platform_channel_cmcd",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )

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
            if concurrency_ok and stream_concurrency_ok and fast_identity_ok and fast_geo_ok and fast_ua_device_ok and fast_manifest_ok and fast_bandwidth_ok and fast_cmcd_ok:
                run(
                    concurrency_html_cmd,
                    cwd=concurrency_dashboard_dir,
                    env=env,
                    step_name="concurrency_dashboard_html",
                    log_dir=log_dir,
                    allow_failure=args.continue_on_error,
                )
            else:
                reason = "FAST/STREAM concurrency, platform/channel identity, platform/channel geo, platform/channel UA device, platform/channel manifest, platform/channel bandwidth, or platform/channel CMCD failed; skipped concurrency HTML refresh to avoid stale data"
                print(f"\n[skip] concurrency_dashboard_html: {reason}")
                record_skip("concurrency_dashboard_html", reason)
    else:
        print("\n[skip] FAST/STREAM concurrency step skipped.")

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

        latency_sources = ["fast", "stream"] if args.latency_source == "both" else [args.latency_source]
        for latency_source in latency_sources:
            source_start = latency_start
            source_end = latency_end
            latency_cmd = [
                python,
                str(latency_incremental_script),
                "--lake",
                str(lake_root),
                "--source",
                latency_source,
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
            if source_start and source_end:
                latency_cmd.extend(["--start", source_start, "--end", source_end])
            elif args.latency_window_days and args.latency_window_days > 0:
                latest_latency = _latest_lake_day(lake_root / f"source={latency_source}") or _latest_lake_day(lake_root)
                if latest_latency:
                    latency_start_date = latest_latency - timedelta(days=args.latency_window_days - 1)
                    latency_cmd.extend(["--start", latency_start_date.isoformat(), "--end", latest_latency.isoformat()])
            if args.dry_run:
                latency_cmd.append("--dry-run")
            run(
                latency_cmd,
                cwd=etl_root,
                env=env,
                step_name=f"latency_profile_{latency_source}",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
                retry_on_memory=True,
            )
    else:
        print("\n[skip] latency dashboard skipped.")

    identity_ok = True
    if not args.skip_identity_mart:
        identity_start = args.identity_start
        identity_end = args.identity_end
        if not (identity_start and identity_end) and args.etl1_daily_date:
            daily_dates = _daily_profile_dates(lake_root, date.fromisoformat(args.etl1_daily_date))
            if daily_dates:
                identity_start = min(daily_dates).isoformat()
                identity_end = max(daily_dates).isoformat()
            else:
                identity_start = args.etl1_daily_date
                identity_end = args.etl1_daily_date

        identity_cmd = [
            python,
            str(identity_mart_script),
            "--lake",
            str(lake_root),
            "--out-dir",
            str(output_root / "identity"),
            "--state",
            str(output_root / "identity" / "identity_mart_state.json"),
            "--threads",
            str(max(1, int(args.identity_threads))),
            "--memory-limit",
            args.identity_memory,
        ]
        if args.identity_source:
            identity_cmd.extend(["--source", args.identity_source])
        if identity_start and identity_end:
            identity_cmd.extend(["--start", identity_start, "--end", identity_end])
        if args.dry_run:
            identity_cmd.append("--dry-run")
        identity_ok = run(
            identity_cmd,
            cwd=etl_root,
            env=env,
            step_name="identity_mart",
            log_dir=log_dir,
            allow_failure=args.continue_on_error,
            retry_on_memory=True,
        )
    else:
        print("\n[skip] identity mart skipped.")

    if identity_ok and not args.skip_overview:
        overview_report_after_ok = run(
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
            step_name="overview_report_xlsx_after_latency_identity",
            log_dir=log_dir,
            allow_failure=args.continue_on_error,
            retry_on_memory=True,
        )
        overview_after_identity_cmd = [
            python,
            str(overview_dashboard_dir / "generate_dashboard.py"),
            "--data-dir",
            str(overview_data_dir),
            str(overview_data_dir / "overview_report.xlsx"),
            str(overview_html),
        ]
        if args.dry_run:
            overview_after_identity_cmd.append("--dry-run")
        if overview_report_after_ok:
            run(
                overview_after_identity_cmd,
                cwd=overview_dashboard_dir,
                env=env,
                step_name="overview_dashboard_html_after_identity",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
            )
        else:
            reason = "overview_report_xlsx_after_latency_identity failed; skipped final HTML refresh"
            print(f"\n[skip] overview_dashboard_html_after_identity: {reason}")
            record_skip("overview_dashboard_html_after_identity", reason)

    content_ok = True
    if not args.skip_content_mart:
        content_start = args.content_start
        content_end = args.content_end
        if not (content_start and content_end) and args.etl1_daily_date:
            daily_dates = _daily_profile_dates(lake_root, date.fromisoformat(args.etl1_daily_date))
            if daily_dates:
                content_start = min(daily_dates).isoformat()
                content_end = max(daily_dates).isoformat()
            else:
                content_start = args.etl1_daily_date
                content_end = args.etl1_daily_date

        content_cmd = [
            python,
            str(content_mart_script),
            "--lake",
            str(lake_root),
            "--out-dir",
            str(output_root / "content"),
            "--state",
            str(output_root / "content" / "content_mart_state.json"),
            "--source",
            args.content_source,
            "--threads",
            str(max(1, int(args.content_threads))),
            "--memory-limit",
            args.content_memory,
        ]
        if content_start and content_end:
            content_cmd.extend(["--start", content_start, "--end", content_end])
        if args.dry_run:
            content_cmd.append("--dry-run")
        content_ok = run(
            content_cmd,
            cwd=etl_root,
            env=env,
            step_name="content_title_mart",
            log_dir=log_dir,
            allow_failure=args.continue_on_error,
            retry_on_memory=True,
        )
    else:
        print("\n[skip] content title mart skipped.")

    if not args.skip_audience:
        audience_cmd = [
            python,
            str(audience_dashboard_script),
            "--output-root",
            str(output_root),
            "--out",
            str(audience_out),
            "--title",
            "Veto Audience Operations",
        ]
        if args.dry_run:
            audience_cmd.append("--dry-run")
        if identity_ok and content_ok:
            run(
                audience_cmd,
                cwd=audience_dashboard_dir,
                env=env,
                step_name="audience_ops_dashboard_html",
                log_dir=log_dir,
                allow_failure=args.continue_on_error,
            )
        else:
            reason = "identity/content mart failed; skipped audience HTML refresh to avoid publishing stale data"
            print(f"\n[skip] audience_ops_dashboard_html: {reason}")
            record_skip("audience_ops_dashboard_html", reason)
    else:
        print("\n[skip] audience operations dashboard skipped.")

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
        run(
            watch_cmd,
            cwd=watch_dir,
            env=env,
            step_name="watch_hours_dashboard_html",
            log_dir=log_dir,
            allow_failure=args.continue_on_error,
        )
    else:
        print("\n[skip] watch-hours dashboard skipped.")

    print("\nPipeline complete.")
    if RUN_RECORDER is not None:
        RUN_RECORDER.finish("complete")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        if RUN_RECORDER is not None and RUN_RECORDER.data.get("status") == "running":
            RUN_RECORDER.finish("failed")
        raise
    except Exception as exc:
        if RUN_RECORDER is not None:
            RUN_RECORDER.record_step(
                {
                    "step": "orchestrator",
                    "status": "failed",
                    "allow_failure": False,
                    "started_at": datetime.now().isoformat(timespec="seconds"),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "exit_code": 1,
                    **_classify_failure(str(exc)),
                    "log_tail": str(exc),
                }
            )
            RUN_RECORDER.finish("failed")
        raise
