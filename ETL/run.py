#!/usr/bin/env python3
"""Single front door for the Veto ETL folder.

This file is intentionally small: it keeps one command for daily use while the
existing stage scripts stay as internal implementation details.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ETL_ROOT = Path(__file__).resolve().parent
PIPELINE = ETL_ROOT / "src" / "orchestrator" / "run_pipeline.py"
DAILY_PS1 = ETL_ROOT / "run_daily_pipeline.ps1"


def run(cmd: list[str]) -> None:
    nice = " ".join(f'"{part}"' if " " in part else part for part in cmd)
    print(f"\n[run] {nice}")
    try:
        subprocess.run(cmd, cwd=str(ETL_ROOT), check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"Command failed with exit code {exc.returncode}: {nice}") from None


def pipeline_args(command: str) -> list[str]:
    if command == "all":
        return []
    if command == "etl":
        return ["--skip-watch", "--skip-overview", "--skip-deep-profile", "--skip-device-snapshot"]
    if command == "dashboards":
        return ["--skip-etl"]
    if command == "watch":
        return ["--skip-etl", "--skip-overview", "--skip-device-snapshot"]
    if command == "overview":
        return ["--skip-etl", "--skip-watch", "--skip-deep-profile"]
    raise ValueError(f"Unsupported command: {command}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Veto ETL folder from one command.",
        epilog="Use '--' before extra run_pipeline.py arguments, e.g. python run.py dashboards -- --dry-run",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["all", "etl", "dashboards", "watch", "overview", "sync-yesterday"],
        help="What to run. Default: all.",
    )
    parser.add_argument(
        "extra",
        nargs=argparse.REMAINDER,
        help="Extra arguments passed to run_pipeline.py or run_daily_pipeline.ps1.",
    )
    args = parser.parse_args()

    extra = args.extra[1:] if args.extra[:1] == ["--"] else args.extra

    if args.command == "sync-yesterday":
        run(["powershell", "-ExecutionPolicy", "Bypass", "-File", str(DAILY_PS1), *extra])
        return

    run([sys.executable, str(PIPELINE), *pipeline_args(args.command), *extra])


if __name__ == "__main__":
    main()
