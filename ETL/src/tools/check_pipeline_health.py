#!/usr/bin/env python3
"""Print the latest ETL pipeline health summary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from textwrap import shorten


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ETL_ROOT / "output"


def load_summary(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"Pipeline summary not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def print_step(step: dict, show_tail: bool) -> None:
    status = step.get("status", "")
    name = step.get("step", "")
    attempt = step.get("attempt", "")
    error_class = step.get("error_class", "")
    exit_code = step.get("exit_code", "")
    log_path = step.get("log_path", "")
    hint = step.get("hint", "")

    prefix = f"- {status.upper():8} {name}"
    details = []
    if attempt:
        details.append(f"attempt={attempt}")
    if exit_code != "":
        details.append(f"exit={exit_code}")
    if error_class:
        details.append(f"class={error_class}")
    if details:
        prefix += " (" + ", ".join(details) + ")"
    print(prefix)
    if hint:
        print(f"  action: {hint}")
    if log_path:
        print(f"  log   : {log_path}")
    if show_tail and step.get("log_tail"):
        print("  tail  : " + shorten(str(step["log_tail"]).replace("\n", " | "), width=800))


def main() -> None:
    parser = argparse.ArgumentParser(description="Show latest ETL pipeline health summary.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--summary", type=Path, default=None)
    parser.add_argument("--all", action="store_true", help="Show all steps, not only issues.")
    parser.add_argument("--tail", action="store_true", help="Show short captured tail for failed steps.")
    args = parser.parse_args()

    summary_path = args.summary or args.output_root / "state" / "pipeline_last_run.json"
    data = load_summary(summary_path.expanduser().resolve())
    steps = data.get("steps", [])
    issues = [
        step
        for step in steps
        if step.get("status") in {"failed", "retrying"}
        or (step.get("status") == "skipped" and "failed" in str(step.get("reason", "")).lower())
    ]

    print(f"Run id     : {data.get('run_id', '')}")
    print(f"Status     : {data.get('status', '')}")
    print(f"Target date: {data.get('target_date', '') or 'n/a'}")
    print(f"Started    : {data.get('started_at', '')}")
    print(f"Finished   : {data.get('finished_at', '') or 'not finished'}")
    print(f"Warnings   : {data.get('warning_count', 0)}")
    print(f"Failures   : {data.get('failure_count', 0)}")
    print(f"Summary    : {summary_path}")
    print()

    if args.all:
        print("Steps:")
        for step in steps:
            print_step(step, args.tail)
        return

    print("Issues:")
    if not issues:
        print("- none")
        return
    for step in issues:
        print_step(step, args.tail)


if __name__ == "__main__":
    main()
