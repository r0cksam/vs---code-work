#!/usr/bin/env python3
"""Generate a standalone dark-mode FAST concurrency dashboard."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
for path in [SRC_ROOT, ETL_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common.chartjs import load_chartjs  # noqa: E402
from common.render import chartjs_script, json_blob, render_template  # noqa: E402


DEFAULT_DATA_DIR = Path(
    os.getenv(
        "VG_CONCURRENCY_DIR",
        str(ETL_ROOT / "output" / "watch_hours" / "concurrency"),
    )
)
DEFAULT_OUT = Path(
    os.getenv(
        "VG_CONCURRENCY_HTML",
        str(DEFAULT_DATA_DIR / "veto_concurrency.html"),
    )
)
CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"
IST = ZoneInfo("Asia/Kolkata")


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Required parquet not found: {path}")
    return pd.read_parquet(path)


def clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    text_cols = [
        "log_date",
        "source",
        "minute_utc",
        "minute_ist",
        "reqHost",
        "platform_key",
        "platform_name",
        "candidate_id",
        "channel_name",
        "peak_unique_viewers_minute_ist",
        "peak_unique_ua_minute_ist",
        "peak_segment_minute_ist",
    ]
    for col in text_cols:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    if "pair_key" not in out.columns and not out.empty:
        def text_series(name: str) -> pd.Series:
            if name in out.columns:
                return out[name].fillna("").astype(str)
            return pd.Series([""] * len(out), index=out.index, dtype=str)

        out["pair_key"] = (
            text_series("source")
            + "|"
            + text_series("platform_key")
            + "|"
            + text_series("candidate_id")
            + "|"
            + text_series("channel_name")
        )
    return out


def records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    if df.empty:
        return []
    out = df[[col for col in columns if col in df.columns]].copy()
    return json.loads(out.to_json(orient="records", date_format="iso"))


def write_ua_viewer_exports(data_dir: Path, minute: pd.DataFrame, dry_run: bool = False) -> dict:
    csv_path = data_dir / "concurrency_ua_viewers.csv"
    parquet_path = data_dir / "concurrency_ua_viewers.parquet"
    required = {"minute_ist", "channel_name", "platform_name", "unique_ua_viewers"}
    if minute.empty or not required.issubset(minute.columns):
        empty = pd.DataFrame(
            columns=[
                "Timestamp (IST)",
                "Channel name",
                "Platform name",
                "Number of viewers(UA)",
            ]
        )
        if not dry_run:
            empty.to_csv(csv_path, index=False, encoding="utf-8")
            empty.to_parquet(parquet_path, index=False, compression="zstd")
        return {"csv": str(csv_path), "parquet": str(parquet_path), "rows": 0}

    export = minute[
        ["minute_ist", "channel_name", "platform_name", "unique_ua_viewers"]
    ].rename(
        columns={
            "minute_ist": "Timestamp (IST)",
            "channel_name": "Channel name",
            "platform_name": "Platform name",
            "unique_ua_viewers": "Number of viewers(UA)",
        }
    )
    export["Number of viewers(UA)"] = (
        pd.to_numeric(export["Number of viewers(UA)"], errors="coerce")
        .fillna(0)
        .round()
        .astype("int64")
    )
    export = (
        export.groupby(
            ["Timestamp (IST)", "Channel name", "Platform name"],
            as_index=False,
            sort=False,
        )["Number of viewers(UA)"]
        .sum()
        .sort_values(["Timestamp (IST)", "Platform name", "Channel name"])
    )
    if not dry_run:
        export.to_csv(csv_path, index=False, encoding="utf-8")
        export.to_parquet(parquet_path, index=False, compression="zstd")
    return {"csv": str(csv_path), "parquet": str(parquet_path), "rows": int(len(export))}


def build_data(data_dir: Path, title: str) -> dict:
    minute_path = data_dir / "concurrency_minute.parquet"
    summary_path = data_dir / "concurrency_summary.parquet"
    manifest_path = data_dir / "concurrency_manifest.json"

    minute = clean_frame(read_parquet(minute_path))
    summary = clean_frame(read_parquet(summary_path))
    if not minute.empty:
        minute = minute.sort_values(["log_date", "platform_name", "channel_name", "candidate_id", "minute_ist"])
    if not summary.empty:
        summary = summary.sort_values(["log_date", "platform_name", "channel_name", "candidate_id"])

    dates = (
        sorted(str(day) for day in minute["log_date"].dropna().unique())
        if not minute.empty and "log_date" in minute.columns
        else []
    )
    current_ist_date = datetime.now(IST).date().isoformat()
    full_dates = [day for day in dates if day < current_ist_date]
    latest_full_date = full_dates[-1] if full_dates else (dates[-1] if dates else "")

    stats = {
        "minute_rows": int(len(minute)),
        "summary_rows": int(len(summary)),
        "first_date": dates[0] if dates else "",
        "last_date": dates[-1] if dates else "",
        "latest_full_date": latest_full_date,
        "current_ist_date": current_ist_date,
        "current_date_hidden": bool(current_ist_date in dates and latest_full_date != current_ist_date),
        "dates": len(dates),
        "platforms": int(minute["platform_name"].nunique()) if not minute.empty and "platform_name" in minute.columns else 0,
        "channels": int(minute["channel_name"].nunique()) if not minute.empty and "channel_name" in minute.columns else 0,
        "pairs": int(minute[["platform_key", "candidate_id", "channel_name"]].drop_duplicates().shape[0])
        if not minute.empty and {"platform_key", "candidate_id", "channel_name"}.issubset(minute.columns)
        else 0,
        "peak_unique_viewers": int(pd.to_numeric(minute.get("unique_viewers", pd.Series(dtype=float)), errors="coerce").max() or 0)
        if not minute.empty
        else 0,
        "peak_unique_ua_viewers": int(
            pd.to_numeric(minute.get("unique_ua_viewers", pd.Series(dtype=float)), errors="coerce").max() or 0
        )
        if not minute.empty
        else 0,
    }

    return {
        "title": title,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_dir": str(data_dir),
        "files": {
            "minute": str(minute_path),
            "summary": str(summary_path),
            "manifest": str(manifest_path),
        },
        "stats": stats,
        "manifest": read_json(manifest_path),
        "minute": records(
            minute,
            [
                "log_date",
                "source",
                "minute_utc",
                "minute_ist",
                "reqHost",
                "platform_key",
                "platform_name",
                "candidate_id",
                "channel_name",
                "raw_ts_rows",
                "status_200_ts_rows",
                "distinct_hosts",
                "unique_viewers",
                "unique_ua_viewers",
                "segment_viewers_estimate",
                "status_200_segment_viewers_estimate",
                "pair_key",
            ],
        ),
        "summary": records(
            summary,
            [
                "log_date",
                "source",
                "reqHost",
                "platform_key",
                "platform_name",
                "candidate_id",
                "channel_name",
                "distinct_hosts",
                "minute_count",
                "raw_ts_rows",
                "status_200_ts_rows",
                "avg_unique_viewers",
                "peak_unique_viewers",
                "peak_unique_viewers_minute_ist",
                "p95_unique_viewers",
                "avg_unique_ua_viewers",
                "peak_unique_ua_viewers",
                "peak_unique_ua_minute_ist",
                "p95_unique_ua_viewers",
                "avg_segment_viewers_estimate",
                "peak_segment_viewers_estimate",
                "peak_segment_minute_ist",
                "avg_status_200_segment_viewers_estimate",
                "peak_status_200_segment_viewers_estimate",
                "pair_key",
            ],
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate standalone FAST concurrency HTML dashboard.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--title", default="Veto Concurrency")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data_dir = args.data_dir.expanduser().resolve()
    out_path = args.out.expanduser().resolve()
    data = build_data(data_dir, args.title)
    minute_for_export = clean_frame(read_parquet(data_dir / "concurrency_minute.parquet"))
    data["files"]["ua_viewers_export"] = write_ua_viewer_exports(
        data_dir,
        minute_for_export,
        dry_run=args.dry_run,
    )
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        DATA_BLOB=json_blob(data),
        CHARTJS_TAG=chartjs_script(chartjs),
    )

    if args.dry_run:
        print(f"[Dry run] Concurrency dashboard OK - {len(html):,} chars")
        print(f"  Would write: {out_path}")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Concurrency dashboard written: {out_path}")
    print(f"Size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
