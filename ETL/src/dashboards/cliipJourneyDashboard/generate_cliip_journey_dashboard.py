#!/usr/bin/env python3
"""Generate a static HTML dashboard from CLIIP journey parquet outputs."""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import duckdb


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
DEFAULT_ROOT = ETL_ROOT / "output" / "exports" / "cliip_journey"
CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"

for path in [SRC_ROOT, ETL_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common.chartjs import load_chartjs  # noqa: E402
from common.render import chartjs_script, json_blob, render_template  # noqa: E402


REQUIRED_PARQUETS = {
    "daily_journey_summary": "daily_journey_summary.parquet",
    "date_channel_volume": "date_channel_volume.parquet",
    "hourly_channel_volume": "hourly_channel_volume.parquet",
    "time_bucket_channel_volume": "time_bucket_channel_volume.parquet",
    "journey_segments": "journey_segments.parquet",
}


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def latest_input_dir(root: Path) -> Path:
    if not root.exists():
        raise SystemExit(f"CLIIP journey output root not found: {root}")

    candidates: list[Path] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if all((child / name).exists() for name in REQUIRED_PARQUETS.values()):
            candidates.append(child)

    if not candidates:
        raise SystemExit(f"No complete CLIIP journey folder found under: {root}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


def fetch_records(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    df = con.execute(sql, params or []).fetchdf()
    return json.loads(df.to_json(orient="records", date_format="iso"))


def read_manifest(input_dir: Path) -> dict[str, Any]:
    manifest_path = input_dir / "analysis_manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return {"manifest_error": str(exc)}


def parquet_files(input_dir: Path) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for path in sorted(input_dir.glob("*")):
        if path.suffix.lower() not in {".parquet", ".json", ".html"}:
            continue
        stat = path.stat()
        files.append(
            {
                "name": path.name,
                "path": str(path.resolve()),
                "relative_path": path.name,
                "size_mb": round(stat.st_size / 1024 / 1024, 3),
                "updated": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return files


def build_data(
    con: duckdb.DuckDBPyConnection,
    input_dir: Path,
    title: str,
    top_segments: int,
    top_switches: int,
) -> dict[str, Any]:
    paths = {key: input_dir / filename for key, filename in REQUIRED_PARQUETS.items()}
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise SystemExit("CLIIP journey folder is incomplete. Missing: " + ", ".join(missing))

    daily_path = paths["daily_journey_summary"]
    date_channel_path = paths["date_channel_volume"]
    hourly_path = paths["hourly_channel_volume"]
    bucket_path = paths["time_bucket_channel_volume"]
    segment_path = paths["journey_segments"]

    daily = fetch_records(
        con,
        """
        SELECT *
        FROM read_parquet(?)
        ORDER BY log_date
        """,
        [str(daily_path)],
    )
    date_channel = fetch_records(
        con,
        """
        SELECT *
        FROM read_parquet(?)
        ORDER BY log_date, raw_watch_minutes DESC, row_count DESC, channel_name
        """,
        [str(date_channel_path)],
    )
    hourly = fetch_records(
        con,
        """
        SELECT *
        FROM read_parquet(?)
        ORDER BY log_date, hour_ist, raw_watch_minutes DESC, row_count DESC, channel_name
        """,
        [str(hourly_path)],
    )
    buckets = fetch_records(
        con,
        """
        SELECT *
        FROM read_parquet(?)
        ORDER BY bucket_start_ist, raw_watch_minutes DESC, row_count DESC, channel_name
        """,
        [str(bucket_path)],
    )
    channel_summary = fetch_records(
        con,
        """
        SELECT
            channel_name,
            COUNT(DISTINCT log_date)::BIGINT AS days,
            SUM(row_count)::BIGINT AS row_count,
            SUM(ts_rows)::BIGINT AS ts_rows,
            SUM(playlist_rows)::BIGINT AS playlist_rows,
            SUM(status_200_rows)::BIGINT AS status_200_rows,
            SUM(non_200_rows)::BIGINT AS non_200_rows,
            ROUND(SUM(raw_watch_minutes), 3) AS raw_watch_minutes,
            ROUND(SUM(status_200_watch_minutes), 3) AS status_200_watch_minutes,
            ROUND(SUM(wall_clock_minutes), 3) AS wall_clock_minutes,
            COUNT(DISTINCT sample_reqHost)::BIGINT AS distinct_hosts,
            COUNT(DISTINCT sample_reqPath)::BIGINT AS distinct_paths,
            ANY_VALUE(sample_reqHost) AS sample_reqHost,
            ANY_VALUE(sample_reqPath) AS sample_reqPath
        FROM read_parquet(?)
        GROUP BY channel_name
        ORDER BY raw_watch_minutes DESC, row_count DESC, channel_name
        """,
        [str(date_channel_path)],
    )
    segment_stats = fetch_records(
        con,
        """
        SELECT
            COUNT(*)::BIGINT AS segment_count,
            SUM(CASE WHEN row_count > 1 THEN 1 ELSE 0 END)::BIGINT AS multi_row_segments,
            SUM(CASE WHEN raw_watch_minutes > 0 THEN 1 ELSE 0 END)::BIGINT AS watch_segments,
            MAX(row_count)::BIGINT AS max_segment_rows,
            ROUND(MAX(raw_watch_minutes), 3) AS max_segment_raw_minutes,
            ROUND(MAX(wall_clock_minutes), 3) AS max_segment_wall_minutes
        FROM read_parquet(?)
        """,
        [str(segment_path)],
    )
    top_segments = max(1, int(top_segments))
    top_segment_rows = fetch_records(
        con,
        f"""
        SELECT
            segment_id,
            start_date,
            start_time_ist,
            end_time_ist,
            channel_name,
            row_count,
            ts_rows,
            playlist_rows,
            status_200_rows,
            non_200_rows,
            raw_watch_minutes,
            status_200_watch_minutes,
            wall_clock_minutes,
            distinct_hosts,
            distinct_candidates,
            distinct_status_codes,
            sample_reqHost,
            sample_reqPath,
            gap_to_next_seconds
        FROM read_parquet(?)
        WHERE row_count > 1 OR raw_watch_minutes > 0 OR wall_clock_minutes > 0
        ORDER BY raw_watch_minutes DESC, row_count DESC, start_time_ist
        LIMIT {top_segments}
        """,
        [str(segment_path)],
    )
    switch_summary = fetch_records(
        con,
        f"""
        WITH s AS (
            SELECT
                channel_name AS from_channel,
                LEAD(channel_name) OVER (ORDER BY segment_id) AS to_channel
            FROM read_parquet(?)
        )
        SELECT
            from_channel,
            to_channel,
            COUNT(*)::BIGINT AS switches
        FROM s
        WHERE to_channel IS NOT NULL
          AND from_channel <> to_channel
        GROUP BY from_channel, to_channel
        ORDER BY switches DESC
        LIMIT {max(1, int(top_switches))}
        """,
        [str(segment_path)],
    )
    daily_switches = fetch_records(
        con,
        """
        WITH s AS (
            SELECT
                start_date AS log_date,
                channel_name,
                LEAD(channel_name) OVER (ORDER BY segment_id) AS next_channel
            FROM read_parquet(?)
        )
        SELECT
            log_date,
            COUNT(*)::BIGINT AS segment_count,
            SUM(CASE WHEN next_channel IS NOT NULL AND channel_name <> next_channel THEN 1 ELSE 0 END)::BIGINT AS channel_switches
        FROM s
        GROUP BY log_date
        ORDER BY log_date
        """,
        [str(segment_path)],
    )
    time_bounds = fetch_records(
        con,
        """
        SELECT
            MIN(CAST(start_time_ist AS VARCHAR)) AS first_seen_ist,
            MAX(CAST(end_time_ist AS VARCHAR)) AS last_seen_ist
        FROM read_parquet(?)
        """,
        [str(segment_path)],
    )

    manifest = read_manifest(input_dir)
    min_date = min((str(row["log_date"]) for row in daily), default="")
    max_date = max((str(row["log_date"]) for row in daily), default="")
    first_seen_ist = str((time_bounds[0] if time_bounds else {}).get("first_seen_ist") or "")
    last_seen_ist = str((time_bounds[0] if time_bounds else {}).get("last_seen_ist") or "")

    return {
        "meta": {
            "title": title,
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "input_dir": str(input_dir.resolve()),
            "input_raw_parquet": manifest.get("input", ""),
            "min_date": min_date,
            "max_date": max_date,
            "first_seen_ist": first_seen_ist,
            "last_seen_ist": last_seen_ist,
            "top_segments_limit": top_segments,
            "top_switches_limit": top_switches,
            "gap_seconds": manifest.get("gap_seconds"),
            "bucket_minutes": manifest.get("bucket_minutes"),
        },
        "daily": daily,
        "date_channel": date_channel,
        "hourly": hourly,
        "buckets": buckets,
        "channel_summary": channel_summary,
        "segment_stats": segment_stats[0] if segment_stats else {},
        "top_segments": top_segment_rows,
        "switch_summary": switch_summary,
        "daily_switches": daily_switches,
        "files": parquet_files(input_dir),
        "manifest": manifest,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a CLIIP journey dashboard HTML from parquet summaries.")
    parser.add_argument("--input-dir", type=Path, default=None, help="Folder containing CLIIP journey parquet outputs.")
    parser.add_argument("--root", type=Path, default=Path(os.getenv("VG_CLIIP_JOURNEY_ROOT", DEFAULT_ROOT)))
    parser.add_argument("--out", type=Path, default=None, help="Output HTML path. Default: <input-dir>/cliip_journey_dashboard.html")
    parser.add_argument("--title", default="CLIIP Journey Dashboard")
    parser.add_argument("--top-segments", type=int, default=1500)
    parser.add_argument("--top-switches", type=int, default=500)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_dir = args.input_dir.expanduser().resolve() if args.input_dir else latest_input_dir(args.root.expanduser().resolve())
    out_path = args.out.expanduser().resolve() if args.out else input_dir / "cliip_journey_dashboard.html"

    con = duckdb.connect()
    con.execute(f"SET threads={max(1, args.threads)}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")

    print(f"Building CLIIP journey dashboard from: {input_dir}")
    data = build_data(con, input_dir, args.title, args.top_segments, args.top_switches)
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        CHARTJS_TAG=chartjs_script(chartjs),
        DATA_BLOB=json_blob(data),
    )

    if args.dry_run:
        print(f"\n[Dry run] Dashboard OK - {len(html):,} HTML chars")
        print(f"  Would write to: {out_path}")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"\nDashboard written: {out_path}")
    print(f"Size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
