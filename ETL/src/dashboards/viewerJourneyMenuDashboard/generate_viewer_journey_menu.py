#!/usr/bin/env python3
"""Generate the static Viewer Journey Menu dashboard from a cliIP index parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import duckdb


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
DEFAULT_INDEX = ETL_ROOT / "output" / "exports" / "viewer_journey" / "viewer_journey_index.parquet"
DEFAULT_OUT = ETL_ROOT / "output" / "exports" / "viewer_journey" / "viewer_journey_menu.html"
DEFAULT_JOURNEY_ROOT = ETL_ROOT / "output" / "exports" / "cliip_journey"
CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"

for path in [SRC_ROOT, ETL_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common.chartjs import load_chartjs  # noqa: E402
from common.render import chartjs_script, json_blob, render_template  # noqa: E402


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def slug(value: str | None, max_len: int = 80) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return (text or "all")[:max_len].strip("_") or "all"


def cliip_slug(cliip: str) -> str:
    clean = slug(cliip, 54)
    digest = hashlib.blake2b(str(cliip).encode("utf-8"), digest_size=5).hexdigest()
    return f"{clean}_{digest}"


def read_json_safe(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def existing_dashboards(root: Path, output_path: Path) -> dict[str, str]:
    """Map cliIP slug to existing journey dashboard relative path."""
    out: dict[str, str] = {}
    if not root.exists():
        return out
    base = output_path.parent.resolve()
    for html in root.rglob("cliip_journey_dashboard.html"):
        folder = html.parent.name
        if not folder.startswith("raw_cliip_"):
            continue
        tail = folder[len("raw_cliip_") :]
        marker_positions = [
            pos for pos in [
                tail.find("_all_sources_"),
                tail.find("_stream_"),
                tail.find("_fast_"),
            ]
            if pos > 0
        ]
        if not marker_positions:
            continue
        slug_key = tail[: min(marker_positions)]
        try:
            rel = os.path.relpath(html.resolve(), base).replace("\\", "/")
        except ValueError:
            rel = html.resolve().as_posix()
        out.setdefault(slug_key, rel)
    return out


def fetch_records(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    df = con.execute(sql, params or []).fetchdf()
    return json.loads(df.to_json(orient="records", date_format="iso"))


def build_data(args: argparse.Namespace) -> dict[str, Any]:
    index_path = args.index.expanduser().resolve()
    if not index_path.exists():
        raise SystemExit(f"Viewer journey index not found: {index_path}")

    con = duckdb.connect()
    con.execute(f"SET threads={max(1, int(args.threads))}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{q(index_path)}')").fetchdf()
    columns = set(schema["column_name"].astype(str).tolist()) if "column_name" in schema.columns else set()
    dedup_raw_col = "dedup_raw_watch_hours" if "dedup_raw_watch_hours" in columns else "raw_watch_hours"
    dedup_200_col = (
        "dedup_status_200_watch_hours"
        if "dedup_status_200_watch_hours" in columns
        else "status_200_watch_hours"
    )
    row_aliases = []
    if "dedup_raw_watch_hours" not in columns:
        row_aliases.append("raw_watch_hours AS dedup_raw_watch_hours")
    if "dedup_status_200_watch_hours" not in columns:
        row_aliases.append("status_200_watch_hours AS dedup_status_200_watch_hours")
    if "dedup_ts_paths" not in columns:
        row_aliases.append("raw_ts_rows AS dedup_ts_paths")
    if "dedup_status_200_ts_paths" not in columns:
        row_aliases.append("status_200_ts_rows AS dedup_status_200_ts_paths")
    row_alias_sql = (",\n            " + ",\n            ".join(row_aliases)) if row_aliases else ""

    stats = fetch_records(
        con,
        f"""
        SELECT
            COUNT(*) AS index_rows,
            MIN(log_date) AS min_date,
            MAX(log_date) AS max_date,
            MIN(first_seen_ist) AS first_seen_ist,
            MAX(last_seen_ist) AS last_seen_ist,
            COUNT(DISTINCT cliIP) AS cliips,
            COUNT(DISTINCT source) AS sources,
            COUNT(DISTINCT channel_name) AS channels,
            ROUND(SUM(raw_watch_hours), 3) AS raw_watch_hours,
            ROUND(SUM(status_200_watch_hours), 3) AS status_200_watch_hours,
            ROUND(SUM({dedup_raw_col}), 3) AS dedup_raw_watch_hours,
            ROUND(SUM({dedup_200_col}), 3) AS dedup_status_200_watch_hours,
            SUM(row_count)::BIGINT AS raw_rows
        FROM read_parquet(?)
        """,
        [str(index_path)],
    )[0]
    source_rows = fetch_records(
        con,
        f"""
        SELECT
            source,
            COUNT(*) AS index_rows,
            MIN(log_date) AS min_date,
            MAX(log_date) AS max_date,
            MIN(first_seen_ist) AS first_seen_ist,
            MAX(last_seen_ist) AS last_seen_ist,
            COUNT(DISTINCT cliIP) AS cliips,
            COUNT(DISTINCT channel_name) AS channels,
            ROUND(SUM(raw_watch_hours), 3) AS raw_watch_hours,
            ROUND(SUM(status_200_watch_hours), 3) AS status_200_watch_hours,
            ROUND(SUM({dedup_raw_col}), 3) AS dedup_raw_watch_hours,
            ROUND(SUM({dedup_200_col}), 3) AS dedup_status_200_watch_hours,
            SUM(row_count)::BIGINT AS raw_rows
        FROM read_parquet(?)
        GROUP BY source
        ORDER BY raw_watch_hours DESC
        """,
        [str(index_path)],
    )
    rows = fetch_records(
        con,
        f"""
        SELECT *
            {row_alias_sql}
        FROM read_parquet(?)
        ORDER BY raw_watch_hours DESC, row_count DESC, last_seen_ist DESC
        LIMIT {max(1, int(args.embed_limit))}
        """,
        [str(index_path)],
    )
    dash_map = existing_dashboards(args.journey_root.expanduser().resolve(), args.out.expanduser().resolve())
    for row in rows:
        key = cliip_slug(str(row.get("cliIP") or ""))
        row["cliip_slug"] = key
        row["journey_dashboard"] = dash_map.get(key, "")

    con.close()
    return {
        "meta": {
            "title": args.title,
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "index_path": str(index_path),
            "embed_limit": int(args.embed_limit),
            "embedded_rows": len(rows),
            "journey_root": str(args.journey_root.expanduser().resolve()),
        },
        "stats": stats,
        "sources": source_rows,
        "rows": rows,
        "manifest": read_json_safe(index_path.with_suffix(".manifest.json")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Viewer Journey Menu dashboard.")
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--journey-root", type=Path, default=DEFAULT_JOURNEY_ROOT)
    parser.add_argument("--title", default="Viewer Journey Menu")
    parser.add_argument(
        "--embed-limit",
        type=int,
        default=200_000,
        help="Max index rows embedded in HTML. Increase for wider filtering, reduce for lighter browser load.",
    )
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.out = args.out.expanduser().resolve()
    data = build_data(args)
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        CHARTJS_TAG=chartjs_script(chartjs),
        DATA_BLOB=json_blob(data),
    )

    if args.dry_run:
        print(f"[Dry run] Viewer Journey Menu OK - {len(html):,} HTML chars")
        print(f"Would write: {args.out}")
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"Viewer Journey Menu written: {args.out}")
    print(f"Size: {args.out.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
