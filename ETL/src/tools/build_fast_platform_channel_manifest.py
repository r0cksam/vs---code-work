"""Build FAST platform/channel daily manifest-view aggregates from .m3u8 lake rows."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from build_concurrency import (
    DEFAULT_LAKE_FOLDER,
    DEFAULT_OUT_DIR,
    checked_dates,
    connect,
    date_filter_sql,
    minute_ist_sql,
    platform_key_sql,
    platform_name_sql,
    q,
    source_filter,
    table_count,
    write_append_table,
)
from vglive_core import channel_candidate_sql


def manifest_kind_sql(path_expr: str) -> str:
    path = f"lower(COALESCE(CAST({path_expr} AS VARCHAR), ''))"
    return f"""
CASE
    WHEN {path} LIKE '%/master.m3u8' OR {path} LIKE '%master%.m3u8' THEN 'master'
    WHEN {path} LIKE '%/playlist.m3u8' THEN 'playlist'
    WHEN regexp_matches({path}, '(^|/)main(_[0-9]+)?\\.m3u8$') THEN 'main'
    ELSE 'other_m3u8'
END
"""


def build_manifest_table(con, args: argparse.Namespace) -> None:
    start, end = checked_dates(args)
    lake_glob = q(args.lake / "**" / "*.parquet")
    partition_filter = date_filter_sql(start, end)
    candidate_expr = channel_candidate_sql("reqPath")
    kind_expr = manifest_kind_sql("reqPath")

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE fast_platform_channel_manifest_new AS
        WITH base AS (
            SELECT
                COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d') AS log_date,
                lower(COALESCE(reqHost, '')) AS reqHost,
                COALESCE(NULLIF(cliIP, ''), NULL) AS cliIP,
                NULLIF(trim(regexp_replace(COALESCE(CAST(UA AS VARCHAR), ''), '\\s+', ' ', 'g')), '') AS UA,
                COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', ''), ''), 'Unknown') AS statusCode,
                lower(COALESCE(CAST(reqPath AS VARCHAR), '')) AS reqPath,
                {candidate_expr} AS candidate_id,
                {kind_expr} AS manifest_kind
            FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
            WHERE {source_filter(args.source)}
              AND ({partition_filter})
              AND lower(COALESCE(CAST(reqPath AS VARCHAR), '')) LIKE '%.m3u8'
              AND TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
        ),
        resolved AS (
            SELECT
                b.*,
                COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name,
                {platform_name_sql("b.reqHost")} AS platform_name,
                {platform_key_sql("b.reqHost")} AS platform_key,
                CASE
                    WHEN p.path_channel_name IS NOT NULL THEN 'path_map'
                    WHEN h.host_channel_name IS NOT NULL THEN 'host_map'
                    WHEN NULLIF(b.candidate_id, '') IS NOT NULL THEN 'unmapped_path_candidate'
                    ELSE 'no_channel_evidence'
                END AS channel_evidence
            FROM base b
            LEFT JOIN host_map h ON b.reqHost = h.reqHost
            LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
        )
        SELECT
            log_date,
            source,
            platform_key,
            platform_name,
            candidate_id,
            channel_name,
            any_value(reqHost ORDER BY reqHost) AS reqHost,
            COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
            manifest_kind,
            channel_evidence,
            COUNT(*)::BIGINT AS m3u8_rows,
            COUNT(*) FILTER (WHERE statusCode = '200')::BIGINT AS status_200_m3u8_rows,
            COUNT(*) FILTER (WHERE statusCode <> '200')::BIGINT AS non_200_m3u8_rows,
            COUNT(DISTINCT cliIP)::BIGINT AS approx_unique_ips,
            COUNT(DISTINCT UA)::BIGINT AS distinct_uas,
            COUNT(DISTINCT COALESCE(cliIP, '') || '|' || COALESCE(UA, ''))::BIGINT AS distinct_ipua_pairs,
            any_value(reqPath ORDER BY reqPath) AS sample_reqpath
        FROM resolved
        GROUP BY 1,2,3,4,5,6,9,10
        """
    )


def write_manifest(args: argparse.Namespace, new_rows: int, output_path: Path) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "lake": str(args.lake.resolve()),
        "output": str(output_path.resolve()),
        "date_range_replaced": {
            "start": args.start or "",
            "end": args.end or "",
        },
        "metric_notes": {
            "m3u8_rows": "All FAST .m3u8 playlist/manifest request rows at platform/channel grain.",
            "status_200_m3u8_rows": "HTTP 200 FAST .m3u8 request rows.",
            "non_200_m3u8_rows": "Non-200 FAST .m3u8 request rows.",
            "approx_unique_ips": "Exact daily distinct cliIP for each FAST platform/channel/manifest-kind.",
            "channel_evidence": "path_map/host_map are mapped; unmapped_path_candidate stays visible as channel_name=Other.",
        },
        "new_rows": new_rows,
    }
    manifest_path = output_path.with_name("fast_platform_channel_manifest_daily_manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FAST platform/channel manifest-view daily mart.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--source", choices=["fast"], default="fast")
    parser.add_argument("--start", default=None, help="IST lake date start, YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="IST lake date end, YYYY-MM-DD.")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--temp-dir", type=Path, default=Path(__file__).resolve().parents[2] / "output" / "cache" / "duckdb_temp")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    start, end = checked_dates(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(args)
    try:
        build_manifest_table(con, args)
        new_rows = table_count(con, "fast_platform_channel_manifest_new")
        if new_rows <= 0:
            raise SystemExit("No FAST .m3u8 rows found for the selected platform/channel manifest range.")

        output_path = args.out_dir / "fast_platform_channel_manifest_daily.parquet"
        write_append_table(con, "fast_platform_channel_manifest_new", output_path, args.source, start, end)
        write_manifest(args, new_rows, output_path)
    finally:
        con.close()

    print(f"FAST platform/channel manifest parquet: {output_path}")
    print(json.dumps({"new_rows": new_rows}, indent=2))


if __name__ == "__main__":
    main()
