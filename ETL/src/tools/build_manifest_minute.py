#!/usr/bin/env python3
"""Build minute-level manifest request aggregates from the parquet lake."""

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
    minute_utc_sql,
    platform_key_sql,
    platform_name_sql,
    q,
    source_filter,
    table_count,
    write_append_table,
)
from vglive_core import channel_candidate_sql


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def decoded_query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    raw_value = query_param_sql(param_name, query_col)
    return f"COALESCE(try(url_decode(NULLIF({raw_value}, ''))), NULLIF({raw_value}, ''))"


def label_sql(expr: str, fallback: str = "Unknown / NA") -> str:
    return f"COALESCE(NULLIF(trim(CAST({expr} AS VARCHAR)), ''), '{fallback}')"


def stream_platform_name_sql() -> str:
    # STREAM rows usually do not have platform in host; use app queryStr evidence
    # where present, otherwise keep one broad STREAM bucket.
    platform = decoded_query_param_sql("platform")
    device = decoded_query_param_sql("device")
    return f"""
CASE
    WHEN NULLIF(trim(CAST({platform} AS VARCHAR)), '') IS NOT NULL THEN {label_sql(platform, "STREAM")}
    WHEN NULLIF(trim(CAST({device} AS VARCHAR)), '') IS NOT NULL THEN {label_sql(device, "STREAM")}
    ELSE 'STREAM'
END
"""


def resolved_platform_name_sql(source: str) -> str:
    if source.lower() == "stream":
        return stream_platform_name_sql()
    return platform_name_sql("reqHost")


def resolved_platform_key_sql(source: str) -> str:
    if source.lower() == "stream":
        return "lower(regexp_replace(" + resolved_platform_name_sql(source) + ", '[^a-zA-Z0-9]+', '_', 'g'))"
    return platform_key_sql("reqHost")


def build_manifest_minute_table(con, args: argparse.Namespace) -> None:
    start, end = checked_dates(args)
    lake_glob = q(args.lake / "**" / "*.parquet")
    candidate_expr = channel_candidate_sql("reqPath")
    partition_filter = date_filter_sql(start, end)
    platform_name_expr = resolved_platform_name_sql(args.source)
    platform_key_expr = resolved_platform_key_sql(args.source)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE manifest_minute_new AS
        WITH base AS (
            SELECT
                COALESCE(CAST(source AS VARCHAR), '{args.source.lower()}') AS source,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d') AS log_date,
                strftime({minute_utc_sql("reqTimeSec")}, '%Y-%m-%d %H:%M:%S') AS minute_utc,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d %H:%M:%S') AS minute_ist,
                lower(COALESCE(reqHost, '')) AS reqHost,
                COALESCE(NULLIF(cliIP, ''), NULL) AS cliIP,
                NULLIF(trim(regexp_replace(COALESCE(CAST(UA AS VARCHAR), ''), '\\s+', ' ', 'g')), '') AS UA,
                COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', ''), ''), 'Unknown') AS statusCode,
                lower(COALESCE(CAST(reqPath AS VARCHAR), '')) AS reqPath,
                {candidate_expr} AS candidate_id,
                {platform_name_expr} AS platform_name,
                {platform_key_expr} AS platform_key
            FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
            WHERE {source_filter(args.source)}
              AND ({partition_filter})
              AND lower(COALESCE(CAST(reqPath AS VARCHAR), '')) LIKE '%.m3u8'
              AND TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
        ),
        resolved AS (
            SELECT
                b.*,
                COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name
            FROM base b
            LEFT JOIN host_map h ON b.reqHost = h.reqHost
            LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
        )
        SELECT
            log_date,
            source,
            minute_utc,
            minute_ist,
            platform_key,
            platform_name,
            candidate_id,
            channel_name,
            any_value(reqHost ORDER BY reqHost) AS reqHost,
            COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
            COUNT(*)::BIGINT AS m3u8_rows,
            COUNT(*) FILTER (WHERE statusCode = '200')::BIGINT AS status_200_m3u8_rows,
            COUNT(*) FILTER (WHERE statusCode <> '200')::BIGINT AS non_200_m3u8_rows,
            COUNT(DISTINCT cliIP)::BIGINT AS unique_ips,
            COUNT(DISTINCT UA)::BIGINT AS unique_uas,
            COUNT(DISTINCT COALESCE(cliIP, '') || '|' || COALESCE(UA, ''))::BIGINT AS unique_ipua_pairs
        FROM resolved
        GROUP BY 1,2,3,4,5,6,7,8
        """
    )


def write_manifest(args: argparse.Namespace, new_rows: int, output_path: Path) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "lake": str(args.lake.resolve()),
        "output": str(output_path.resolve()),
        "date_range_replaced": {"start": args.start or "", "end": args.end or ""},
        "metric_notes": {
            "m3u8_rows": "All .m3u8 manifest request rows per IST minute.",
            "status_200_m3u8_rows": "HTTP 200 .m3u8 manifest request rows per IST minute.",
            "non_200_m3u8_rows": "Non-200 .m3u8 manifest request rows per IST minute.",
            "unique_ips": "Distinct cliIP values per minute.",
            "unique_ipua_pairs": "Distinct cliIP + normalized UA signatures per minute.",
        },
        "new_rows": new_rows,
    }
    manifest_path = output_path.with_name("manifest_minute_manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build minute-level manifest request aggregates.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--source", choices=["fast", "stream"], required=True)
    parser.add_argument("--start", default=None, help="IST lake date start, YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="IST lake date end, YYYY-MM-DD.")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "output" / "cache" / "duckdb_temp",
    )
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    start, end = checked_dates(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(args)
    try:
        build_manifest_minute_table(con, args)
        new_rows = table_count(con, "manifest_minute_new")
        if new_rows <= 0:
            raise SystemExit(f"No {args.source.upper()} .m3u8 rows found for the selected manifest-minute range.")

        output_path = args.out_dir / "manifest_minute.parquet"
        write_append_table(con, "manifest_minute_new", output_path, args.source, start, end)
        write_manifest(args, new_rows, output_path)
    finally:
        con.close()

    print(f"Manifest minute parquet: {output_path}")
    print(json.dumps({"source": args.source, "new_rows": new_rows}, indent=2))


if __name__ == "__main__":
    main()
