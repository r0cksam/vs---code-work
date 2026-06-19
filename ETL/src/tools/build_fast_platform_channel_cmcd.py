"""Build FAST platform/channel CMCD playback-session aggregates from .ts lake rows."""

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


HOURS_PER_MILLISECOND = 1 / 3_600_000


def build_cmcd_table(con, args: argparse.Namespace) -> None:
    start, end = checked_dates(args)
    lake_glob = q(args.lake / "**" / "*.parquet")
    partition_filter = date_filter_sql(start, end)
    candidate_expr = channel_candidate_sql("reqPath")

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE fast_platform_channel_cmcd_new AS
        WITH base AS (
            SELECT
                COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d') AS log_date,
                lower(COALESCE(reqHost, '')) AS reqHost,
                COALESCE(NULLIF(cliIP, ''), NULL) AS cliIP,
                NULLIF(trim(regexp_replace(COALESCE(CAST(UA AS VARCHAR), ''), '\\s+', ' ', 'g')), '') AS UA,
                COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', ''), ''), 'Unknown') AS statusCode,
                {candidate_expr} AS candidate_id,
                NULLIF(COALESCE(try(url_decode(CAST(cmcd AS VARCHAR))), CAST(cmcd AS VARCHAR)), '') AS cmcd_decoded
            FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
            WHERE {source_filter(args.source)}
              AND ({partition_filter})
              AND lower(COALESCE(CAST(reqPath AS VARCHAR), '')) LIKE '%.ts'
              AND TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
        ),
        parsed AS (
            SELECT
                *,
                NULLIF(regexp_extract(cmcd_decoded, '(?i)(?:^|[,;])sid="?([^",;]+)"?', 1), '') AS cmcd_sid,
                NULLIF(regexp_extract(cmcd_decoded, '(?i)(?:^|[,;])cid="?([^",;]+)"?', 1), '') AS cmcd_cid,
                TRY_CAST(NULLIF(regexp_extract(cmcd_decoded, '(?i)(?:^|[,;])d=([0-9.]+)', 1), '') AS DOUBLE)
                    AS cmcd_duration_ms,
                TRY_CAST(NULLIF(regexp_extract(cmcd_decoded, '(?i)(?:^|[,;])br=([0-9.]+)', 1), '') AS DOUBLE)
                    AS cmcd_encoded_bitrate,
                TRY_CAST(NULLIF(regexp_extract(cmcd_decoded, '(?i)(?:^|[,;])mtp=([0-9.]+)', 1), '') AS DOUBLE)
                    AS cmcd_measured_throughput
            FROM base
        ),
        resolved AS (
            SELECT
                p.*,
                COALESCE(h.host_channel_name, m.path_channel_name, 'Other') AS channel_name,
                {platform_name_sql("p.reqHost")} AS platform_name,
                {platform_key_sql("p.reqHost")} AS platform_key,
                CASE
                    WHEN m.path_channel_name IS NOT NULL THEN 'path_map'
                    WHEN h.host_channel_name IS NOT NULL THEN 'host_map'
                    WHEN NULLIF(p.candidate_id, '') IS NOT NULL THEN 'unmapped_path_candidate'
                    ELSE 'no_channel_evidence'
                END AS channel_evidence
            FROM parsed p
            LEFT JOIN host_map h ON p.reqHost = h.reqHost
            LEFT JOIN path_map m ON p.candidate_id = m.candidate_id
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
            channel_evidence,
            COUNT(*)::BIGINT AS raw_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200')::BIGINT AS status_200_ts_rows,
            COUNT(*) FILTER (WHERE cmcd_decoded IS NOT NULL)::BIGINT AS cmcd_rows,
            COUNT(*) FILTER (WHERE cmcd_sid IS NOT NULL)::BIGINT AS cmcd_sid_rows,
            COUNT(DISTINCT cmcd_sid)::BIGINT AS distinct_cmcd_sessions,
            COUNT(DISTINCT cmcd_cid)::BIGINT AS distinct_cmcd_content_ids,
            SUM(COALESCE(cmcd_duration_ms, 0)) * {HOURS_PER_MILLISECOND:.16f}
                AS cmcd_reported_duration_hours,
            AVG(cmcd_encoded_bitrate) AS avg_cmcd_encoded_bitrate,
            AVG(cmcd_measured_throughput) AS avg_cmcd_measured_throughput,
            COUNT(DISTINCT cliIP)::BIGINT AS approx_unique_ips,
            COUNT(DISTINCT UA)::BIGINT AS distinct_uas,
            COUNT(DISTINCT COALESCE(cliIP, '') || '|' || COALESCE(UA, ''))::BIGINT AS distinct_ipua_pairs
        FROM resolved
        GROUP BY 1,2,3,4,5,6,9
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
            "distinct_cmcd_sessions": "Exact daily distinct CMCD sid values on FAST .ts rows. This is playback-session telemetry, not app queryStr session_id.",
            "distinct_cmcd_content_ids": "Exact daily distinct CMCD cid values on FAST .ts rows.",
            "cmcd_reported_duration_hours": "Sum of CMCD d= duration milliseconds converted to hours; only rows carrying CMCD contribute.",
            "cmcd_sid_rows": "Rows carrying a parseable CMCD sid.",
            "raw_ts_rows": "All FAST .ts rows for the platform/channel; use with cmcd_sid_rows for coverage.",
        },
        "new_rows": new_rows,
    }
    manifest_path = output_path.with_name("fast_platform_channel_cmcd_daily_manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FAST platform/channel CMCD playback-session mart.")
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
        build_cmcd_table(con, args)
        new_rows = table_count(con, "fast_platform_channel_cmcd_new")
        if new_rows <= 0:
            raise SystemExit("No FAST .ts rows found for the selected platform/channel CMCD range.")

        output_path = args.out_dir / "fast_platform_channel_cmcd_daily.parquet"
        write_append_table(con, "fast_platform_channel_cmcd_new", output_path, args.source, start, end)
        write_manifest(args, new_rows, output_path)
    finally:
        con.close()

    print(f"FAST platform/channel CMCD parquet: {output_path}")
    print(json.dumps({"new_rows": new_rows}, indent=2))


if __name__ == "__main__":
    main()
