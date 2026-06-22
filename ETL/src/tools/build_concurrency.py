#!/usr/bin/env python3
"""Build minute-level concurrency aggregates from the parquet lake."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = ETL_ROOT / "src" / "profile"
if str(PROFILE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROFILE_ROOT))

from vglive_core import (  # noqa: E402
    DEFAULT_LAKE_FOLDER,
    HOST_MAP,
    PATH_MAP,
    build_partition_filter,
    channel_candidate_sql,
)


DEFAULT_OUT_DIR = ETL_ROOT / "output" / "watch_hours" / "concurrency"
IST_OFFSET_SECONDS = 19_800
SEGMENTS_PER_MINUTE = 10.0


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def checked_dates(args: argparse.Namespace) -> tuple[date | None, date | None]:
    start = parse_date(args.start)
    end = parse_date(args.end)
    if (start is None) != (end is None):
        raise SystemExit("Use both --start and --end, or neither.")
    if start and end and start > end:
        raise SystemExit("--start cannot be after --end.")
    return start, end


def register_maps(con: duckdb.DuckDBPyConnection) -> None:
    host_df = pd.DataFrame(
        [{"reqHost": host, "host_channel_name": name} for host, name in HOST_MAP.items()]
    )
    path_df = pd.DataFrame(
        [{"candidate_id": candidate, "path_channel_name": name} for candidate, name in PATH_MAP.items()]
    )
    con.register("host_map_df", host_df)
    con.register("path_map_df", path_df)
    con.execute("CREATE OR REPLACE TEMP TABLE host_map AS SELECT * FROM host_map_df")
    con.execute("CREATE OR REPLACE TEMP TABLE path_map AS SELECT * FROM path_map_df")


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, int(args.threads))}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={sql_text(q(args.temp_dir))}")
    register_maps(con)
    return con


def source_filter(source: str) -> str:
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = {sql_text(source.lower())}"


def platform_name_sql(host_col: str = "reqHost") -> str:
    h = f"lower(COALESCE({host_col}, ''))"
    return f"""
CASE
    WHEN {h} LIKE '%indiatv-samsung%' THEN 'Samsung TV Plus - IN'
    WHEN {h} LIKE '%veto-samsung%' THEN 'Samsung TV Plus - IN'
    WHEN {h} LIKE '%indiatv-tcl%' THEN 'TCL'
    WHEN {h} LIKE '%indiatv-cloudtv%' THEN 'CloudTV'
    WHEN {h} LIKE '%indiatv-vi%' THEN 'Vi Movies & TV'
    WHEN {h} = '' THEN 'Unknown FAST Platform'
    ELSE regexp_replace({h}, '\\.akamaized\\.net$', '')
END
"""


def platform_key_sql(host_col: str = "reqHost") -> str:
    h = f"lower(COALESCE({host_col}, ''))"
    return f"""
CASE
    WHEN {h} LIKE '%indiatv-samsung%' THEN 'samsung'
    WHEN {h} LIKE '%veto-samsung%' THEN 'samsung'
    WHEN {h} LIKE '%indiatv-tcl%' THEN 'tcl'
    WHEN {h} LIKE '%indiatv-cloudtv%' THEN 'cloudtv'
    WHEN {h} LIKE '%indiatv-vi%' THEN 'vi'
    WHEN {h} = '' THEN 'unknown'
    ELSE regexp_replace(regexp_replace({h}, '\\.akamaized\\.net$', ''), '[^a-z0-9]+', '_')
END
"""


def minute_utc_sql(epoch_col: str = "reqTimeSec") -> str:
    return (
        "epoch_ms(CAST(FLOOR(TRY_CAST("
        f"{epoch_col}"
        " AS DOUBLE) / 60) * 60000 AS BIGINT))"
    )


def minute_ist_sql(epoch_col: str = "reqTimeSec") -> str:
    return (
        "epoch_ms(CAST(FLOOR((TRY_CAST("
        f"{epoch_col}"
        f" AS DOUBLE) + {IST_OFFSET_SECONDS}) / 60) * 60000 AS BIGINT))"
    )


def date_filter_sql(start: date | None, end: date | None) -> str:
    return build_partition_filter(start, end) if start and end else "1=1"


def resolved_platform_name_sql(source: str) -> str:
    if source.lower() == "stream":
        return "'STREAM'"
    return platform_name_sql("b.reqHost")


def resolved_platform_key_sql(source: str) -> str:
    if source.lower() == "stream":
        return "'stream'"
    return platform_key_sql("b.reqHost")


def build_new_tables(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    start, end = checked_dates(args)
    lake_glob = q(args.lake / "**" / "*.parquet")
    candidate_expr = channel_candidate_sql("reqPath")
    partition_filter = date_filter_sql(start, end)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE concurrency_resolved_new AS
        WITH base AS (
            SELECT
                COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d') AS log_date,
                strftime({minute_utc_sql("reqTimeSec")}, '%Y-%m-%d %H:%M:%S') AS minute_utc,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d %H:%M:%S') AS minute_ist,
                lower(COALESCE(reqHost, '')) AS reqHost,
                COALESCE(NULLIF(cliIP, ''), NULL) AS cliIP,
                NULLIF(trim(regexp_replace(COALESCE(CAST(UA AS VARCHAR), ''), '\\s+', ' ', 'g')), '') AS UA,
                COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', ''), ''), 'Unknown') AS statusCode,
                {candidate_expr} AS candidate_id
            FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
            WHERE {source_filter(args.source)}
              AND ({partition_filter})
              AND lower(COALESCE(reqPath, '')) LIKE '%.ts'
              AND TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
        ),
        resolved AS (
            SELECT
                b.*,
                COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name,
                {resolved_platform_name_sql(args.source)} AS platform_name,
                {resolved_platform_key_sql(args.source)} AS platform_key
            FROM base b
            LEFT JOIN host_map h ON b.reqHost = h.reqHost
            LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
        )
        SELECT * FROM resolved
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE concurrency_minute_new AS
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
            COUNT(*)::BIGINT AS raw_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200')::BIGINT AS status_200_ts_rows,
            COUNT(DISTINCT cliIP)::BIGINT AS unique_viewers,
            COUNT(DISTINCT UA)::BIGINT AS unique_ua_viewers,
            ROUND(COUNT(*) / {SEGMENTS_PER_MINUTE}, 3) AS segment_viewers_estimate,
            ROUND(COUNT(*) FILTER (WHERE statusCode = '200') / {SEGMENTS_PER_MINUTE}, 3)
                AS status_200_segment_viewers_estimate
        FROM concurrency_resolved_new
        GROUP BY 1,2,3,4,5,6,7,8
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE concurrency_status_minute_new AS
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
            statusCode AS status_code,
            COUNT(*)::BIGINT AS status_ts_rows,
            COUNT(DISTINCT cliIP)::BIGINT AS status_unique_viewers,
            COUNT(DISTINCT UA)::BIGINT AS status_unique_ua_viewers,
            ROUND(COUNT(*) / {SEGMENTS_PER_MINUTE}, 3) AS status_segment_viewers_estimate
        FROM concurrency_resolved_new
        GROUP BY 1,2,3,4,5,6,7,8,10
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE concurrency_summary_new AS
        WITH minute_summary AS (
            SELECT
                log_date,
                source,
                platform_key,
                platform_name,
                candidate_id,
                channel_name,
                any_value(reqHost ORDER BY reqHost) AS reqHost,
                MAX(distinct_hosts)::BIGINT AS distinct_hosts,
                COUNT(*)::BIGINT AS minute_count,
                SUM(raw_ts_rows)::BIGINT AS raw_ts_rows,
                SUM(status_200_ts_rows)::BIGINT AS status_200_ts_rows,
                ROUND(AVG(unique_viewers), 3) AS avg_unique_viewers,
                MAX(unique_viewers)::BIGINT AS peak_unique_viewers,
                any_value(minute_ist ORDER BY unique_viewers DESC, minute_ist) AS peak_unique_viewers_minute_ist,
                ROUND(quantile_cont(unique_viewers, 0.95), 3) AS p95_unique_viewers,
                ROUND(AVG(unique_ua_viewers), 3) AS avg_unique_ua_viewers,
                MAX(unique_ua_viewers)::BIGINT AS peak_unique_ua_viewers,
                any_value(minute_ist ORDER BY unique_ua_viewers DESC, minute_ist) AS peak_unique_ua_minute_ist,
                ROUND(quantile_cont(unique_ua_viewers, 0.95), 3) AS p95_unique_ua_viewers,
                ROUND(AVG(segment_viewers_estimate), 3) AS avg_segment_viewers_estimate,
                ROUND(MAX(segment_viewers_estimate), 3) AS peak_segment_viewers_estimate,
                any_value(minute_ist ORDER BY segment_viewers_estimate DESC, minute_ist) AS peak_segment_minute_ist,
                ROUND(AVG(status_200_segment_viewers_estimate), 3) AS avg_status_200_segment_viewers_estimate,
                ROUND(MAX(status_200_segment_viewers_estimate), 3) AS peak_status_200_segment_viewers_estimate
            FROM concurrency_minute_new
            GROUP BY 1,2,3,4,5,6
        ),
        daily_identity AS (
            SELECT
                log_date,
                source,
                platform_key,
                platform_name,
                candidate_id,
                channel_name,
                COUNT(DISTINCT cliIP)::BIGINT AS distinct_cliips,
                COUNT(DISTINCT UA)::BIGINT AS distinct_uas,
                COUNT(DISTINCT COALESCE(cliIP, '') || '|' || COALESCE(UA, ''))::BIGINT AS distinct_ipua_pairs
            FROM concurrency_resolved_new
            GROUP BY 1,2,3,4,5,6
        )
        SELECT
            m.*,
            COALESCE(d.distinct_cliips, 0)::BIGINT AS distinct_cliips,
            COALESCE(d.distinct_uas, 0)::BIGINT AS distinct_uas,
            COALESCE(d.distinct_ipua_pairs, 0)::BIGINT AS distinct_ipua_pairs
        FROM minute_summary m
        LEFT JOIN daily_identity d USING (
            log_date,
            source,
            platform_key,
            platform_name,
            candidate_id,
            channel_name
        )
        """
    )


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)


def table_columns(con: duckdb.DuckDBPyConnection, table_sql: str) -> list[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {table_sql} LIMIT 0").fetchall()
    return [str(row[0]) for row in rows]


def copy_table(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO '{q(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(path)


def write_append_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    out_path: Path,
    source: str,
    start: date | None,
    end: date | None,
) -> None:
    if out_path.exists() and start and end:
        existing_sql = f"read_parquet('{q(out_path)}')"
        if table_columns(con, existing_sql) == table_columns(con, table_name):
            keep_where = (
                f"NOT (source = {sql_text(source)} AND log_date >= {sql_text(start.isoformat())} "
                f"AND log_date <= {sql_text(end.isoformat())})"
            )
            sql = f"""
                SELECT * FROM {existing_sql}
                WHERE {keep_where}
                UNION ALL
                SELECT * FROM {table_name}
            """
        else:
            sql = f"SELECT * FROM {table_name}"
    else:
        sql = f"SELECT * FROM {table_name}"
    copy_table(con, sql, out_path)


def summarize_output(
    con: duckdb.DuckDBPyConnection,
    minute_path: Path,
    summary_path: Path,
    status_minute_path: Path,
) -> dict:
    if not minute_path.exists():
        return {}
    stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS minute_rows,
            MIN(log_date) AS first_date,
            MAX(log_date) AS last_date,
            COUNT(DISTINCT log_date) AS dates,
            COUNT(DISTINCT platform_name) AS platforms,
            COUNT(DISTINCT channel_name) AS channels,
            COUNT(DISTINCT platform_key || '|' || candidate_id || '|' || channel_name) AS platform_channel_candidates,
            MAX(unique_viewers) AS peak_unique_viewers,
            ROUND(AVG(unique_viewers), 3) AS avg_unique_viewers,
            MAX(unique_ua_viewers) AS peak_unique_ua_viewers,
            ROUND(AVG(unique_ua_viewers), 3) AS avg_unique_ua_viewers,
            MAX(segment_viewers_estimate) AS peak_segment_viewers_estimate
        FROM read_parquet('{q(minute_path)}')
        """
    ).fetchdf().iloc[0].to_dict() | {
        "summary_rows": table_count_from_path(con, summary_path),
    }
    if summary_path.exists():
        summary_stats = con.execute(
            f"""
            SELECT
                MAX(distinct_cliips) AS peak_daily_distinct_cliips,
                SUM(distinct_cliips) AS daily_distinct_cliip_sum,
                MAX(distinct_uas) AS peak_daily_distinct_uas,
                SUM(distinct_uas) AS daily_distinct_ua_sum
            FROM read_parquet('{q(summary_path)}')
            """
        ).fetchdf().iloc[0].to_dict()
        stats |= summary_stats
    if status_minute_path.exists():
        status_stats = con.execute(
            f"""
            SELECT
                COUNT(*) AS status_minute_rows,
                COUNT(DISTINCT status_code) AS status_codes,
                string_agg(DISTINCT status_code, ',' ORDER BY status_code) AS status_code_list
            FROM read_parquet('{q(status_minute_path)}')
            """
        ).fetchdf().iloc[0].to_dict()
        stats |= status_stats
    return stats


def table_count_from_path(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    if not path.exists():
        return 0
    return int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{q(path)}')").fetchone()[0] or 0)


def write_manifest(
    args: argparse.Namespace,
    start: date | None,
    end: date | None,
    minute_path: Path,
    summary_path: Path,
    status_minute_path: Path,
    new_counts: dict,
    output_stats: dict,
) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "lake": str(args.lake.resolve()),
        "date_range_replaced": {
            "start": start.isoformat() if start else "",
            "end": end.isoformat() if end else "",
        },
        "metric_notes": {
            "unique_viewers": "Exact distinct cliIP count per minute.",
            "unique_ua_viewers": "Exact distinct normalized User-Agent string count per minute.",
            "distinct_cliips": "Exact daily distinct cliIP count for each source/platform/channel on .ts rows. STREAM platform is labelled STREAM because app platform is not present on .ts rows.",
            "distinct_uas": "Exact daily distinct normalized User-Agent count for each source/platform/channel on .ts rows. STREAM platform is labelled STREAM because app platform is not present on .ts rows.",
            "distinct_ipua_pairs": "Exact daily distinct cliIP + User-Agent pair count for each source/platform/channel on .ts rows. STREAM platform is labelled STREAM because app platform is not present on .ts rows.",
            "segment_viewers_estimate": "raw .ts rows divided by 10 six-second segments per minute.",
            "status_200_segment_viewers_estimate": "HTTP 200 .ts rows divided by 10 six-second segments per minute.",
            "status_code_segment_viewers_estimate": "Selected HTTP status .ts rows divided by 10 six-second segments per minute.",
            "status_unique_viewers": "Exact distinct cliIP count per minute for a selected HTTP status code.",
            "status_unique_ua_viewers": "Exact distinct normalized User-Agent count per minute for a selected HTTP status code.",
        },
        "files": {
            "minute": str(minute_path.resolve()),
            "summary": str(summary_path.resolve()),
            "status_minute": str(status_minute_path.resolve()),
        },
        "new_counts": new_counts,
        "output_stats": output_stats,
    }
    path = args.out_dir / "concurrency_manifest.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build minute-level concurrency parquet outputs.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--source", choices=["fast", "stream"], default="fast")
    parser.add_argument("--start", default=None, help="IST lake date start, YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="IST lake date end, YYYY-MM-DD.")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    start, end = checked_dates(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(args)
    try:
        build_new_tables(con, args)
        new_counts = {
            "minute_rows": table_count(con, "concurrency_minute_new"),
            "status_minute_rows": table_count(con, "concurrency_status_minute_new"),
            "summary_rows": table_count(con, "concurrency_summary_new"),
        }
        if new_counts["minute_rows"] <= 0:
            raise SystemExit(f"No {args.source.upper()} .ts rows found for the selected concurrency range.")

        minute_path = args.out_dir / "concurrency_minute.parquet"
        status_minute_path = args.out_dir / "concurrency_status_minute.parquet"
        summary_path = args.out_dir / "concurrency_summary.parquet"
        write_append_table(con, "concurrency_minute_new", minute_path, args.source, start, end)
        write_append_table(con, "concurrency_status_minute_new", status_minute_path, args.source, start, end)
        write_append_table(con, "concurrency_summary_new", summary_path, args.source, start, end)
        output_stats = summarize_output(con, minute_path, summary_path, status_minute_path)
        write_manifest(args, start, end, minute_path, summary_path, status_minute_path, new_counts, output_stats)
    finally:
        con.close()

    print(f"Concurrency minute parquet: {minute_path}")
    print(f"Concurrency status minute parquet: {status_minute_path}")
    print(f"Concurrency summary parquet: {summary_path}")
    print(json.dumps({"new_counts": new_counts, "output_stats": output_stats}, indent=2, default=str))


if __name__ == "__main__":
    main()
