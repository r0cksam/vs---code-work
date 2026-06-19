#!/usr/bin/env python3
"""Build a reusable cliIP viewer index for the Viewer Journey Menu dashboard."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = ETL_ROOT / "src" / "profile"
if str(PROFILE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROFILE_ROOT))

from vglive_core import (  # noqa: E402
    CHUNK_DURATION_HOURS,
    DEFAULT_LAKE_FOLDER,
    HOST_MAP,
    PATH_MAP,
    build_partition_filter,
    channel_candidate_sql,
)


IST_OFFSET_SECONDS = 19_800
DEFAULT_OUT_DIR = ETL_ROOT / "output" / "exports" / "viewer_journey"


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def safe_decoded_query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    raw_value = query_param_sql(param_name, query_col)
    return (
        f"COALESCE(try(url_decode(NULLIF({raw_value}, ''))), "
        f"NULLIF({raw_value}, ''))"
    )


def url_decode_text_sql(column_expr: str) -> str:
    return (
        f"COALESCE(try(url_decode(CAST({column_expr} AS VARCHAR))), "
        f"CAST({column_expr} AS VARCHAR))"
    )


def ist_timestamp_expr(epoch_expr: str) -> str:
    return (
        "epoch_ms(CAST(FLOOR(("
        f"TRY_CAST({epoch_expr} AS DOUBLE) + {IST_OFFSET_SECONDS}"
        ") * 1000) AS BIGINT))"
    )


def row_kind_filter(row_kind: str) -> str:
    if row_kind == "watch":
        return "lower(reqPath) LIKE '%.ts'"
    if row_kind == "playlist":
        return "lower(reqPath) LIKE '%.m3u8'"
    if row_kind == "media":
        return "(lower(reqPath) LIKE '%.ts' OR lower(reqPath) LIKE '%.m3u8')"
    return "1=1"


def source_filter(source: str | None) -> str:
    if not source:
        return "1=1"
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = lower({sql_text(source)})"


def checked_partition_filter(args: argparse.Namespace) -> str:
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end, or neither for all available dates.")
    if start_date and end_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end.")
    return build_partition_filter(start_date, end_date)


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
    return con


def build_sql(args: argparse.Namespace) -> str:
    lake_glob = q(args.lake / "**" / "*.parquet")
    candidate_expr = channel_candidate_sql("reqPath")
    where_sql = " AND ".join(
        f"({part})"
        for part in [
            checked_partition_filter(args),
            source_filter(args.source),
            row_kind_filter(args.row_kind),
            "NULLIF(CAST(cliIP AS VARCHAR), '') IS NOT NULL",
        ]
    )
    return f"""
    WITH raw AS (
        SELECT
            printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS log_date,
            COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
            CAST(cliIP AS VARCHAR) AS cliIP,
            lower(reqHost) AS reqHost,
            reqPath,
            regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', '') AS statusCode,
            {ist_timestamp_expr("reqTimeSec")} AS req_time_ist,
            {url_decode_text_sql("country")} AS country,
            {url_decode_text_sql("state")} AS state,
            {url_decode_text_sql("city")} AS city,
            UA,
            {candidate_expr} AS candidate_id,
            {safe_decoded_query_param_sql("session_id")} AS session_id_raw,
            {safe_decoded_query_param_sql("device_id")} AS device_id_raw,
            lower(reqPath) LIKE '%.ts' AS is_ts,
            lower(reqPath) LIKE '%.m3u8' AS is_playlist
        FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
        WHERE {where_sql}
    ),
    resolved AS (
        SELECT
            raw.*,
            COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name
        FROM raw
        LEFT JOIN host_map h ON raw.reqHost = h.reqHost
        LEFT JOIN path_map p ON raw.candidate_id = p.candidate_id
    ),
    identity_source AS (
        SELECT
            source,
            log_date,
            cliIP,
            channel_name,
            req_time_ist,
            session_id_raw,
            device_id_raw
        FROM resolved
        WHERE NULLIF(session_id_raw, '') IS NOT NULL
            OR NULLIF(device_id_raw, '') IS NOT NULL
    ),
    identity_hour AS (
        SELECT
            source,
            log_date,
            cliIP,
            channel_name,
            date_trunc('hour', req_time_ist) AS hour_ist,
            CASE
                WHEN COUNT(DISTINCT NULLIF(session_id_raw, '')) = 1
                    THEN MAX(NULLIF(session_id_raw, ''))
                ELSE NULL
            END AS hour_session_id,
            CASE
                WHEN COUNT(DISTINCT NULLIF(device_id_raw, '')) = 1
                    THEN MAX(NULLIF(device_id_raw, ''))
                ELSE NULL
            END AS hour_device_id
        FROM identity_source
        GROUP BY source, log_date, cliIP, channel_name, hour_ist
    ),
    identity_day AS (
        SELECT
            source,
            log_date,
            cliIP,
            channel_name,
            CASE
                WHEN COUNT(DISTINCT NULLIF(session_id_raw, '')) = 1
                    THEN MAX(NULLIF(session_id_raw, ''))
                ELSE NULL
            END AS day_unique_session_id,
            CASE
                WHEN COUNT(DISTINCT NULLIF(device_id_raw, '')) = 1
                    THEN MAX(NULLIF(device_id_raw, ''))
                ELSE NULL
            END AS day_unique_device_id
        FROM identity_source
        GROUP BY source, log_date, cliIP, channel_name
    ),
    media_hourly AS (
        SELECT
            log_date,
            source,
            cliIP,
            country,
            state,
            city,
            channel_name,
            statusCode,
            date_trunc('hour', req_time_ist) AS hour_ist,
            COUNT(*)::BIGINT AS row_count,
            SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS raw_ts_rows,
            SUM(CASE WHEN is_ts AND statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_ts_rows,
            COUNT(DISTINCT CASE WHEN is_ts THEN reqPath ELSE NULL END)::BIGINT AS dedup_ts_paths,
            COUNT(DISTINCT CASE WHEN is_ts AND statusCode = '200' THEN reqPath ELSE NULL END)::BIGINT
                AS dedup_status_200_ts_paths,
            SUM(CASE WHEN is_playlist THEN 1 ELSE 0 END)::BIGINT AS playlist_rows,
            ROUND(SUM(CASE WHEN is_ts THEN {CHUNK_DURATION_HOURS} ELSE 0 END), 6) AS raw_watch_hours,
            ROUND(SUM(CASE WHEN is_ts AND statusCode = '200' THEN {CHUNK_DURATION_HOURS} ELSE 0 END), 6)
                AS status_200_watch_hours,
            ROUND(COUNT(DISTINCT CASE WHEN is_ts THEN reqPath ELSE NULL END) * {CHUNK_DURATION_HOURS}, 6)
                AS dedup_raw_watch_hours,
            ROUND(
                COUNT(DISTINCT CASE WHEN is_ts AND statusCode = '200' THEN reqPath ELSE NULL END)
                    * {CHUNK_DURATION_HOURS},
                6
            ) AS dedup_status_200_watch_hours,
            COUNT(DISTINCT NULLIF(UA, ''))::BIGINT AS distinct_ua,
            COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
            MIN(req_time_ist) AS first_seen_ist,
            MAX(req_time_ist) AS last_seen_ist,
            ANY_VALUE(reqHost) AS sample_reqHost,
            ANY_VALUE(reqPath) AS sample_reqPath
        FROM resolved
        GROUP BY log_date, source, cliIP, country, state, city, channel_name, statusCode, hour_ist
    ),
    attributed AS (
        SELECT
            m.*,
            COALESCE(h.hour_session_id, d.day_unique_session_id, '') AS session_id,
            COALESCE(h.hour_device_id, d.day_unique_device_id, '') AS device_id
        FROM media_hourly m
        LEFT JOIN identity_hour h
            ON m.source = h.source
            AND m.log_date = h.log_date
            AND m.cliIP = h.cliIP
            AND m.channel_name = h.channel_name
            AND m.hour_ist = h.hour_ist
        LEFT JOIN identity_day d
            ON m.source = d.source
            AND m.log_date = d.log_date
            AND m.cliIP = d.cliIP
            AND m.channel_name = d.channel_name
    )
    SELECT
        log_date,
        source,
        cliIP,
        COALESCE(NULLIF(country, ''), 'Unknown / NA') AS country,
        COALESCE(NULLIF(state, ''), 'Unknown / NA') AS state,
        COALESCE(NULLIF(city, ''), 'Unknown / NA') AS city,
        channel_name,
        COALESCE(NULLIF(session_id, ''), 'Unknown / NA') AS session_id,
        COALESCE(NULLIF(device_id, ''), 'Unknown / NA') AS device_id,
        COALESCE(NULLIF(statusCode, ''), 'Unknown') AS statusCode,
        SUM(row_count)::BIGINT AS row_count,
        SUM(raw_ts_rows)::BIGINT AS raw_ts_rows,
        SUM(status_200_ts_rows)::BIGINT AS status_200_ts_rows,
        SUM(dedup_ts_paths)::BIGINT AS dedup_ts_paths,
        SUM(dedup_status_200_ts_paths)::BIGINT AS dedup_status_200_ts_paths,
        SUM(playlist_rows)::BIGINT AS playlist_rows,
        ROUND(SUM(raw_watch_hours), 6) AS raw_watch_hours,
        ROUND(SUM(status_200_watch_hours), 6) AS status_200_watch_hours,
        ROUND(SUM(dedup_raw_watch_hours), 6) AS dedup_raw_watch_hours,
        ROUND(SUM(dedup_status_200_watch_hours), 6) AS dedup_status_200_watch_hours,
        COUNT(DISTINCT NULLIF(session_id, ''))::BIGINT AS distinct_session_id,
        COUNT(DISTINCT NULLIF(device_id, ''))::BIGINT AS distinct_device_id,
        SUM(distinct_ua)::BIGINT AS distinct_ua,
        MAX(distinct_hosts)::BIGINT AS distinct_hosts,
        MIN(first_seen_ist) AS first_seen_ist,
        MAX(last_seen_ist) AS last_seen_ist,
        ANY_VALUE(sample_reqHost) AS sample_reqHost,
        ANY_VALUE(sample_reqPath) AS sample_reqPath
    FROM attributed
    GROUP BY log_date, source, cliIP, country, state, city, channel_name, session_id, device_id, statusCode
    ORDER BY log_date, source, raw_watch_hours DESC, row_count DESC
    """


def copy_query(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO '{q(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(out_path)


def iter_dates(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def parquet_list_sql(paths: list[Path]) -> str:
    return "[" + ", ".join(sql_text(q(path)) for path in paths) + "]"


def copy_query_by_day(con: duckdb.DuckDBPyConnection, args: argparse.Namespace, out_path: Path) -> None:
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if not start_date or not end_date or start_date == end_date:
        copy_query(con, build_sql(args), out_path)
        return

    parts_dir = out_path.parent / f"{out_path.stem}_parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    part_paths: list[Path] = []
    for day in iter_dates(start_date, end_date):
        day_text = day.isoformat()
        source_text = args.source or "all"
        part_path = parts_dir / f"{out_path.stem}_{source_text}_{day_text}.parquet"
        day_args = argparse.Namespace(**vars(args))
        day_args.start = day_text
        day_args.end = day_text
        print(f"Building part: {day_text} source={source_text}")
        copy_query(con, build_sql(day_args), part_path)
        part_paths.append(part_path)

    combine_sql = f"SELECT * FROM read_parquet({parquet_list_sql(part_paths)}, union_by_name=1)"
    copy_query(con, combine_sql, out_path)


def write_manifest(args: argparse.Namespace, out_path: Path, stats: dict) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake": str(args.lake.resolve()),
        "source": args.source or "all",
        "row_kind": args.row_kind,
        "start": args.start or "",
        "end": args.end or "",
        "multi_day_strategy": "day_parts" if args.start and args.end and parse_date(args.start) != parse_date(args.end) else "single_query",
        "output": str(out_path.resolve()),
        "stats": stats,
    }
    with out_path.with_suffix(".manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build viewer journey menu index parquet.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR / "viewer_journey_index.parquet")
    parser.add_argument("--start", help="Start date YYYY-MM-DD. Omit with --end for all available dates.")
    parser.add_argument("--end", help="End date YYYY-MM-DD. Omit with --start for all available dates.")
    parser.add_argument("--source", choices=["stream", "fast"], help="Optional source filter.")
    parser.add_argument(
        "--row-kind",
        choices=["all", "media", "watch", "playlist"],
        default="media",
        help="Rows included in the index. media=.ts+.m3u8 and is the dashboard-friendly default.",
    )
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--temp-dir", type=Path, default=DEFAULT_OUT_DIR / "_duckdb_tmp")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out = args.out.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    con = connect(args)
    register_maps(con)
    sql = build_sql(args)

    if args.dry_run:
        preview = con.execute(f"SELECT COUNT(*) AS rows FROM ({sql})").fetchdf()
        print("[Dry run] viewer index query OK")
        print(preview.to_string(index=False))
        return

    print(f"Lake : {args.lake}")
    print(f"Out  : {args.out}")
    copy_query_by_day(con, args, args.out)
    stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS index_rows,
            MIN(log_date) AS min_date,
            MAX(log_date) AS max_date,
            COUNT(DISTINCT cliIP) AS cliips,
            COUNT(DISTINCT source) AS sources,
            COUNT(DISTINCT channel_name) AS channels,
            ROUND(SUM(raw_watch_hours), 3) AS raw_watch_hours,
            ROUND(SUM(status_200_watch_hours), 3) AS status_200_watch_hours,
            ROUND(SUM(dedup_raw_watch_hours), 3) AS dedup_raw_watch_hours,
            ROUND(SUM(dedup_status_200_watch_hours), 3) AS dedup_status_200_watch_hours,
            SUM(row_count)::BIGINT AS raw_rows
        FROM read_parquet('{q(args.out)}')
        """
    ).fetchdf()
    stats_dict = stats.iloc[0].to_dict() if len(stats) else {}
    write_manifest(args, args.out, stats_dict)
    print(f"Index written: {args.out}")
    print(f"Size MB: {args.out.stat().st_size / 1024 / 1024:.2f}")
    print(stats.to_string(index=False))


if __name__ == "__main__":
    main()
