#!/usr/bin/env python3
"""Export date/channel volume for one cliIP directly from the lake."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = ETL_ROOT / "src" / "profile"
if str(PROFILE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROFILE_ROOT))

from vglive_core import CHUNK_DURATION_HOURS, DEFAULT_LAKE_FOLDER, HOST_MAP, PATH_MAP, channel_candidate_sql  # noqa: E402


DEFAULT_OUT_ROOT = ETL_ROOT / "output" / "exports" / "cliip_journey"
IST_OFFSET_SECONDS = 19_800


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def slug(value: str | None, max_len: int = 80) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return (text or "all")[:max_len].strip("_") or "all"


def cliip_slug(cliip: str) -> str:
    clean = slug(cliip, 54)
    digest = hashlib.blake2b(str(cliip).encode("utf-8"), digest_size=5).hexdigest()
    return f"{clean}_{digest}"


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def ist_timestamp_expr(epoch_expr: str) -> str:
    return (
        "epoch_ms(CAST(FLOOR(("
        f"TRY_CAST({epoch_expr} AS DOUBLE) + {IST_OFFSET_SECONDS}"
        ") * 1000) AS BIGINT))"
    )


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


def source_filter(source: str | None) -> str:
    if not source:
        return "1=1"
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = lower({sql_text(source)})"


def export_sql(args: argparse.Namespace) -> str:
    candidate_expr = channel_candidate_sql("reqPath")
    lake_glob = q(args.lake / "**" / "*.parquet")
    return f"""
    WITH raw AS (
        SELECT
            printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS log_date,
            COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
            {ist_timestamp_expr("reqTimeSec")} AS req_time_ist,
            cliIP,
            lower(reqHost) AS reqHost,
            reqPath,
            regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', '') AS statusCode,
            {candidate_expr} AS candidate_id,
            {query_param_sql("session_id")} AS session_id,
            {query_param_sql("device_id")} AS device_id,
            lower(reqPath) LIKE '%.ts' AS is_ts,
            lower(reqPath) LIKE '%.m3u8' AS is_playlist
        FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
        WHERE cliIP = {sql_text(args.cliip)}
          AND {source_filter(args.source)}
    ),
    resolved AS (
        SELECT
            raw.*,
            COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name
        FROM raw
        LEFT JOIN host_map h ON raw.reqHost = h.reqHost
        LEFT JOIN path_map p ON raw.candidate_id = p.candidate_id
    )
    SELECT
        log_date,
        source,
        channel_name,
        COUNT(*)::BIGINT AS row_count,
        SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
        SUM(CASE WHEN is_playlist THEN 1 ELSE 0 END)::BIGINT AS playlist_rows,
        SUM(CASE WHEN statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_rows,
        SUM(CASE WHEN statusCode <> '200' OR statusCode IS NULL THEN 1 ELSE 0 END)::BIGINT AS non_200_rows,
        ROUND(SUM(CASE WHEN is_ts THEN {CHUNK_DURATION_HOURS} ELSE 0 END), 6) AS raw_watch_hours,
        ROUND(SUM(CASE WHEN is_ts THEN {CHUNK_DURATION_HOURS} ELSE 0 END) * 60, 3) AS raw_watch_minutes,
        ROUND(SUM(CASE WHEN is_ts AND statusCode = '200' THEN {CHUNK_DURATION_HOURS} ELSE 0 END), 6) AS status_200_watch_hours,
        ROUND(SUM(CASE WHEN is_ts AND statusCode = '200' THEN {CHUNK_DURATION_HOURS} ELSE 0 END) * 60, 3) AS status_200_watch_minutes,
        MIN(req_time_ist) AS first_seen_ist,
        MAX(req_time_ist) AS last_seen_ist,
        ROUND(date_diff('millisecond', MIN(req_time_ist), MAX(req_time_ist)) / 60000.0, 3) AS wall_clock_minutes,
        COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
        COUNT(DISTINCT candidate_id)::BIGINT AS distinct_candidates,
        COUNT(DISTINCT statusCode)::BIGINT AS distinct_status_codes,
        COUNT(DISTINCT NULLIF(session_id, ''))::BIGINT AS distinct_session_id,
        COUNT(DISTINCT NULLIF(device_id, ''))::BIGINT AS distinct_device_id,
        ANY_VALUE(reqHost) AS sample_reqHost,
        ANY_VALUE(reqPath) AS sample_reqPath
    FROM resolved
    GROUP BY log_date, source, channel_name
    ORDER BY log_date, source, raw_watch_minutes DESC, row_count DESC, channel_name
    """


def default_out_dir(args: argparse.Namespace, con: duckdb.DuckDBPyConnection) -> Path:
    range_row = con.execute(
        f"""
        SELECT
            MIN(printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER))) AS min_date,
            MAX(printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER))) AS max_date
        FROM read_parquet('{q(args.lake / "**" / "*.parquet")}', hive_partitioning=1, union_by_name=1)
        WHERE cliIP = {sql_text(args.cliip)}
          AND {source_filter(args.source)}
        """
    ).fetchone()
    min_date = range_row[0] if range_row and range_row[0] else "no_dates"
    max_date = range_row[1] if range_row and range_row[1] else "no_dates"
    return DEFAULT_OUT_ROOT / f"raw_cliip_{cliip_slug(args.cliip)}_{args.source or 'all_sources'}_all_available_{min_date}_to_{max_date}"


def copy_query(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO '{q(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(out_path)


def write_manifest(args: argparse.Namespace, out_dir: Path, out_path: Path, stats: dict) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "cliIP": args.cliip,
        "source": args.source or "all",
        "lake": str(args.lake.resolve()),
        "output": str(out_path.resolve()),
        "stats": stats,
    }
    with (out_dir / "date_channel_volume_manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export one cliIP date/channel volume parquet for all available dates.")
    parser.add_argument("--cliip", required=True)
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--source", choices=["stream", "fast"], default=None)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    con = connect(args)
    register_maps(con)
    out_dir = args.out_dir.expanduser().resolve() if args.out_dir else default_out_dir(args, con).resolve()
    out_path = out_dir / "date_channel_volume.parquet"

    count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{q(args.lake / "**" / "*.parquet")}', hive_partitioning=1, union_by_name=1)
        WHERE cliIP = {sql_text(args.cliip)}
          AND {source_filter(args.source)}
        """
    ).fetchone()[0]
    if not count:
        con.close()
        raise SystemExit(f"No rows found for cliIP: {args.cliip}")

    copy_query(con, export_sql(args), out_path)
    stats_row = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            MIN(log_date) AS min_date,
            MAX(log_date) AS max_date,
            COUNT(DISTINCT log_date) AS days,
            COUNT(DISTINCT source) AS sources,
            COUNT(DISTINCT channel_name) AS channels,
            SUM(row_count) AS raw_rows,
            ROUND(SUM(raw_watch_minutes), 3) AS raw_watch_minutes,
            ROUND(SUM(status_200_watch_minutes), 3) AS status_200_watch_minutes
        FROM read_parquet('{q(out_path)}')
        """
    ).fetchdf()
    stats = stats_row.iloc[0].to_dict() if len(stats_row) else {}
    write_manifest(args, out_dir, out_path, stats)
    con.close()

    print(f"date_channel_volume written: {out_path}")
    print(f"Size MB: {out_path.stat().st_size / 1024 / 1024:.2f}")
    print(stats_row.to_string(index=False))


if __name__ == "__main__":
    main()
