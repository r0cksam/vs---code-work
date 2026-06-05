#!/usr/bin/env python3
"""Profile UA/queryStr device signals and decode known model codes."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LAKE = ETL_ROOT / "data" / "lake"
DEFAULT_MAP = ETL_ROOT / "config" / "device_decode" / "amazon_fire_tv_models.csv"
DEFAULT_OUT = ETL_ROOT / "output" / "device_decode"
IST_OFFSET_SECONDS = 19_800


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def ist_date_expr(epoch_expr: str) -> str:
    return (
        "CAST(epoch_ms(CAST(FLOOR(("
        f"TRY_CAST({epoch_expr} AS DOUBLE) + {IST_OFFSET_SECONDS}"
        ") * 1000) AS BIGINT)) AS DATE)"
    )


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def date_filter(args: argparse.Namespace) -> str:
    parts = []
    day_expr = ist_date_expr("reqTimeSec")
    if args.start:
        parts.append(f"{day_expr} >= DATE {sql_text(args.start)}")
    if args.end:
        parts.append(f"{day_expr} <= DATE {sql_text(args.end)}")
    return " AND ".join(parts) if parts else "1=1"


def source_filter(source: str | None) -> str:
    if not source:
        return "1=1"
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = lower({sql_text(source)})"


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, int(args.threads))}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={sql_text(q(args.temp_dir))}")
    return con


def base_sql(args: argparse.Namespace) -> str:
    lake_glob = q(args.lake / "**" / "*.parquet")
    map_path = q(args.map)
    where_sql = f"({date_filter(args)}) AND ({source_filter(args.source)})"
    code_regex = "(?i)(AFT[A-Z0-9]{1,12}|B0[A-Z0-9]{8,12})"
    return f"""
    WITH base AS (
        SELECT
            CAST({ist_date_expr("reqTimeSec")} AS VARCHAR) AS log_date,
            COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
            cliIP,
            UA,
            queryStr,
            lower(reqHost) AS reqHost,
            reqPath,
            regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', '') AS statusCode,
            upper(NULLIF(regexp_extract(COALESCE(UA, ''), {sql_text(code_regex)}, 1), '')) AS ua_device_code,
            NULLIF({query_param_sql("platform")}, '') AS platform,
            NULLIF({query_param_sql("device")}, '') AS query_device,
            NULLIF({query_param_sql("device_id")}, '') AS device_id,
            lower(reqPath) LIKE '%.ts' AS is_ts
        FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
        WHERE {where_sql}
          AND (NULLIF(UA, '') IS NOT NULL OR NULLIF(queryStr, '') IS NOT NULL)
    ),
    mapping AS (
        SELECT
            upper(device_code) AS device_code,
            vendor,
            decoded_name,
            product_family,
            generation,
            model_number,
            release_year,
            source AS mapping_source,
            status AS mapping_status,
            notes AS mapping_notes
        FROM read_csv_auto('{map_path}', HEADER=true)
    ),
    detected AS (
        SELECT
            base.*,
            ua_device_code AS device_code,
            CASE
                WHEN ua_device_code IS NOT NULL THEN 'ua_model_code'
                WHEN lower(COALESCE(UA, '')) LIKE '%fire tv%' THEN 'ua_fire_tv_no_code'
                WHEN lower(COALESCE(UA, '')) LIKE '%android%' THEN 'ua_android'
                WHEN lower(COALESCE(UA, '')) LIKE '%iphone%' THEN 'ua_iphone'
                WHEN lower(COALESCE(UA, '')) LIKE '%ipad%' THEN 'ua_ipad'
                WHEN lower(COALESCE(UA, '')) LIKE '%tizen%' THEN 'ua_samsung_tizen'
                WHEN lower(COALESCE(UA, '')) LIKE '%webos%' THEN 'ua_lg_webos'
                WHEN lower(COALESCE(UA, '')) LIKE '%roku%' THEN 'ua_roku'
                WHEN lower(COALESCE(UA, '')) LIKE '%appletv%' THEN 'ua_apple_tv'
                ELSE 'ua_other_or_query_only'
            END AS signal_source
        FROM base
    ),
    resolved AS (
        SELECT
            detected.*,
            mapping.vendor,
            mapping.decoded_name,
            mapping.product_family,
            mapping.generation,
            mapping.model_number,
            mapping.release_year,
            mapping.mapping_source,
            mapping.mapping_status,
            mapping.mapping_notes,
            CASE
                WHEN detected.device_code IS NOT NULL AND mapping.device_code IS NOT NULL THEN 'decoded'
                WHEN detected.device_code IS NOT NULL THEN 'unknown_model_code'
                WHEN detected.signal_source LIKE 'ua_%' THEN 'bucketed_not_model_specific'
                ELSE 'unknown'
            END AS decode_status,
            CASE
                WHEN mapping.device_code IS NOT NULL THEN mapping.decoded_name
                WHEN detected.signal_source = 'ua_fire_tv_no_code' THEN 'Amazon Fire TV (code not found)'
                WHEN detected.signal_source = 'ua_android' THEN 'Android device'
                WHEN detected.signal_source = 'ua_iphone' THEN 'iPhone'
                WHEN detected.signal_source = 'ua_ipad' THEN 'iPad'
                WHEN detected.signal_source = 'ua_samsung_tizen' THEN 'Samsung/Tizen TV'
                WHEN detected.signal_source = 'ua_lg_webos' THEN 'LG/webOS TV'
                WHEN detected.signal_source = 'ua_roku' THEN 'Roku device'
                WHEN detected.signal_source = 'ua_apple_tv' THEN 'Apple TV'
                ELSE 'Unknown / NA'
            END AS decoded_device_name
        FROM detected
        LEFT JOIN mapping ON detected.device_code = mapping.device_code
    )
    SELECT * FROM resolved
    """


def write_copy(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path, fmt: str = "parquet") -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    tmp.unlink(missing_ok=True)
    if fmt == "csv":
        con.execute(f"COPY ({sql}) TO '{q(tmp)}' (HEADER, DELIMITER ',')")
    else:
        con.execute(f"COPY ({sql}) TO '{q(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp.replace(out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Decode UA/queryStr device model signals using local mapping.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", help="Optional IST start date YYYY-MM-DD")
    parser.add_argument("--end", help="Optional IST end date YYYY-MM-DD")
    parser.add_argument("--source", choices=["stream", "fast"], default=None)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.map = args.map.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake not found: {args.lake}")
    if not args.map.exists():
        raise SystemExit(f"Device map not found: {args.map}")

    con = connect(args)
    base = base_sql(args)
    suffix = "_".join(
        part
        for part in [
            args.source or "all_sources",
            args.start or "all_start",
            "to",
            args.end or "all_end",
        ]
        if part
    )
    summary_path = args.out_dir / f"device_decode_summary_{suffix}.parquet"
    unknown_path = args.out_dir / f"unknown_device_codes_{suffix}.csv"
    ua_path = args.out_dir / f"top_user_agents_{suffix}.parquet"
    manifest_path = args.out_dir / f"device_decode_manifest_{suffix}.json"

    summary_sql = f"""
    WITH resolved AS ({base})
    SELECT
        log_date,
        source,
        decode_status,
        signal_source,
        COALESCE(vendor, 'Unknown') AS vendor,
        COALESCE(device_code, '') AS device_code,
        decoded_device_name,
        COALESCE(product_family, '') AS product_family,
        COALESCE(generation, '') AS generation,
        COALESCE(model_number, '') AS model_number,
        COALESCE(CAST(release_year AS VARCHAR), '') AS release_year,
        COALESCE(platform, '') AS platform,
        COALESCE(query_device, '') AS query_device,
        COUNT(*)::BIGINT AS rows,
        SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
        SUM(CASE WHEN statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_rows,
        COUNT(DISTINCT NULLIF(cliIP, ''))::BIGINT AS approx_ips,
        COUNT(DISTINCT NULLIF(device_id, ''))::BIGINT AS distinct_device_ids,
        ANY_VALUE(UA) AS sample_UA,
        ANY_VALUE(reqHost) AS sample_reqHost,
        ANY_VALUE(reqPath) AS sample_reqPath
    FROM resolved
    GROUP BY ALL
    ORDER BY log_date, source, rows DESC
    """
    unknown_sql = f"""
    WITH resolved AS ({base})
    SELECT
        device_code,
        COUNT(*)::BIGINT AS rows,
        SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
        COUNT(DISTINCT NULLIF(cliIP, ''))::BIGINT AS approx_ips,
        MIN(log_date) AS first_date,
        MAX(log_date) AS last_date,
        ANY_VALUE(UA) AS sample_UA,
        ANY_VALUE(reqHost) AS sample_reqHost,
        ANY_VALUE(reqPath) AS sample_reqPath
    FROM resolved
    WHERE decode_status = 'unknown_model_code'
    GROUP BY device_code
    ORDER BY rows DESC
    """
    ua_sql = f"""
    WITH resolved AS ({base})
    SELECT
        source,
        decode_status,
        signal_source,
        COALESCE(device_code, '') AS device_code,
        decoded_device_name,
        UA,
        COUNT(*)::BIGINT AS rows,
        COUNT(DISTINCT NULLIF(cliIP, ''))::BIGINT AS approx_ips,
        MIN(log_date) AS first_date,
        MAX(log_date) AS last_date
    FROM resolved
    WHERE NULLIF(UA, '') IS NOT NULL
    GROUP BY ALL
    ORDER BY rows DESC
    LIMIT 10000
    """

    write_copy(con, summary_sql, summary_path)
    write_copy(con, unknown_sql, unknown_path, fmt="csv")
    write_copy(con, ua_sql, ua_path)

    stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS summary_rows,
            SUM(rows) AS raw_rows,
            SUM(CASE WHEN decode_status = 'decoded' THEN rows ELSE 0 END) AS decoded_rows,
            SUM(CASE WHEN decode_status = 'unknown_model_code' THEN rows ELSE 0 END) AS unknown_code_rows,
            COUNT(DISTINCT device_code) FILTER (WHERE device_code <> '') AS device_codes
        FROM read_parquet('{q(summary_path)}')
        """
    ).fetchdf()
    con.close()

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake": str(args.lake),
        "map": str(args.map),
        "source": args.source or "all",
        "start": args.start,
        "end": args.end,
        "outputs": {
            "summary": str(summary_path),
            "unknown_codes": str(unknown_path),
            "top_user_agents": str(ua_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Summary written : {summary_path}")
    print(f"Unknowns written: {unknown_path}")
    print(f"Top UA written  : {ua_path}")
    print(stats.to_string(index=False))


if __name__ == "__main__":
    main()
