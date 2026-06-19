from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from datetime import datetime
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
DEFAULT_OUT_DIR = ETL_ROOT / "output" / "exports" / "raw_parquet"


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def slug(value: str | None, max_len: int = 80) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return (text or "all")[:max_len].strip("_") or "all"


def cliip_slug(cliip: str) -> str:
    clean = slug(cliip, 54)
    digest = hashlib.blake2b(str(cliip).encode("utf-8"), digest_size=5).hexdigest()
    return f"{clean}_{digest}"


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


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


def extension_sql(path_col: str = "reqPath") -> str:
    return f"""
CASE
    WHEN {path_col} IS NULL OR trim({path_col}) = '' THEN '<empty>'
    WHEN lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1)) = '' THEN '<none>'
    ELSE lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1))
END
"""


def device_type_sql(ua_col: str = "UA", platform_col: str = "platform", device_col: str = "device_name") -> str:
    return f"""
CASE
    WHEN lower(coalesce({platform_col}, '')) LIKE '%android_tv%'
      OR lower(coalesce({device_col}, '')) LIKE '%tv%'
      OR lower(coalesce({ua_col}, '')) ~ 'smarttv|hismarttv|bravia|tizen|webos|appletv|firetv|roku|\\btv\\b'
        THEN 'Smart TV'
    WHEN lower(coalesce({ua_col}, '')) LIKE '%android%' THEN 'Android'
    WHEN lower(coalesce({ua_col}, '')) LIKE '%iphone%' THEN 'iPhone'
    WHEN lower(coalesce({ua_col}, '')) LIKE '%ipad%' THEN 'iPad'
    WHEN lower(coalesce({ua_col}, '')) LIKE '%windows%' THEN 'Windows'
    WHEN lower(coalesce({ua_col}, '')) LIKE '%mac os%' OR lower(coalesce({ua_col}, '')) LIKE '%macintosh%' THEN 'Mac'
    WHEN lower(coalesce({ua_col}, '')) LIKE '%linux%' THEN 'Linux'
    ELSE 'Other'
END
"""


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


def checked_date_filter(args: argparse.Namespace) -> str:
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end.")
    if not start_date or not end_date:
        raise SystemExit("Use --start YYYY-MM-DD and --end YYYY-MM-DD.")
    if start_date > end_date:
        raise SystemExit("--start cannot be after --end.")
    return build_partition_filter(start_date, end_date)


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


def base_where(args: argparse.Namespace) -> str:
    return " AND ".join(
        f"({part})"
        for part in [
            checked_date_filter(args),
            source_filter(args.source),
            row_kind_filter(args.row_kind),
        ]
    )


def glob_sql(args: argparse.Namespace) -> str:
    return q(args.lake / "**" / "*.parquet")


def default_list_path(args: argparse.Namespace) -> Path:
    parts = ["cliip_list", args.source or "all_sources", args.row_kind, args.start, "to", args.end]
    return DEFAULT_OUT_DIR / ("_".join(parts) + ".parquet")


def default_export_path(args: argparse.Namespace) -> Path:
    parts = ["raw_cliip", cliip_slug(args.cliip), args.source or "all_sources", args.row_kind, args.start, "to", args.end]
    return DEFAULT_OUT_DIR / ("_".join(parts) + ".parquet")


def list_cliip_sql(args: argparse.Namespace, limit: int | None = None) -> str:
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    return f"""
SELECT
    cliIP,
    COUNT(*) AS rows,
    COUNT(*) FILTER (WHERE lower(reqPath) LIKE '%.ts') AS raw_ts_rows,
    COUNT(*) FILTER (WHERE statusCode = '200' AND lower(reqPath) LIKE '%.ts') AS status_200_ts_rows,
    COUNT(*) FILTER (WHERE lower(reqPath) LIKE '%.m3u8') AS playlist_rows,
    COUNT(*) FILTER (WHERE lower(reqPath) LIKE '%.ts') * {CHUNK_DURATION_HOURS} AS raw_watch_hours,
    COUNT(*) FILTER (WHERE statusCode = '200' AND lower(reqPath) LIKE '%.ts') * {CHUNK_DURATION_HOURS} AS status_200_watch_hours,
    COUNT(DISTINCT lower(reqHost)) AS distinct_hosts,
    COUNT(DISTINCT {query_param_sql("session_id")}) AS distinct_session_id,
    COUNT(DISTINCT {query_param_sql("device_id")}) AS distinct_device_id,
    MIN({ist_timestamp_expr("reqTimeSec")}) AS first_seen_ist,
    MAX({ist_timestamp_expr("reqTimeSec")}) AS last_seen_ist,
    any_value(lower(reqHost)) AS sample_reqHost,
    any_value(reqPath) AS sample_reqPath
FROM read_parquet('{glob_sql(args)}', hive_partitioning=1)
WHERE {base_where(args)}
  AND NULLIF(cliIP, '') IS NOT NULL
GROUP BY 1
ORDER BY rows DESC
{limit_sql}
"""


def export_sql(args: argparse.Namespace) -> str:
    candidate_expr = channel_candidate_sql("reqPath")
    querystr_column = "queryStr," if args.include_querystr else ""
    order_clause = "ORDER BY req_time_ist, reqHost, reqPath" if args.sort else ""
    return f"""
WITH raw AS (
    SELECT
        printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS log_date,
        COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
        {ist_timestamp_expr("reqTimeSec")} AS req_time_ist,
        TRY_CAST(reqTimeSec AS DOUBLE) AS req_time_utc_epoch,
        cliIP,
        UA,
        lower(reqHost) AS reqHost,
        reqPath,
        {querystr_column}
        statusCode,
        {url_decode_text_sql("country")} AS country,
        {url_decode_text_sql("state")} AS state,
        {url_decode_text_sql("city")} AS city,
        asn,
        billingRegion,
        cacheStatus,
        cacheable,
        errorCode,
        startupError,
        cmcd,
        rspContentType,
        totalBytes,
        rspContentLen,
        bytes,
        objSize,
        fileSizeBucket,
        overheadBytes,
        lastByte,
        maxAgeSec,
        serverCountry,
        tlsOverheadTimeMSec,
        tlsVersion,
        transferTimeMSec,
        turnAroundTimeMSec,
        reqEndTimeMSec,
        timeToFirstByte,
        throughput,
        version,
        {candidate_expr} AS candidate_id,
        {query_param_sql("channel")} AS query_channel,
        {query_param_sql("channel_name")} AS query_channel_name,
        {query_param_sql("platform")} AS platform,
        {query_param_sql("device")} AS device_name,
        {query_param_sql("session_id")} AS session_id,
        {query_param_sql("device_id")} AS device_id,
        {query_param_sql("content_title")} AS content_title,
        {query_param_sql("category_name")} AS category_name,
        {query_param_sql("m")} AS m_value,
        lower(reqPath) LIKE '%.ts' AS is_ts,
        lower(reqPath) LIKE '%.m3u8' AS is_playlist,
        {extension_sql("reqPath")} AS extension
    FROM read_parquet('{glob_sql(args)}', hive_partitioning=1)
    WHERE {base_where(args)}
      AND cliIP = {sql_text(args.cliip)}
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
    req_time_ist,
    req_time_utc_epoch,
    channel_name,
    country,
    state,
    city,
    cliIP,
    session_id,
    device_id,
    platform,
    device_name,
    {device_type_sql()} AS device_type,
    UA,
    reqHost,
    candidate_id,
    reqPath,
    extension,
    statusCode,
    is_ts,
    is_playlist,
    CASE WHEN is_ts THEN {CHUNK_DURATION_HOURS} ELSE 0 END AS raw_watch_hours,
    CASE WHEN is_ts AND statusCode = '200' THEN {CHUNK_DURATION_HOURS} ELSE 0 END AS status_200_watch_hours,
    asn,
    billingRegion,
    cacheStatus,
    cacheable,
    errorCode,
    startupError,
    cmcd,
    rspContentType,
    totalBytes,
    rspContentLen,
    bytes,
    objSize,
    fileSizeBucket,
    overheadBytes,
    lastByte,
    maxAgeSec,
    serverCountry,
    tlsOverheadTimeMSec,
    tlsVersion,
    transferTimeMSec,
    turnAroundTimeMSec,
    reqEndTimeMSec,
    timeToFirstByte,
    throughput,
    version,
    query_channel,
    query_channel_name,
    content_title,
    category_name,
    m_value
    {", queryStr" if args.include_querystr else ""}
FROM resolved
{order_clause}
"""


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(args.threads)}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{q(args.temp_dir)}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    return con


def copy_query(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"tmp_{out_path.stem}_{os.getpid()}{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO '{q(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(out_path)


def run_list(args: argparse.Namespace) -> Path:
    con = connect(args)
    out_path = args.out.expanduser().resolve() if args.out else default_list_path(args).resolve()
    copy_query(con, list_cliip_sql(args, args.list_limit), out_path)
    preview = con.execute(
        f"SELECT * FROM read_parquet('{q(out_path)}') ORDER BY rows DESC LIMIT {int(args.top_n)}"
    ).fetchdf()
    con.close()
    print(f"cliIP list written: {out_path}")
    print(f"Size MB: {out_path.stat().st_size / 1024 / 1024:.2f}")
    print(preview.to_string(index=False))
    return out_path


def run_export(args: argparse.Namespace) -> Path:
    con = connect(args)
    register_maps(con)
    count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{glob_sql(args)}', hive_partitioning=1)
        WHERE {base_where(args)}
          AND cliIP = {sql_text(args.cliip)}
        """
    ).fetchone()[0]
    if not count:
        con.close()
        raise SystemExit(
            f"cliIP not found for selected date range/source/row-kind: {args.cliip}. "
            "Run with --list first to see valid cliIP values."
        )

    out_path = args.out.expanduser().resolve() if args.out else default_export_path(args).resolve()
    copy_query(con, export_sql(args), out_path)
    stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            SUM(raw_watch_hours) AS raw_watch_hours,
            SUM(status_200_watch_hours) AS status_200_watch_hours,
            COUNT(DISTINCT channel_name) AS distinct_channels,
            COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
            COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id,
            MIN(req_time_ist) AS first_seen_ist,
            MAX(req_time_ist) AS last_seen_ist
        FROM read_parquet('{q(out_path)}')
        """
    ).fetchdf()
    con.close()
    print(f"Export written: {out_path}")
    print(f"Size MB: {out_path.stat().st_size / 1024 / 1024:.2f}")
    print(stats.to_string(index=False))
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="List cliIP values or export one cliIP's raw rows as parquet.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out", type=Path, help="Output parquet path.")
    parser.add_argument("--start", required=True, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date, YYYY-MM-DD")
    parser.add_argument("--source", choices=["stream", "fast"], help="Optional source filter.")
    parser.add_argument("--cliip", help="Exact cliIP value to export.")
    parser.add_argument("--list", action="store_true", help="Write/list valid cliIP values for the date range.")
    parser.add_argument("--top-n", type=int, default=50, help="Rows to print from the cliIP list.")
    parser.add_argument("--list-limit", type=int, default=100_000, help="Max cliIP rows stored in list parquet.")
    parser.add_argument(
        "--row-kind",
        choices=["all", "media", "watch", "playlist"],
        default="all",
        help="all=all paths, media=.ts+.m3u8, watch=.ts only, playlist=.m3u8 only.",
    )
    parser.add_argument(
        "--include-querystr",
        action="store_true",
        help="Include full queryStr in raw export. Off by default because it can contain token/hdnts values.",
    )
    parser.add_argument("--sort", action="store_true", help="Sort raw output by IST time, host, path. Slower for large exports.")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--temp-dir", type=Path, default=DEFAULT_OUT_DIR / "_duckdb_tmp")
    args = parser.parse_args()

    if args.list:
        run_list(args)
    elif args.cliip:
        run_export(args)
    else:
        raise SystemExit("Use --list to list valid cliIP values, or --cliip <value> to export one cliIP.")


if __name__ == "__main__":
    main()
