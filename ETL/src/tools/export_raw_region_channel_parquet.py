from __future__ import annotations

import argparse
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


def slug(value: str | None) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return text or "all"


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


def blank_geo_filter(column: str) -> str:
    return f"NULLIF(trim(coalesce({column}, '')), '') IS NULL"


def text_geo_filter(column: str, value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"unknown", "unknown / na", "na", "n/a", "<empty>", ""}:
        return blank_geo_filter(column)
    return f"lower(coalesce({column}, '')) = lower({sql_text(value)})"


def raw_scope_where(args: argparse.Namespace, partition_filter: str) -> str:
    country_expr = url_decode_text_sql("country")
    state_expr = url_decode_text_sql("state")
    city_expr = url_decode_text_sql("city")
    clauses = [partition_filter]
    if args.source:
        clauses.append(f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = lower({sql_text(args.source)})")
    if args.country:
        clauses.append(text_geo_filter(country_expr, args.country))
    if args.state:
        clauses.append(text_geo_filter(state_expr, args.state))
    if args.city:
        clauses.append(text_geo_filter(city_expr, args.city))
    if args.row_kind == "watch":
        clauses.append("lower(reqPath) LIKE '%.ts'")
    elif args.row_kind == "playlist":
        clauses.append("lower(reqPath) LIKE '%.m3u8'")
    elif args.row_kind == "media":
        clauses.append("(lower(reqPath) LIKE '%.ts' OR lower(reqPath) LIKE '%.m3u8')")
    return " AND ".join(f"({part})" for part in clauses)


def default_output_path(args: argparse.Namespace) -> Path:
    parts = [
        "raw",
        slug(args.channel),
        slug(args.state),
        slug(args.city) if args.city else "all_cities",
        args.source or "all_sources",
        args.row_kind,
    ]
    if args.start and args.end:
        parts.extend([args.start, "to", args.end])
    else:
        parts.append("all_dates")
    return DEFAULT_OUT_DIR / ("_".join(parts) + ".parquet")


def build_export_sql(args: argparse.Namespace) -> str:
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end, or neither.")
    if start_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end.")

    partition_filter = build_partition_filter(start_date, end_date)
    raw_where = raw_scope_where(args, partition_filter)
    candidate_expr = channel_candidate_sql("reqPath")
    querystr_column = "queryStr," if args.include_querystr else ""
    glob = q(args.lake / "**" / "*.parquet")
    order_clause = "ORDER BY req_time_ist, cliIP, reqPath" if args.sort else ""

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
    FROM read_parquet('{glob}', hive_partitioning=1)
    WHERE {raw_where}
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
WHERE lower(channel_name) = lower({sql_text(args.channel)})
{order_clause}
"""


def write_export(args: argparse.Namespace) -> Path:
    out_path = args.out.expanduser().resolve() if args.out else default_output_path(args).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"tmp_{out_path.stem}_{os.getpid()}{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(args.threads)}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{q(args.temp_dir)}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    register_maps(con)
    sql = build_export_sql(args)
    con.execute(f"COPY ({sql}) TO '{q(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(out_path)

    stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            SUM(raw_watch_hours) AS raw_watch_hours,
            SUM(status_200_watch_hours) AS status_200_watch_hours,
            COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
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
    parser = argparse.ArgumentParser(description="Export raw mapped region/channel rows as parquet.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--start", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", help="End date, YYYY-MM-DD")
    parser.add_argument("--source", choices=["stream", "fast"], help="Optional source filter.")
    parser.add_argument("--country", default="IN", help="Country code filter. Default: IN.")
    parser.add_argument("--state", required=True, help="State/region filter, for example Maharashtra.")
    parser.add_argument("--city", help="Optional city filter. Use 'Unknown / NA' for blank city.")
    parser.add_argument("--channel", required=True, help="Mapped channel name, for example Manorama.")
    parser.add_argument(
        "--row-kind",
        choices=["all", "media", "watch", "playlist"],
        default="media",
        help="media=.ts+.m3u8, watch=.ts only, playlist=.m3u8 only, all=all paths.",
    )
    parser.add_argument(
        "--include-querystr",
        action="store_true",
        help="Include full queryStr. Off by default because it can contain token/hdnts values.",
    )
    parser.add_argument("--sort", action="store_true", help="Sort output by IST time, IP, path. Slower for large exports.")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--temp-dir", type=Path, default=DEFAULT_OUT_DIR / "_duckdb_tmp")
    args = parser.parse_args()
    write_export(args)


if __name__ == "__main__":
    main()
