from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from vglive_core import (
    CHUNK_DURATION_HOURS,
    DEFAULT_LAKE_FOLDER,
    HOST_MAP,
    PATH_MAP,
    build_partition_filter,
    channel_candidate_sql,
    profile_querystr_channels,
)


DEFAULT_OUT = Path(
    os.getenv(
        "VG_DASH_PROFILE_DIR",
        str(Path.home() / "Veto Stream Logs" / "vglive_channel_profile" / "deep_profile"),
    )
)
ACTIVE_OUTPUT_FORMAT = "parquet"


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def output_path(path: Path) -> Path:
    suffix = ".parquet" if ACTIVE_OUTPUT_FORMAT == "parquet" else ".csv"
    return path.with_suffix(suffix)


def alternate_output_path(path: Path) -> Path:
    suffix = ".csv" if ACTIVE_OUTPUT_FORMAT == "parquet" else ".parquet"
    return path.with_suffix(suffix)


def write_frame(df: pd.DataFrame, out_file: Path) -> Path:
    actual = output_path(out_file)
    actual.parent.mkdir(parents=True, exist_ok=True)
    if ACTIVE_OUTPUT_FORMAT == "parquet":
        df.to_parquet(actual, index=False, compression="zstd")
    else:
        df.to_csv(actual, index=False)
    print(f"wrote {actual}")
    return actual


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def extension_sql(path_col: str = "reqPath") -> str:
    return f"""
CASE
    WHEN {path_col} IS NULL OR trim({path_col}) = '' THEN '<empty>'
    WHEN lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1)) = '' THEN '<none>'
    ELSE lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1))
END
"""


def quality_sql(path_col: str = "reqPath") -> str:
    return f"""
CASE
    WHEN lower({path_col}) LIKE '%1080%' THEN '1080p'
    WHEN lower({path_col}) LIKE '%720%' THEN '720p'
    WHEN lower({path_col}) LIKE '%540%' THEN '540p'
    WHEN lower({path_col}) LIKE '%504%' THEN '504p'
    WHEN lower({path_col}) LIKE '%480%' THEN '480p'
    WHEN lower({path_col}) LIKE '%360%' THEN '360p'
    WHEN lower({path_col}) LIKE '%.m3u8' THEN 'playlist'
    ELSE 'unknown'
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


def copy_query(con: duckdb.DuckDBPyConnection, sql: str, out_file: Path) -> None:
    actual = output_path(out_file)
    actual.parent.mkdir(parents=True, exist_ok=True)
    if ACTIVE_OUTPUT_FORMAT == "parquet":
        con.execute(f"COPY ({sql}) TO '{q(actual)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    else:
        con.execute(f"COPY ({sql}) TO '{q(actual)}' (HEADER, DELIMITER ',')")
    print(f"wrote {actual}")


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


def write_schema(con: duckdb.DuckDBPyConnection, glob: str, out: Path) -> None:
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=1) LIMIT 0").fetchdf()
    write_frame(df, out / "schema.csv")


def write_file_inventory(lake: Path, out: Path) -> None:
    rows = []
    for file in lake.glob("**/*.parquet"):
        date = ""
        parts = {piece.split("=", 1)[0]: piece.split("=", 1)[1] for piece in file.parts if "=" in piece}
        if {"year", "month", "day"} <= set(parts):
            date = f"{parts['year']}-{parts['month']}-{parts['day']}"
        rows.append(
            {
                "date": date,
                "file": str(file),
                "size_bytes": file.stat().st_size,
                "size_mb": round(file.stat().st_size / 1024 / 1024, 3),
            }
        )
    df = pd.DataFrame(rows).sort_values(["date", "file"])
    write_frame(df, out / "file_inventory.csv")


def write_column_fill(con: duckdb.DuckDBPyConnection, glob: str, out: Path, where_sql: str) -> None:
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=1) LIMIT 0").fetchdf()
    columns = [row["column_name"] for _, row in schema.iterrows()]
    select_parts = ["COUNT(*) AS total_rows"]
    for column in columns:
        escaped = column.replace('"', '""')
        select_parts.append(
            f"SUM(CASE WHEN NULLIF(trim(CAST(\"{escaped}\" AS VARCHAR)), '') IS NOT NULL THEN 1 ELSE 0 END) AS \"{escaped}__non_empty\""
        )
    result = con.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM read_parquet('{glob}', hive_partitioning=1)
        WHERE {where_sql}
        """
    ).fetchone()

    total = int(result[0] or 0)
    rows = []
    for idx, column in enumerate(columns, start=1):
        non_empty = int(result[idx] or 0)
        rows.append(
            {
                "column_name": column,
                "total_rows": total,
                "non_empty_rows": non_empty,
                "empty_rows": total - non_empty,
                "non_empty_pct": round((non_empty / total * 100), 4) if total else 0,
            }
        )
    write_frame(pd.DataFrame(rows).sort_values("non_empty_pct", ascending=False), out / "column_fill_rate.csv")


def write_empty_file(out_file: Path, columns: list[str]) -> None:
    actual = output_path(out_file)
    if not actual.exists():
        write_frame(pd.DataFrame(columns=columns), out_file)


def refresh_artifact(mode: str, out_file: Path, columns: list[str]) -> bool:
    actual = output_path(out_file)
    alternate = alternate_output_path(out_file)
    if mode == "refresh":
        return True
    if mode == "reuse" and actual.exists():
        print(f"reused {actual}")
        return False
    if mode == "reuse" and alternate.exists():
        print(f"reused alternate {alternate}")
        return False
    if mode == "reuse":
        print(f"missing {actual}; skipping expensive refresh")
    else:
        print(f"skipped {actual}")
    write_empty_file(out_file, columns)
    return False


def channel_base_cte(where_sql: str) -> str:
    candidate_expr = channel_candidate_sql("reqPath")
    return f"""
WITH base AS (
    SELECT
        year,
        month,
        day,
        cliIP,
        lower(reqHost) AS reqHost,
        {candidate_expr} AS candidate_id,
        reqPath,
        statusCode,
        reqTimeSec,
        UA,
        queryStr,
        {query_param_sql("platform")} AS platform,
        {query_param_sql("device")} AS device_name
    FROM lake_rows
    WHERE reqPath LIKE '%.ts'
      AND {where_sql}
),
resolved AS (
    SELECT
        b.*,
        COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name
    FROM base b
    LEFT JOIN host_map h ON b.reqHost = h.reqHost
    LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
)
"""


def main() -> None:
    global ACTIVE_OUTPUT_FORMAT
    parser = argparse.ArgumentParser(description="Build deep aggregate profiles for the VgLive lake.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", help="End date, YYYY-MM-DD")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--top-n", type=int, default=1000)
    parser.add_argument(
        "--output-format",
        "--format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Profile artifact format. Parquet is faster/smaller; CSV remains supported for compatibility.",
    )
    parser.add_argument(
        "--column-fill",
        choices=["reuse", "refresh", "skip"],
        default="reuse",
        help="Column fill scans every column across the lake. Default reuses an existing CSV or skips if missing.",
    )
    parser.add_argument(
        "--querystr-profile",
        choices=["reuse", "refresh", "skip"],
        default="reuse",
        help="Detailed queryStr channel QA is expensive. Default reuses an existing CSV or skips if missing.",
    )
    parser.add_argument(
        "--top-values",
        choices=["reuse", "refresh", "skip"],
        default="skip",
        help="queryStr/cmcd top-value extracts are large evidence scans. Default skips them.",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Refresh every expensive evidence file. Use when rebuilding the full forensic profile.",
    )
    args = parser.parse_args()
    ACTIVE_OUTPUT_FORMAT = args.output_format
    if args.full_refresh:
        args.column_fill = "refresh"
        args.querystr_profile = "refresh"
        args.top_values = "refresh"

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end, or neither.")
    if start_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end.")

    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    temp_dir = out / "_duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    glob = q(args.lake / "**" / "*.parquet")
    partition_filter = build_partition_filter(start_date, end_date)
    where_sql = partition_filter
    date_label = "all_dates" if start_date is None else f"{start_date}_to_{end_date}"

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(args.threads)}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    con.execute(f"PRAGMA temp_directory='{q(temp_dir)}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute("PRAGMA enable_progress_bar")

    print(f"Lake: {args.lake}")
    print(f"Out: {out}")
    print(f"Date scope: {date_label}")
    print(f"Threads: {args.threads}")
    print(f"Memory limit: {args.memory_limit}")
    print(f"Temp: {temp_dir}")
    print(f"Output format: {args.output_format}")
    print(f"Column fill: {args.column_fill}")
    print(f"QueryStr profile: {args.querystr_profile}")
    print(f"Top values: {args.top_values}")

    con.execute(f"""
        CREATE OR REPLACE VIEW lake_rows AS
        SELECT * FROM read_parquet('{glob}', hive_partitioning=1)
    """)
    register_maps(con)

    write_schema(con, glob, out)
    write_file_inventory(args.lake, out)
    column_fill_path = out / "column_fill_rate.csv"
    if refresh_artifact(args.column_fill, column_fill_path, ["column_name", "total_rows", "non_empty_rows", "empty_rows", "non_empty_pct"]):
        write_column_fill(con, glob, out, where_sql)

    ext_expr = extension_sql("reqPath")
    quality_expr = quality_sql("reqPath")

    copy_query(
        con,
        f"""
        SELECT
            printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS log_date,
            COUNT(*) AS rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_rows,
            COUNT(*) FILTER (WHERE statusCode <> '200') AS non_200_rows,
            COUNT(*) FILTER (WHERE reqPath LIKE '%.ts') AS raw_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS status_200_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200' AND lower(reqPath) LIKE '%.m3u8') AS m3u8_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            approx_count_distinct(reqHost) AS distinct_hosts,
            SUM(TRY_CAST(totalBytes AS DOUBLE)) AS total_bytes,
            SUM(TRY_CAST(rspContentLen AS DOUBLE)) AS response_content_len
        FROM lake_rows
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
        """,
        out / "daily_volume.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            statusCode,
            COUNT(*) AS rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            any_value(reqPath) AS sample_reqPath
        FROM lake_rows
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY rows DESC
        """,
        out / "status_codes.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            {ext_expr} AS extension,
            COUNT(*) AS rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_rows,
            COUNT(*) FILTER (WHERE statusCode <> '200') AS non_200_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            any_value(reqPath) AS sample_reqPath
        FROM lake_rows
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY rows DESC
        """,
        out / "extensions.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            lower(reqHost) AS reqHost,
            {ext_expr} AS extension,
            COUNT(*) AS rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_rows,
            COUNT(*) FILTER (WHERE statusCode <> '200') AS non_200_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            any_value(reqPath) AS sample_reqPath
        FROM lake_rows
        WHERE {where_sql}
        GROUP BY 1, 2
        ORDER BY rows DESC
        """,
        out / "host_extension.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            lower(reqHost) AS reqHost,
            COUNT(*) AS rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_rows,
            COUNT(*) FILTER (WHERE statusCode <> '200') AS non_200_rows,
            COUNT(*) FILTER (WHERE reqPath LIKE '%.ts') AS raw_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS status_200_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS ts_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            approx_count_distinct(reqPath) AS approx_distinct_paths,
            any_value(reqPath) AS sample_reqPath
        FROM lake_rows
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY rows DESC
        """,
        out / "hosts_overview.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            lower(reqHost) AS reqHost,
            cacheStatus,
            cacheable,
            COUNT(*) AS rows,
            approx_count_distinct(cliIP) AS approx_unique_ips
        FROM lake_rows
        WHERE {where_sql}
        GROUP BY 1, 2, 3
        ORDER BY rows DESC
        """,
        out / "cache_by_host.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            lower(reqHost) AS reqHost,
            statusCode,
            errorCode,
            startupError,
            COUNT(*) AS rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            any_value(reqPath) AS sample_reqPath
        FROM lake_rows
        WHERE statusCode <> '200' AND {where_sql}
        GROUP BY 1, 2, 3, 4
        ORDER BY rows DESC
        LIMIT {int(args.top_n)}
        """,
        out / "errors_by_host.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            lower(reqHost) AS reqHost,
            {ext_expr} AS extension,
            COUNT(*) AS rows,
            approx_quantile(TRY_CAST(timeToFirstByte AS DOUBLE), 0.50) AS ttfb_p50_ms,
            approx_quantile(TRY_CAST(timeToFirstByte AS DOUBLE), 0.95) AS ttfb_p95_ms,
            approx_quantile(TRY_CAST(transferTimeMSec AS DOUBLE), 0.50) AS transfer_p50_ms,
            approx_quantile(TRY_CAST(transferTimeMSec AS DOUBLE), 0.95) AS transfer_p95_ms,
            approx_quantile(TRY_CAST(turnAroundTimeMSec AS DOUBLE), 0.50) AS turnaround_p50_ms,
            approx_quantile(TRY_CAST(turnAroundTimeMSec AS DOUBLE), 0.95) AS turnaround_p95_ms,
            approx_quantile(TRY_CAST(throughput AS DOUBLE), 0.50) AS throughput_p50,
            approx_quantile(TRY_CAST(throughput AS DOUBLE), 0.05) AS throughput_p05,
            AVG(TRY_CAST(totalBytes AS DOUBLE)) AS avg_total_bytes
        FROM lake_rows
        WHERE statusCode = '200' AND {where_sql}
        GROUP BY 1, 2
        ORDER BY rows DESC
        LIMIT {int(args.top_n)}
        """,
        out / "performance_by_host_extension.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            country,
            state,
            city,
            COUNT(*) AS ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            approx_count_distinct(reqHost) AS distinct_hosts
        FROM lake_rows
        WHERE reqPath LIKE '%.ts' AND {where_sql}
        GROUP BY 1, 2, 3
        ORDER BY ts_rows DESC
        LIMIT {int(args.top_n)}
        """,
        out / "geo_top.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            asn,
            COUNT(*) AS ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            approx_count_distinct(reqHost) AS distinct_hosts,
            any_value(reqHost) AS sample_reqHost
        FROM lake_rows
        WHERE reqPath LIKE '%.ts' AND {where_sql}
        GROUP BY 1
        ORDER BY ts_rows DESC
        LIMIT {int(args.top_n)}
        """,
        out / "asn_top.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            UA,
            COUNT(*) AS rows,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            approx_count_distinct(reqHost) AS distinct_hosts
        FROM lake_rows
        WHERE statusCode = '200' AND {where_sql}
        GROUP BY 1
        ORDER BY rows DESC
        LIMIT {int(args.top_n)}
        """,
        out / "ua_top.csv",
    )

    copy_query(
        con,
        f"""
        SELECT
            COUNT(*) AS rows_with_querystr,
            COUNT(*) FILTER (WHERE queryStr LIKE '%channel=%') AS channel_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%channel_name=%') AS channel_name_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%session_id=%') AS session_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%device_id=%') AS device_id_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%platform=%') AS platform_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%device=%') AS device_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%content_title=%') AS content_title_rows,
            COUNT(*) FILTER (WHERE queryStr LIKE '%category_name=%') AS category_name_rows,
            any_value(queryStr) AS sample_queryStr
        FROM lake_rows
        WHERE queryStr IS NOT NULL AND queryStr <> '' AND {where_sql}
        """,
        out / "querystr_param_presence.csv",
    )

    querystr_top_path = out / "querystr_top_values.csv"
    if refresh_artifact(args.top_values, querystr_top_path, ["queryStr", "rows", "approx_unique_ips", "sample_reqPath"]):
        copy_query(
            con,
            f"""
            SELECT
                queryStr,
                COUNT(*) AS rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                any_value(reqPath) AS sample_reqPath
            FROM lake_rows
            WHERE queryStr IS NOT NULL AND queryStr <> '' AND {where_sql}
            GROUP BY 1
            ORDER BY rows DESC
            LIMIT {int(args.top_n)}
            """,
            querystr_top_path,
        )

    copy_query(
        con,
        f"""
        SELECT
            COUNT(*) AS rows_with_cmcd,
            COUNT(*) FILTER (WHERE cmcd LIKE '%br=%') AS br_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%d=%') AS duration_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%mtp=%') AS measured_throughput_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%ot=%') AS object_type_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%sf=%') AS streaming_format_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%sid=%') AS session_id_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%st=%') AS stream_type_rows,
            COUNT(*) FILTER (WHERE cmcd LIKE '%tb=%') AS top_bitrate_rows,
            any_value(cmcd) AS sample_cmcd
        FROM lake_rows
        WHERE cmcd IS NOT NULL AND cmcd <> '' AND {where_sql}
        """,
        out / "cmcd_presence.csv",
    )

    cmcd_top_path = out / "cmcd_top_values.csv"
    if refresh_artifact(args.top_values, cmcd_top_path, ["cmcd", "rows", "sample_reqPath"]):
        copy_query(
            con,
            f"""
            SELECT
                cmcd,
                COUNT(*) AS rows,
                any_value(reqPath) AS sample_reqPath
            FROM lake_rows
            WHERE cmcd IS NOT NULL AND cmcd <> '' AND {where_sql}
            GROUP BY 1
            ORDER BY rows DESC
            LIMIT {int(args.top_n)}
            """,
            cmcd_top_path,
        )

    cte = channel_base_cte(where_sql)
    copy_query(
        con,
        f"""
        {cte}
        SELECT
            channel_name,
            COUNT(*) AS raw_ts_chunks,
            COUNT(*) * {CHUNK_DURATION_HOURS} AS raw_watch_hours,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_chunks,
            COUNT(*) FILTER (WHERE statusCode = '200') * {CHUNK_DURATION_HOURS} AS status_200_watch_hours,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            approx_count_distinct(reqPath) AS approx_distinct_segments,
            approx_count_distinct(reqHost) AS distinct_hosts,
            MIN(to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE))) AS first_seen,
            MAX(to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE))) AS last_seen
        FROM resolved
        GROUP BY 1
        ORDER BY raw_ts_chunks DESC
        """,
        out / "channel_summary.csv",
    )

    copy_query(
        con,
        f"""
        {cte}
        SELECT
            printf('%04d-%02d-%02d', year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS log_date,
            channel_name,
            COUNT(*) AS raw_ts_chunks,
            COUNT(*) * {CHUNK_DURATION_HOURS} AS raw_watch_hours,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_chunks,
            COUNT(*) FILTER (WHERE statusCode = '200') * {CHUNK_DURATION_HOURS} AS status_200_watch_hours,
            approx_count_distinct(cliIP) AS approx_unique_ips
        FROM resolved
        GROUP BY 1, 2
        ORDER BY 1, raw_ts_chunks DESC
        """,
        out / "channel_daily.csv",
    )

    copy_query(
        con,
        f"""
        {cte}
        SELECT
            reqHost,
            candidate_id,
            channel_name,
            {quality_expr} AS quality_bucket,
            COUNT(*) AS raw_ts_chunks,
            COUNT(*) * {CHUNK_DURATION_HOURS} AS raw_watch_hours,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_chunks,
            COUNT(*) FILTER (WHERE statusCode = '200') * {CHUNK_DURATION_HOURS} AS status_200_watch_hours,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            any_value(reqPath) AS sample_reqPath
        FROM resolved
        GROUP BY 1, 2, 3, 4
        ORDER BY raw_ts_chunks DESC
        LIMIT {int(args.top_n)}
        """,
        out / "path_candidate_quality.csv",
    )

    copy_query(
        con,
        f"""
        {cte}
        SELECT
            reqHost,
            candidate_id,
            COUNT(*) AS raw_ts_chunks,
            COUNT(*) * {CHUNK_DURATION_HOURS} AS raw_watch_hours,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_chunks,
            COUNT(*) FILTER (WHERE statusCode = '200') * {CHUNK_DURATION_HOURS} AS status_200_watch_hours,
            approx_count_distinct(cliIP) AS approx_unique_ips,
            any_value(reqPath) AS sample_reqPath
        FROM resolved
        WHERE channel_name = 'Other'
        GROUP BY 1, 2
        ORDER BY raw_ts_chunks DESC
        LIMIT {int(args.top_n)}
        """,
        out / "unmapped_candidates.csv",
    )

    copy_query(
        con,
        f"""
        {cte}
        SELECT
            channel_name,
            {device_type_sql()} AS device_type,
            COUNT(*) AS ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_ts_rows,
            approx_count_distinct(cliIP) AS approx_unique_ips
        FROM resolved
        GROUP BY 1, 2
        ORDER BY ts_rows DESC
        LIMIT {int(args.top_n)}
        """,
        out / "device_type_by_channel.csv",
    )

    querystr_profile_path = out / "querystr_channel_profile.csv"
    querystr_profile_columns = [
        "review_status",
        "pure_channel",
        "raw_channel",
        "mapped_channel",
        "reqHost",
        "candidate_id",
        "requests",
        "sessions",
        "devices",
        "unique_viewers",
        "sample_reqPath",
        "sample_queryStr",
    ]
    if refresh_artifact(args.querystr_profile, querystr_profile_path, querystr_profile_columns):
        querystr_df = profile_querystr_channels(
            lake_path=args.lake,
            start_date=start_date,
            end_date=end_date,
            top_n=max(int(args.top_n), 5000),
            ts_only=False,
        )
        write_frame(querystr_df, querystr_profile_path)

    con.close()
    print("Deep profile complete.")


if __name__ == "__main__":
    main()
