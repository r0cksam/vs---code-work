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
EXCEL_ROW_LIMIT = 1_048_576
DEFAULT_EXPORT_DIR = ETL_ROOT / "output" / "exports"


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").lower()
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


def build_scope_where(args: argparse.Namespace) -> str:
    clauses = []
    if args.source:
        clauses.append(f"lower(source) = lower({sql_text(args.source)})")
    if args.country:
        clauses.append(f"lower(coalesce(country, '')) = lower({sql_text(args.country)})")
    if args.state:
        if args.state.strip().lower() in {"unknown", "unknown / na", "na", "n/a", "<empty>"}:
            clauses.append("nullif(trim(coalesce(state, '')), '') IS NULL")
        else:
            clauses.append(f"lower(coalesce(state, '')) = lower({sql_text(args.state)})")
    if args.city:
        if args.city.strip().lower() in {"unknown", "unknown / na", "na", "n/a", "<empty>"}:
            clauses.append("nullif(trim(coalesce(city, '')), '') IS NULL")
        else:
            clauses.append(f"lower(coalesce(city, '')) = lower({sql_text(args.city)})")
    if args.channel:
        clauses.append(f"lower(channel_name) = lower({sql_text(args.channel)})")
    if args.row_kind == "watch":
        clauses.append("is_ts")
    elif args.row_kind == "playlist":
        clauses.append("is_playlist")
    elif args.row_kind == "media":
        clauses.append("(is_ts OR is_playlist)")
    return " AND ".join(clauses) if clauses else "1=1"


def create_base_view(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end, or neither.")
    if start_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end.")

    partition_filter = build_partition_filter(start_date, end_date)
    glob = q(args.lake / "**" / "*.parquet")
    candidate_expr = channel_candidate_sql("reqPath")

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW detail_base AS
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
                queryStr,
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
                TRY_CAST(totalBytes AS DOUBLE) AS totalBytes,
                TRY_CAST(rspContentLen AS DOUBLE) AS rspContentLen,
                TRY_CAST(timeToFirstByte AS DOUBLE) AS timeToFirstByte,
                TRY_CAST(transferTimeMSec AS DOUBLE) AS transferTimeMSec,
                TRY_CAST(throughput AS DOUBLE) AS throughput,
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
            WHERE {partition_filter}
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
            *,
            {device_type_sql()} AS device_type,
            CASE WHEN is_ts THEN {CHUNK_DURATION_HOURS} ELSE 0 END AS raw_watch_hours,
            CASE WHEN is_ts AND statusCode = '200' THEN {CHUNK_DURATION_HOURS} ELSE 0 END AS status_200_watch_hours
        FROM resolved
        """
    )
    # Materialise the narrow slice once. Keeping this as a view would rescan the
    # full lake for every summary sheet.
    con.execute(f"CREATE OR REPLACE TEMP TABLE scoped_rows AS SELECT * FROM detail_base WHERE {build_scope_where(args)}")


def read_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def string_list(column: str) -> str:
    return f"string_agg(DISTINCT NULLIF({column}, ''), ', ' ORDER BY NULLIF({column}, ''))"


def build_dataframes(con: duckdb.DuckDBPyConnection, raw_limit: int, entity_limit: int, include_querystr: bool) -> dict[str, pd.DataFrame]:
    raw_total = con.execute("SELECT COUNT(*) FROM scoped_rows").fetchone()[0]
    raw_take = min(raw_total, raw_limit, EXCEL_ROW_LIMIT - 1)
    run = lambda sql: read_df(con, sql)

    raw_columns = [
        "log_date",
        "source",
        "req_time_ist",
        "channel_name",
        "country",
        "state",
        "city",
        "cliIP",
        "session_id",
        "device_id",
        "platform",
        "device_name",
        "device_type",
        "UA",
        "reqHost",
        "candidate_id",
        "reqPath",
        "extension",
        "statusCode",
        "is_ts",
        "is_playlist",
        "raw_watch_hours",
        "status_200_watch_hours",
        "asn",
        "billingRegion",
        "cacheStatus",
        "cacheable",
        "errorCode",
        "startupError",
        "rspContentType",
        "totalBytes",
        "rspContentLen",
        "timeToFirstByte",
        "transferTimeMSec",
        "throughput",
        "query_channel",
        "query_channel_name",
        "content_title",
        "category_name",
        "m_value",
        "cmcd",
    ]
    if include_querystr:
        raw_columns.append("queryStr")

    data = {
        "summary": run(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id,
                COUNT(DISTINCT NULLIF(UA, '')) AS distinct_user_agents,
                COUNT(DISTINCT NULLIF(reqPath, '')) AS distinct_reqPath,
                MIN(req_time_ist) AS first_seen_ist,
                MAX(req_time_ist) AS last_seen_ist,
                {raw_total} AS raw_rows_available,
                {raw_take} AS raw_rows_written
            FROM scoped_rows
            """
        ),
        "source_summary": run(
            """
            SELECT
                source,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id
            FROM scoped_rows
            GROUP BY 1
            ORDER BY raw_watch_hours DESC
            """
        ),
        "city_summary": run(
            """
            SELECT
                COALESCE(NULLIF(city, ''), 'Unknown / NA') AS city,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id
            FROM scoped_rows
            GROUP BY 1
            ORDER BY raw_watch_hours DESC
            """
        ),
        "device_type_summary": run(
            """
            SELECT
                device_type,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id
            FROM scoped_rows
            GROUP BY 1
            ORDER BY raw_watch_hours DESC
            """
        ),
        "status_summary": run(
            """
            SELECT
                statusCode,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP
            FROM scoped_rows
            GROUP BY 1
            ORDER BY rows DESC
            """
        ),
        "ip_summary": run(
            f"""
            SELECT
                cliIP,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id,
                COUNT(DISTINCT NULLIF(UA, '')) AS distinct_user_agents,
                {string_list("COALESCE(NULLIF(city, ''), 'Unknown / NA')")} AS cities,
                {string_list("device_type")} AS device_types,
                MIN(req_time_ist) AS first_seen_ist,
                MAX(req_time_ist) AS last_seen_ist,
                any_value(UA) AS sample_UA
            FROM scoped_rows
            WHERE NULLIF(cliIP, '') IS NOT NULL
            GROUP BY 1
            ORDER BY raw_watch_hours DESC, rows DESC
            LIMIT {int(entity_limit)}
            """
        ),
        "session_summary": run(
            f"""
            SELECT
                session_id,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id,
                {string_list("COALESCE(NULLIF(city, ''), 'Unknown / NA')")} AS cities,
                {string_list("device_type")} AS device_types,
                MIN(req_time_ist) AS first_seen_ist,
                MAX(req_time_ist) AS last_seen_ist,
                any_value(cliIP) AS sample_cliIP,
                any_value(device_id) AS sample_device_id,
                any_value(UA) AS sample_UA
            FROM scoped_rows
            WHERE NULLIF(session_id, '') IS NOT NULL
            GROUP BY 1
            ORDER BY raw_watch_hours DESC, rows DESC
            LIMIT {int(entity_limit)}
            """
        ),
        "device_id_summary": run(
            f"""
            SELECT
                device_id,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                {string_list("COALESCE(NULLIF(city, ''), 'Unknown / NA')")} AS cities,
                {string_list("device_type")} AS device_types,
                MIN(req_time_ist) AS first_seen_ist,
                MAX(req_time_ist) AS last_seen_ist,
                any_value(cliIP) AS sample_cliIP,
                any_value(session_id) AS sample_session_id,
                any_value(UA) AS sample_UA
            FROM scoped_rows
            WHERE NULLIF(device_id, '') IS NOT NULL
            GROUP BY 1
            ORDER BY raw_watch_hours DESC, rows DESC
            LIMIT {int(entity_limit)}
            """
        ),
        "ua_summary": run(
            f"""
            SELECT
                UA,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE is_ts) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE is_ts AND statusCode = '200') AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE is_playlist) AS playlist_rows,
                SUM(raw_watch_hours) AS raw_watch_hours,
                SUM(status_200_watch_hours) AS status_200_watch_hours,
                COUNT(DISTINCT NULLIF(cliIP, '')) AS distinct_cliIP,
                COUNT(DISTINCT NULLIF(session_id, '')) AS distinct_session_id,
                COUNT(DISTINCT NULLIF(device_id, '')) AS distinct_device_id
            FROM scoped_rows
            WHERE NULLIF(UA, '') IS NOT NULL
            GROUP BY 1
            ORDER BY raw_watch_hours DESC, rows DESC
            LIMIT {int(entity_limit)}
            """
        ),
        "raw_rows": run(
            f"""
            SELECT {", ".join(raw_columns)}
            FROM scoped_rows
            ORDER BY req_time_ist, cliIP, reqPath
            LIMIT {int(raw_take)}
            """
        ),
    }
    return data


def write_parquet_if_needed(
    con: duckdb.DuckDBPyConnection,
    out_xlsx: Path,
    raw_total: int,
    raw_limit: int,
    include_querystr: bool,
) -> Path | None:
    if raw_total <= raw_limit and raw_total < EXCEL_ROW_LIMIT:
        return None
    raw_cols = "*" if include_querystr else "* EXCLUDE (queryStr)"
    out_parquet = out_xlsx.with_suffix(".raw_rows.parquet")
    con.execute(
        f"""
        COPY (
            SELECT {raw_cols}
            FROM scoped_rows
            ORDER BY req_time_ist, cliIP, reqPath
        ) TO '{q(out_parquet)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return out_parquet


def write_excel(data: dict[str, pd.DataFrame], out_xlsx: Path) -> None:
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for sheet, df in data.items():
            safe_sheet = sheet[:31]
            df.to_excel(writer, sheet_name=safe_sheet, index=False)
            worksheet = writer.sheets[safe_sheet]
            worksheet.freeze_panes = "A2"
            for idx, column in enumerate(df.columns, start=1):
                sample = [str(column)] + [str(v) for v in df[column].head(50).fillna("").tolist()]
                width = min(max(len(v) for v in sample) + 2, 60)
                worksheet.column_dimensions[worksheet.cell(row=1, column=idx).column_letter].width = width


def default_output(args: argparse.Namespace) -> Path:
    parts = [
        "watch_detail",
        slug(args.channel or "all_channels"),
        slug(args.state or "all_states"),
    ]
    if args.city:
        parts.append(slug(args.city))
    if args.start and args.end:
        parts.extend([args.start, "to", args.end])
    parts.append(datetime.now().strftime("%Y%m%d_%H%M%S"))
    return DEFAULT_EXPORT_DIR / ("_".join(parts) + ".xlsx")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export detailed region/channel watch-hour evidence to Excel.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--start", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", help="End date, YYYY-MM-DD")
    parser.add_argument("--source", choices=["stream", "fast"], help="Optional source filter.")
    parser.add_argument("--country", default="IN", help="Country code filter. Default: IN.")
    parser.add_argument("--state", required=True, help="State/region filter. Use 'Unknown / NA' for blank state.")
    parser.add_argument("--city", help="Optional city filter. Use 'Unknown / NA' for blank city.")
    parser.add_argument("--channel", required=True, help="Mapped channel name, for example '9XM Jhakaas'.")
    parser.add_argument(
        "--row-kind",
        choices=["all", "media", "watch", "playlist"],
        default="media",
        help="Rows to export: media=.ts+.m3u8, watch=.ts only, playlist=.m3u8 only, all=all paths.",
    )
    parser.add_argument("--raw-limit", type=int, default=1_000_000, help="Max raw rows written into Excel.")
    parser.add_argument("--entity-limit", type=int, default=100_000, help="Max rows for IP/session/device/UA summary sheets.")
    parser.add_argument(
        "--include-querystr",
        action="store_true",
        help="Include full queryStr in raw_rows. Off by default because it can contain token/hdnts values.",
    )
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--temp-dir", type=Path, default=DEFAULT_EXPORT_DIR / "_duckdb_tmp")
    args = parser.parse_args()

    out_xlsx = args.out.expanduser().resolve() if args.out else default_output(args).resolve()
    args.temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(args.threads)}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    con.execute(f"PRAGMA temp_directory='{q(args.temp_dir)}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    register_maps(con)
    create_base_view(con, args)

    data = build_dataframes(
        con,
        raw_limit=max(0, int(args.raw_limit)),
        entity_limit=max(1, int(args.entity_limit)),
        include_querystr=bool(args.include_querystr),
    )
    raw_total = int(data["summary"].iloc[0]["raw_rows_available"] or 0)
    raw_parquet = write_parquet_if_needed(con, out_xlsx, raw_total, int(args.raw_limit), bool(args.include_querystr))
    if raw_parquet:
        data["summary"]["full_raw_parquet"] = str(raw_parquet)

    write_excel(data, out_xlsx)
    con.close()

    print(f"Export written: {out_xlsx}")
    print(f"Raw rows available: {raw_total:,}")
    print(f"Raw rows in Excel: {int(data['summary'].iloc[0]['raw_rows_written']):,}")
    if raw_parquet:
        print(f"Full raw parquet: {raw_parquet}")


if __name__ == "__main__":
    main()
