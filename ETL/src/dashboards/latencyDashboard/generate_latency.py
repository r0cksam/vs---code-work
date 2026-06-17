#!/usr/bin/env python3
"""Generate the Veto Latency dashboard from the parquet lake."""

from __future__ import annotations

import argparse
import json
import logging  # FIX-11
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable  # FIX-7
from zoneinfo import ZoneInfo  # FIX-4

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)  # FIX-11


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
PROFILE_ROOT = SRC_ROOT / "profile"
# Expected package layout: ETL/src/common and ETL/src/profile must be importable beside ETL/src/dashboards. # FIX-2
# Preferred execution is package mode; direct script execution uses importlib without mutating sys.path. # FIX-2
if __package__:  # FIX-2
    from ...common.chartjs import load_chartjs  # type: ignore[import-not-found] # FIX-2
    from ...common.render import json_blob, render_template  # type: ignore[import-not-found] # FIX-2
    from ...common.source_ranges import true_source_ranges_from_lake  # type: ignore[import-not-found] # FIX-2
    from ...profile.vglive_core import (  # type: ignore[import-not-found] # FIX-2
        DEFAULT_LAKE_FOLDER,  # FIX-2
        HOST_MAP,  # FIX-2
        PATH_MAP,  # FIX-2
        build_partition_filter,  # FIX-2
        channel_candidate_sql,  # FIX-2
    )  # FIX-2
else:  # FIX-2
    import importlib.util  # FIX-2

    def _load_module(module_name: str, path: Path):  # FIX-2
        spec = importlib.util.spec_from_file_location(module_name, path)  # FIX-2
        if spec is None or spec.loader is None:  # FIX-2
            raise ImportError(f"Could not load {module_name} from {path}")  # FIX-2
        module = importlib.util.module_from_spec(spec)  # FIX-2
        sys.modules[module_name] = module  # FIX-2
        spec.loader.exec_module(module)  # FIX-2
        return module  # FIX-2

    _chartjs_module = _load_module("veto_common_chartjs_latency", SRC_ROOT / "common" / "chartjs.py")  # FIX-2
    _render_module = _load_module("veto_common_render_latency", SRC_ROOT / "common" / "render.py")  # FIX-2
    _source_ranges_module = _load_module("veto_common_source_ranges_latency", SRC_ROOT / "common" / "source_ranges.py")  # FIX-2
    _core_module = _load_module("veto_profile_vglive_core_latency", PROFILE_ROOT / "vglive_core.py")  # FIX-2
    load_chartjs = _chartjs_module.load_chartjs  # FIX-2
    json_blob = _render_module.json_blob  # FIX-2
    render_template = _render_module.render_template  # FIX-2
    true_source_ranges_from_lake = _source_ranges_module.true_source_ranges_from_lake  # FIX-2
    DEFAULT_LAKE_FOLDER = _core_module.DEFAULT_LAKE_FOLDER  # FIX-2
    HOST_MAP = _core_module.HOST_MAP  # FIX-2
    PATH_MAP = _core_module.PATH_MAP  # FIX-2
    build_partition_filter = _core_module.build_partition_filter  # FIX-2
    channel_candidate_sql = _core_module.channel_candidate_sql  # FIX-2


DEFAULT_OUT = Path(
    os.getenv(
        "VG_LATENCY_HTML",
        str(ETL_ROOT / "output" / "latency" / "veto_latency.html"),
    )
)
DEFAULT_PROFILE_OUT = Path(
    os.getenv(
        "VG_LATENCY_PROFILE_DIR",
        str(ETL_ROOT / "output" / "latency" / "profile"),
    )
)
CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"
EXPORT_FLOAT_PRECISION = 6  # 6dp is sufficient for ms-level latency values # FIX-8
IST_ZONE = ZoneInfo("Asia/Kolkata")  # FIX-4
# IST is used because all log timestamps are stored in Indian Standard Time. # FIX-4
SAFE_SOURCE_VALUES = {"all", "fast", "stream"}  # FIX-1
SAFE_SQL_COLUMNS = {  # FIX-1
    "reqPath",  # FIX-1
    "reqTimeSec",  # FIX-1
    "source",  # FIX-1
    "r.source",  # FIX-1
    "reqHost",  # FIX-1
    "r.reqHost",  # FIX-1
    "country",  # FIX-1
    "state",  # FIX-1
    "city",  # FIX-1
}  # FIX-1
STATUS_CODE_MEANINGS = {
    "000": "No HTTP response code logged; often an aborted or incomplete request.",
    "200": "OK: request succeeded.",
    "206": "Partial Content: byte-range response, common for media delivery.",
    "301": "Moved Permanently: permanent redirect.",
    "302": "Found: temporary redirect.",
    "304": "Not Modified: cache validation response.",
    "400": "Bad Request: invalid request.",
    "401": "Unauthorized: authentication required or failed.",
    "403": "Forbidden: access refused.",
    "404": "Not Found: object was unavailable.",
    "408": "Request Timeout: client did not complete request in time.",
    "429": "Too Many Requests: rate limiting or throttling.",
    "500": "Internal Server Error: origin/server-side failure.",
    "502": "Bad Gateway: invalid upstream response.",
    "503": "Service Unavailable: upstream temporarily unavailable.",
    "504": "Gateway Timeout: upstream did not respond in time.",
}


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def assert_safe_source(source: str) -> str:  # FIX-1
    if source not in SAFE_SOURCE_VALUES:  # FIX-1
        raise ValueError(f"Unsafe source value for SQL: {source!r}")  # FIX-1
    return source  # FIX-1


def assert_safe_sql_column(column: str) -> str:  # FIX-1
    if column not in SAFE_SQL_COLUMNS:  # FIX-1
        raise ValueError(f"Unsafe SQL column expression: {column!r}")  # FIX-1
    return column  # FIX-1


def ist_offset_seconds() -> int:  # FIX-4
    offset = datetime.now(IST_ZONE).utcoffset()  # FIX-4
    if offset is None:  # FIX-4
        raise RuntimeError("Asia/Kolkata UTC offset is unavailable.")  # FIX-4
    return int(offset.total_seconds())  # FIX-4


def execute_or_dryrun(dry_run_msg: str, write_fn: Callable[[], None], dry_run: bool):  # FIX-N2
    if dry_run:  # FIX-7
        logger.warning(dry_run_msg)  # FIX-N2
        return False  # FIX-7
    write_fn()  # FIX-7
    return True  # FIX-7


def validate_lake_path(lake: Path) -> None:  # FIX-3
    # VG_LAKE_BASE_ROOT can allow lake paths outside the default ETL tree. # FIX-N4
    expected_base = Path(os.getenv("VG_LAKE_BASE_ROOT", str(ETL_ROOT))).expanduser().resolve()  # FIX-N4
    if not lake.exists() or not lake.is_dir():  # FIX-3
        raise ValueError(f"Lake folder not found or not a directory: {lake}")  # FIX-3
    if not lake.is_relative_to(expected_base):  # FIX-3
        raise ValueError(f"Lake folder escapes expected ETL root {expected_base}: {lake}")  # FIX-3


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def latest_lake_day(lake_root: Path) -> date | None:
    days: list[date] = []
    for path in lake_root.glob("**/day=*"):
        try:
            parts = {
                piece.split("=", 1)[0]: piece.split("=", 1)[1]
                for piece in path.parts
                if "=" in piece
            }
            days.append(date(int(parts["year"]), int(parts["month"]), int(parts["day"])))
        except (KeyError, ValueError, IndexError):
            continue
    return max(days) if days else None


def checked_dates(args: argparse.Namespace) -> tuple[date | None, date | None]:
    start = parse_date(args.start)
    end = parse_date(args.end)
    if (start is None) != (end is None):
        raise ValueError("Use both --start and --end, or neither.")  # FIX-10
    if start and end and start > end:
        raise ValueError("--start cannot be after --end.")  # FIX-10
    if not start and not end and args.window_days and args.window_days > 0:
        latest = latest_lake_day(args.lake)
        if latest:
            end = latest
            start = latest - timedelta(days=args.window_days - 1)
    return start, end


def source_filter_sql(source: str) -> str:
    source = assert_safe_source(source)  # FIX-1
    if source == "all":
        return "1=1"
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = {sql_text(source)}"


def extension_sql(path_col: str = "reqPath") -> str:
    path_col = assert_safe_sql_column(path_col)  # FIX-1
    return f"""
CASE
    WHEN {path_col} IS NULL OR trim({path_col}) = '' THEN '<empty>'
    WHEN lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1)) = '' THEN '<none>'
    ELSE lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1))
END
"""


def ist_timestamp_expr(epoch_expr: str = "reqTimeSec") -> str:
    epoch_expr = assert_safe_sql_column(epoch_expr)  # FIX-1
    offset_seconds = ist_offset_seconds()  # FIX-4
    return (
        "epoch_ms(CAST(FLOOR(("
        f"TRY_CAST({epoch_expr} AS DOUBLE) + {offset_seconds}"  # FIX-4
        ") * 1000) AS BIGINT))"
    )


def clean_text_sql(column_expr: str) -> str:
    return f"NULLIF(trim(COALESCE(CAST({column_expr} AS VARCHAR), '')), '')"


# FIX-9: DuckDB try() intentionally suppresses URL decode errors here.
# FIX-9: The fallback chain url_decoded -> raw string -> 'Unknown / NA' is deliberate.
# FIX-9: Malformed encodings should not crash the pipeline; data quality issues surface as 'Unknown / NA'.
def decoded_text_sql(column_expr: str) -> str:
    return (
        f"COALESCE(NULLIF(trim(try(url_decode(CAST({column_expr} AS VARCHAR)))), ''), "
        f"NULLIF(trim(CAST({column_expr} AS VARCHAR)), ''), 'Unknown / NA')"
    )


def platform_name_sql(source_expr: str = "source", host_expr: str = "reqHost") -> str:
    source_expr = assert_safe_sql_column(source_expr)  # FIX-1
    host_expr = assert_safe_sql_column(host_expr)  # FIX-1
    host = f"lower(COALESCE({host_expr}, ''))"
    src = f"lower(COALESCE({source_expr}, 'stream'))"
    return f"""
CASE
    WHEN {src} = 'fast' AND ({host} LIKE '%indiatv-samsung%' OR {host} LIKE '%veto-samsung%')
        THEN 'Samsung TV Plus - IN'
    WHEN {src} = 'fast' AND {host} LIKE '%indiatv-tcl%' THEN 'TCL'
    WHEN {src} = 'fast' AND {host} LIKE '%indiatv-cloudtv%' THEN 'CloudTV'
    WHEN {src} = 'fast' AND {host} LIKE '%indiatv-vi%' THEN 'Vi Movies & TV'
    WHEN {host} = '' THEN 'Unknown Host'
    ELSE regexp_replace({host}, '\\.akamaized\\.net$', '')
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


def metric_sql() -> str:
    return """
    COUNT(*)::BIGINT AS rows,
    COUNT(*) FILTER (WHERE status_code = '200')::BIGINT AS status_200_rows,
    COUNT(*) FILTER (WHERE status_code <> '200')::BIGINT AS non_200_rows,
    COUNT(*) FILTER (WHERE status_code LIKE '5%')::BIGINT AS status_5xx_rows,
    approx_count_distinct(cliIP)::BIGINT AS approx_unique_ips,
    COUNT(*) FILTER (WHERE cacheStatus = '1')::BIGINT AS cache_hit_rows,
    COUNT(*) FILTER (WHERE timeToFirstByte_ms IS NOT NULL)::BIGINT AS ttfb_rows,
    approx_quantile(timeToFirstByte_ms, 0.50) AS ttfb_p50_ms,
    approx_quantile(timeToFirstByte_ms, 0.95) AS ttfb_p95_ms,
    approx_quantile(timeToFirstByte_ms, 0.99) AS ttfb_p99_ms,
    approx_quantile(turnAroundTime_ms, 0.50) AS turnaround_p50_ms,
    approx_quantile(turnAroundTime_ms, 0.95) AS turnaround_p95_ms,
    approx_quantile(turnAroundTime_ms, 0.99) AS turnaround_p99_ms,
    approx_quantile(transferTime_ms, 0.50) AS transfer_p50_ms,
    approx_quantile(transferTime_ms, 0.95) AS transfer_p95_ms,
    approx_quantile(throughput_value, 0.05) AS throughput_p05,
    approx_quantile(throughput_value, 0.50) AS throughput_p50,
    approx_quantile(tlsOverhead_ms, 0.95) AS tls_overhead_p95_ms,
    AVG(totalBytes_value) AS avg_total_bytes
    """


def write_frame(df: pd.DataFrame, path: Path, dry_run: bool) -> None:
    def _write(df=df, path=path) -> None:  # FIX-M1
        path.parent.mkdir(parents=True, exist_ok=True)  # FIX-7
        tmp = path.with_name(f"tmp_{path.stem}_{os.getpid()}{path.suffix}")  # FIX-7
        tmp.unlink(missing_ok=True)  # FIX-7
        df.to_parquet(tmp, index=False, compression="zstd")  # FIX-7
        tmp.replace(path)  # FIX-7
        logger.info(f"wrote {path} ({len(df):,} rows)")  # FIX-11

    execute_or_dryrun(f"[dry-run] would write {len(df):,} rows -> {path}", _write, dry_run)  # FIX-7


def fetch_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:  # FIX-5
        return con.execute(sql).fetchdf()  # FIX-5
    except duckdb.Error as exc:  # FIX-5
        safe_sql = sql[:200].replace("\n", " ")  # FIX-5
        safe_description = f"{type(exc).__name__}: {safe_sql}"  # FIX-5
        logger.error("DuckDB fetch_df failed: %s", safe_description)  # FIX-5
        raise RuntimeError(f"fetch_df failed: {safe_description}") from exc  # FIX-5


def build_tables(args: argparse.Namespace) -> dict[str, pd.DataFrame]:
    start, end = checked_dates(args)
    glob = q(args.lake / "**" / "*.parquet")
    partition_filter = build_partition_filter(start, end) if start and end else "1=1"
    source_filter = source_filter_sql(args.source)
    candidate_expr = channel_candidate_sql(assert_safe_sql_column("reqPath"))  # FIX-1
    country_expr = decoded_text_sql(assert_safe_sql_column("country"))  # FIX-1
    state_expr = decoded_text_sql(assert_safe_sql_column("state"))  # FIX-1
    city_expr = decoded_text_sql(assert_safe_sql_column("city"))  # FIX-1

    con = None  # FIX-6
    try:
        con = connect(args)  # FIX-6
        logger.info("Creating latency base table...")  # FIX-11
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE latency_base AS
            WITH raw AS (
                SELECT
                    COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
                    strftime({ist_timestamp_expr("reqTimeSec")}, '%Y-%m-%d') AS log_date,
                    CAST(strftime({ist_timestamp_expr("reqTimeSec")}, '%H') AS INTEGER) AS hour_ist,
                    lower(COALESCE(reqHost, '')) AS reqHost,
                    {candidate_expr} AS candidate_id,
                    COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', ''), ''), '000') AS status_code,
                    {extension_sql("reqPath")} AS extension,
                    {country_expr} AS country,
                    {state_expr} AS state,
                    {city_expr} AS city,
                    COALESCE(NULLIF(CAST(cacheStatus AS VARCHAR), ''), 'Unknown') AS cacheStatus,
                    COALESCE(NULLIF(CAST(cacheable AS VARCHAR), ''), 'Unknown') AS cacheable,
                    NULLIF(CAST(cliIP AS VARCHAR), '') AS cliIP,
                    TRY_CAST(timeToFirstByte AS DOUBLE) AS timeToFirstByte_ms,
                    TRY_CAST(turnAroundTimeMSec AS DOUBLE) AS turnAroundTime_ms,
                    TRY_CAST(transferTimeMSec AS DOUBLE) AS transferTime_ms,
                    TRY_CAST(throughput AS DOUBLE) AS throughput_value,
                    TRY_CAST(tlsOverheadTimeMSec AS DOUBLE) AS tlsOverhead_ms,
                    TRY_CAST(totalBytes AS DOUBLE) AS totalBytes_value,
                    TRY_CAST(reqTimeSec AS DOUBLE) AS req_epoch
                FROM read_parquet('{glob}', hive_partitioning=1, union_by_name=1)
                WHERE ({partition_filter})
                  AND ({source_filter})
                  AND TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
                  AND lower(COALESCE(reqPath, '')) LIKE '%.%'
                  AND (
                    TRY_CAST(timeToFirstByte AS DOUBLE) IS NOT NULL
                    OR TRY_CAST(turnAroundTimeMSec AS DOUBLE) IS NOT NULL
                    OR TRY_CAST(transferTimeMSec AS DOUBLE) IS NOT NULL
                  )
            )
            SELECT
                r.*,
                COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name,
                {platform_name_sql("r.source", "r.reqHost")} AS platform_name
            FROM raw r
            LEFT JOIN host_map h ON r.reqHost = h.reqHost
            LEFT JOIN path_map p ON r.candidate_id = p.candidate_id
            """
        )

        tables: dict[str, pd.DataFrame] = {}
        tables["daily"] = fetch_df(
            con,
            f"""
            SELECT log_date, source, extension, {metric_sql()}
            FROM latency_base
            GROUP BY 1,2,3
            ORDER BY log_date, source, extension
            """,
        )
        tables["hourly"] = fetch_df(
            con,
            f"""
            SELECT log_date, hour_ist, source, extension, {metric_sql()}
            FROM latency_base
            GROUP BY 1,2,3,4
            ORDER BY log_date, hour_ist, source, extension
            """,
        )
        tables["channel_daily"] = fetch_df(
            con,
            f"""
            SELECT log_date, source, extension, channel_name, {metric_sql()}
            FROM latency_base
            GROUP BY 1,2,3,4
            ORDER BY log_date, source, extension, rows DESC
            """,
        )
        tables["host_daily"] = fetch_df(
            con,
            f"""
            SELECT log_date, source, extension, platform_name, reqHost, {metric_sql()}
            FROM latency_base
            GROUP BY 1,2,3,4,5
            ORDER BY log_date, source, extension, rows DESC
            """,
        )
        tables["status_daily"] = fetch_df(
            con,
            f"""
            SELECT log_date, source, extension, status_code, {metric_sql()}
            FROM latency_base
            GROUP BY 1,2,3,4
            ORDER BY log_date, source, extension, status_code
            """,
        )
        tables["cache_daily"] = fetch_df(
            con,
            f"""
            SELECT log_date, source, extension, cacheStatus, cacheable, {metric_sql()}
            FROM latency_base
            GROUP BY 1,2,3,4,5
            ORDER BY log_date, source, extension, rows DESC
            """,
        )
        tables["geo_daily"] = fetch_df(
            con,
            f"""
            WITH top_geo AS (
                SELECT country, state, city, COUNT(*) AS rows
                FROM latency_base
                GROUP BY 1,2,3
                ORDER BY rows DESC
                LIMIT {int(args.top_n)}
            )
            SELECT b.log_date, b.source, b.extension, b.country, b.state, b.city, {metric_sql()}
            FROM latency_base b
            INNER JOIN top_geo g USING (country, state, city)
            GROUP BY 1,2,3,4,5,6
            ORDER BY log_date, source, extension, rows DESC
            """,
        )
        tables["summary"] = fetch_df(
            con,
            f"""
            SELECT
                MIN(log_date) AS first_date,
                MAX(log_date) AS last_date,
                MIN(req_epoch) AS first_epoch,
                MAX(req_epoch) AS last_epoch,
                COUNT(*)::BIGINT AS rows,
                COUNT(*) FILTER (WHERE lower(extension) = 'ts')::BIGINT AS ts_rows,
                COUNT(*) FILTER (WHERE lower(extension) = 'm3u8')::BIGINT AS playlist_rows,
                COUNT(DISTINCT source)::BIGINT AS sources,
                COUNT(DISTINCT channel_name)::BIGINT AS channels,
                COUNT(DISTINCT reqHost)::BIGINT AS hosts,
                COUNT(*) FILTER (WHERE timeToFirstByte_ms IS NOT NULL)::BIGINT AS ttfb_rows,
                approx_quantile(timeToFirstByte_ms, 0.95) AS ttfb_p95_ms,
                approx_quantile(turnAroundTime_ms, 0.95) AS turnaround_p95_ms,
                approx_quantile(transferTime_ms, 0.95) AS transfer_p95_ms,
                approx_quantile(throughput_value, 0.05) AS throughput_p05
            FROM latency_base
            """,
        )
        return tables
    finally:
        if con is not None:  # FIX-6
            con.close()  # FIX-6


def read_profile_table(profile_dir: Path, name: str) -> pd.DataFrame:
    path = profile_dir / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Latency profile table not found: {path}")  # FIX-N3
    return pd.read_parquet(path)


def load_tables_from_profile(profile_dir: Path) -> dict[str, pd.DataFrame]:
    names = [
        "daily",
        "hourly",
        "channel_daily",
        "host_daily",
        "status_daily",
        "cache_daily",
        "geo_daily",
    ]
    tables = {name: read_profile_table(profile_dir, name) for name in names}
    summary_path = profile_dir / "summary.parquet"
    if summary_path.exists():
        tables["summary"] = pd.read_parquet(summary_path)
    else:
        raise FileNotFoundError(  # FIX-12
            f"summary.parquet not found in profile dir: {profile_dir}. "  # FIX-12
            "Re-run without --from-profile to regenerate all profile tables."  # FIX-12
        )  # FIX-12
    return tables


def clean_records(df: pd.DataFrame, columns: list[str] | None = None) -> list[dict]:
    if df.empty:
        return []
    out = df.copy()
    if columns:
        out = out[[col for col in columns if col in out.columns]]
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].round(EXPORT_FLOAT_PRECISION)  # FIX-8
        elif pd.api.types.is_integer_dtype(out[col]):
            continue
        else:
            out[col] = out[col].fillna("").astype(str)
    return json.loads(out.to_json(orient="records"))


def build_payload(tables: dict[str, pd.DataFrame], args: argparse.Namespace) -> dict:
    summary = clean_records(tables.get("summary", pd.DataFrame()))
    stats = summary[0] if summary else {}
    source_dates: dict[str, list[str]] = {}
    daily = tables.get("daily", pd.DataFrame())
    if not daily.empty and {"source", "log_date"}.issubset(daily.columns):
        for source, group in daily.groupby(daily["source"].fillna("").astype(str).str.lower()):
            source_dates[source] = sorted(group["log_date"].dropna().astype(str).unique().tolist())
    source_true_ranges = true_source_ranges_from_lake(source_dates, args.lake)
    return {
        "title": args.title,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lake": str(args.lake),
        "stats": stats,
        "source_true_ranges": source_true_ranges,
        "status_meanings": STATUS_CODE_MEANINGS,
        "notes": {
            "primary_metric": "timeToFirstByte is the primary CDN latency metric.",
            "range_quantiles": "Static HTML re-aggregates daily quantile summaries with row-weighted values for selected ranges; daily trend points are generated directly from lake rows.",
            "extensions": "Segment (.ts) and playlist (.m3u8) latency should be reviewed separately.",
        },
        "daily": clean_records(tables["daily"]),
        "hourly": clean_records(tables["hourly"]),
        "channel_daily": clean_records(tables["channel_daily"]),
        "host_daily": clean_records(tables["host_daily"]),
        "status_daily": clean_records(tables["status_daily"]),
        "cache_daily": clean_records(tables["cache_daily"]),
        "geo_daily": clean_records(tables["geo_daily"]),
    }


def main() -> None:
    logging.basicConfig(  # FIX-11
        level=logging.INFO,  # FIX-11
        format="%(asctime)s %(levelname)s %(message)s",  # FIX-11
        datefmt="%Y-%m-%d %H:%M:%S",  # FIX-11
    )  # FIX-11
    parser = argparse.ArgumentParser(description="Generate Veto Latency HTML dashboard.")
    parser.add_argument("--lake", type=Path, default=Path(os.getenv("VG_ETL_LAKE_ROOT", str(DEFAULT_LAKE_FOLDER))))
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--profile-out", type=Path, default=DEFAULT_PROFILE_OUT)
    parser.add_argument("--title", default="Veto Latency")
    parser.add_argument("--start", default=None, help="IST start date YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="IST end date YYYY-MM-DD.")
    parser.add_argument("--window-days", type=int, default=0, help="Use last N lake days when --start/--end are absent.")
    parser.add_argument("--source", choices=["all", "fast", "stream"], default="all")
    parser.add_argument("--from-profile", action="store_true", help="Render HTML from existing latency profile parquet tables.")
    parser.add_argument("--top-n", type=int, default=500, help="Top geographies retained in daily latency table.")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out = args.out.expanduser().resolve()
    args.profile_out = args.profile_out.expanduser().resolve()
    args.source = assert_safe_source(args.source)  # FIX-1
    try:  # FIX-3
        validate_lake_path(args.lake)  # FIX-3
    except ValueError as exc:  # FIX-3
        raise SystemExit(str(exc)) from exc  # FIX-3

    if args.from_profile:
        logger.info(f"Rendering latency dashboard from profile: {args.profile_out}")  # FIX-11
        try:  # FIX-N3
            tables = load_tables_from_profile(args.profile_out)  # FIX-N3
        except FileNotFoundError as e:  # FIX-N3
            raise SystemExit(str(e)) from e  # FIX-N3
    else:
        logger.info(f"Building latency dashboard from: {args.lake}")  # FIX-11
        try:  # FIX-10
            start, end = checked_dates(args)  # FIX-10
        except ValueError as e:  # FIX-10
            raise SystemExit(str(e)) from e  # FIX-10
        if start and end:
            logger.info(f"Date scope: {start.isoformat()} -> {end.isoformat()}")  # FIX-11
        else:
            logger.info("Date scope: all available lake partitions")  # FIX-11
        logger.info(f"Source scope: {args.source}")  # FIX-11
        tables = build_tables(args)
        for name, frame in tables.items():
            write_frame(frame, args.profile_out / f"{name}.parquet", args.dry_run)

        manifest = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "lake": str(args.lake),
            "start": start.isoformat() if start else "",
            "end": end.isoformat() if end else "",
            "source": args.source,
            "rows": {name: int(len(frame)) for name, frame in tables.items()},
        }
        manifest_path = args.profile_out / "latency_manifest.json"  # FIX-M2

        def _write_manifest() -> None:  # FIX-7
            manifest_path = args.profile_out / "latency_manifest.json"  # FIX-M2
            args.profile_out.mkdir(parents=True, exist_ok=True)  # FIX-7
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")  # FIX-7

        execute_or_dryrun(f"[dry-run] would write: {manifest_path}", _write_manifest, args.dry_run)  # FIX-7

    payload = build_payload(tables, args)
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        DATA_BLOB=json_blob(payload),
        CHARTJS=chartjs or "window.Chart=null;",
    )
    def _write_html() -> None:  # FIX-7
        args.out.parent.mkdir(parents=True, exist_ok=True)  # FIX-7
        args.out.write_text(html, encoding="utf-8")  # FIX-7
        logger.info(f"Dashboard written: {args.out}")  # FIX-11
        logger.info(f"Size: {args.out.stat().st_size / 1024:.1f} KB")  # FIX-11

    if not execute_or_dryrun(f"[dry-run] HTML chars: {len(html):,} — would write: {args.out}", _write_html, args.dry_run):  # FIX-N1
        return


if __name__ == "__main__":
    main()
