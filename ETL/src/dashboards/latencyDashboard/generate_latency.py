#!/usr/bin/env python3
"""Generate the Veto Latency dashboard from the parquet lake."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
PROFILE_ROOT = SRC_ROOT / "profile"
for path in [ETL_ROOT, SRC_ROOT, PROFILE_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common.chartjs import load_chartjs  # noqa: E402
from common.render import json_blob, render_template  # noqa: E402
from common.source_ranges import true_source_ranges_from_lake  # noqa: E402
from vglive_core import (  # noqa: E402
    DEFAULT_LAKE_FOLDER,
    HOST_MAP,
    PATH_MAP,
    build_partition_filter,
    channel_candidate_sql,
)


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
IST_OFFSET_SECONDS = 19_800
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
        raise SystemExit("Use both --start and --end, or neither.")
    if start and end and start > end:
        raise SystemExit("--start cannot be after --end.")
    if not start and not end and args.window_days and args.window_days > 0:
        latest = latest_lake_day(args.lake)
        if latest:
            end = latest
            start = latest - timedelta(days=args.window_days - 1)
    return start, end


def source_filter_sql(source: str) -> str:
    if source == "all":
        return "1=1"
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = {sql_text(source)}"


def extension_sql(path_col: str = "reqPath") -> str:
    return f"""
CASE
    WHEN {path_col} IS NULL OR trim({path_col}) = '' THEN '<empty>'
    WHEN lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1)) = '' THEN '<none>'
    ELSE lower(regexp_extract({path_col}, '\\.([A-Za-z0-9]+)(?:\\?|$)', 1))
END
"""


def ist_timestamp_expr(epoch_expr: str = "reqTimeSec") -> str:
    return (
        "epoch_ms(CAST(FLOOR(("
        f"TRY_CAST({epoch_expr} AS DOUBLE) + {IST_OFFSET_SECONDS}"
        ") * 1000) AS BIGINT))"
    )


def clean_text_sql(column_expr: str) -> str:
    return f"NULLIF(trim(COALESCE(CAST({column_expr} AS VARCHAR), '')), '')"


def decoded_text_sql(column_expr: str) -> str:
    return (
        f"COALESCE(NULLIF(trim(try(url_decode(CAST({column_expr} AS VARCHAR)))), ''), "
        f"NULLIF(trim(CAST({column_expr} AS VARCHAR)), ''), 'Unknown / NA')"
    )


def platform_name_sql(source_expr: str = "source", host_expr: str = "reqHost") -> str:
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
    if dry_run:
        print(f"[dry-run] would write {len(df):,} rows -> {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"tmp_{path.stem}_{os.getpid()}{path.suffix}")
    tmp.unlink(missing_ok=True)
    df.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)
    print(f"wrote {path} ({len(df):,} rows)")


def fetch_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def build_tables(args: argparse.Namespace) -> dict[str, pd.DataFrame]:
    start, end = checked_dates(args)
    glob = q(args.lake / "**" / "*.parquet")
    partition_filter = build_partition_filter(start, end) if start and end else "1=1"
    source_filter = source_filter_sql(args.source)
    candidate_expr = channel_candidate_sql("reqPath")

    con = connect(args)
    try:
        print("Creating latency base table...")
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
                    {decoded_text_sql("country")} AS country,
                    {decoded_text_sql("state")} AS state,
                    {decoded_text_sql("city")} AS city,
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
        con.close()


def read_profile_table(profile_dir: Path, name: str) -> pd.DataFrame:
    path = profile_dir / f"{name}.parquet"
    if not path.exists():
        raise SystemExit(f"Latency profile table not found: {path}")
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
        daily = tables["daily"]
        rows = int(daily["rows"].sum()) if "rows" in daily else 0
        tables["summary"] = pd.DataFrame(
            [
                {
                    "first_date": str(daily["log_date"].min()) if not daily.empty else "",
                    "last_date": str(daily["log_date"].max()) if not daily.empty else "",
                    "rows": rows,
                    "ts_rows": int(daily.loc[daily["extension"].astype(str).str.lower() == "ts", "rows"].sum())
                    if "extension" in daily and "rows" in daily
                    else 0,
                    "playlist_rows": int(daily.loc[daily["extension"].astype(str).str.lower() == "m3u8", "rows"].sum())
                    if "extension" in daily and "rows" in daily
                    else 0,
                    "sources": int(daily["source"].nunique()) if "source" in daily else 0,
                    "channels": int(tables["channel_daily"]["channel_name"].nunique())
                    if "channel_name" in tables["channel_daily"]
                    else 0,
                    "hosts": int(tables["host_daily"]["reqHost"].nunique())
                    if "reqHost" in tables["host_daily"]
                    else 0,
                }
            ]
        )
    return tables


def clean_records(df: pd.DataFrame, columns: list[str] | None = None) -> list[dict]:
    if df.empty:
        return []
    out = df.copy()
    if columns:
        out = out[[col for col in columns if col in out.columns]]
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].round(6)
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
    if not args.from_profile and not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    if args.from_profile:
        print(f"Rendering latency dashboard from profile: {args.profile_out}")
        tables = load_tables_from_profile(args.profile_out)
    else:
        print(f"Building latency dashboard from: {args.lake}")
        start, end = checked_dates(args)
        if start and end:
            print(f"Date scope: {start.isoformat()} -> {end.isoformat()}")
        else:
            print("Date scope: all available lake partitions")
        print(f"Source scope: {args.source}")
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
        if not args.dry_run:
            args.profile_out.mkdir(parents=True, exist_ok=True)
            (args.profile_out / "latency_manifest.json").write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )

    payload = build_payload(tables, args)
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        DATA_BLOB=json_blob(payload),
        CHARTJS=chartjs or "window.Chart=null;",
    )
    if args.dry_run:
        print(f"[dry-run] HTML chars: {len(html):,}")
        print(f"[dry-run] would write: {args.out}")
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"Dashboard written: {args.out}")
    print(f"Size: {args.out.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
