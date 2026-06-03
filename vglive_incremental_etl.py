from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from vglive_core import DEFAULT_LAKE_FOLDER, HOST_MAP, PATH_MAP


ROOT = Path(__file__).resolve().parent
DEFAULT_PROFILE_OUT = ROOT / "vglive_channel_profile" / "deep_profile_full"
DEFAULT_STORE = ROOT / "vglive_channel_profile" / "etl_store"
CHUNK_SECONDS = 6.0
CHUNK_HOURS = CHUNK_SECONDS / 3600.0
LOG_DATE_SQL = "make_date(CAST(year AS INTEGER), CAST(month AS INTEGER), CAST(day AS INTEGER))"


@dataclass(frozen=True)
class RunConfig:
    lake: Path
    store: Path
    profile_out: Path
    threads: int
    memory_limit: str
    full_refresh: bool
    force_dates: set[str]
    process_dates: set[str]
    dry_run: bool
    top_n: int


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_path(path: Path) -> str:
    return quote_sql(str(path).replace("\\", "/"))


def parse_log_date(path: Path, lake: Path) -> str | None:
    try:
        rel = path.relative_to(lake)
    except ValueError:
        return None
    parts = rel.parts
    values: dict[str, str] = {}
    plain: list[str] = []
    for part in parts[:-1]:
        if "=" in part:
            key, value = part.split("=", 1)
            values[key.lower()] = value
        else:
            plain.append(part)
    if {"year", "month", "day"}.issubset(values):
        year, month, day = values["year"], values["month"], values["day"]
    elif len(plain) >= 3:
        year, month, day = plain[0], plain[1], plain[2]
    else:
        return None
    if not (year.isdigit() and month.isdigit() and day.isdigit()):
        return None
    try:
        return date(int(year), int(month), int(day)).isoformat()
    except ValueError:
        return None


def manifest_signature(rows: pd.DataFrame) -> str:
    h = hashlib.sha1()
    for row in rows.sort_values("file").itertuples(index=False):
        h.update(
            f"{row.file}|{row.size_bytes}|{row.mtime_ns}|{row.row_count}\n".encode(
                "utf-8"
            )
        )
    return h.hexdigest()


def build_manifest(lake: Path) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for path in lake.rglob("*.parquet"):
        log_date = parse_log_date(path, lake)
        if not log_date:
            continue
        stat = path.stat()
        try:
            row_count = pq.read_metadata(path).num_rows
        except Exception:
            row_count = None
        records.append(
            {
                "log_date": log_date,
                "file": str(path.resolve()),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "row_count": None if row_count is None else int(row_count),
            }
        )
    if not records:
        return pd.DataFrame(
            columns=["log_date", "file", "size_bytes", "mtime_ns", "row_count"]
        )
    return pd.DataFrame(records).sort_values(["log_date", "file"]).reset_index(drop=True)


def load_parquet(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression="zstd")


def date_signatures(manifest: pd.DataFrame) -> dict[str, str]:
    if manifest.empty:
        return {}
    return {
        log_date: manifest_signature(group)
        for log_date, group in manifest.groupby("log_date", dropna=False)
    }


def find_dirty_dates(
    new_manifest: pd.DataFrame,
    old_manifest: pd.DataFrame,
    full_refresh: bool,
    force_dates: set[str],
) -> list[str]:
    new_dates = set(new_manifest["log_date"].astype(str)) if not new_manifest.empty else set()
    if full_refresh or old_manifest.empty:
        dirty = set(new_dates)
    else:
        old_sigs = date_signatures(old_manifest)
        new_sigs = date_signatures(new_manifest)
        dirty = {
            log_date
            for log_date in new_dates | set(old_sigs)
            if old_sigs.get(log_date) != new_sigs.get(log_date)
        }
    dirty |= force_dates
    return sorted(d for d in dirty if d in new_dates)


NORMALIZED_COLUMNS: dict[str, str] = {
    "reqHost": "VARCHAR",
    "reqPath": "VARCHAR",
    "cliIP": "VARCHAR",
    "statusCode": "INTEGER",
    "asn": "VARCHAR",
    "country": "VARCHAR",
    "state": "VARCHAR",
    "city": "VARCHAR",
    "userAgent": "VARCHAR",
    "cacheStatus": "VARCHAR",
    "cacheable": "VARCHAR",
    "errorCode": "VARCHAR",
    "startupError": "VARCHAR",
    "queryStr": "VARCHAR",
}


def available_columns(files: Iterable[str]) -> set[str]:
    cols = {"year", "month", "day"}
    for file in files:
        try:
            cols.update(pq.read_schema(file).names)
        except Exception:
            continue
    return cols


def raw_source_sql(files: Iterable[str]) -> str:
    parts = ", ".join(sql_path(Path(p)) for p in files)
    return f"read_parquet([{parts}], hive_partitioning=1, union_by_name=true)"


def source_sql(files: Iterable[str]) -> str:
    file_list = list(files)
    raw = raw_source_sql(file_list)
    cols = available_columns(file_list)
    select_parts = [
        "year",
        "month",
        "day",
    ]
    for name, sql_type in NORMALIZED_COLUMNS.items():
        if name in cols:
            expr = f"TRY_CAST({name} AS {sql_type})"
        else:
            expr = f"CAST(NULL AS {sql_type})"
        select_parts.append(f"{expr} AS {name}")
    return f"(SELECT {', '.join(select_parts)} FROM {raw})"


def register_maps(con: duckdb.DuckDBPyConnection) -> None:
    host_df = pd.DataFrame(
        [{"reqHost": key, "host_channel": value} for key, value in HOST_MAP.items()]
    )
    path_df = pd.DataFrame(
        [{"path_key": key, "path_channel": value} for key, value in PATH_MAP.items()]
    )
    con.register("host_map_df", host_df)
    con.register("path_map_df", path_df)


def channel_cte(src: str) -> str:
    host_cases = "\n".join(
        f"            WHEN lower(reqHost) = {quote_sql(k.lower())} THEN {quote_sql(v)}"
        for k, v in HOST_MAP.items()
    )
    path_cases = "\n".join(
        f"            WHEN lower(reqPath) LIKE '%' || {quote_sql(k.lower())} || '%' THEN {quote_sql(v)}"
        for k, v in PATH_MAP.items()
    )
    return f"""
WITH base AS (
    SELECT
        {LOG_DATE_SQL} AS log_date,
        reqHost,
        reqPath,
        cliIP,
        statusCode,
        asn,
        country,
        state,
        city,
        userAgent,
        cacheStatus,
        cacheable,
        errorCode,
        startupError,
        queryStr,
        lower(COALESCE(reqHost, '')) AS host_lower,
        lower(COALESCE(reqPath, '')) AS path_lower
    FROM {src}
),
resolved AS (
    SELECT
        *,
        COALESCE(
            CASE
{host_cases}
            END,
            CASE
{path_cases}
            END,
            'Other'
        ) AS channel_name
    FROM base
)
"""


def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def setup_connection(config: RunConfig) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(config.threads)}")
    con.execute(f"PRAGMA memory_limit={quote_sql(config.memory_limit)}")
    con.execute("PRAGMA preserve_insertion_order=false")
    register_maps(con)
    return con


def fresh_tables(
    con: duckdb.DuckDBPyConnection, files: list[str]
) -> dict[str, pd.DataFrame]:
    src = source_sql(files)
    cte = channel_cte(src)
    ts_filter = "lower(COALESCE(reqPath, '')) LIKE '%.ts'"

    queries: dict[str, str] = {
        "daily_volume": f"""
            SELECT
                {LOG_DATE_SQL} AS log_date,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE {ts_filter}) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE {ts_filter} AND statusCode = 200) AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE {ts_filter} AND statusCode != 200) AS non_200_ts_rows,
                COUNT(*) FILTER (WHERE lower(COALESCE(reqPath, '')) LIKE '%.m3u8') AS m3u8_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                approx_count_distinct(reqHost) AS distinct_hosts,
                COUNT(*) FILTER (WHERE statusCode = 200) AS status_200_rows,
                COUNT(*) FILTER (WHERE statusCode != 200) AS non_200_rows
            FROM {src}
            GROUP BY 1
        """,
        "status_codes_daily": f"""
            SELECT
                {LOG_DATE_SQL} AS log_date,
                statusCode,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE {ts_filter}) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE {ts_filter} AND statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                any_value(reqPath) AS sample_reqPath
            FROM {src}
            GROUP BY 1, 2
        """,
        "extensions_daily": f"""
            SELECT
                {LOG_DATE_SQL} AS log_date,
                regexp_extract(lower(COALESCE(reqPath, '')), '\\\\.([a-z0-9]{{1,8}})(?:\\\\?|$)', 1) AS extension,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE statusCode = 200) AS status_200_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                any_value(reqPath) AS sample_reqPath
            FROM {src}
            GROUP BY 1, 2
        """,
        "hosts_daily": f"""
            SELECT
                {LOG_DATE_SQL} AS log_date,
                reqHost,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE statusCode = 200) AS status_200_rows,
                COUNT(*) FILTER (WHERE statusCode != 200) AS non_200_rows,
                COUNT(*) FILTER (WHERE {ts_filter}) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE {ts_filter} AND statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips
            FROM {src}
            GROUP BY 1, 2
        """,
        "geo_daily": f"""
            WITH cleaned AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    COALESCE(NULLIF(country, ''), 'Unknown') AS country_clean,
                    COALESCE(NULLIF(state, ''), 'Unknown') AS state_clean,
                    COALESCE(NULLIF(city, ''), 'Unknown') AS city_clean,
                    reqPath,
                    statusCode,
                    cliIP,
                    reqHost
                FROM {src}
                WHERE {ts_filter}
            )
            SELECT
                log_date,
                country_clean AS country,
                state_clean AS state,
                city_clean AS city,
                COUNT(*) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                approx_count_distinct(reqHost) AS distinct_hosts
            FROM cleaned
            GROUP BY 1, 2, 3, 4
        """,
        "asn_daily": f"""
            WITH cleaned AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    COALESCE(CAST(asn AS VARCHAR), 'Unknown') AS asn_clean,
                    statusCode,
                    cliIP,
                    reqHost
                FROM {src}
                WHERE {ts_filter}
            )
            SELECT
                log_date,
                asn_clean AS asn,
                COUNT(*) AS raw_ts_rows,
                COUNT(*) FILTER (WHERE statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                approx_count_distinct(reqHost) AS distinct_hosts,
                any_value(reqHost) AS sample_reqHost
            FROM cleaned
            GROUP BY 1, 2
        """,
        "cache_daily": f"""
            WITH cleaned AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    reqHost,
                    COALESCE(NULLIF(cacheStatus, ''), 'Unknown') AS cache_status_clean,
                    COALESCE(NULLIF(cacheable, ''), 'Unknown') AS cacheable_clean,
                    reqPath,
                    statusCode,
                    cliIP
                FROM {src}
            )
            SELECT
                log_date,
                reqHost,
                cache_status_clean AS cacheStatus,
                cacheable_clean AS cacheable,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE lower(COALESCE(reqPath, '')) LIKE '%.ts') AS raw_ts_rows,
                COUNT(*) FILTER (WHERE lower(COALESCE(reqPath, '')) LIKE '%.ts' AND statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips
            FROM cleaned
            GROUP BY 1, 2, 3, 4
        """,
        "errors_daily": f"""
            WITH cleaned AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    reqHost,
                    statusCode,
                    COALESCE(NULLIF(errorCode, ''), 'None') AS error_code_clean,
                    COALESCE(NULLIF(startupError, ''), 'None') AS startup_error_clean,
                    reqPath,
                    cliIP
                FROM {src}
                WHERE statusCode IS NOT NULL AND statusCode != 200
            )
            SELECT
                log_date,
                reqHost,
                statusCode,
                error_code_clean AS errorCode,
                startup_error_clean AS startupError,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE lower(COALESCE(reqPath, '')) LIKE '%.ts') AS raw_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                any_value(reqPath) AS sample_reqPath
            FROM cleaned
            GROUP BY 1, 2, 3, 4, 5
        """,
        "query_params_daily": f"""
            WITH source_rows AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    lower(COALESCE(queryStr, '')) AS query_lower,
                    queryStr
                FROM {src}
                WHERE queryStr IS NOT NULL AND queryStr != ''
            )
            SELECT
                log_date,
                COUNT(*) AS rows_with_querystr,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)channel=')) AS channel_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)channel_name=')) AS channel_name_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)(session|session_id|sid)=')) AS session_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)(device_id|did)=')) AS device_id_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)platform=')) AS platform_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)(device|device_name)=')) AS device_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)content_title=')) AS content_title_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|&)category_name=')) AS category_name_rows,
                any_value(queryStr) AS sample_queryStr
            FROM source_rows
            GROUP BY 1
        """,
        "cmcd_daily": f"""
            WITH source_rows AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    lower(COALESCE(queryStr, '')) AS query_lower,
                    queryStr
                FROM {src}
                WHERE queryStr ILIKE '%cmcd%'
            )
            SELECT
                log_date,
                COUNT(*) AS rows_with_cmcd,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])br=')) AS br_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])d=')) AS duration_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])mtp=')) AS measured_throughput_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])ot=')) AS object_type_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])sf=')) AS streaming_format_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])sid=')) AS session_id_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])st=')) AS stream_type_rows,
                COUNT(*) FILTER (WHERE regexp_matches(query_lower, '(^|[,&])tb=')) AS top_bitrate_rows,
                any_value(queryStr) AS sample_cmcd
            FROM source_rows
            GROUP BY 1
        """,
        "user_agents_daily": f"""
            WITH cleaned AS (
                SELECT
                    {LOG_DATE_SQL} AS log_date,
                    COALESCE(NULLIF(userAgent, ''), 'Unknown') AS user_agent_clean,
                    reqPath,
                    statusCode,
                    cliIP,
                    reqHost
                FROM {src}
            )
            SELECT
                log_date,
                user_agent_clean AS userAgent,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE lower(COALESCE(reqPath, '')) LIKE '%.ts') AS raw_ts_rows,
                COUNT(*) FILTER (WHERE lower(COALESCE(reqPath, '')) LIKE '%.ts' AND statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                approx_count_distinct(reqHost) AS distinct_hosts
            FROM cleaned
            GROUP BY 1, 2
        """,
    }

    channel_queries: dict[str, str] = {
        "channel_daily": f"""
            {cte}
            SELECT
                log_date,
                channel_name,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts') AS raw_ts_chunks,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts' AND statusCode = 200) AS status_200_ts_chunks,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts' AND statusCode != 200) AS non_200_ts_chunks,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.m3u8') AS m3u8_rows,
                approx_count_distinct(cliIP) FILTER (WHERE path_lower LIKE '%.ts') AS approx_unique_ips
            FROM resolved
            GROUP BY 1, 2
        """,
        "device_type_by_channel_daily": f"""
            {cte}
            SELECT
                log_date,
                channel_name,
                CASE
                    WHEN userAgent ILIKE '%android%' THEN 'Android'
                    WHEN userAgent ILIKE '%iphone%' OR userAgent ILIKE '%ipad%' THEN 'iOS'
                    WHEN userAgent ILIKE '%smart-tv%' OR userAgent ILIKE '%smarttv%' OR userAgent ILIKE '%tizen%' OR userAgent ILIKE '%webos%' THEN 'Smart TV'
                    WHEN userAgent ILIKE '%windows%' OR userAgent ILIKE '%macintosh%' OR userAgent ILIKE '%linux%' THEN 'Desktop'
                    WHEN userAgent IS NULL OR userAgent = '' THEN 'Unknown'
                    ELSE 'Other'
                END AS device_type,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts') AS raw_ts_rows,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts' AND statusCode = 200) AS status_200_ts_rows,
                approx_count_distinct(cliIP) AS approx_unique_ips
            FROM resolved
            GROUP BY 1, 2, 3
        """,
        "mapping_quality_daily": f"""
            {cte}
            SELECT
                log_date,
                reqHost,
                COALESCE(
                    NULLIF(regexp_extract(path_lower, '(vglive-sk-[0-9]+)', 1), ''),
                    NULLIF(regexp_extract(path_lower, '^/?([^/?]+)', 1), ''),
                    'unknown'
                ) AS candidate_id,
                channel_name,
                CASE
                    WHEN channel_name != 'Other' THEN 'mapped'
                    WHEN path_lower LIKE '%.ts' THEN 'unmapped_ts'
                    ELSE 'unmapped_other'
                END AS quality_bucket,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts') AS raw_ts_chunks,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts' AND statusCode = 200) AS status_200_ts_chunks,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                any_value(reqPath) AS sample_reqPath
            FROM resolved
            GROUP BY 1, 2, 3, 4, 5
        """,
        "unmapped_candidates_daily": f"""
            {cte}
            SELECT
                log_date,
                reqHost,
                COALESCE(
                    NULLIF(regexp_extract(path_lower, '(vglive-sk-[0-9]+)', 1), ''),
                    NULLIF(regexp_extract(path_lower, '^/?([^/?]+)', 1), ''),
                    'unknown'
                ) AS candidate_id,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts') AS raw_ts_chunks,
                COUNT(*) FILTER (WHERE path_lower LIKE '%.ts' AND statusCode = 200) AS status_200_ts_chunks,
                approx_count_distinct(cliIP) AS approx_unique_ips,
                any_value(reqPath) AS sample_reqPath
            FROM resolved
            WHERE channel_name = 'Other'
            GROUP BY 1, 2, 3
        """,
    }

    out: dict[str, pd.DataFrame] = {}
    for name, sql in queries.items():
        out[name] = run_query(con, sql)
    for name, sql in channel_queries.items():
        out[name] = run_query(con, sql)

    if "channel_daily" in out and not out["channel_daily"].empty:
        cd = out["channel_daily"]
        cd["raw_watch_hours"] = cd["raw_ts_chunks"].fillna(0).astype(float) * CHUNK_HOURS
        cd["status_200_watch_hours"] = (
            cd["status_200_ts_chunks"].fillna(0).astype(float) * CHUNK_HOURS
        )

    for table in ("daily_volume", "status_codes_daily", "geo_daily", "asn_daily", "hosts_daily"):
        if table in out and not out[table].empty:
            df = out[table]
            if "raw_ts_rows" in df.columns:
                df["raw_watch_hours"] = df["raw_ts_rows"].fillna(0).astype(float) * CHUNK_HOURS
            if "status_200_ts_rows" in df.columns:
                df["status_200_watch_hours"] = (
                    df["status_200_ts_rows"].fillna(0).astype(float) * CHUNK_HOURS
                )
    for table in ("mapping_quality_daily", "unmapped_candidates_daily"):
        if table in out and not out[table].empty:
            df = out[table]
            df["raw_watch_hours"] = df["raw_ts_chunks"].fillna(0).astype(float) * CHUNK_HOURS
            df["status_200_watch_hours"] = (
                df["status_200_ts_chunks"].fillna(0).astype(float) * CHUNK_HOURS
            )

    return out


def merge_by_date(existing: pd.DataFrame, fresh: pd.DataFrame, dates: set[str]) -> pd.DataFrame:
    if fresh.empty:
        return existing
    fresh = fresh.copy()
    fresh["log_date"] = pd.to_datetime(fresh["log_date"]).dt.date.astype(str)
    if existing.empty:
        return fresh.reset_index(drop=True)
    existing = existing.copy()
    existing["log_date"] = pd.to_datetime(existing["log_date"]).dt.date.astype(str)
    kept = existing[~existing["log_date"].isin(dates)]
    merged = pd.concat([kept, fresh], ignore_index=True, sort=False)
    return merged.sort_values([c for c in ["log_date"] if c in merged.columns]).reset_index(drop=True)


def table_path(store: Path, table: str) -> Path:
    return store / "tables" / f"{table}.parquet"


def update_store(store: Path, tables: dict[str, pd.DataFrame], dirty_dates: list[str]) -> None:
    dates = set(dirty_dates)
    for name, fresh in tables.items():
        path = table_path(store, name)
        existing = load_parquet(path)
        merged = merge_by_date(existing, fresh, dates)
        write_parquet(merged, path)


def top_rows(df: pd.DataFrame, value_col: str, n: int) -> pd.DataFrame:
    if df.empty or value_col not in df.columns:
        return df
    return df.sort_values(value_col, ascending=False).head(n).reset_index(drop=True)


def group_sum_max(
    df: pd.DataFrame,
    by: list[str],
    sum_cols: list[str],
    max_cols: list[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=by + sum_cols + (max_cols or []))
    agg: dict[str, str] = {col: "sum" for col in sum_cols if col in df.columns}
    for col in max_cols or []:
        if col in df.columns:
            agg[col] = "max"
    if not by:
        return pd.DataFrame([{col: getattr(df[col], func)() for col, func in agg.items()}])
    out = df.groupby(by, dropna=False, as_index=False).agg(agg)
    return out


def materialize_profile(store: Path, profile_out: Path, manifest: pd.DataFrame, top_n: int) -> None:
    profile_out.mkdir(parents=True, exist_ok=True)

    daily = load_parquet(table_path(store, "daily_volume"))
    if not daily.empty:
        daily = daily.copy()
        if "rows" not in daily.columns and "total_rows" in daily.columns:
            daily["rows"] = daily["total_rows"]
        if "ts_rows" not in daily.columns and "raw_ts_rows" in daily.columns:
            daily["ts_rows"] = daily["raw_ts_rows"]
        write_parquet(daily.sort_values("log_date"), profile_out / "daily_volume.parquet")

    channel_daily = load_parquet(table_path(store, "channel_daily"))
    if not channel_daily.empty:
        write_parquet(
            channel_daily.sort_values(["log_date", "channel_name"]),
            profile_out / "channel_daily.parquet",
        )
        channel_summary = group_sum_max(
            channel_daily,
            ["channel_name"],
            [
                "raw_ts_chunks",
                "status_200_ts_chunks",
                "non_200_ts_chunks",
                "m3u8_rows",
                "raw_watch_hours",
                "status_200_watch_hours",
            ],
            ["approx_unique_ips"],
        )
        channel_summary = channel_summary.sort_values(
            "raw_watch_hours", ascending=False
        ).reset_index(drop=True)
        write_parquet(channel_summary, profile_out / "channel_summary.parquet")

    status_daily = load_parquet(table_path(store, "status_codes_daily"))
    if not status_daily.empty:
        status = group_sum_max(
            status_daily,
            ["statusCode"],
            ["rows", "raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
            ["approx_unique_ips"],
        )
        sample = status_daily.groupby("statusCode", dropna=False)["sample_reqPath"].first().reset_index()
        status = status.merge(sample, on="statusCode", how="left")
        status = status.sort_values("rows", ascending=False).reset_index(drop=True)
        write_parquet(status, profile_out / "status_codes.parquet")

    extensions_daily = load_parquet(table_path(store, "extensions_daily"))
    if not extensions_daily.empty:
        extensions = group_sum_max(
            extensions_daily,
            ["extension"],
            ["rows", "status_200_rows"],
            ["approx_unique_ips"],
        )
        sample = extensions_daily.groupby("extension", dropna=False)["sample_reqPath"].first().reset_index()
        extensions = extensions.merge(sample, on="extension", how="left")
        extensions = top_rows(extensions, "rows", top_n)
        write_parquet(extensions, profile_out / "extensions.parquet")

    hosts_daily = load_parquet(table_path(store, "hosts_daily"))
    if not hosts_daily.empty:
        hosts = group_sum_max(
            hosts_daily,
            ["reqHost"],
            [
                "rows",
                "status_200_rows",
                "non_200_rows",
                "raw_ts_rows",
                "status_200_ts_rows",
                "raw_watch_hours",
                "status_200_watch_hours",
            ],
            ["approx_unique_ips"],
        )
        hosts["non_200_pct"] = hosts["non_200_rows"] / hosts["rows"].replace(0, pd.NA) * 100.0
        hosts["ts_rows"] = hosts["raw_ts_rows"]
        hosts = hosts.sort_values("rows", ascending=False).reset_index(drop=True)
        write_parquet(hosts, profile_out / "hosts_overview.parquet")

    geo_daily = load_parquet(table_path(store, "geo_daily"))
    if not geo_daily.empty:
        geo = group_sum_max(
            geo_daily,
            ["country", "state", "city"],
            ["raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
            ["approx_unique_ips", "distinct_hosts"],
        )
        geo["ts_rows"] = geo["raw_ts_rows"]
        geo = top_rows(geo, "raw_watch_hours", top_n)
        write_parquet(geo, profile_out / "geo_top.parquet")

    asn_daily = load_parquet(table_path(store, "asn_daily"))
    if not asn_daily.empty:
        asn = group_sum_max(
            asn_daily,
            ["asn"],
            ["raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
            ["approx_unique_ips", "distinct_hosts"],
        )
        sample = asn_daily.groupby("asn", dropna=False)["sample_reqHost"].first().reset_index()
        asn = asn.merge(sample, on="asn", how="left")
        asn["ts_rows"] = asn["raw_ts_rows"]
        asn = top_rows(asn, "raw_watch_hours", top_n)
        write_parquet(asn, profile_out / "asn_top.parquet")

    device_daily = load_parquet(table_path(store, "device_type_by_channel_daily"))
    if not device_daily.empty:
        device = group_sum_max(
            device_daily,
            ["channel_name", "device_type"],
            ["raw_ts_rows", "status_200_ts_rows"],
            ["approx_unique_ips"],
        )
        device["ts_rows"] = device["raw_ts_rows"]
        device["raw_watch_hours"] = device["raw_ts_rows"].fillna(0).astype(float) * CHUNK_HOURS
        device["status_200_watch_hours"] = (
            device["status_200_ts_rows"].fillna(0).astype(float) * CHUNK_HOURS
        )
        device = device.sort_values("raw_watch_hours", ascending=False).reset_index(drop=True)
        write_parquet(device, profile_out / "device_type_by_channel.parquet")

    cache_daily = load_parquet(table_path(store, "cache_daily"))
    if not cache_daily.empty:
        cache = group_sum_max(
            cache_daily,
            ["reqHost", "cacheStatus", "cacheable"],
            ["rows", "raw_ts_rows", "status_200_ts_rows"],
            ["approx_unique_ips"],
        )
        cache = cache.sort_values("rows", ascending=False).reset_index(drop=True)
        write_parquet(cache, profile_out / "cache_by_host.parquet")

    errors_daily = load_parquet(table_path(store, "errors_daily"))
    if not errors_daily.empty:
        errors = group_sum_max(
            errors_daily,
            ["reqHost", "statusCode", "errorCode", "startupError"],
            ["rows", "raw_ts_rows"],
            ["approx_unique_ips"],
        )
        sample = (
            errors_daily.groupby(["reqHost", "statusCode", "errorCode", "startupError"], dropna=False)[
                "sample_reqPath"
            ]
            .first()
            .reset_index()
        )
        errors = errors.merge(
            sample, on=["reqHost", "statusCode", "errorCode", "startupError"], how="left"
        )
        errors = top_rows(errors, "rows", top_n)
        write_parquet(errors, profile_out / "errors_by_host.parquet")

    params_daily = load_parquet(table_path(store, "query_params_daily"))
    if not params_daily.empty:
        sum_cols = [
            "rows_with_querystr",
            "channel_rows",
            "channel_name_rows",
            "session_rows",
            "device_id_rows",
            "platform_rows",
            "device_rows",
            "content_title_rows",
            "category_name_rows",
        ]
        params = group_sum_max(params_daily, [], sum_cols)
        params["sample_queryStr"] = params_daily["sample_queryStr"].dropna().astype(str).head(1).squeeze() if "sample_queryStr" in params_daily.columns and params_daily["sample_queryStr"].notna().any() else ""
        write_parquet(params, profile_out / "querystr_param_presence.parquet")

    cmcd_daily = load_parquet(table_path(store, "cmcd_daily"))
    if not cmcd_daily.empty:
        sum_cols = [
            "rows_with_cmcd",
            "br_rows",
            "duration_rows",
            "measured_throughput_rows",
            "object_type_rows",
            "streaming_format_rows",
            "session_id_rows",
            "stream_type_rows",
            "top_bitrate_rows",
        ]
        cmcd = group_sum_max(
            cmcd_daily,
            [],
            sum_cols,
        )
        cmcd["sample_cmcd"] = cmcd_daily["sample_cmcd"].dropna().astype(str).head(1).squeeze() if "sample_cmcd" in cmcd_daily.columns and cmcd_daily["sample_cmcd"].notna().any() else ""
        write_parquet(cmcd, profile_out / "cmcd_presence.parquet")

    ua_daily = load_parquet(table_path(store, "user_agents_daily"))
    if not ua_daily.empty:
        ua = group_sum_max(
            ua_daily,
            ["userAgent"],
            ["rows", "raw_ts_rows", "status_200_ts_rows"],
            ["approx_unique_ips", "distinct_hosts"],
        )
        ua = ua.rename(columns={"userAgent": "UA"})
        ua = top_rows(ua, "rows", top_n)
        write_parquet(ua, profile_out / "ua_top.parquet")

    quality_daily = load_parquet(table_path(store, "mapping_quality_daily"))
    if not quality_daily.empty:
        quality = group_sum_max(
            quality_daily,
            ["reqHost", "candidate_id", "channel_name", "quality_bucket"],
            [
                "rows",
                "raw_ts_chunks",
                "status_200_ts_chunks",
                "raw_watch_hours",
                "status_200_watch_hours",
            ],
            ["approx_unique_ips"],
        )
        sample = (
            quality_daily.groupby(["reqHost", "candidate_id", "channel_name", "quality_bucket"], dropna=False)[
                "sample_reqPath"
            ]
            .first()
            .reset_index()
        )
        quality = quality.merge(
            sample, on=["reqHost", "candidate_id", "channel_name", "quality_bucket"], how="left"
        )
        quality = top_rows(quality, "rows", top_n)
        write_parquet(quality, profile_out / "path_candidate_quality.parquet")

    unmapped_daily = load_parquet(table_path(store, "unmapped_candidates_daily"))
    if not unmapped_daily.empty:
        unmapped = group_sum_max(
            unmapped_daily,
            ["reqHost", "candidate_id"],
            [
                "rows",
                "raw_ts_chunks",
                "status_200_ts_chunks",
                "raw_watch_hours",
                "status_200_watch_hours",
            ],
            ["approx_unique_ips"],
        )
        sample = (
            unmapped_daily.groupby(["reqHost", "candidate_id"], dropna=False)["sample_reqPath"]
            .first()
            .reset_index()
        )
        unmapped = unmapped.merge(sample, on=["reqHost", "candidate_id"], how="left")
        unmapped = top_rows(unmapped, "raw_ts_chunks", top_n)
        write_parquet(unmapped, profile_out / "unmapped_candidates.parquet")

    if not manifest.empty:
        files = manifest.copy()
        files["date"] = files["log_date"]
        files["size_mb"] = files["size_bytes"].astype(float) / (1024 * 1024)
        write_parquet(files, profile_out / "file_inventory.parquet")


def copy_unchanged_csv_fallbacks(profile_out: Path) -> None:
    """Keep older report readers alive without forcing CSV as the main data path."""
    for parquet_path in profile_out.glob("*.parquet"):
        csv_path = parquet_path.with_suffix(".csv")
        if csv_path.exists():
            continue
        # The current report reads parquet first, so CSV mirrors are optional.


def run(config: RunConfig) -> int:
    started = time.perf_counter()
    config.store.mkdir(parents=True, exist_ok=True)
    config.profile_out.mkdir(parents=True, exist_ok=True)

    print(f"[manifest] scanning parquet metadata in {config.lake}")
    new_manifest = build_manifest(config.lake)
    if new_manifest.empty:
        raise SystemExit(f"No parquet files found under {config.lake}")

    manifest_path = config.store / "manifest.parquet"
    old_manifest = load_parquet(manifest_path)
    dirty_dates = find_dirty_dates(
        new_manifest, old_manifest, config.full_refresh, config.force_dates
    )
    if config.process_dates:
        dirty_dates = [d for d in dirty_dates if d in config.process_dates]

    total_rows = int(new_manifest["row_count"].fillna(0).sum())
    print(
        f"[manifest] files={len(new_manifest):,} dates={new_manifest['log_date'].nunique():,} rows={total_rows:,}"
    )
    if dirty_dates:
        print(f"[dirty] {len(dirty_dates):,} date(s): {', '.join(dirty_dates[:20])}")
        if len(dirty_dates) > 20:
            print(f"[dirty] ... plus {len(dirty_dates) - 20:,} more")
    else:
        print("[dirty] no changed lake dates; profile is already current")

    if config.dry_run:
        return 0

    if dirty_dates:
        files = (
            new_manifest[new_manifest["log_date"].isin(dirty_dates)]["file"]
            .astype(str)
            .tolist()
        )
        print(f"[etl] aggregating {len(files):,} changed parquet file(s)")
        con = setup_connection(config)
        tables = fresh_tables(con, files)
        con.close()
        update_store(config.store, tables, dirty_dates)
    else:
        print("[etl] skipping aggregate refresh")

    write_parquet(new_manifest, manifest_path)
    print("[profile] materializing report parquet tables")
    materialize_profile(config.store, config.profile_out, new_manifest, config.top_n)

    meta = {
        "lake": str(config.lake),
        "store": str(config.store),
        "profile_out": str(config.profile_out),
        "full_refresh": config.full_refresh,
        "dirty_dates": dirty_dates,
        "file_count": int(len(new_manifest)),
        "date_count": int(new_manifest["log_date"].nunique()),
        "row_count": total_rows,
        "seconds": round(time.perf_counter() - started, 3),
    }
    (config.store / "last_run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[done] {meta['seconds']} sec")
    return 0


def parse_force_dates(raw: str | None) -> set[str]:
    if not raw:
        return set()
    out: set[str] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        out.add(date.fromisoformat(item).isoformat())
    return out


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Incremental VgLive ETL for fast, accurate watch-hour reporting."
    )
    parser.add_argument("--lake", default=str(DEFAULT_LAKE_FOLDER), help="Lake folder")
    parser.add_argument("--store", default=str(DEFAULT_STORE), help="Incremental ETL store")
    parser.add_argument(
        "--profile-out",
        default=str(DEFAULT_PROFILE_OUT),
        help="Materialized dashboard/report profile folder",
    )
    parser.add_argument("--threads", type=int, default=6, help="DuckDB worker threads")
    parser.add_argument("--memory-limit", default="18GB", help="DuckDB memory limit")
    parser.add_argument(
        "--force-dates",
        default="",
        help="Comma-separated YYYY-MM-DD dates to refresh even if unchanged",
    )
    parser.add_argument(
        "--process-dates",
        default="",
        help="Limit this run to these comma-separated YYYY-MM-DD dates",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Rebuild all dates into the incremental aggregate store",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only scan the manifest and report dirty dates",
    )
    parser.add_argument("--top-n", type=int, default=500, help="Rows to keep for ranked outputs")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    config = RunConfig(
        lake=Path(args.lake),
        store=Path(args.store),
        profile_out=Path(args.profile_out),
        threads=args.threads,
        memory_limit=args.memory_limit,
        full_refresh=args.full_refresh,
        force_dates=parse_force_dates(args.force_dates),
        process_dates=parse_force_dates(args.process_dates),
        dry_run=args.dry_run,
        top_n=args.top_n,
    )
    return run(config)


if __name__ == "__main__":
    raise SystemExit(main())
