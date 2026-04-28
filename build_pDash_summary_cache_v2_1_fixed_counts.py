"""
build_pDash_summary_cache_v2_1_fixed_counts.py
============================================
Builds a persistent DuckDB analytics cache for large CDN Parquet logs.

This version is adapted for your inspected parquet schema:
- country exists
- state exists and should be used as region/state
- city exists
- queryStr often does NOT contain device_id/session_id
- channel is reliably available from reqPath as vglive-sk-xxxxxx
- device_type is inferred from UA
- unique devices are estimated from cliIP + UA when queryStr device_id is missing

Install:
    pip install duckdb pandas pyarrow

Recommended command:
    python build_pDash_summary_cache_v2_1_fixed_counts.py ^
      --input "D:\\Vs - Code Work\\cleaned_output" ^
      --db "D:\\Vs - Code Work\\pDash_analytics_cache.duckdb" ^
      --query-col queryStr ^
      --time-col reqTimeSec ^
      --path-col reqPath ^
      --ua-col UA ^
      --ip-col cliIP ^
      --country-col country ^
      --region-col state ^
      --city-col city ^
      --threads 6 ^
      --memory-limit 10GB ^
      --temp-directory "D:\\duckdb_temp"
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

CACHE_VERSION = "summary-cache-v2.1-log-fallback-dedup-channel-map"

DEVICE_SUFFIX_TOKENS = {
    "firetv", "firestick", "fireos",
    "androidtv", "android",
    "web", "webos",
    "lg", "lgtv",
    "apple", "appletv", "ios", "iphone", "ipad",
    "samsung", "samsungtv", "tizen",
    "roku", "mi", "mitv", "xiaomi",
    "sony", "bravia",
    "id", "tv", "mobile", "phone", "tablet",
}


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "|", msg, flush=True)


def qident(identifier: str) -> str:
    if not identifier:
        raise ValueError("Empty column name")
    return '"' + str(identifier).replace('"', '""') + '"'


def sql_str(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_list(values: Iterable[str]) -> str:
    return "[" + ", ".join(sql_str(v) for v in values) + "]"


def collect_parquet_files(inputs: list[str], recursive: bool = False) -> list[str]:
    files: list[str] = []
    for raw in inputs:
        p = Path(raw).expanduser()
        if p.is_file() and p.suffix.lower() == ".parquet":
            files.append(str(p.resolve()))
        elif p.is_dir():
            pattern = "**/*.parquet" if recursive else "*.parquet"
            files.extend(str(x.resolve()) for x in sorted(p.glob(pattern)) if x.is_file())
        else:
            log(f"WARNING: skipped missing input: {raw}")
    return sorted(dict.fromkeys(files))


def norm_token(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def expand_device_tokens(*values: str) -> set[str]:
    expanded = set(DEVICE_SUFFIX_TOKENS)
    for val in values:
        tok = norm_token(val)
        if not tok:
            continue
        expanded.add(tok)
        if tok.endswith("tv") and len(tok) > 2:
            expanded.add(tok[:-2])
        if tok == "androidtv":
            expanded.add("android")
        if tok == "appletv":
            expanded.add("apple")
        if tok == "lgtv":
            expanded.add("lg")
        if tok == "samsungtv":
            expanded.add("samsung")
        if tok == "firestick":
            expanded.add("firetv")
    return expanded


def normalize_channel_name_smart(channel_raw: str, platform: str = "", device_name: str = "") -> str:
    """Same style as your Query String Analyzer pure-channel cleaner."""
    if channel_raw is None:
        return "unknown"
    raw = urllib.parse.unquote_plus(str(channel_raw or "").strip())
    platform = urllib.parse.unquote_plus(str(platform or ""))
    device_name = urllib.parse.unquote_plus(str(device_name or ""))
    if raw.lower() in {"", "nan", "none", "null", "(null)"}:
        return "unknown"

    raw = re.sub(r"\s*_\s*", "_", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    clean = raw
    tokens = expand_device_tokens(platform, device_name)
    while "_" in clean:
        base, suffix = clean.rsplit("_", 1)
        if norm_token(suffix) in tokens:
            clean = base.strip("_ ").strip()
            continue
        break
    return (clean or "unknown").casefold()


def event_time_expr(time_col: str) -> str:
    ts = qident(time_col)
    # Handles seconds with decimals, milliseconds, microseconds, nanoseconds.
    return f"""
    CASE
        WHEN abs(TRY_CAST({ts} AS DOUBLE)) >= 100000000000000000 THEN to_timestamp(TRY_CAST({ts} AS DOUBLE) / 1000000000.0)
        WHEN abs(TRY_CAST({ts} AS DOUBLE)) >= 100000000000000 THEN to_timestamp(TRY_CAST({ts} AS DOUBLE) / 1000000.0)
        WHEN abs(TRY_CAST({ts} AS DOUBLE)) >= 100000000000 THEN to_timestamp(TRY_CAST({ts} AS DOUBLE) / 1000.0)
        ELSE to_timestamp(TRY_CAST({ts} AS DOUBLE))
    END
    """


def optional_col_value(col: str | None, default_sql: str = "''") -> str:
    return f"CAST({qident(col)} AS VARCHAR)" if col else default_sql


def clean_location_expr(col: str | None, alias: str) -> str:
    if not col:
        return f"'Unknown' AS {alias}"
    c = qident(col)
    return f"COALESCE(NULLIF(NULLIF(NULLIF(TRIM(CAST({c} AS VARCHAR)), ''), '-'), '^'), 'Unknown') AS {alias}"


def device_type_expr(ua_col: str, platform_expr: str) -> str:
    ua = optional_col_value(ua_col)
    return f"""
    CASE
        WHEN lower(coalesce({platform_expr}, '')) LIKE '%android_tv%'
             OR lower(coalesce({ua}, '')) ~ 'smarttv|hismarttv|bravia|cloudtv|xstream|tata sky binge|\\btv\\b' THEN 'Smart TV'
        WHEN lower(coalesce({ua}, '')) LIKE '%android%' THEN 'Android'
        WHEN lower(coalesce({ua}, '')) LIKE '%iphone%' THEN 'iPhone'
        WHEN lower(coalesce({ua}, '')) LIKE '%ipad%' THEN 'iPad'
        WHEN lower(coalesce({ua}, '')) LIKE '%windows%' THEN 'Windows'
        WHEN lower(coalesce({ua}, '')) LIKE '%mac%' THEN 'Mac'
        ELSE 'Other'
    END
    """


def build_cache(args: argparse.Namespace) -> None:
    parquet_files = collect_parquet_files(args.input, recursive=args.recursive)
    if not parquet_files:
        raise SystemExit("No parquet files found. Check --input path(s).")

    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    log(f"Parquet files: {len(parquet_files):,}")
    log(f"Cache DB: {db_path}")
    log("Opening DuckDB...")
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(args.threads)}")
    con.execute("PRAGMA enable_object_cache")
    if args.memory_limit:
        con.execute(f"PRAGMA memory_limit={sql_str(args.memory_limit)}")
    if args.temp_directory:
        Path(args.temp_directory).mkdir(parents=True, exist_ok=True)
        con.execute(f"PRAGMA temp_directory={sql_str(str(Path(args.temp_directory).resolve()))}")

    files_sql = sql_list(parquet_files)
    qs = qident(args.query_col)
    path = qident(args.path_col)
    et = event_time_expr(args.time_col)

    log("Creating metadata tables...")
    con.execute("""
        CREATE TABLE IF NOT EXISTS cache_meta (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    meta = {
        "cache_version": CACHE_VERSION,
        "input_files": len(parquet_files),
        "query_col": args.query_col,
        "time_col": args.time_col,
        "path_col": args.path_col,
        "ua_col": args.ua_col,
        "ip_col": args.ip_col,
        "country_col": args.country_col,
        "region_col": args.region_col,
        "city_col": args.city_col,
        "device_id_note": "Uses queryStr device_id when present; otherwise estimated as md5(cliIP + UA).",
        "session_id_note": "Uses queryStr session_id when present; otherwise estimated as md5(estimated_device_id + date + hour).",
    }
    for k, v in meta.items():
        con.execute("INSERT OR REPLACE INTO cache_meta VALUES (?, ?, current_timestamp)", [k, str(v)])

    log("Registering file inventory...")
    inv_rows = []
    for f in parquet_files:
        p = Path(f)
        st = p.stat()
        inv_rows.append({
            "file_path": f,
            "file_name": p.name,
            "file_size": int(st.st_size),
            "modified_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
        })
    inv_df = pd.DataFrame(inv_rows)
    con.register("_inv_df", inv_df)
    con.execute("CREATE OR REPLACE TABLE source_files AS SELECT * FROM _inv_df")
    con.unregister("_inv_df")

    platform_expr = f"COALESCE(regexp_extract({qs}, '(?:^|&)platform=([^&]+)', 1), '')"
    device_name_expr = f"COALESCE(regexp_extract({qs}, '(?:^|&)device=([^&]+)', 1), '')"
    raw_channel_expr = f"""
        COALESCE(
            NULLIF(regexp_extract({qs}, '(?:^|&)channel=([^&]+)', 1), ''),
            NULLIF(regexp_extract({qs}, '(?:^|&)channel_name=([^&]+)', 1), ''),
            NULLIF(regexp_extract({path}, '(vglive-sk-[0-9]+)', 1), ''),
            'Unknown'
        )
    """

    log("Step 1/5: scanning distinct raw channel/platform/device combinations...")
    # No session/device filter here: your files often have blank queryStr, so channel should fall back to reqPath.
    con.execute(f"""
        CREATE OR REPLACE TABLE channel_raw_combos AS
        SELECT DISTINCT
            {raw_channel_expr} AS raw_channel,
            {platform_expr} AS platform,
            {device_name_expr} AS device_name
        FROM read_parquet({files_sql}, union_by_name=true)
        WHERE TRY_CAST({qident(args.time_col)} AS DOUBLE) IS NOT NULL
    """)
    combos = con.execute("SELECT raw_channel, platform, device_name FROM channel_raw_combos").df()
    log(f"Raw channel combos: {len(combos):,}")

    log("Step 2/5: applying QSA pure-channel cleaner to combos...")
    if combos.empty:
        map_df = pd.DataFrame(columns=["raw_channel", "platform", "device_name", "channel_name"])
    else:
        # IMPORTANT: keep the original raw key columns for the SQL join.
        # Older builder decoded these key columns before saving channel_clean_map.
        # Different encoded raw values could collapse to the same decoded value and
        # make channel_clean_map non-unique, causing request-count inflation when
        # joined back to raw rows. We compute channel_name from decoded copies,
        # but keep raw_channel/platform/device_name as the exact join key.
        map_df = combos.copy()
        for c in ["raw_channel", "platform", "device_name"]:
            map_df[c] = map_df[c].fillna("").astype(str)
        decoded = map_df[["raw_channel", "platform", "device_name"]].copy()
        for c in ["raw_channel", "platform", "device_name"]:
            decoded[c] = decoded[c].fillna("").astype(str).map(urllib.parse.unquote_plus)
        map_df["channel_name"] = decoded.apply(
            lambda r: normalize_channel_name_smart(r["raw_channel"], r["platform"], r["device_name"]),
            axis=1,
        )
        before = len(map_df)
        map_df = map_df.drop_duplicates(subset=["raw_channel", "platform", "device_name"], keep="first")
        dropped = before - len(map_df)
        if dropped:
            log(f"Deduped channel_clean_map join keys: dropped {dropped:,} duplicate mapping rows")
    con.register("_channel_map_df", map_df)
    con.execute("""
        CREATE OR REPLACE TABLE channel_clean_map AS
        SELECT raw_channel, platform, device_name, any_value(channel_name) AS channel_name
        FROM _channel_map_df
        GROUP BY 1,2,3
    """)
    con.unregister("_channel_map_df")

    dup_keys = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT raw_channel, platform, device_name, COUNT(*) AS n
            FROM channel_clean_map
            GROUP BY 1,2,3
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if dup_keys:
        raise RuntimeError(f"channel_clean_map still has {dup_keys:,} duplicate join keys; refusing to build inflated cache")

    log("Step 3/5: building compact cached base table behavior_device_day...")
    country = clean_location_expr(args.country_col, "loc_country")
    region = clean_location_expr(args.region_col, "loc_region")
    city = clean_location_expr(args.city_col, "loc_city")
    dtype = device_type_expr(args.ua_col, platform_expr)
    ua_value = optional_col_value(args.ua_col)
    ip_value = optional_col_value(args.ip_col)

    # The estimated device key is necessary for this inspected schema because actual device_id is missing.
    # It is still stable enough for city/channel/device analysis, but it is not true player telemetry.
    con.execute(f"""
        CREATE OR REPLACE TABLE behavior_device_day AS
        WITH extracted AS (
            SELECT
                ({et}) AS event_time,
                ({et})::DATE AS event_date,
                {country},
                {region},
                {city},
                {raw_channel_expr} AS raw_channel,
                {platform_expr} AS platform,
                {device_name_expr} AS device_name,
                {dtype} AS device_type,
                regexp_extract({qs}, '(?:^|&)device_id=([^&]+)', 1) AS q_device_id,
                regexp_extract({qs}, '(?:^|&)session_id=([^&]+)', 1) AS q_session_id,
                {ip_value} AS cli_ip,
                {ua_value} AS ua_text
            FROM read_parquet({files_sql}, union_by_name=true)
            WHERE TRY_CAST({qident(args.time_col)} AS DOUBLE) IS NOT NULL
        ), clean AS (
            SELECT
                *,
                CASE
                    WHEN q_device_id IS NOT NULL AND q_device_id <> '' THEN q_device_id
                    ELSE 'est_' || md5(coalesce(cli_ip, '') || '|' || coalesce(ua_text, ''))
                END AS final_device_id,
                CASE
                    WHEN q_device_id IS NOT NULL AND q_device_id <> '' THEN 'queryStr.device_id'
                    ELSE 'estimated_cliIP_UA'
                END AS device_id_source
            FROM extracted
            WHERE event_date BETWEEN DATE '2000-01-01' AND DATE '2100-01-01'
        ), clean2 AS (
            SELECT
                *,
                CASE
                    WHEN q_session_id IS NOT NULL AND q_session_id <> '' THEN q_session_id
                    ELSE 'est_' || md5(final_device_id || '|' || CAST(event_date AS VARCHAR) || '|' || CAST(EXTRACT('hour' FROM event_time) AS VARCHAR))
                END AS final_session_id,
                CASE
                    WHEN q_session_id IS NOT NULL AND q_session_id <> '' THEN 'queryStr.session_id'
                    ELSE 'estimated_device_date_hour'
                END AS session_id_source
            FROM clean
            WHERE final_device_id IS NOT NULL AND final_device_id <> ''
        )
        SELECT
            c.event_date,
            c.loc_country AS country,
            c.loc_region AS region,
            c.loc_city AS city,
            COALESCE(m.channel_name, lower(c.raw_channel), 'unknown') AS channel_name,
            c.raw_channel,
            c.platform,
            c.device_name,
            c.device_type,
            c.final_device_id AS device_id,
            c.final_session_id AS session_id,
            c.device_id_source,
            c.session_id_source,
            COUNT(*) AS requests
        FROM clean2 c
        LEFT JOIN channel_clean_map m
          ON c.raw_channel = m.raw_channel
         AND c.platform = m.platform
         AND c.device_name = m.device_name
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    """)

    log("Step 4/5: optimizing cached table...")
    try:
        con.execute("CREATE OR REPLACE TABLE behavior_device_day AS SELECT * FROM behavior_device_day ORDER BY event_date, country, region, city")
    except Exception as e:
        log(f"WARNING: sort optimization skipped: {e}")

    log("Step 5/5: building small summary tables...")
    con.execute("""
        CREATE OR REPLACE TABLE available_dates AS
        SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date,
               COUNT(*) AS cached_rows,
               COUNT(DISTINCT event_date) AS active_days,
               SUM(requests) AS raw_requests
        FROM behavior_device_day
    """)
    con.execute("""
        CREATE OR REPLACE TABLE cache_quality AS
        SELECT
            device_id_source,
            session_id_source,
            SUM(requests) AS requests,
            COUNT(DISTINCT device_id) AS devices,
            COUNT(DISTINCT session_id) AS sessions
        FROM behavior_device_day
        GROUP BY 1,2
        ORDER BY requests DESC
    """)
    con.execute("""
        CREATE OR REPLACE TABLE daily_country_summary AS
        SELECT event_date, country,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT device_id) AS devices,
               COUNT(DISTINCT region) AS regions,
               COUNT(DISTINCT city) AS cities,
               COUNT(DISTINCT channel_name) AS channels,
               COUNT(DISTINCT device_type) AS device_types
        FROM behavior_device_day
        GROUP BY 1,2
    """)
    con.execute("""
        CREATE OR REPLACE TABLE daily_region_summary AS
        SELECT event_date, country, region,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT device_id) AS devices,
               COUNT(DISTINCT city) AS cities,
               COUNT(DISTINCT channel_name) AS channels,
               COUNT(DISTINCT device_type) AS device_types
        FROM behavior_device_day
        GROUP BY 1,2,3
    """)
    con.execute("""
        CREATE OR REPLACE TABLE daily_city_summary AS
        SELECT event_date, country, region, city,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT device_id) AS devices,
               COUNT(DISTINCT channel_name) AS channels,
               COUNT(DISTINCT device_type) AS device_types
        FROM behavior_device_day
        GROUP BY 1,2,3,4
    """)
    con.execute("""
        CREATE OR REPLACE TABLE daily_device_type_summary AS
        SELECT event_date, device_type,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT device_id) AS devices,
               COUNT(DISTINCT country) AS countries,
               COUNT(DISTINCT region) AS regions,
               COUNT(DISTINCT city) AS cities,
               COUNT(DISTINCT channel_name) AS channels
        FROM behavior_device_day
        GROUP BY 1,2
    """)
    con.execute("""
        CREATE OR REPLACE TABLE daily_city_channel_device_type AS
        SELECT event_date, country, region, city, channel_name, device_type,
               SUM(requests) AS requests,
               COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT device_id) AS devices
        FROM behavior_device_day
        GROUP BY 1,2,3,4,5,6
    """)

    row = con.execute("SELECT * FROM available_dates").fetchone()
    base_rows = con.execute("SELECT COUNT(*) FROM behavior_device_day").fetchone()[0]
    quality = con.execute("SELECT * FROM cache_quality").df()

    log("Validation: comparing cached SUM(requests) to raw parquet COUNT(*)...")
    raw_count = con.execute(f"SELECT COUNT(*) FROM read_parquet({files_sql}, union_by_name=true)").fetchone()[0]
    cached_requests = int(row[4] or 0)
    diff = cached_requests - int(raw_count or 0)

    con.execute("INSERT OR REPLACE INTO cache_meta VALUES ('raw_count_validation', ?, current_timestamp)", [str(raw_count)])
    con.execute("INSERT OR REPLACE INTO cache_meta VALUES ('cached_requests_validation', ?, current_timestamp)", [str(cached_requests)])
    con.execute("INSERT OR REPLACE INTO cache_meta VALUES ('request_count_diff', ?, current_timestamp)", [str(diff)])

    log("DONE")
    log(f"Available dates: {row[0]} -> {row[1]} | active days: {row[3]:,}")
    log(f"Cached base rows: {base_rows:,}")
    log(f"Raw parquet rows: {int(raw_count or 0):,}")
    log(f"Raw requests represented: {cached_requests:,}")
    if diff == 0:
        log("Validation PASS: cached requests exactly match raw parquet row count.")
    else:
        log(f"Validation WARNING: cached requests differ from raw count by {diff:,}. Run validate_pDash_cache.py before trusting dashboard totals.")
    log("Device/session source breakdown:")
    print(quality.to_string(index=False), flush=True)
    log(f"Viewer command: streamlit run pDash_summary_viewer_v2.py -- --db {db_path}")
    con.close()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build DuckDB summary cache for pDash big Parquet analysis")
    ap.add_argument("--input", nargs="+", required=True, help="Parquet file or folder path(s)")
    ap.add_argument("--recursive", action="store_true", help="Find parquet files recursively under input folders")
    ap.add_argument("--db", default="pDash_analytics_cache.duckdb", help="Output DuckDB cache path")
    ap.add_argument("--query-col", default="queryStr")
    ap.add_argument("--time-col", default="reqTimeSec")
    ap.add_argument("--path-col", default="reqPath")
    ap.add_argument("--ua-col", default="UA")
    ap.add_argument("--ip-col", default="cliIP")
    ap.add_argument("--country-col", default="country")
    ap.add_argument("--region-col", default="state", help="Your inspected files use state, not region.")
    ap.add_argument("--city-col", default="city")
    ap.add_argument("--threads", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    ap.add_argument("--memory-limit", default="", help="DuckDB memory limit, e.g. 12GB. Empty = DuckDB default")
    ap.add_argument("--temp-directory", default="", help="Fast local temp directory for DuckDB spills")
    return ap.parse_args()


if __name__ == "__main__":
    try:
        build_cache(parse_args())
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise SystemExit(130)
