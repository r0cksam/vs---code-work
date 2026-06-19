"""Build FAST platform/channel decoded UA device daily aggregates from .ts rows."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from build_concurrency import (
    DEFAULT_LAKE_FOLDER,
    DEFAULT_OUT_DIR,
    checked_dates,
    connect,
    date_filter_sql,
    minute_ist_sql,
    platform_key_sql,
    platform_name_sql,
    q,
    source_filter,
    table_count,
    write_append_table,
)
from vglive_core import channel_candidate_sql


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOOKUP = ETL_ROOT / "output" / "device_decode" / "ua_decode_lookup_both_all.parquet"
HOURS_PER_TS_SEGMENT = 6 / 3600


LOOKUP_COLUMNS = [
    "ua_norm",
    "decode_status",
    "decoder_method",
    "confidence",
    "device_type",
    "form_factor",
    "brand",
    "model",
    "model_code",
    "product_family",
    "generation",
    "os_name",
    "os_version",
    "os_family",
    "browser_name",
    "browser_version",
    "browser_engine",
    "app_player",
    "api_device_type",
    "api_brand",
    "api_model",
    "api_os_name",
    "api_browser_name",
]


def clean_expr(column: str, fallback: str) -> str:
    return f"""
CASE
    WHEN NULLIF(trim(COALESCE({column}, '')), '') IS NULL THEN '{fallback}'
    WHEN lower(trim(COALESCE({column}, ''))) IN ('nan', 'none', 'null', 'unknown / na', 'unknown') THEN '{fallback}'
    ELSE trim(COALESCE({column}, ''))
END
"""


def ua_norm_sql(column_expr: str) -> str:
    return (
        "NULLIF(trim(regexp_replace(regexp_replace("
        f"COALESCE(try(url_decode(CAST({column_expr} AS VARCHAR))), CAST({column_expr} AS VARCHAR)), "
        "'\\+', ' ', 'g'), '\\s+', ' ', 'g')), '')"
    )


def ensure_lookup_table(con, lookup_path: Path) -> None:
    if lookup_path.exists():
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE ua_lookup_raw AS
            SELECT * FROM read_parquet('{q(lookup_path)}')
            """
        )
        existing_cols = [str(row[0]) for row in con.execute("DESCRIBE ua_lookup_raw").fetchall()]
        for col in LOOKUP_COLUMNS:
            if col not in existing_cols:
                con.execute(f"ALTER TABLE ua_lookup_raw ADD COLUMN {col} VARCHAR")
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE ua_lookup AS
            SELECT *
            FROM (
                SELECT
                    ua_norm,
                    decode_status,
                    decoder_method,
                    confidence,
                    device_type,
                    form_factor,
                    brand,
                    model,
                    model_code,
                    product_family,
                    generation,
                    os_name,
                    os_version,
                    os_family,
                    browser_name,
                    browser_version,
                    browser_engine,
                    app_player,
                    api_device_type,
                    api_brand,
                    api_model,
                    api_os_name,
                    api_browser_name,
                    row_number() OVER (PARTITION BY ua_norm ORDER BY ua_norm) AS rn
                FROM ua_lookup_raw
                WHERE NULLIF(trim(COALESCE(ua_norm, '')), '') IS NOT NULL
            )
            WHERE rn = 1
            """
        )
        return

    empty = pd.DataFrame(columns=LOOKUP_COLUMNS)
    con.register("ua_lookup_df", empty)
    con.execute("CREATE OR REPLACE TEMP TABLE ua_lookup AS SELECT * FROM ua_lookup_df")


def build_device_table(con, args: argparse.Namespace) -> None:
    start, end = checked_dates(args)
    lake_glob = q(args.lake / "**" / "*.parquet")
    partition_filter = date_filter_sql(start, end)
    candidate_expr = channel_candidate_sql("reqPath")
    hours_expr = f"{HOURS_PER_TS_SEGMENT:.16f}"
    ua_norm_expr = ua_norm_sql("UA")

    ensure_lookup_table(con, args.ua_lookup)

    brand_label = f"""
CASE
    WHEN decode_status = 'malformed' THEN 'Malformed / Noise'
    WHEN decode_status IN ('unknown', 'not_in_lookup') THEN 'Unknown / Needs Review'
    ELSE {clean_expr('brand', 'Brand Not Exposed In UA')}
END
"""
    model_label = f"""
CASE
    WHEN decode_status = 'malformed' THEN 'Malformed / Noise'
    WHEN decode_status IN ('unknown', 'not_in_lookup') THEN 'Unknown / Needs Review'
    ELSE {clean_expr('model', 'Model Not Exposed In UA')}
END
"""
    os_label = """
CASE
    WHEN NULLIF(trim(COALESCE(os_name, '') || ' ' || COALESCE(os_version, '')), '') IS NULL THEN 'OS Not Exposed In UA'
    ELSE trim(COALESCE(os_name, '') || ' ' || COALESCE(os_version, ''))
END
"""
    browser_label = """
CASE
    WHEN NULLIF(trim(COALESCE(browser_name, '') || ' ' || COALESCE(browser_version, '')), '') IS NULL THEN 'Browser Not Exposed In UA'
    ELSE trim(COALESCE(browser_name, '') || ' ' || COALESCE(browser_version, ''))
END
"""
    decode_quality = """
CASE
    WHEN decode_status = 'decoded_local' THEN 'Verified Local Decode'
    WHEN decode_status = 'decoded_api'
      AND (NULLIF(trim(COALESCE(api_brand, '')), '') IS NOT NULL OR NULLIF(trim(COALESCE(api_model, '')), '') IS NOT NULL)
        THEN 'API Brand/Model Enriched'
    WHEN decode_status = 'decoded_api' AND NULLIF(trim(COALESCE(api_device_type, '')), '') IS NOT NULL THEN 'API Enriched'
    WHEN decode_status = 'decoded_api' THEN 'API Generic / Weak'
    WHEN decode_status = 'malformed' THEN 'Malformed / Noise'
    WHEN decode_status = 'unknown' THEN 'Unknown / Needs Review'
    ELSE 'Not In UA Lookup'
END
"""

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE fast_platform_channel_ua_device_new AS
        WITH base AS (
            SELECT
                COALESCE(CAST(source AS VARCHAR), 'stream') AS source,
                strftime({minute_ist_sql("reqTimeSec")}, '%Y-%m-%d') AS log_date,
                lower(COALESCE(reqHost, '')) AS reqHost,
                COALESCE(NULLIF(cliIP, ''), NULL) AS cliIP,
                {ua_norm_expr} AS ua_norm,
                COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', ''), ''), 'Unknown') AS statusCode,
                {candidate_expr} AS candidate_id
            FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
            WHERE {source_filter(args.source)}
              AND ({partition_filter})
              AND lower(COALESCE(reqPath, '')) LIKE '%.ts'
              AND TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
        ),
        resolved AS (
            SELECT
                b.*,
                COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name,
                {platform_name_sql("b.reqHost")} AS platform_name,
                {platform_key_sql("b.reqHost")} AS platform_key
            FROM base b
            LEFT JOIN host_map h ON b.reqHost = h.reqHost
            LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
        ),
        enriched AS (
            SELECT
                r.*,
                COALESCE(NULLIF(trim(l.decode_status), ''), 'not_in_lookup') AS decode_status,
                {clean_expr('l.device_type', 'Unknown Device Type')} AS device_type_label,
                {clean_expr('l.form_factor', 'Unknown Form Factor')} AS form_factor_label,
                {brand_label} AS brand_label,
                {model_label} AS model_label,
                {clean_expr('l.model_code', 'Model Code Not Exposed In UA')} AS model_code_label,
                {clean_expr('l.product_family', 'Product Family Not Exposed In UA')} AS product_family_label,
                {clean_expr('l.generation', 'Generation Not Exposed In UA')} AS generation_label,
                {os_label} AS os_label,
                {clean_expr('l.os_family', 'OS Family Not Exposed In UA')} AS os_family_label,
                {browser_label} AS browser_label,
                {clean_expr('l.app_player', 'Player Not Exposed In UA')} AS app_player_label,
                {decode_quality} AS decode_quality
            FROM resolved r
            LEFT JOIN ua_lookup l ON r.ua_norm = l.ua_norm
        )
        SELECT
            log_date,
            source,
            platform_key,
            platform_name,
            candidate_id,
            channel_name,
            any_value(reqHost ORDER BY reqHost) AS reqHost,
            COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
            decode_status,
            decode_quality,
            device_type_label,
            form_factor_label,
            brand_label,
            model_label,
            model_code_label,
            product_family_label,
            generation_label,
            os_label,
            os_family_label,
            browser_label,
            app_player_label,
            COUNT(*)::BIGINT AS raw_ts_rows,
            COUNT(*) FILTER (WHERE statusCode = '200')::BIGINT AS status_200_ts_rows,
            COUNT(*) * {hours_expr} AS raw_watch_hours,
            COUNT(*) FILTER (WHERE statusCode = '200') * {hours_expr} AS status_200_watch_hours,
            COUNT(DISTINCT cliIP)::BIGINT AS approx_unique_ips,
            COUNT(DISTINCT ua_norm)::BIGINT AS distinct_uas,
            COUNT(DISTINCT COALESCE(cliIP, '') || '|' || COALESCE(ua_norm, ''))::BIGINT AS distinct_ipua_pairs
        FROM enriched
        GROUP BY 1,2,3,4,5,6,9,10,11,12,13,14,15,16,17,18,19,20,21
        """
    )


def write_manifest(args: argparse.Namespace, new_rows: int, output_path: Path) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "lake": str(args.lake.resolve()),
        "ua_lookup": str(args.ua_lookup.resolve()) if args.ua_lookup.exists() else "",
        "output": str(output_path.resolve()),
        "date_range_replaced": {
            "start": args.start or "",
            "end": args.end or "",
        },
        "metric_notes": {
            "raw_watch_hours": "All .ts segment rows multiplied by 6 seconds.",
            "status_200_watch_hours": "Status 200 .ts segment rows multiplied by 6 seconds.",
            "approx_unique_ips": "Exact daily distinct cliIP for each FAST platform/channel/decoded device bucket on .ts rows.",
            "distinct_ipua_pairs": "Exact daily distinct cliIP + normalized User-Agent pair for each bucket.",
        },
        "new_rows": new_rows,
    }
    manifest_path = output_path.with_name("fast_platform_channel_ua_device_daily_manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FAST platform/channel decoded UA device daily mart.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--ua-lookup", type=Path, default=DEFAULT_LOOKUP)
    parser.add_argument("--source", choices=["fast"], default="fast")
    parser.add_argument("--start", default=None, help="IST lake date start, YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="IST lake date end, YYYY-MM-DD.")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--temp-dir", type=Path, default=Path(__file__).resolve().parents[2] / "output" / "cache" / "duckdb_temp")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    args.ua_lookup = args.ua_lookup.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    start, end = checked_dates(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(args)
    try:
        build_device_table(con, args)
        new_rows = table_count(con, "fast_platform_channel_ua_device_new")
        if new_rows <= 0:
            raise SystemExit("No FAST .ts rows found for the selected platform/channel UA device range.")

        output_path = args.out_dir / "fast_platform_channel_ua_device_daily.parquet"
        write_append_table(con, "fast_platform_channel_ua_device_new", output_path, args.source, start, end)
        write_manifest(args, new_rows, output_path)
    finally:
        con.close()

    print(f"FAST platform/channel UA device parquet: {output_path}")
    print(json.dumps({"new_rows": new_rows}, indent=2))


if __name__ == "__main__":
    main()
