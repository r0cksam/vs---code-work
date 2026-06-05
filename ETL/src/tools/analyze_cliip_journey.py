"""Analyze one exported raw cliIP parquet into behavior/journey parquet tables."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT_ROOT = ETL_ROOT / "output" / "exports" / "cliip_journey"


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def safe_stem(path: Path) -> str:
    return path.stem[:160].strip("_") or "cliip_journey"


def copy_parquet(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(
        f"""
        COPY ({sql})
        TO '{q(tmp_path)}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    tmp_path.replace(out_path)


def source_sql(input_path: Path) -> str:
    return f"""
    SELECT
        log_date,
        source,
        req_time_ist,
        COALESCE(NULLIF(channel_name, ''), 'Unknown / NA') AS channel_name,
        country,
        state,
        city,
        cliIP,
        COALESCE(NULLIF(session_id, ''), 'Unknown / NA') AS session_id,
        COALESCE(NULLIF(device_id, ''), 'Unknown / NA') AS device_id,
        COALESCE(NULLIF(platform, ''), 'Unknown / NA') AS platform,
        COALESCE(NULLIF(device_type, ''), 'Unknown / NA') AS device_type,
        UA,
        reqHost,
        candidate_id,
        reqPath,
        extension,
        regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', '') AS statusCode,
        COALESCE(is_ts, false) AS is_ts,
        COALESCE(is_playlist, false) AS is_playlist,
        CAST(COALESCE(raw_watch_hours, 0) AS DOUBLE) AS raw_watch_hours,
        CAST(COALESCE(status_200_watch_hours, 0) AS DOUBLE) AS status_200_watch_hours
    FROM read_parquet('{q(input_path)}')
    WHERE req_time_ist IS NOT NULL
    """


def aggregate_select(group_cols: list[str]) -> str:
    groups = ",\n        ".join(group_cols)
    return f"""
    SELECT
        {groups},
        COUNT(*)::BIGINT AS row_count,
        SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
        SUM(CASE WHEN is_playlist THEN 1 ELSE 0 END)::BIGINT AS playlist_rows,
        SUM(CASE WHEN statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_rows,
        SUM(CASE WHEN statusCode <> '200' OR statusCode IS NULL THEN 1 ELSE 0 END)::BIGINT AS non_200_rows,
        ROUND(SUM(raw_watch_hours), 6) AS raw_watch_hours,
        ROUND(SUM(raw_watch_hours) * 60, 3) AS raw_watch_minutes,
        ROUND(SUM(status_200_watch_hours), 6) AS status_200_watch_hours,
        ROUND(SUM(status_200_watch_hours) * 60, 3) AS status_200_watch_minutes,
        MIN(req_time_ist) AS first_seen_ist,
        MAX(req_time_ist) AS last_seen_ist,
        ROUND(date_diff('millisecond', MIN(req_time_ist), MAX(req_time_ist)) / 60000.0, 3) AS wall_clock_minutes,
        COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
        COUNT(DISTINCT candidate_id)::BIGINT AS distinct_candidates,
        COUNT(DISTINCT statusCode)::BIGINT AS distinct_status_codes,
        ANY_VALUE(reqHost) AS sample_reqHost,
        ANY_VALUE(reqPath) AS sample_reqPath
    FROM raw
    GROUP BY {", ".join(str(i + 1) for i in range(len(group_cols)))}
    """


def date_channel_sql() -> str:
    return f"""
    WITH raw AS ({source_sql_placeholder()})
    {aggregate_select(["log_date", "channel_name"])}
    ORDER BY log_date, row_count DESC, raw_watch_minutes DESC, channel_name
    """


def source_sql_placeholder() -> str:
    return "__SOURCE_SQL__"


def materialized_query(input_path: Path, body: str) -> str:
    return body.replace(source_sql_placeholder(), source_sql(input_path))


def build_date_channel_sql(input_path: Path) -> str:
    return materialized_query(
        input_path,
        f"""
        WITH raw AS ({source_sql_placeholder()})
        {aggregate_select(["log_date", "channel_name"])}
        ORDER BY log_date, row_count DESC, raw_watch_minutes DESC, channel_name
        """,
    )


def build_hourly_sql(input_path: Path) -> str:
    return materialized_query(
        input_path,
        f"""
        WITH raw AS ({source_sql_placeholder()})
        {aggregate_select([
            "log_date",
            "CAST(strftime(req_time_ist, '%H') AS INTEGER) AS hour_ist",
            "date_trunc('hour', req_time_ist) AS hour_start_ist",
            "channel_name",
        ])}
        ORDER BY log_date, hour_ist, row_count DESC, raw_watch_minutes DESC, channel_name
        """,
    )


def build_bucket_sql(input_path: Path, bucket_minutes: int) -> str:
    bucket_minutes = max(1, min(60, int(bucket_minutes)))
    bucket_expr = (
        "date_trunc('hour', req_time_ist) + "
        f"CAST(FLOOR(date_part('minute', req_time_ist) / {bucket_minutes}) * {bucket_minutes} AS INTEGER) "
        "* INTERVAL 1 MINUTE"
    )
    return materialized_query(
        input_path,
        f"""
        WITH raw AS ({source_sql_placeholder()})
        {aggregate_select([
            "log_date",
            f"{bucket_expr} AS bucket_start_ist",
            "channel_name",
        ])}
        ORDER BY log_date, bucket_start_ist, row_count DESC, raw_watch_minutes DESC, channel_name
        """,
    )


def build_journey_sql(input_path: Path, gap_seconds: int) -> str:
    gap_seconds = max(1, int(gap_seconds))
    return materialized_query(
        input_path,
        f"""
        WITH raw AS ({source_sql_placeholder()}),
        ordered AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    ORDER BY req_time_ist, channel_name, reqHost, reqPath, statusCode
                ) AS rn
            FROM raw
        ),
        marked AS (
            SELECT
                *,
                CASE
                    WHEN LAG(req_time_ist) OVER w IS NULL THEN 1
                    WHEN channel_name <> LAG(channel_name) OVER w THEN 1
                    WHEN date_diff('second', LAG(req_time_ist) OVER w, req_time_ist) > {gap_seconds} THEN 1
                    ELSE 0
                END AS new_segment
            FROM ordered
            WINDOW w AS (ORDER BY rn)
        ),
        segmented AS (
            SELECT
                *,
                SUM(new_segment) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS segment_id
            FROM marked
        ),
        segments AS (
            SELECT
                segment_id::BIGINT AS segment_id,
                MIN(log_date) AS start_date,
                MAX(log_date) AS end_date,
                MIN(req_time_ist) AS start_time_ist,
                MAX(req_time_ist) AS end_time_ist,
                CAST(strftime(MIN(req_time_ist), '%H') AS INTEGER) AS start_hour_ist,
                channel_name,
                COUNT(*)::BIGINT AS row_count,
                SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
                SUM(CASE WHEN is_playlist THEN 1 ELSE 0 END)::BIGINT AS playlist_rows,
                SUM(CASE WHEN statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_rows,
                SUM(CASE WHEN statusCode <> '200' OR statusCode IS NULL THEN 1 ELSE 0 END)::BIGINT AS non_200_rows,
                ROUND(SUM(raw_watch_hours), 6) AS raw_watch_hours,
                ROUND(SUM(raw_watch_hours) * 60, 3) AS raw_watch_minutes,
                ROUND(SUM(status_200_watch_hours), 6) AS status_200_watch_hours,
                ROUND(SUM(status_200_watch_hours) * 60, 3) AS status_200_watch_minutes,
                ROUND(date_diff('millisecond', MIN(req_time_ist), MAX(req_time_ist)) / 60000.0, 3) AS wall_clock_minutes,
                COUNT(DISTINCT reqHost)::BIGINT AS distinct_hosts,
                COUNT(DISTINCT candidate_id)::BIGINT AS distinct_candidates,
                COUNT(DISTINCT statusCode)::BIGINT AS distinct_status_codes,
                ANY_VALUE(reqHost) AS sample_reqHost,
                ANY_VALUE(reqPath) AS sample_reqPath
            FROM segmented
            GROUP BY segment_id, channel_name
        )
        SELECT
            *,
            date_diff(
                'second',
                end_time_ist,
                LEAD(start_time_ist) OVER (ORDER BY segment_id)
            ) AS gap_to_next_seconds
        FROM segments
        ORDER BY segment_id
        """,
    )


def build_daily_journey_sql(input_path: Path, gap_seconds: int) -> str:
    journey = build_journey_sql(input_path, gap_seconds)
    return f"""
    WITH journey AS ({journey})
    SELECT
        start_date AS log_date,
        COUNT(*)::BIGINT AS journey_segments,
        COUNT(DISTINCT channel_name)::BIGINT AS distinct_channels,
        MIN(start_time_ist) AS first_seen_ist,
        MAX(end_time_ist) AS last_seen_ist,
        ROUND(SUM(raw_watch_minutes), 3) AS raw_watch_minutes,
        ROUND(SUM(status_200_watch_minutes), 3) AS status_200_watch_minutes,
        ROUND(SUM(wall_clock_minutes), 3) AS wall_clock_minutes,
        SUM(row_count)::BIGINT AS row_count,
        SUM(ts_rows)::BIGINT AS ts_rows,
        SUM(playlist_rows)::BIGINT AS playlist_rows
    FROM journey
    GROUP BY start_date
    ORDER BY log_date
    """


def write_manifest(out_dir: Path, input_path: Path, outputs: dict[str, Path], args: argparse.Namespace) -> None:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": str(input_path),
        "gap_seconds": args.gap_seconds,
        "bucket_minutes": args.bucket_minutes,
        "outputs": {key: str(value) for key, value in outputs.items()},
    }
    path = out_dir / "analysis_manifest.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def print_summary(con: duckdb.DuckDBPyConnection, outputs: dict[str, Path]) -> None:
    print("\nSummary")
    print(con.execute(
        """
        SELECT
            COUNT(*) AS rows,
            MIN(log_date) AS min_date,
            MAX(log_date) AS max_date,
            COUNT(DISTINCT log_date) AS days,
            ROUND(SUM(raw_watch_minutes), 3) AS raw_watch_minutes,
            ROUND(SUM(status_200_watch_minutes), 3) AS status_200_watch_minutes
        FROM read_parquet(?)
        """,
        [str(outputs["date_channel_volume"])],
    ).fetchdf().to_string(index=False))
    print("\nTop date/channel volume")
    print(con.execute(
        """
        SELECT log_date, channel_name, row_count, raw_watch_minutes, status_200_watch_minutes
        FROM read_parquet(?)
        ORDER BY row_count DESC
        LIMIT 15
        """,
        [str(outputs["date_channel_volume"])],
    ).fetchdf().to_string(index=False))
    print("\nLongest journey segments")
    print(con.execute(
        """
        SELECT start_time_ist, end_time_ist, channel_name, row_count, raw_watch_minutes, wall_clock_minutes
        FROM read_parquet(?)
        ORDER BY raw_watch_minutes DESC, wall_clock_minutes DESC
        LIMIT 15
        """,
        [str(outputs["journey_segments"])],
    ).fetchdf().to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a raw cliIP parquet into behavior/journey parquet outputs.")
    parser.add_argument("--input", required=True, type=Path, help="Raw cliIP parquet exported by export_raw_cliip_parquet.py.")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output folder. Defaults under output/exports/cliip_journey.")
    parser.add_argument("--gap-seconds", type=int, default=600, help="Start a new journey segment after this idle gap.")
    parser.add_argument("--bucket-minutes", type=int, default=15, help="Time bucket size for time_bucket_channel_volume.")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    args = parser.parse_args()

    input_path = args.input.expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"Input parquet not found: {input_path}")

    out_dir = (args.out_dir.expanduser().resolve() if args.out_dir else DEFAULT_OUT_ROOT / safe_stem(input_path))
    out_dir.mkdir(parents=True, exist_ok=True)
    args.temp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET threads={max(1, args.threads)}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute(f"SET temp_directory={sql_text(q(args.temp_dir))}")
    con.execute("SET preserve_insertion_order=false")

    outputs = {
        "date_channel_volume": out_dir / "date_channel_volume.parquet",
        "hourly_channel_volume": out_dir / "hourly_channel_volume.parquet",
        "time_bucket_channel_volume": out_dir / "time_bucket_channel_volume.parquet",
        "journey_segments": out_dir / "journey_segments.parquet",
        "daily_journey_summary": out_dir / "daily_journey_summary.parquet",
    }

    print(f"Input : {input_path}")
    print(f"Out   : {out_dir}")
    print(f"Gap   : {args.gap_seconds} seconds")

    jobs = [
        ("date_channel_volume", build_date_channel_sql(input_path)),
        ("hourly_channel_volume", build_hourly_sql(input_path)),
        ("time_bucket_channel_volume", build_bucket_sql(input_path, args.bucket_minutes)),
        ("journey_segments", build_journey_sql(input_path, args.gap_seconds)),
        ("daily_journey_summary", build_daily_journey_sql(input_path, args.gap_seconds)),
    ]

    for name, sql in jobs:
        print(f"[write] {name}")
        copy_parquet(con, sql, outputs[name])

    write_manifest(out_dir, input_path, outputs, args)
    print_summary(con, outputs)

    print("\nOutputs")
    for name, path in outputs.items():
        size_mb = path.stat().st_size / 1024 / 1024 if path.exists() else 0
        print(f"  {name}: {path} ({size_mb:.2f} MB)")
    print(f"  manifest: {out_dir / 'analysis_manifest.json'}")


if __name__ == "__main__":
    main()
