"""Rebuild FAST concurrency summary from existing minute and identity marts."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONCURRENCY_DIR = ETL_ROOT / "output" / "watch_hours" / "concurrency"


def q(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild concurrency_summary.parquet from concurrency_minute.parquet.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_CONCURRENCY_DIR)
    args = parser.parse_args()

    data_dir = args.data_dir.expanduser().resolve()
    minute = data_dir / "concurrency_minute.parquet"
    identity = data_dir / "fast_platform_channel_identity_daily.parquet"
    output = data_dir / "concurrency_summary.parquet"
    tmp = output.with_name(f"{output.stem}.rebuilt.tmp{output.suffix}")

    if not minute.exists():
        raise SystemExit(f"Minute mart not found: {minute}")
    if not identity.exists():
        raise SystemExit(f"Identity mart not found: {identity}")

    con = duckdb.connect()
    try:
        con.execute("SET preserve_insertion_order=false")
        sql = f"""
        WITH minute_summary AS (
            SELECT
                log_date,
                source,
                platform_key,
                platform_name,
                candidate_id,
                channel_name,
                any_value(reqHost ORDER BY reqHost) AS reqHost,
                max(distinct_hosts)::BIGINT AS distinct_hosts,
                count(*)::BIGINT AS minute_count,
                sum(raw_ts_rows)::BIGINT AS raw_ts_rows,
                sum(status_200_ts_rows)::BIGINT AS status_200_ts_rows,
                round(avg(unique_viewers), 3) AS avg_unique_viewers,
                max(unique_viewers)::BIGINT AS peak_unique_viewers,
                any_value(minute_ist ORDER BY unique_viewers DESC, minute_ist) AS peak_unique_viewers_minute_ist,
                round(quantile_cont(unique_viewers, 0.95), 3) AS p95_unique_viewers,
                round(avg(unique_ua_viewers), 3) AS avg_unique_ua_viewers,
                max(unique_ua_viewers)::BIGINT AS peak_unique_ua_viewers,
                any_value(minute_ist ORDER BY unique_ua_viewers DESC, minute_ist) AS peak_unique_ua_minute_ist,
                round(quantile_cont(unique_ua_viewers, 0.95), 3) AS p95_unique_ua_viewers,
                round(avg(segment_viewers_estimate), 3) AS avg_segment_viewers_estimate,
                max(segment_viewers_estimate) AS peak_segment_viewers_estimate,
                any_value(minute_ist ORDER BY segment_viewers_estimate DESC, minute_ist) AS peak_segment_minute_ist,
                round(avg(status_200_segment_viewers_estimate), 3) AS avg_status_200_segment_viewers_estimate,
                max(status_200_segment_viewers_estimate) AS peak_status_200_segment_viewers_estimate
            FROM read_parquet('{q(minute)}')
            GROUP BY 1,2,3,4,5,6
        ),
        identity AS (
            SELECT
                log_date,
                source,
                platform_key,
                platform_name,
                candidate_id,
                channel_name,
                sum(distinct_cliips)::BIGINT AS distinct_cliips,
                sum(distinct_uas)::BIGINT AS distinct_uas,
                sum(distinct_ipua_pairs)::BIGINT AS distinct_ipua_pairs
            FROM read_parquet('{q(identity)}')
            GROUP BY 1,2,3,4,5,6
        )
        SELECT
            m.*,
            coalesce(i.distinct_cliips, 0)::BIGINT AS distinct_cliips,
            coalesce(i.distinct_uas, 0)::BIGINT AS distinct_uas,
            coalesce(i.distinct_ipua_pairs, 0)::BIGINT AS distinct_ipua_pairs
        FROM minute_summary m
        LEFT JOIN identity i USING (log_date, source, platform_key, platform_name, candidate_id, channel_name)
        ORDER BY log_date, platform_name, channel_name
        """
        tmp.unlink(missing_ok=True)
        con.execute(f"COPY ({sql}) TO '{q(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        tmp.replace(output)
        stats = con.execute(
            f"""
            SELECT
                count(*) AS rows,
                min(log_date) AS first_date,
                max(log_date) AS last_date,
                sum(raw_ts_rows)::BIGINT AS raw_ts_rows
            FROM read_parquet('{q(output)}')
            """
        ).fetchdf()
    finally:
        con.close()

    print(f"Rebuilt concurrency summary: {output}")
    print(stats.to_string(index=False))


if __name__ == "__main__":
    main()
