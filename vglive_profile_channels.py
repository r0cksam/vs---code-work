from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from vglive_core import channel_candidate_sql


DEFAULT_LAKE = Path(os.getenv("VG_DASH_LAKE_ROOT", os.getenv("VG_ETL_LAKE_ROOT", str(Path.home() / "Veto Stream Logs" / "lake"))))
DEFAULT_OUT = Path(os.getenv("VG_DASH_PROFILE_DIR", Path.home() / "Veto Stream Logs Profile"))


def q(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def build_candidate_sql(parquet_glob: str) -> str:
    candidate_expr = channel_candidate_sql("reqPath")
    return f"""
WITH base AS (
    SELECT
        lower(reqHost) AS reqHost,
        lower(split_part(ltrim(reqPath, '/'), '/', 1)) AS path_seg1,
        lower(split_part(ltrim(reqPath, '/'), '/', 2)) AS path_seg2,
        {candidate_expr} AS candidate_id,
        cliIP,
        reqPath,
        CAST(reqTimeSec AS DOUBLE) AS req_epoch
    FROM read_parquet('{parquet_glob}', hive_partitioning=1)
    WHERE statusCode = '200' AND reqPath LIKE '%.ts'
)
SELECT
    reqHost,
    candidate_id,
    any_value(path_seg1) AS sample_path_seg1,
    any_value(path_seg2) AS sample_path_seg2,
    COUNT(DISTINCT path_seg1) AS distinct_path_seg1,
    COUNT(DISTINCT path_seg2) AS distinct_path_seg2,
    COUNT(*) AS chunks,
    COUNT(DISTINCT cliIP) AS unique_viewers,
    MIN(to_timestamp(req_epoch)) AS first_seen,
    MAX(to_timestamp(req_epoch)) AS last_seen,
    any_value(reqPath) AS sample_reqPath
FROM base
GROUP BY 1, 2
ORDER BY chunks DESC
"""


def build_path_segment_sql(parquet_glob: str) -> str:
    return f"""
WITH base AS (
    SELECT
        lower(reqHost) AS reqHost,
        lower(split_part(ltrim(reqPath, '/'), '/', 1)) AS path_seg1,
        lower(split_part(ltrim(reqPath, '/'), '/', 2)) AS path_seg2,
        cliIP,
        reqPath
    FROM read_parquet('{parquet_glob}', hive_partitioning=1)
    WHERE statusCode = '200' AND reqPath LIKE '%.ts'
)
SELECT
    reqHost,
    path_seg1,
    CASE
        WHEN path_seg2 LIKE '%.ts' THEN '<segment-file>'
        WHEN path_seg2 LIKE '%.m4s' THEN '<segment-file>'
        WHEN path_seg2 ~ '^[0-9a-f]{{16,}}' THEN '<hashed-token>'
        ELSE path_seg2
    END AS path_seg2_class,
    COUNT(*) AS chunks,
    COUNT(DISTINCT cliIP) AS unique_viewers,
    COUNT(DISTINCT path_seg2) AS distinct_path_seg2,
    any_value(reqPath) AS sample_reqPath
FROM base
GROUP BY 1, 2, 3
ORDER BY chunks DESC
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile VgLive channel identity candidates from a hive parquet lake.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()

    lake = args.lake
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    parquet_glob = q(lake / "**" / "*.parquet")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute("PRAGMA enable_progress_bar")

    print(f"Lake: {lake}")
    print(f"Output: {out}")

    con.execute(f"""
        COPY (
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE statusCode = '200') AS status_200_rows,
                COUNT(*) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS ok_ts_rows,
                COUNT(DISTINCT reqHost) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS distinct_hosts,
                MIN(to_timestamp(CAST(reqTimeSec AS DOUBLE))) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS first_ts_time,
                MAX(to_timestamp(CAST(reqTimeSec AS DOUBLE))) FILTER (WHERE statusCode = '200' AND reqPath LIKE '%.ts') AS last_ts_time
            FROM read_parquet('{parquet_glob}', hive_partitioning=1)
        ) TO '{q(out / "scope.csv")}' (HEADER, DELIMITER ',');
    """)

    con.execute(f"""
        COPY (
            SELECT
                lower(reqHost) AS reqHost,
                COUNT(*) AS chunks,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                any_value(reqPath) AS sample_reqPath
            FROM read_parquet('{parquet_glob}', hive_partitioning=1)
            WHERE statusCode = '200' AND reqPath LIKE '%.ts'
            GROUP BY 1
            ORDER BY chunks DESC
        ) TO '{q(out / "hosts.csv")}' (HEADER, DELIMITER ',');
    """)

    con.execute(f"""
        COPY ({build_candidate_sql(parquet_glob)})
        TO '{q(out / "channel_candidates.csv")}' (HEADER, DELIMITER ',');
    """)

    con.execute(f"""
        COPY ({build_path_segment_sql(parquet_glob)})
        TO '{q(out / "path_segments.csv")}' (HEADER, DELIMITER ',');
    """)

    con.close()
    print("Done. Review channel_candidates.csv first.")


if __name__ == "__main__":
    main()
