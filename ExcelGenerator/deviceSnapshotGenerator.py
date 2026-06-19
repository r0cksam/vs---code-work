#!/usr/bin/env python3
"""
deviceSnapshotGenerator.py
Generate full-lake device intelligence CSVs from Veto stream logs.

Configure paths via --lake, --snapshot-csv, --daily-csv, or environment vars.
    py deviceSnapshotGenerator.py
"""

from __future__ import annotations

import logging
import argparse
import os
import subprocess
import sys
from pathlib import Path

try:
    import duckdb
    import psutil
    import pyarrow.parquet as pq
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "psutil", "pyarrow", "--quiet"])
    import duckdb
    import psutil
    import pyarrow.parquet as pq


DEFAULT_LAKE_ROOT = Path(os.getenv("VG_ETL_LAKE_ROOT", str(Path.home() / "Veto Stream Logs" / "lake")))
DEFAULT_CSV_OUT = Path(os.getenv("VG_DEVICE_SNAPSHOT_OUT", Path.home() / "Veto Stream Logs" / "overview" / "device_snapshot.csv"))
DEFAULT_DAILY_CSV_OUT = Path(os.getenv("VG_DEVICE_DAILY_OUT", Path.home() / "Veto Stream Logs" / "overview" / "device_daily.csv"))

logging.basicConfig(level=logging.INFO, format="  %(levelname)-7s %(message)s")
log = logging.getLogger("device_snapshot")


def get_conn() -> duckdb.DuckDBPyConnection:
    mem_avail = psutil.virtual_memory().available / (1024 ** 3)
    cpus = os.cpu_count() or 4
    mem_gb = max(4, min(int(mem_avail * 0.70), 48))
    threads = max(1, min(cpus // 4, 4))
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem_gb}GB'")
    con.execute("SET preserve_insertion_order=false")
    try:
        con.execute("PRAGMA enable_progress_bar")
    except Exception:
        pass
    return con


def lake_reader(lake_root: Path) -> str:
    return (
        f"read_parquet('{lake_root.as_posix()}/**/*.parquet', "
        f"hive_partitioning=true, union_by_name=true)"
    )


def qs_extract(qs_col: str, param: str) -> str:
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"


def log_step(step: int, total: int, message: str) -> None:
    bar_len = 24
    filled = round(bar_len * step / total)
    bar = "#" * filled + "-" * (bar_len - filled)
    log.info(f"[{bar}] {step}/{total} {message}")


def csv_data_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as f:
        return max(0, sum(1 for _ in f) - 1)


def append_csv_without_header(src: Path, dst: Path) -> int:
    if not src.exists():
        return 0
    added = 0
    dst_exists = dst.exists() and dst.stat().st_size > 0
    with src.open("r", encoding="utf-8", newline="") as rf, dst.open("a", encoding="utf-8", newline="") as wf:
        for idx, line in enumerate(rf):
            if idx == 0 and dst_exists:
                continue
            wf.write(line)
            if idx > 0 or not dst_exists:
                added += 1
    return max(0, added - (0 if dst_exists else 1))


def latest_daily_date(con: duckdb.DuckDBPyConnection, path: Path) -> str | None:
    if not path.exists() or csv_data_lines(path) == 0:
        return None
    safe_path = path.as_posix()
    result = con.execute(f"""
        SELECT MAX(CAST(utc_date AS DATE)) AS max_date
        FROM read_csv_auto('{safe_path}', HEADER=true)
    """).fetchone()[0]
    return str(result) if result else None


def pa_schema(lake_root: Path) -> set[str]:
    cols: set[str] = set()
    skip = {"year", "month", "day"}
    for pf in lake_root.rglob("*.parquet"):
        try:
            schema = pq.read_schema(pf)
            cols.update(name for name in schema.names if name not in skip)
        except Exception:
            pass
        if len(cols) > 8:
            break
    return cols


def run_snapshot(
    lake_root: Path,
    snapshot_csv: Path,
    daily_csv: Path,
) -> None:
    daily_tmp = daily_csv.with_name("_device_daily_new.csv")

    if not lake_root.is_dir():
        raise SystemExit(f"Lake folder not found: {lake_root}")

    cols = pa_schema(lake_root)
    required = {"queryStr", "reqTimeSec"}
    missing = required - cols
    if missing:
        raise SystemExit(f"Missing required column(s): {', '.join(sorted(missing))}")

    has_ip = "cliIP" in cols
    has_ua = "UA" in cols

    reader = lake_reader(lake_root)
    device_expr = qs_extract("queryStr", "device_id")
    session_expr = qs_extract("queryStr", "session_id")
    ip_select = "cliIP" if has_ip else "NULL AS cliIP"
    ua_select = "UA" if has_ua else "NULL AS UA"
    ip_expr = "COUNT(DISTINCT cliIP)" if has_ip else "0"
    ipua_expr = "COUNT(DISTINCT (cliIP, UA))" if has_ip and has_ua else "0"

    daily_sql_template = """
        COPY (
            WITH base AS (
                SELECT
                    {device_expr} AS device_id,
                    CAST(to_timestamp(CAST(reqTimeSec AS DOUBLE)) AS DATE) AS utc_date,
                    {ip_select},
                    {ua_select},
                    {session_expr} AS session_id
                FROM {reader}
                WHERE queryStr IS NOT NULL
                  AND queryStr LIKE '%device_id=%'
                  AND {device_expr} IS NOT NULL
                  {date_filter}
            )
            SELECT
                device_id,
                utc_date,
                COUNT(*) AS rows_on_date,
                {ip_expr} AS distinct_ip,
                {ipua_expr} AS distinct_ip_ua,
                COUNT(DISTINCT session_id) AS distinct_sessions
            FROM base
            GROUP BY device_id, utc_date
            ORDER BY utc_date DESC, rows_on_date DESC
        ) TO '{daily_out}' (HEADER, DELIMITER ',');
    """

    snapshot_sql = f"""
        COPY (
            SELECT
                device_id,
                MIN(CAST(utc_date AS DATE)) AS first_seen_utc_date,
                MAX(CAST(utc_date AS DATE)) AS last_seen_utc_date,
                COUNT(*) AS days_seen,
                SUM(rows_on_date) AS total_rows,
                SUM(distinct_ip) AS distinct_ip_day_sum,
                SUM(distinct_ip_ua) AS distinct_ip_ua_day_sum,
                SUM(distinct_sessions) AS distinct_sessions_day_sum
            FROM read_csv_auto('{daily_csv.as_posix()}', HEADER=true)
            GROUP BY device_id
            ORDER BY last_seen_utc_date DESC, total_rows DESC
        ) TO '{snapshot_csv.as_posix()}' (HEADER, DELIMITER ',');
    """

    log.info(f"Lake folder: {lake_root}")
    log.info(f"Snapshot CSV: {snapshot_csv}")
    log.info(f"Daily CSV   : {daily_csv}")
    snapshot_csv.parent.mkdir(parents=True, exist_ok=True)
    daily_csv.parent.mkdir(parents=True, exist_ok=True)

    log_step(1, 5, "Schema checked and SQL prepared")
    con = get_conn()
    try:
        max_existing_date = latest_daily_date(con, daily_csv)
        if max_existing_date:
            log.info(f"Existing daily CSV found. Latest loaded date: {max_existing_date}")
            date_filter = (
                "AND CAST(to_timestamp(CAST(reqTimeSec AS DOUBLE)) AS DATE) > "
                f"DATE '{max_existing_date}'"
            )
            daily_out = daily_tmp
        else:
            log.info("No existing daily CSV found. First run will scan the full lake.")
            date_filter = ""
            daily_out = daily_csv

        if daily_tmp.exists():
            daily_tmp.unlink()

        daily_sql = daily_sql_template.format(
            device_expr=device_expr,
            session_expr=session_expr,
            ip_select=ip_select,
            ua_select=ua_select,
            reader=reader,
            date_filter=date_filter,
            ip_expr=ip_expr,
            ipua_expr=ipua_expr,
            daily_out=daily_out.as_posix(),
        )

        log_step(2, 5, "Exporting new device_daily rows")
        con.execute(daily_sql)

        if daily_out == daily_tmp:
            new_lines = csv_data_lines(daily_tmp)
            if new_lines == 0:
                log_step(3, 5, "No new dates found; daily CSV unchanged")
                daily_tmp.unlink(missing_ok=True)
            else:
                appended = append_csv_without_header(daily_tmp, daily_csv)
                daily_tmp.unlink(missing_ok=True)
                log_step(3, 5, f"Appended {appended:,} daily rows")
        else:
            log_step(3, 5, f"Wrote {csv_data_lines(daily_csv):,} daily rows")

        log_step(4, 5, "Refreshing device_snapshot.csv from device_daily.csv")
        con.execute(snapshot_sql)
    finally:
        con.close()

    log_step(5, 5, "Device CSV generation complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate device snapshot CSVs.")
    parser.add_argument("--lake", default=str(DEFAULT_LAKE_ROOT))
    parser.add_argument("--snapshot-csv", default=str(DEFAULT_CSV_OUT))
    parser.add_argument("--daily-csv", default=str(DEFAULT_DAILY_CSV_OUT))
    args = parser.parse_args()

    run_snapshot(
        lake_root=Path(args.lake).expanduser().resolve(),
        snapshot_csv=Path(args.snapshot_csv).expanduser().resolve(),
        daily_csv=Path(args.daily_csv).expanduser().resolve(),
    )


if __name__ == "__main__":
    main()
