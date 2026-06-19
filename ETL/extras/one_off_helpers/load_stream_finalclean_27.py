from pathlib import Path
import time

import duckdb


BASE = Path(r"D:\Veto Logs Backup\Vs - Code Work\ETL\data")
LAKE = BASE / "lake"
SOURCE_ID = "stream_2026_05_27"
SOURCE_FILE = Path(r"Z:\Veto Logs Backup\Veto Stream Logs\05\27_final_clean.parquet")
IST_OFFSET_SECONDS = 19_800


def sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ist_timestamp_expr(epoch_expr: str) -> str:
    return (
        "epoch_ms(CAST(FLOOR(("
        f"CAST({epoch_expr} AS DOUBLE) + {IST_OFFSET_SECONDS}"
        ") * 1000) AS BIGINT))"
    )


def remove_existing_prefix(prefix: str) -> int:
    removed = 0
    if not LAKE.exists():
        return 0
    for path in LAKE.rglob(f"{prefix}_*.parquet"):
        path.unlink(missing_ok=True)
        removed += 1
    return removed


def parquet_columns(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{sql_path(path)}') LIMIT 0"
    ).fetchall()
    return {str(row[0]).lower() for row in rows}


def parquet_row_count(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    row = con.execute(
        f"""
        SELECT COALESCE(SUM(row_group_num_rows), 0)::BIGINT
        FROM parquet_metadata('{sql_path(path)}')
        """
    ).fetchone()
    return int(row[0] or 0)


def main() -> None:
    if not SOURCE_FILE.exists():
        raise SystemExit(f"Missing source parquet: {SOURCE_FILE}")

    LAKE.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=8")
    con.execute("SET memory_limit='20GB'")
    con.execute("SET preserve_insertion_order=false")

    prefix = f"part_{SOURCE_ID}"
    start = time.time()
    rows = parquet_row_count(con, SOURCE_FILE)
    existing_columns = parquet_columns(con, SOURCE_FILE)
    partition_cols = [c for c in ("source", "year", "month", "day") if c in existing_columns]
    select_star = (
        f"* EXCLUDE ({', '.join(sql_ident(c) for c in partition_cols)})"
        if partition_cols
        else "*"
    )

    removed = remove_existing_prefix(prefix)
    print(f"[load] {SOURCE_FILE.name} -> source=stream prefix={prefix} rows={rows:,} removed_old={removed}")
    con.execute(
        f"""
        COPY (
            SELECT
                {select_star},
                'stream' AS source,
                strftime({ist_timestamp_expr("reqTimeSec")}, '%Y') AS year,
                strftime({ist_timestamp_expr("reqTimeSec")}, '%m') AS month,
                strftime({ist_timestamp_expr("reqTimeSec")}, '%d') AS day
            FROM read_parquet('{sql_path(SOURCE_FILE)}')
        )
        TO '{sql_path(LAKE)}'
        (
            FORMAT PARQUET,
            PARTITION_BY (source, year, month, day),
            FILENAME_PATTERN '{prefix}_{{i}}',
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 3,
            OVERWRITE_OR_IGNORE true
        )
        """
    )
    promoted = sum(1 for _ in LAKE.rglob(f"{prefix}_*.parquet"))
    if rows > 0 and promoted == 0:
        raise RuntimeError(f"No lake parquet files produced for {SOURCE_FILE}")
    print(f"[done] {SOURCE_FILE.name} promoted_files={promoted} elapsed_sec={time.time() - start:.1f}")
    con.close()


if __name__ == "__main__":
    main()
