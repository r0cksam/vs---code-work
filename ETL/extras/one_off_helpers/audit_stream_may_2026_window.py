from datetime import date, timedelta
from pathlib import Path

import duckdb


YEAR = 2026
MONTH = 5
Z_DIR = Path(r"Z:\Veto Logs Backup\Veto Stream Logs\05")
LAKE_ROOT = Path(r"D:\Veto Logs Backup\Vs - Code Work\ETL\data\lake\source=stream")
DAYS = [f"{day:02d}" for day in range(1, 32)]


def rows_for_files(con: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    if not files:
        return 0
    row = con.execute(
        "SELECT COALESCE(SUM(row_group_num_rows), 0)::BIGINT FROM parquet_metadata(?)",
        [[p.as_posix() for p in files]],
    ).fetchone()
    return int(row[0] or 0)


def partition_dirs(day_text: str) -> list[Path]:
    current = date(YEAR, MONTH, int(day_text))
    # A UTC source day can land in IST source-day and source-day+1 partitions.
    return [
        LAKE_ROOT
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
        for dt in (current, current + timedelta(days=1))
    ]


def lake_files_for_day(day_text: str) -> tuple[list[Path], list[Path]]:
    legacy_prefix = f"part_stream_legacy_{day_text}_"
    may_prefix = f"part_stream_{YEAR}_{MONTH:02d}_{day_text}_"
    legacy_files: list[Path] = []
    may_files: list[Path] = []
    for folder in partition_dirs(day_text):
        if not folder.exists():
            continue
        legacy_files.extend(sorted(folder.glob(f"{legacy_prefix}*.parquet")))
        may_files.extend(sorted(folder.glob(f"{may_prefix}*.parquet")))
    return legacy_files, may_files


def main() -> None:
    con = duckdb.connect()
    print("day,z_exists,z_rows,legacy_files,legacy_rows,may_files,may_rows,total_lake_rows,status")
    for day_text in DAYS:
        z_file = Z_DIR / f"{day_text}_final_clean.parquet"
        z_rows = rows_for_files(con, [z_file]) if z_file.exists() else 0
        legacy_files, may_files = lake_files_for_day(day_text)
        legacy_rows = rows_for_files(con, legacy_files)
        may_rows = rows_for_files(con, may_files)
        total = legacy_rows + may_rows
        tolerance = max(10_000, int(z_rows * 0.005))
        if not z_file.exists():
            status = "missing_z"
        elif total == 0:
            status = "missing_lake_prefix"
        elif abs(total - z_rows) <= tolerance:
            status = "ok"
        elif total > z_rows:
            status = "possible_duplicate_or_wrong_prefix"
        else:
            status = "partial_lake"
        print(
            f"{day_text},{z_file.exists()},{z_rows},{len(legacy_files)},{legacy_rows},"
            f"{len(may_files)},{may_rows},{total},{status}"
        )
    con.close()


if __name__ == "__main__":
    main()
