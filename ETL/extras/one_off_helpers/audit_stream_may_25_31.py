from pathlib import Path

import duckdb


Z_DIR = Path(r"Z:\Veto Logs Backup\Veto Stream Logs\05")
LAKE_DIR = Path(r"D:\Veto Logs Backup\Vs - Code Work\ETL\data\lake\source=stream")
DAYS = ["25", "26", "27", "28", "29", "30", "31"]


def rows_for_files(con: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    if not files:
        return 0
    row = con.execute(
        "SELECT COALESCE(SUM(row_group_num_rows), 0)::BIGINT FROM parquet_metadata(?)",
        [[p.as_posix() for p in files]],
    ).fetchone()
    return int(row[0] or 0)


def main() -> None:
    con = duckdb.connect()
    print("day,z_exists,z_rows,legacy_files,legacy_rows,may_files,may_rows,total_lake_rows,status")
    for day in DAYS:
        z_file = Z_DIR / f"{day}_final_clean.parquet"
        z_rows = rows_for_files(con, [z_file]) if z_file.exists() else 0
        legacy_files = list(LAKE_DIR.rglob(f"part_stream_legacy_{day}_*.parquet"))
        may_files = list(LAKE_DIR.rglob(f"part_stream_2026_05_{day}_*.parquet"))
        legacy_rows = rows_for_files(con, legacy_files)
        may_rows = rows_for_files(con, may_files)
        total = legacy_rows + may_rows
        if not z_file.exists():
            status = "missing_z"
        elif total == 0:
            status = "missing_lake_prefix"
        elif abs(total - z_rows) <= max(10_000, z_rows * 0.005):
            status = "ok"
        else:
            status = "row_mismatch"
        print(
            f"{day},{z_file.exists()},{z_rows},{len(legacy_files)},{legacy_rows},"
            f"{len(may_files)},{may_rows},{total},{status}"
        )
    con.close()


if __name__ == "__main__":
    main()
