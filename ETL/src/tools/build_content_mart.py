#!/usr/bin/env python3
"""Build reusable content-title marts from CDN queryStr evidence."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = ETL_ROOT / "src" / "profile"
if str(PROFILE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROFILE_ROOT))

from vglive_core import DEFAULT_LAKE_FOLDER, HOST_MAP, PATH_MAP, channel_candidate_sql  # noqa: E402


DEFAULT_OUT_DIR = ETL_ROOT / "output" / "content"
CHUNK_DURATION_HOURS = 6 / 3600
TABLE_NAME = "content_daily"
MART_VERSION = 3


@dataclass(frozen=True)
class Partition:
    source: str
    year: int
    month: int
    day: int
    path: Path
    files: tuple[Path, ...]

    @property
    def date_text(self) -> str:
        return f"{self.year:04d}-{self.month:02d}-{self.day:02d}"

    @property
    def key(self) -> str:
        return f"{self.source}/{self.date_text}"


def sql_text(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def safe_decoded_query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    raw_value = query_param_sql(param_name, query_col)
    return f"COALESCE(try(url_decode(NULLIF({raw_value}, ''))), NULLIF({raw_value}, ''))"


def normalized_sql(expr: str) -> str:
    return f"NULLIF(trim(CAST({expr} AS VARCHAR)), '')"


def label_sql(expr: str) -> str:
    return f"COALESCE(NULLIF(trim(CAST({expr} AS VARCHAR)), ''), 'Unknown / NA')"


def list_sql(paths: tuple[Path, ...]) -> str:
    return "[" + ", ".join(sql_text(q(path)) for path in paths) + "]"


def register_maps(con: duckdb.DuckDBPyConnection) -> None:
    host_df = pd.DataFrame(
        [{"reqHost": host, "host_channel_name": name} for host, name in HOST_MAP.items()]
    )
    path_df = pd.DataFrame(
        [{"candidate_id": candidate, "path_channel_name": name} for candidate, name in PATH_MAP.items()]
    )
    con.register("host_map_df", host_df)
    con.register("path_map_df", path_df)
    con.execute("CREATE OR REPLACE TEMP TABLE host_map AS SELECT * FROM host_map_df")
    con.execute("CREATE OR REPLACE TEMP TABLE path_map AS SELECT * FROM path_map_df")


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, int(args.threads))}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={sql_text(q(args.temp_dir))}")
    register_maps(con)
    return con


def discover_partitions(lake: Path, source: str | None, start: str | None, end: str | None) -> list[Partition]:
    start_date = parse_date(start)
    end_date = parse_date(end)
    if start_date and end_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end")

    partitions: list[Partition] = []
    for source_dir in sorted(lake.glob("source=*")):
        if not source_dir.is_dir():
            continue
        source_name = source_dir.name.split("=", 1)[-1].lower()
        if source and source.lower() != "all" and source_name != source.lower():
            continue
        for day_dir in sorted(source_dir.rglob("day=*")):
            try:
                year = int(day_dir.parent.parent.name.split("=", 1)[-1])
                month = int(day_dir.parent.name.split("=", 1)[-1])
                day = int(day_dir.name.split("=", 1)[-1])
            except (IndexError, ValueError):
                continue
            date_value = date(year, month, day)
            if start_date and date_value < start_date:
                continue
            if end_date and date_value > end_date:
                continue
            files = tuple(sorted(day_dir.glob("*.parquet")))
            if files:
                partitions.append(Partition(source_name, year, month, day, day_dir, files))
    return partitions


def partition_signature(partition: Partition) -> dict[str, Any]:
    sizes: list[int] = []
    mtimes: list[int] = []
    for file in partition.files:
        try:
            stat = file.stat()
        except OSError:
            continue
        sizes.append(stat.st_size)
        mtimes.append(stat.st_mtime_ns)
    return {
        "source": partition.source,
        "date": partition.date_text,
        "file_count": len(sizes),
        "bytes": int(sum(sizes)),
        "max_mtime_ns": int(max(mtimes) if mtimes else 0),
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"partitions": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"partitions": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(path)


def part_path(out_dir: Path, partition: Partition) -> Path:
    return (
        out_dir
        / "parts"
        / TABLE_NAME
        / f"source={partition.source}"
        / f"year={partition.year:04d}"
        / f"month={partition.month:02d}"
        / f"day={partition.day:02d}"
        / f"{TABLE_NAME}_{partition.source}_{partition.date_text}.parquet"
    )


def part_files(out_dir: Path) -> list[Path]:
    return sorted((out_dir / "parts" / TABLE_NAME).rglob("*.parquet"))


def copy_sql(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO {sql_text(q(tmp_path))} (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(out_path)


def empty_content_sql() -> str:
    return """
        SELECT
            ''::VARCHAR AS source,
            ''::VARCHAR AS log_date,
            ''::VARCHAR AS channel_name,
            ''::VARCHAR AS platform_name,
            ''::VARCHAR AS content_title,
            ''::VARCHAR AS category_name,
            0::BIGINT AS rows,
            0::BIGINT AS raw_ts_rows,
            0::BIGINT AS status_200_ts_rows,
            0::BIGINT AS m3u8_rows,
            0::BIGINT AS status_200_m3u8_rows,
            0::BIGINT AS non_200_rows,
            0::DOUBLE AS raw_watch_hours,
            0::DOUBLE AS status_200_watch_hours,
            0::BIGINT AS approx_unique_ips,
            0::BIGINT AS approx_sessions,
            0::BIGINT AS approx_devices
        WHERE false
    """


def build_partition(con: duckdb.DuckDBPyConnection, out_dir: Path, partition: Partition) -> dict[str, int]:
    reader = f"read_parquet({list_sql(partition.files)}, hive_partitioning=1, union_by_name=1)"
    candidate_expr = channel_candidate_sql("reqPath")
    content_expr = label_sql(safe_decoded_query_param_sql("content_title"))
    category_expr = label_sql(safe_decoded_query_param_sql("category_name"))
    platform_expr = label_sql(safe_decoded_query_param_sql("platform"))
    session_expr = normalized_sql(safe_decoded_query_param_sql("session_id"))
    device_expr = normalized_sql(safe_decoded_query_param_sql("device_id"))
    status_expr = "COALESCE(NULLIF(regexp_replace(CAST(statusCode AS VARCHAR), '\\\\.0$', ''), ''), 'Unknown')"
    out_path = part_path(out_dir, partition)
    sql = f"""
        WITH base AS (
            SELECT
                {sql_text(partition.source)} AS source,
                {sql_text(partition.date_text)} AS log_date,
                lower(COALESCE(reqHost, '')) AS reqHost,
                {candidate_expr} AS candidate_id,
                {platform_expr} AS platform_name,
                {content_expr} AS content_title,
                {category_expr} AS category_name,
                {session_expr} AS session_id,
                {device_expr} AS device_id,
                NULLIF(CAST(cliIP AS VARCHAR), '') AS cliIP,
                lower(COALESCE(CAST(reqPath AS VARCHAR), '')) AS reqPath,
                {status_expr} AS status_code
            FROM {reader}
            WHERE queryStr IS NOT NULL
              AND queryStr <> ''
              AND queryStr LIKE '%content_title=%'
        ),
        resolved AS (
            SELECT
                b.*,
                COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name
            FROM base b
            LEFT JOIN host_map h ON b.reqHost = h.reqHost
            LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
            WHERE b.content_title IS NOT NULL
        ),
        rolled AS (
            SELECT
                source,
                log_date,
                channel_name,
                platform_name,
                content_title,
                category_name,
                COUNT(*)::BIGINT AS rows,
                COUNT(*) FILTER (WHERE reqPath LIKE '%.ts')::BIGINT AS raw_ts_rows,
                COUNT(*) FILTER (WHERE reqPath LIKE '%.ts' AND status_code = '200')::BIGINT AS status_200_ts_rows,
                COUNT(*) FILTER (WHERE reqPath LIKE '%.m3u8')::BIGINT AS m3u8_rows,
                COUNT(*) FILTER (WHERE reqPath LIKE '%.m3u8' AND status_code = '200')::BIGINT AS status_200_m3u8_rows,
                COUNT(*) FILTER (WHERE status_code <> '200')::BIGINT AS non_200_rows,
                approx_count_distinct(cliIP)::BIGINT AS approx_unique_ips,
                approx_count_distinct(session_id)::BIGINT AS approx_sessions,
                approx_count_distinct(device_id)::BIGINT AS approx_devices
            FROM resolved
            GROUP BY 1, 2, 3, 4, 5, 6
        )
        SELECT
            *,
            CAST(raw_ts_rows AS DOUBLE) * {CHUNK_DURATION_HOURS:.12f} AS raw_watch_hours,
            CAST(status_200_ts_rows AS DOUBLE) * {CHUNK_DURATION_HOURS:.12f} AS status_200_watch_hours
        FROM rolled
    """
    copy_sql(con, sql, out_path)
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet({sql_text(q(out_path))})").fetchone()[0]
    return {"rows": int(rows or 0)}


def combine_outputs(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    files = part_files(out_dir)
    if files:
        copy_sql(con, f"SELECT * FROM read_parquet({list_sql(tuple(files))}, union_by_name=1)", out_dir / f"{TABLE_NAME}.parquet")
    else:
        copy_sql(con, empty_content_sql(), out_dir / f"{TABLE_NAME}.parquet")


def build_stats(con: duckdb.DuckDBPyConnection, out_dir: Path) -> dict[str, Any]:
    path = out_dir / f"{TABLE_NAME}.parquet"
    if not path.exists():
        return {}
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            MIN(log_date) AS min_date,
            MAX(log_date) AS max_date,
            COUNT(DISTINCT source) AS sources,
            SUM(rows) AS source_rows,
            SUM(raw_ts_rows) AS raw_ts_rows,
            SUM(status_200_ts_rows) AS status_200_ts_rows,
            SUM(m3u8_rows) AS m3u8_rows,
            SUM(raw_watch_hours) AS raw_watch_hours,
            SUM(status_200_watch_hours) AS status_200_watch_hours
        FROM read_parquet({sql_text(q(path))})
        """
    ).fetchone()
    if not row:
        return {}
    keys = [
        "rows",
        "min_date",
        "max_date",
        "sources",
        "source_rows",
        "raw_ts_rows",
        "status_200_ts_rows",
        "m3u8_rows",
        "raw_watch_hours",
        "status_200_watch_hours",
    ]
    return dict(zip(keys, row))


def write_manifest(args: argparse.Namespace, out_dir: Path, state: dict[str, Any], stats: dict[str, Any]) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake": str(args.lake),
        "out_dir": str(out_dir),
        "source": args.source,
        "start": args.start,
        "end": args.end,
        "table": str(out_dir / f"{TABLE_NAME}.parquet"),
        "stats": stats,
        "processed_partitions": len(state.get("partitions", {})),
    }
    (out_dir / "content_mart_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build incremental content-title mart from lake queryStr evidence.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--state", type=Path, default=DEFAULT_OUT_DIR / "content_mart_state.json")
    parser.add_argument("--source", choices=["stream", "fast", "all"], default="stream")
    parser.add_argument("--start", default=None, help="IST partition start date YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="IST partition end date YYYY-MM-DD.")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    partitions = discover_partitions(args.lake, args.source, args.start, args.end)
    state = load_state(args.state)
    old_partitions = state.setdefault("partitions", {})
    version_mismatch = state.get("mart_version") != MART_VERSION
    state["mart_version"] = MART_VERSION
    todo: list[tuple[Partition, dict[str, Any]]] = []
    for partition in partitions:
        signature = partition_signature(partition)
        old = old_partitions.get(partition.key, {})
        comparable_old = {k: old.get(k) for k in signature}
        if version_mismatch or comparable_old != signature or not part_path(args.out_dir, partition).exists():
            todo.append((partition, signature))

    print(f"Content mart partitions discovered: {len(partitions)} total, {len(todo)} to process.")
    if args.dry_run:
        for partition, _ in todo[:20]:
            print(f"  would process {partition.key}")
        if len(todo) > 20:
            print(f"  ... {len(todo) - 20} more")
        return

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(args)
    started = datetime.now()
    try:
        for idx, (partition, signature) in enumerate(todo, 1):
            pct = idx * 100 / max(1, len(todo))
            print(f"[{idx}/{len(todo)} {pct:5.1f}%] {partition.key} files={len(partition.files)}")
            counts = build_partition(con, args.out_dir, partition)
            old_partitions[partition.key] = {**signature, "output_counts": counts}
            save_state(args.state, state)
        print("Combining content mart parts...")
        combine_outputs(con, args.out_dir)
        stats = build_stats(con, args.out_dir)
        write_manifest(args, args.out_dir, state, stats)
    finally:
        con.close()
    elapsed = (datetime.now() - started).total_seconds()
    print(f"Content mart complete in {elapsed:.1f}s: {args.out_dir / f'{TABLE_NAME}.parquet'}")


if __name__ == "__main__":
    main()
