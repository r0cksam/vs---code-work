#!/usr/bin/env python3
"""Build reusable device/session identity marts from CDN queryStr evidence."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = ETL_ROOT / "src" / "profile"
if str(PROFILE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROFILE_ROOT))

from vglive_core import DEFAULT_LAKE_FOLDER, HOST_MAP, PATH_MAP, channel_candidate_sql  # noqa: E402


DEFAULT_OUT_DIR = ETL_ROOT / "output" / "identity"
TABLES = [
    "identity_device_daily",
    "identity_session_daily",
    "identity_ipua_daily",
]
AGG_TABLES = [
    "identity_daily",
    "identity_channel_daily",
    "identity_platform_daily",
    "identity_platform_channel_daily",
]


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


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def safe_decoded_query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    raw_value = query_param_sql(param_name, query_col)
    return f"COALESCE(try(url_decode(NULLIF({raw_value}, ''))), NULLIF({raw_value}, ''))"


def normalized_identity_sql(expr: str) -> str:
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
    source_dirs = sorted(lake.glob("source=*"))
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        source_name = source_dir.name.split("=", 1)[-1].lower()
        if source and source_name != source.lower():
            continue
        for day_dir in sorted(source_dir.rglob("day=*")):
            try:
                year = int(day_dir.parent.parent.name.split("=", 1)[-1])
                month = int(day_dir.parent.name.split("=", 1)[-1])
                day = int(day_dir.name.split("=", 1)[-1])
            except (IndexError, ValueError):
                continue
            date_value = datetime(year, month, day).date()
            if start_date and date_value < start_date:
                continue
            if end_date and date_value > end_date:
                continue
            files = tuple(sorted(day_dir.glob("*.parquet")))
            if files:
                partitions.append(Partition(source_name, year, month, day, day_dir, files))
    return partitions


def partition_signature(partition: Partition) -> dict[str, Any]:
    sizes = []
    mtimes = []
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


def part_path(out_dir: Path, table: str, partition: Partition) -> Path:
    return (
        out_dir
        / "parts"
        / table
        / f"source={partition.source}"
        / f"year={partition.year:04d}"
        / f"month={partition.month:02d}"
        / f"day={partition.day:02d}"
        / f"{table}_{partition.source}_{partition.date_text}.parquet"
    )


def copy_sql(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.stem}.tmp{out_path.suffix}")
    tmp_path.unlink(missing_ok=True)
    con.execute(f"COPY ({sql}) TO {sql_text(q(tmp_path))} (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(out_path)


def create_day_base(con: duckdb.DuckDBPyConnection, partition: Partition) -> None:
    candidate_expr = channel_candidate_sql("reqPath")
    reader = f"read_parquet({list_sql(partition.files)}, hive_partitioning=1, union_by_name=1)"
    session_expr = normalized_identity_sql(safe_decoded_query_param_sql("session_id"))
    device_expr = normalized_identity_sql(safe_decoded_query_param_sql("device_id"))
    platform_expr = label_sql(safe_decoded_query_param_sql("platform"))
    device_name_expr = label_sql(safe_decoded_query_param_sql("device"))
    content_expr = label_sql(safe_decoded_query_param_sql("content_title"))
    con.execute("DROP TABLE IF EXISTS identity_base")
    con.execute(
        f"""
        CREATE TEMP TABLE identity_base AS
        WITH raw AS (
            SELECT
                {sql_text(partition.date_text)} AS log_date,
                {sql_text(partition.source)} AS source,
                lower(reqHost) AS reqHost,
                {candidate_expr} AS candidate_id,
                {session_expr} AS session_id,
                {device_expr} AS device_id,
                {platform_expr} AS platform_name,
                {device_name_expr} AS device_name,
                {content_expr} AS content_title,
                NULLIF(CAST(cliIP AS VARCHAR), '') AS cliIP,
                NULLIF(CAST(UA AS VARCHAR), '') AS UA
            FROM {reader}
            WHERE queryStr IS NOT NULL
              AND queryStr <> ''
              AND (queryStr LIKE '%session_id=%' OR queryStr LIKE '%device_id=%')
        )
        SELECT
            raw.log_date,
            raw.source,
            COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name,
            raw.platform_name,
            raw.device_name,
            raw.content_title,
            raw.session_id,
            raw.device_id,
            raw.cliIP,
            raw.UA,
            COALESCE(raw.cliIP, '') || '|' || COALESCE(raw.UA, '') AS ipua_key
        FROM raw
        LEFT JOIN host_map h ON raw.reqHost = h.reqHost
        LEFT JOIN path_map p ON raw.candidate_id = p.candidate_id
        WHERE raw.session_id IS NOT NULL OR raw.device_id IS NOT NULL
        """
    )


def write_day_parts(con: duckdb.DuckDBPyConnection, out_dir: Path, partition: Partition) -> dict[str, int]:
    outputs = {
        "identity_device_daily": f"""
            SELECT
                source,
                log_date,
                channel_name,
                platform_name,
                device_name,
                device_id,
                COUNT(*)::BIGINT AS rows_with_identity,
                COUNT(DISTINCT session_id)::BIGINT AS distinct_sessions,
                COUNT(DISTINCT cliIP)::BIGINT AS distinct_ips,
                COUNT(DISTINCT ipua_key)::BIGINT AS distinct_ipua
            FROM identity_base
            WHERE device_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5, 6
        """,
        "identity_session_daily": f"""
            SELECT
                source,
                log_date,
                channel_name,
                platform_name,
                session_id,
                COUNT(*)::BIGINT AS rows_with_identity,
                COUNT(DISTINCT device_id)::BIGINT AS distinct_devices,
                COUNT(DISTINCT cliIP)::BIGINT AS distinct_ips,
                COUNT(DISTINCT ipua_key)::BIGINT AS distinct_ipua
            FROM identity_base
            WHERE session_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        """,
        "identity_ipua_daily": f"""
            SELECT
                source,
                log_date,
                channel_name,
                platform_name,
                ipua_key,
                COUNT(*)::BIGINT AS rows_with_identity,
                COUNT(DISTINCT device_id)::BIGINT AS distinct_devices,
                COUNT(DISTINCT session_id)::BIGINT AS distinct_sessions
            FROM identity_base
            WHERE ipua_key <> '|'
            GROUP BY 1, 2, 3, 4, 5
        """,
    }
    counts: dict[str, int] = {}
    for table, sql in outputs.items():
        out_path = part_path(out_dir, table, partition)
        copy_sql(con, sql, out_path)
        counts[table] = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({sql_text(q(out_path))})").fetchone()[0])
    return counts


def part_files(out_dir: Path, table: str) -> list[Path]:
    return sorted((out_dir / "parts" / table).rglob("*.parquet"))


def write_empty_outputs(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    schemas = {
        "identity_device_daily": """
            SELECT ''::VARCHAR AS source, ''::VARCHAR AS log_date, ''::VARCHAR AS channel_name,
                   ''::VARCHAR AS platform_name, ''::VARCHAR AS device_name, ''::VARCHAR AS device_id,
                   0::BIGINT AS rows_with_identity, 0::BIGINT AS distinct_sessions,
                   0::BIGINT AS distinct_ips, 0::BIGINT AS distinct_ipua
            WHERE false
        """,
        "identity_session_daily": """
            SELECT ''::VARCHAR AS source, ''::VARCHAR AS log_date, ''::VARCHAR AS channel_name,
                   ''::VARCHAR AS platform_name, ''::VARCHAR AS session_id,
                   0::BIGINT AS rows_with_identity, 0::BIGINT AS distinct_devices,
                   0::BIGINT AS distinct_ips, 0::BIGINT AS distinct_ipua
            WHERE false
        """,
        "identity_ipua_daily": """
            SELECT ''::VARCHAR AS source, ''::VARCHAR AS log_date, ''::VARCHAR AS channel_name,
                   ''::VARCHAR AS platform_name, ''::VARCHAR AS ipua_key,
                   0::BIGINT AS rows_with_identity, 0::BIGINT AS distinct_devices,
                   0::BIGINT AS distinct_sessions
            WHERE false
        """,
    }
    for table, sql in schemas.items():
        if not part_files(out_dir, table):
            copy_sql(con, sql, out_dir / f"{table}.parquet")


def combine_raw_tables(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    write_empty_outputs(con, out_dir)
    for table in TABLES:
        files = part_files(out_dir, table)
        if not files:
            continue
        copy_sql(con, f"SELECT * FROM read_parquet({list_sql(tuple(files))}, union_by_name=1)", out_dir / f"{table}.parquet")


def aggregate_sql(scope_name: str, keys: list[str]) -> str:
    key_select = ", ".join(keys)
    key_select_d = ", ".join([f"d.{k}" for k in keys])
    key_join = " AND ".join([f"d.{k} = s.{k}" for k in keys])
    key_join_ipua = " AND ".join([f"COALESCE(d.{k}, s.{k}) = i.{k}" for k in keys])
    source_scope = keys[0:-1] if keys and keys[-1] == "log_date" else keys
    # Device first-seen is scoped to the selected dimension, so channel-specific
    # filters describe first-seen within that channel/platform, not app-wide first install.
    first_keys = [k for k in keys if k != "log_date"]
    first_group = ", ".join(first_keys + ["device_id"])
    first_join = " AND ".join([f"d.{k} = f.{k}" for k in first_keys] + ["d.device_id = f.device_id"])
    first_select = ", ".join(first_keys + ["device_id", "MIN(log_date) AS first_seen_date"])
    return f"""
    WITH device_first AS (
        SELECT {first_select}
        FROM read_parquet({sql_text(q('__DEVICE__'))})
        GROUP BY {first_group}
    ),
    device_rollup AS (
        SELECT
            {key_select_d},
            COUNT(DISTINCT d.device_id)::BIGINT AS total_devices,
            COUNT(DISTINCT CASE WHEN d.log_date = f.first_seen_date THEN d.device_id END)::BIGINT AS new_devices,
            SUM(d.rows_with_identity)::BIGINT AS device_identity_rows,
            SUM(d.distinct_ips)::BIGINT AS device_distinct_ip_day_sum,
            SUM(d.distinct_ipua)::BIGINT AS device_distinct_ipua_day_sum
        FROM read_parquet({sql_text(q('__DEVICE__'))}) d
        LEFT JOIN device_first f ON {first_join}
        GROUP BY {key_select_d}
    ),
    session_rollup AS (
        SELECT
            {key_select},
            COUNT(DISTINCT session_id)::BIGINT AS total_sessions,
            SUM(rows_with_identity)::BIGINT AS session_identity_rows
        FROM read_parquet({sql_text(q('__SESSION__'))})
        GROUP BY {key_select}
    ),
    ipua_rollup AS (
        SELECT
            {key_select},
            COUNT(DISTINCT ipua_key)::BIGINT AS total_ipua_sessions,
            SUM(rows_with_identity)::BIGINT AS ipua_identity_rows
        FROM read_parquet({sql_text(q('__IPUA__'))})
        GROUP BY {key_select}
    )
    SELECT
        {", ".join([f"COALESCE(d.{k}, s.{k}, i.{k}) AS {k}" for k in keys])},
        COALESCE(d.total_devices, 0)::BIGINT AS total_devices,
        COALESCE(s.total_sessions, 0)::BIGINT AS total_sessions,
        COALESCE(i.total_ipua_sessions, 0)::BIGINT AS total_ipua_sessions,
        COALESCE(d.new_devices, 0)::BIGINT AS new_devices,
        GREATEST(COALESCE(d.total_devices, 0) - COALESCE(d.new_devices, 0), 0)::BIGINT AS returning_devices,
        COALESCE(d.device_identity_rows, 0)::BIGINT AS device_identity_rows,
        COALESCE(s.session_identity_rows, 0)::BIGINT AS session_identity_rows,
        COALESCE(i.ipua_identity_rows, 0)::BIGINT AS ipua_identity_rows,
        COALESCE(d.device_distinct_ip_day_sum, 0)::BIGINT AS device_distinct_ip_day_sum,
        COALESCE(d.device_distinct_ipua_day_sum, 0)::BIGINT AS device_distinct_ipua_day_sum
    FROM device_rollup d
    FULL OUTER JOIN session_rollup s ON {key_join}
    FULL OUTER JOIN ipua_rollup i ON {key_join_ipua}
    ORDER BY {key_select}
    """


def build_aggregate_tables(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    device_path = out_dir / "identity_device_daily.parquet"
    session_path = out_dir / "identity_session_daily.parquet"
    ipua_path = out_dir / "identity_ipua_daily.parquet"
    scopes = {
        "identity_daily": ["source", "log_date"],
        "identity_channel_daily": ["source", "log_date", "channel_name"],
        "identity_platform_daily": ["source", "log_date", "platform_name"],
        "identity_platform_channel_daily": ["source", "log_date", "platform_name", "channel_name"],
    }
    for table, keys in scopes.items():
        sql = aggregate_sql(table, keys)
        sql = sql.replace("__DEVICE__", q(device_path)).replace("__SESSION__", q(session_path)).replace("__IPUA__", q(ipua_path))
        copy_sql(con, sql, out_dir / f"{table}.parquet")


def build_stats(con: duckdb.DuckDBPyConnection, out_dir: Path) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for table in TABLES + AGG_TABLES:
        path = out_dir / f"{table}.parquet"
        if not path.exists():
            continue
        try:
            row = con.execute(
                f"""
                SELECT
                    COUNT(*) AS rows,
                    MIN(log_date) AS min_date,
                    MAX(log_date) AS max_date,
                    COUNT(DISTINCT source) AS sources
                FROM read_parquet({sql_text(q(path))})
                """
            ).fetchdf().iloc[0].to_dict()
        except Exception as exc:
            row = {"error": str(exc)}
        stats[table] = row
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Build reusable CDN queryStr identity marts.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--source", choices=["stream", "fast"], default=None)
    parser.add_argument("--start", help="Start date YYYY-MM-DD. Omit with --end for all available dates.")
    parser.add_argument("--end", help="End date YYYY-MM-DD. Omit with --start for all available dates.")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--temp-dir", type=Path, default=DEFAULT_OUT_DIR / "_duckdb_tmp")
    parser.add_argument("--state", type=Path, default=DEFAULT_OUT_DIR / "identity_mart_state.json")
    parser.add_argument("--force", action="store_true", help="Rebuild matching source/date parts even if signatures match.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    args.state = args.state.expanduser().resolve()
    if not args.lake.exists():
        raise SystemExit(f"Lake folder not found: {args.lake}")

    partitions = discover_partitions(args.lake, args.source, args.start, args.end)
    if not partitions:
        raise SystemExit("No lake parquet partitions found for selected filters.")

    state = load_state(args.state)
    state.setdefault("partitions", {})
    to_process: list[Partition] = []
    for partition in partitions:
        sig = partition_signature(partition)
        prev = state["partitions"].get(partition.key)
        prev_sig = {key: prev.get(key) for key in sig} if isinstance(prev, dict) else None
        outputs_exist = all(part_path(args.out_dir, table, partition).exists() for table in TABLES)
        if args.force or prev_sig != sig or not outputs_exist:
            to_process.append(partition)

    print(f"Lake      : {args.lake}")
    print(f"Out dir   : {args.out_dir}")
    print(f"Partitions: {len(partitions)} total, {len(to_process)} to process")
    if args.dry_run:
        for partition in to_process[:20]:
            print(f"  would process {partition.key}")
        if len(to_process) > 20:
            print(f"  ... {len(to_process) - 20} more")
        return

    con = connect(args)
    processed = 0
    for idx, partition in enumerate(to_process, start=1):
        print(f"[{idx}/{len(to_process)}] {partition.key} files={len(partition.files)}")
        create_day_base(con, partition)
        counts = write_day_parts(con, args.out_dir, partition)
        sig = partition_signature(partition)
        sig["output_counts"] = counts
        state["partitions"][partition.key] = sig
        save_state(args.state, state)
        processed += 1

    combine_raw_tables(con, args.out_dir)
    build_aggregate_tables(con, args.out_dir)
    stats = build_stats(con, args.out_dir)
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake": str(args.lake),
        "out_dir": str(args.out_dir),
        "source": args.source or "all",
        "start": args.start or "",
        "end": args.end or "",
        "partitions_total": len(partitions),
        "partitions_processed": processed,
        "tables": stats,
        "notes": [
            "session_id/device_id are extracted from queryStr with safe URL decoding.",
            "Aggregate tables do not include raw device_id/session_id values.",
            "New/returning device counts are scoped to the aggregate dimension.",
            "FAST currently has no session_id/device_id queryStr evidence, so FAST identity aggregates may be empty.",
        ],
    }
    manifest_path = args.out_dir / "identity_mart_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    print("Identity mart written.")
    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
