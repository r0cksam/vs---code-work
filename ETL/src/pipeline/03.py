"""Create partitioned parquet lake for final clean files in incremental mode."""

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from tqdm import tqdm


ETL_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "run.py").exists()),
    Path(__file__).resolve().parents[2],
)
_env_base = os.getenv("VG_ETL_BASE")
BASE_FOLDER = Path(_env_base).expanduser() if _env_base else ETL_ROOT / "data"
if not _env_base:
    candidates = [
        ETL_ROOT / "data",
        ETL_ROOT,
    ]
    for candidate in candidates:
        if candidate.exists():
            BASE_FOLDER = candidate
            break
LAKE_FOLDER = BASE_FOLDER / "lake"
STATE_FILE = BASE_FOLDER / ".etl_03_state.json"

THREADS = int(os.getenv("VG_ETL_THREADS", "12"))
MEMORY = os.getenv("VG_ETL_MEMORY", "28GB")
COMPRESSION = os.getenv("VG_ETL_COMPRESSION", "ZSTD")
COMP_LEVEL = int(os.getenv("VG_ETL_COMP_LEVEL", "3"))
FINAL_SUFFIX = "_final_clean.parquet"
PARTITION_VERSION = 3
IST_OFFSET_SECONDS = 19_800
PROCESS_SOURCES = {
    item.strip()
    for item in os.getenv("VG_ETL_PROCESS_SOURCES", "").split(",")
    if item.strip()
}
REPLACE_DATES = {
    item.strip()
    for item in os.getenv("VG_ETL_REPLACE_DATES", "").split(",")
    if item.strip()
}


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print(f"[warn] Could not read state file {STATE_FILE}: {exc}. Rechecking all final_clean files.")
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    tmp.replace(STATE_FILE)


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


def parquet_row_count(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    row = con.execute(f"""
        SELECT COALESCE(SUM(row_group_num_rows), 0)::BIGINT
        FROM (
            SELECT DISTINCT file_name, row_group_id, row_group_num_rows
            FROM parquet_metadata('{sql_path(path)}')
        )
    """).fetchone()
    return int(row[0] or 0)


def parquet_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sql_path(path)}')").fetchall()
    return [str(row[0]) for row in rows]


def file_signature(path: Path) -> str:
    stat = path.stat()
    h = hashlib.blake2b(digest_size=16)
    h.update(f"{path.name}|{stat.st_size}|{stat.st_mtime_ns}|".encode("utf-8"))
    return h.hexdigest()


def source_key_from_source_id(source_id: str) -> str:
    source_id_lower = source_id.lower()
    if source_id_lower.startswith("fast_"):
        return "fast"
    return "stream"


def load_stage_jobs() -> list[dict[str, str]] | None:
    raw = os.getenv("VG_ETL_STAGE_JOBS")
    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise SystemExit(f"Could not parse VG_ETL_STAGE_JOBS: {exc}") from exc

    if not isinstance(payload, list):
        raise SystemExit("VG_ETL_STAGE_JOBS must be a JSON list.")

    jobs = []
    for item in payload:
        if not isinstance(item, dict):
            raise SystemExit("Each VG_ETL_STAGE_JOBS item must be an object.")
        final_clean_file = item.get("final_clean_file")
        if not final_clean_file:
            raise SystemExit("Each stage job needs final_clean_file.")
        source_id = item.get("source_id") or source_id_from_final_clean(Path(final_clean_file))
        source_key = item.get("source_key") or source_key_from_source_id(source_id)
        jobs.append(
            {
                "source_id": source_id,
                "source_key": source_key,
                "final_clean_file": str(Path(final_clean_file).expanduser()),
            }
        )
    return jobs


def source_id_from_final_clean(path: Path) -> str:
    if path.name.endswith(FINAL_SUFFIX):
        return path.name[: -len(FINAL_SUFFIX)]
    return path.stem


def safe_source_slug(source_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", source_id).strip("_").lower()
    return slug or "source"


def lake_source_prefix(source_id: str) -> str:
    if re.fullmatch(r"\d+", source_id):
        return f"src{source_id}"
    return f"src_{safe_source_slug(source_id)}"


def lake_file_prefix(source_id: str, source_key: str) -> str:
    source_slug = safe_source_slug(source_key)
    source_id_slug = safe_source_slug(source_id)
    source_prefix = f"{source_slug}_"
    if re.fullmatch(r"\d+", source_id):
        batch_slug = f"legacy_{source_id}"
    elif source_id_slug.startswith(source_prefix):
        batch_slug = source_id_slug[len(source_prefix):]
    else:
        batch_slug = source_id_slug
    return f"part_{source_slug}_{batch_slug}"


def candidate_file_prefixes(source_id: str, source_key: str) -> set[str]:
    return {
        lake_file_prefix(source_id, source_key),
        lake_source_prefix(source_id),
    }


def final_clean_sort_key(path: Path) -> tuple[int, int, str]:
    source_id = source_id_from_final_clean(path)
    if re.fullmatch(r"\d+", source_id):
        return (0, int(source_id), source_id)
    return (1, 0, source_id.lower())


def has_source_partitions(file_prefixes: set[str]) -> bool:
    if not LAKE_FOLDER.exists():
        return False
    for file_prefix in file_prefixes:
        for path in LAKE_FOLDER.rglob(f"{file_prefix}_*.parquet"):
            if path.stat().st_size > 0:
                return True
    return False


def remove_previous_partitions(src_prefix: str) -> int:
    if not LAKE_FOLDER.exists():
        return 0

    removed = 0
    for p in LAKE_FOLDER.rglob(f"{src_prefix}_*.parquet"):
        try:
            p.unlink()
            removed += 1
        except FileNotFoundError:
            pass
    return removed


def remove_date_partitions(day_value: str, source_prefixes: set[str] | None = None) -> int:
    try:
        yyyy, mm, dd = day_value.split("-", 2)
    except ValueError:
        raise SystemExit(f"Invalid VG_ETL_REPLACE_DATES value: {day_value}. Use YYYY-MM-DD.")

    removed = 0
    day_dirs = list(LAKE_FOLDER.glob(f"source=*/year={yyyy}/month={mm}/day={dd}"))
    legacy_day = LAKE_FOLDER / f"year={yyyy}" / f"month={mm}" / f"day={dd}"
    if legacy_day.exists():
        day_dirs.append(legacy_day)
    for day_dir in day_dirs:
        for p in day_dir.glob("*.parquet"):
            if source_prefixes and not any(p.name.startswith(f"{prefix}_") for prefix in source_prefixes):
                continue
            try:
                p.unlink()
                removed += 1
            except FileNotFoundError:
                pass
    return removed


def promote_temp_partitions(temp_prefix: str, src_prefix: str) -> int:
    promoted = 0
    for p in LAKE_FOLDER.rglob(f"{temp_prefix}_*.parquet"):
        target = p.with_name(p.name.replace(f"{temp_prefix}_", f"{src_prefix}_", 1))
        p.replace(target)
        promoted += 1
    return promoted


def move_partitions(from_prefix: str, to_prefix: str) -> int:
    moved = 0
    for p in LAKE_FOLDER.rglob(f"{from_prefix}_*.parquet"):
        target = p.with_name(p.name.replace(f"{from_prefix}_", f"{to_prefix}_", 1))
        p.replace(target)
        moved += 1
    return moved


def prune_empty_dirs() -> int:
    if not LAKE_FOLDER.exists():
        return 0
    removed = 0
    dirs = [p for p in LAKE_FOLDER.rglob("*") if p.is_dir()]
    for path in sorted(dirs, key=lambda p: len(p.parts), reverse=True):
        if path.exists() and not any(path.iterdir()):
            path.rmdir()
            removed += 1
    return removed


def list_summary() -> list[str]:
    if not LAKE_FOLDER.exists():
        return []
    out = []
    source_roots = [p for p in sorted(LAKE_FOLDER.glob("source=*")) if p.is_dir()]
    if not source_roots:
        source_roots = [LAKE_FOLDER]
    for source_root in source_roots:
        source_label = source_root.name if source_root != LAKE_FOLDER else "legacy"
        for year_dir in sorted(source_root.glob("year=*")):
            if not year_dir.is_dir():
                continue
            for month_dir in sorted(year_dir.glob("month=*")):
                if not month_dir.is_dir():
                    continue
                day_dirs = sorted(month_dir.glob("day=*"))
                files = sum(len(list(d.glob("*.parquet"))) for d in day_dirs if d.is_dir())
                if day_dirs:
                    out.append(f"{source_label}/{year_dir.name}/{month_dir.name}: {len(day_dirs)} day(s), {files} file(s)")
    return out[:6]


def main() -> None:
    LAKE_FOLDER.mkdir(parents=True, exist_ok=True)
    state = load_state()

    stage_jobs = load_stage_jobs()
    source_keys: dict[str, str] = {}
    if stage_jobs is not None:
        all_files = sorted(
            [Path(job["final_clean_file"]) for job in stage_jobs],
            key=final_clean_sort_key,
        )
        source_keys = {job["source_id"]: job["source_key"] for job in stage_jobs}
    else:
        all_files = sorted(BASE_FOLDER.glob("*_final_clean.parquet"), key=final_clean_sort_key)
    if PROCESS_SOURCES:
        all_files = [
            f for f in all_files
            if source_id_from_final_clean(f) in PROCESS_SOURCES
        ]
    if not all_files:
        if PROCESS_SOURCES:
            raise SystemExit("03.py scope matched no *_final_clean.parquet files. Check VG_ETL_PROCESS_SOURCES.")
        print("No *_final_clean.parquet files found.")
        return
    replace_file_prefixes = None
    if PROCESS_SOURCES:
        replace_file_prefixes = set()
        for f in all_files:
            source_id = source_id_from_final_clean(f)
            source_key = source_keys.get(source_id, source_key_from_source_id(source_id))
            replace_file_prefixes.update(candidate_file_prefixes(source_id, source_key))
    if PROCESS_SOURCES:
        print(f"Scoped source IDs: {', '.join(sorted(PROCESS_SOURCES))}")
    if REPLACE_DATES:
        for day_value in sorted(REPLACE_DATES):
            removed = remove_date_partitions(day_value, replace_file_prefixes)
            if removed:
                print(f"[replace-date] Removed {removed} existing lake parquet file(s) for {day_value}.")
        prune_empty_dirs()

    pending = []
    for f in all_files:
        source_id = source_id_from_final_clean(f)
        source_key = source_keys.get(source_id, source_key_from_source_id(source_id))
        file_prefix = lake_file_prefix(source_id, source_key)
        legacy_prefix = lake_source_prefix(source_id)
        file_prefixes = {file_prefix, legacy_prefix}
        sig = file_signature(f)
        rec = state.get(f.name)
        if (
            REPLACE_DATES
            or
            not isinstance(rec, dict)
            or rec.get("status") != "ok"
            or rec.get("partition_version") != PARTITION_VERSION
            or rec.get("source_key") != source_key
            or rec.get("file_prefix") != file_prefix
            or rec.get("signature") != sig
            or not has_source_partitions(file_prefixes)
        ):
            pending.append((f, sig, file_prefix, legacy_prefix, source_key))

    print(f"Total final_clean files : {len(all_files)}")
    print(f"Already processed       : {len(all_files) - len(pending)}")
    print(f"New/changed to process  : {len(pending)}")

    if not pending:
        print("Lake is already up to date.")
        return

    con = duckdb.connect()
    con.execute(f"SET threads={THREADS};")
    con.execute(f"SET memory_limit='{MEMORY}';")
    con.execute("SET preserve_insertion_order=false;")

    had_errors = False

    for path, sig, file_prefix, legacy_prefix, source_key in tqdm(pending, unit="file", desc="Partitioning", dynamic_ncols=True):
        start = time.time()
        temp_prefix = f"tmp_{file_prefix}_{int(time.time() * 1000)}"
        backup_prefix = f"backup_{file_prefix}_{int(time.time() * 1000)}"
        legacy_backup_prefix = f"backup_legacy_{file_prefix}_{int(time.time() * 1000)}"
        backed_up = 0
        legacy_backed_up = 0

        try:
            remove_previous_partitions(temp_prefix)
            remove_previous_partitions(backup_prefix)
            remove_previous_partitions(legacy_backup_prefix)

            rows = parquet_row_count(con, path)
            existing_columns = {c.lower() for c in parquet_columns(con, path)}
            partition_cols = [c for c in ("source", "year", "month", "day") if c in existing_columns]
            select_star = (
                f"* EXCLUDE ({', '.join(sql_ident(c) for c in partition_cols)})"
                if partition_cols
                else "*"
            )
            con.execute(f"""
                COPY (
                    SELECT
                        {select_star},
                        '{source_key}' AS source,
                        strftime({ist_timestamp_expr("reqTimeSec")}, '%Y') AS year,
                        strftime({ist_timestamp_expr("reqTimeSec")}, '%m') AS month,
                        strftime({ist_timestamp_expr("reqTimeSec")}, '%d') AS day
                    FROM read_parquet('{sql_path(path)}')
                )
                TO '{sql_path(LAKE_FOLDER)}'
                (
                    FORMAT PARQUET,
                    PARTITION_BY (source, year, month, day),
                    FILENAME_PATTERN '{temp_prefix}_{{i}}',
                    COMPRESSION {COMPRESSION},
                    COMPRESSION_LEVEL {COMP_LEVEL},
                    OVERWRITE_OR_IGNORE true
                )
            """)

            backed_up = move_partitions(file_prefix, backup_prefix)
            legacy_backed_up = move_partitions(legacy_prefix, legacy_backup_prefix)
            promoted = promote_temp_partitions(temp_prefix, file_prefix)
            if rows > 0 and promoted == 0:
                raise RuntimeError("DuckDB COPY completed but no temp lake partitions were promoted.")
            removed = remove_previous_partitions(backup_prefix)
            removed += remove_previous_partitions(legacy_backup_prefix)
            backed_up = 0
            legacy_backed_up = 0
            prune_empty_dirs()

            elapsed = time.time() - start
            state[path.name] = {
                "status": "ok",
                "partition_version": PARTITION_VERSION,
                "partition_timezone": "Asia/Kolkata",
                "source_key": source_key,
                "file_prefix": file_prefix,
                "signature": sig,
                "src_prefix": file_prefix,
                "rows": rows,
                "removed_previous_files": removed,
                "promoted_files": promoted,
                "elapsed_sec": round(elapsed, 2),
                "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state)
            print(f"[done] {path.name} | {rows:,} rows | {elapsed:.1f}s | removed={removed} promoted={promoted}")

        except Exception as e:
            had_errors = True
            remove_previous_partitions(temp_prefix)
            if backed_up:
                remove_previous_partitions(file_prefix)
                restored = move_partitions(backup_prefix, file_prefix)
                print(f"[recover] Restored {restored} previous lake partition file(s) for {file_prefix}.")
            if legacy_backed_up:
                restored = move_partitions(legacy_backup_prefix, legacy_prefix)
                print(f"[recover] Restored {restored} previous lake partition file(s) for {legacy_prefix}.")
            prune_empty_dirs()
            elapsed = time.time() - start
            state[path.name] = {
                "status": "error",
                "partition_version": PARTITION_VERSION,
                "partition_timezone": "Asia/Kolkata",
                "source_key": source_key,
                "file_prefix": file_prefix,
                "signature": sig,
                "src_prefix": file_prefix,
                "error": str(e),
                "elapsed_sec": round(elapsed, 2),
                "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state)
            print(f"[error] {path.name} failed: {e}")

    con.close()
    if had_errors:
        raise SystemExit("03.py failed. Existing source partitions were preserved where possible; check .etl_03_state.json.")
    print("=" * 72)
    print(f"Lake partitioning complete. Lake: {LAKE_FOLDER}")
    print(f"State file: {STATE_FILE}")
    for line in list_summary():
        print(f"  {line}")
    print("=" * 72)


if __name__ == "__main__":
    main()
