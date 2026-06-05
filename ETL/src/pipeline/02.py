import duckdb
import hashlib
import json
import os
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path


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
OUTPUT_FILE_SUFFIX = "_final_clean.parquet"
STATE_FILE = BASE_FOLDER / ".etl_02_state.json"

THREADS = int(os.getenv("VG_ETL_THREADS", "8"))
MEMORY_LIMIT = os.getenv("VG_ETL_MEMORY", "20GB")
TEMP_DIR = Path(os.getenv("VG_ETL_DUCKDB_TEMP", str(ETL_ROOT / "output" / "cache" / "duckdb_temp"))).expanduser()
MAX_TEMP_SIZE = os.getenv("VG_ETL_DUCKDB_MAX_TEMP", "120GB")
COMPRESSION = os.getenv("VG_ETL_STAGE_COMPRESSION", "ZSTD")
COMP_LEVEL = int(os.getenv("VG_ETL_STAGE_COMP_LEVEL", "3"))
DEDUPE_MODE = os.getenv("VG_ETL_DEDUPE_MODE", "auto").strip().lower()
DEDUPE_BUCKETS = int(os.getenv("VG_ETL_DEDUPE_BUCKETS", "32"))
DEDUPE_BUCKET_THRESHOLD_ROWS = int(os.getenv("VG_ETL_DEDUPE_BUCKET_THRESHOLD_ROWS", "15000000"))
SHOW_PROGRESS = os.getenv("VG_ETL_PROGRESS", "0").strip().lower() in {"1", "true", "yes", "on"}
PROCESS_SOURCES = {
    item.strip()
    for item in os.getenv("VG_ETL_PROCESS_SOURCES", "").split(",")
    if item.strip()
}


def folder_signature(folder: Path) -> str:
    h = hashlib.blake2b(digest_size=16)
    files = sorted(folder.glob("*.parquet"), key=lambda p: p.name)
    for p in files:
        stat = p.stat()
        h.update(f"{p.name}|{stat.st_size}|{stat.st_mtime_ns}|".encode("utf-8"))
    return h.hexdigest()


def parquet_folder_sort_key(path: Path) -> tuple[int, int, str]:
    match = re.fullmatch(r"(\d+)_parquet", path.name)
    if match:
        return (0, int(match.group(1)), path.name.lower())
    return (1, 0, path.name.lower())


def load_stage_jobs() -> list[dict] | None:
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
        source_id = str(item.get("source_id") or "").strip()
        parquet_dir = item.get("parquet_dir")
        final_clean_file = item.get("final_clean_file")
        if not source_id or not parquet_dir or not final_clean_file:
            raise SystemExit("Each stage job needs source_id, parquet_dir, and final_clean_file.")
        jobs.append(
            {
                "source_id": source_id,
                "folder": Path(parquet_dir).expanduser(),
                "output_file": Path(final_clean_file).expanduser(),
                "state_key": source_id,
            }
        )
    return jobs


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print(f"[warn] Could not read state file {STATE_FILE}: {exc}. Rechecking all parquet folders.")
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


def parquet_row_count(con: duckdb.DuckDBPyConnection, path_or_glob: str) -> int:
    row = con.execute(f"""
        SELECT COALESCE(SUM(row_group_num_rows), 0)::BIGINT
        FROM (
            SELECT DISTINCT file_name, row_group_id, row_group_num_rows
            FROM parquet_metadata('{path_or_glob}')
        )
    """).fetchone()
    return int(row[0] or 0)


def parquet_columns(con: duckdb.DuckDBPyConnection, path_or_glob: str) -> list[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_or_glob}')").fetchall()
    return [str(row[0]) for row in rows]


def is_memory_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "out of memory" in text or "allocation failure" in text


def dedupe_single(con: duckdb.DuckDBPyConnection, input_glob: str, tmp_output_file: Path) -> str:
    con.execute(f"""
        COPY (
            SELECT DISTINCT *
            FROM read_parquet('{input_glob}')
        )
        TO '{sql_path(tmp_output_file)}'
        (
            FORMAT PARQUET,
            COMPRESSION {COMPRESSION},
            COMPRESSION_LEVEL {COMP_LEVEL}
        );
    """)
    return "single"


def dedupe_bucketed(
    con: duckdb.DuckDBPyConnection,
    input_glob: str,
    tmp_output_file: Path,
    buckets: int,
) -> str:
    buckets = max(2, buckets)
    columns = parquet_columns(con, input_glob)
    if not columns:
        raise RuntimeError("Input parquet has no columns.")

    bucket_dir = tmp_output_file.with_name(f"{tmp_output_file.stem}.dedupe_parts")
    shutil.rmtree(bucket_dir, ignore_errors=True)
    bucket_dir.mkdir(parents=True, exist_ok=True)

    hash_expr = "hash(" + ", ".join(sql_ident(c) for c in columns) + ")"
    try:
        for bucket in range(buckets):
            part_file = bucket_dir / f"part_{bucket:04d}.parquet"
            print(f"  [bucket {bucket + 1:02d}/{buckets:02d}] distinct")
            con.execute(f"""
                COPY (
                    SELECT DISTINCT *
                    FROM read_parquet('{input_glob}')
                    WHERE ({hash_expr} % {buckets}) = {bucket}
                )
                TO '{sql_path(part_file)}'
                (
                    FORMAT PARQUET,
                    COMPRESSION {COMPRESSION},
                    COMPRESSION_LEVEL {COMP_LEVEL}
                );
            """)

        con.execute(f"""
            COPY (
                SELECT *
                FROM read_parquet('{sql_path(bucket_dir)}/*.parquet')
            )
            TO '{sql_path(tmp_output_file)}'
            (
                FORMAT PARQUET,
                COMPRESSION {COMPRESSION},
                COMPRESSION_LEVEL {COMP_LEVEL}
            );
        """)
    finally:
        shutil.rmtree(bucket_dir, ignore_errors=True)

    return f"bucketed_{buckets}"


def dedupe_to_parquet(
    con: duckdb.DuckDBPyConnection,
    folder: Path,
    tmp_output_file: Path,
    input_rows: int,
) -> str:
    mode = DEDUPE_MODE if DEDUPE_MODE in {"auto", "single", "bucketed"} else "auto"
    input_glob = f"{sql_path(folder)}/*.parquet"
    use_bucketed = mode == "bucketed" or (
        mode == "auto" and input_rows >= DEDUPE_BUCKET_THRESHOLD_ROWS
    )

    if use_bucketed:
        print(
            f"[dedupe] using bucketed mode: rows={input_rows:,}, "
            f"buckets={max(2, DEDUPE_BUCKETS)}"
        )
        return dedupe_bucketed(con, input_glob, tmp_output_file, DEDUPE_BUCKETS)

    try:
        print(f"[dedupe] using single-pass mode: rows={input_rows:,}")
        return dedupe_single(con, input_glob, tmp_output_file)
    except Exception as exc:
        if mode == "auto" and is_memory_error(exc):
            tmp_output_file.unlink(missing_ok=True)
            print(f"[warn] single-pass dedupe hit memory error: {exc}")
            print("[dedupe] retrying with bucketed mode.")
            return dedupe_bucketed(con, input_glob, tmp_output_file, DEDUPE_BUCKETS)
        raise


def main() -> None:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={THREADS};")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory='{sql_path(TEMP_DIR)}';")
    con.execute(f"SET max_temp_directory_size='{MAX_TEMP_SIZE}';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute(f"SET enable_progress_bar={'true' if SHOW_PROGRESS else 'false'};")
    con.execute(f"SET enable_progress_bar_print={'true' if SHOW_PROGRESS else 'false'};")

    state = load_state()

    jobs = load_stage_jobs()
    if jobs is None:
        folders = sorted(
            [
                p for p in BASE_FOLDER.iterdir()
                if p.is_dir() and p.name.lower().endswith("_parquet")
            ],
            key=parquet_folder_sort_key,
        )
        if PROCESS_SOURCES:
            folders = [
                p for p in folders
                if p.name[:-len("_parquet")] in PROCESS_SOURCES
            ]
        jobs = [
            {
                "source_id": folder.name[:-len("_parquet")],
                "folder": folder,
                "output_file": BASE_FOLDER / f"{folder.name[:-len('_parquet')]}{OUTPUT_FILE_SUFFIX}",
                "state_key": folder.name,
            }
            for folder in folders
        ]

    print(f"Found {len(jobs)} parquet job(s).")
    if PROCESS_SOURCES:
        print(f"Scoped source IDs: {', '.join(sorted(PROCESS_SOURCES))}")
        if not jobs:
            raise SystemExit("02.py scope matched no parquet folders. Check VG_ETL_PROCESS_SOURCES.")

    had_errors = False

    for job in jobs:
        folder = Path(job["folder"])
        source_id = str(job["source_id"])
        output_file = Path(job["output_file"])
        output_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_output_file = output_file.with_name(f"{output_file.stem}.tmp{output_file.suffix}")
        signature = folder_signature(folder)
        state_key = str(job.get("state_key") or source_id)
        rec = state.get(state_key, {})
        if not isinstance(rec, dict):
            rec = {}

        already_ok = rec.get("status") == "ok" and rec.get("signature") == signature
        if already_ok and output_file.exists() and output_file.stat().st_size > 0:
            print(f"[skip] {source_id} unchanged, output exists.")
            continue

        print(f"[processing] {source_id}")
        print(f"Input : {folder}")
        print(f"Output: {output_file}")
        start_time = time.time()

        try:
            tmp_output_file.unlink(missing_ok=True)
            input_rows = parquet_row_count(con, f"{sql_path(folder)}/*.parquet")
            dedupe_method = dedupe_to_parquet(con, folder, tmp_output_file, input_rows)

            rows = parquet_row_count(con, sql_path(tmp_output_file))
            tmp_output_file.replace(output_file)
            elapsed = time.time() - start_time
            state[state_key] = {
                "status": "ok",
                "signature": signature,
                "source_id": source_id,
                "input_folder": str(folder),
                "output_file": output_file.name,
                "dedupe_method": dedupe_method,
                "input_rows": input_rows,
                "rows": rows,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "elapsed_sec": round(elapsed, 2),
            }
            save_state(state)
            print(f"[done] {source_id} in {elapsed / 60:.1f} min ({rows:,} rows)")

        except Exception as e:
            had_errors = True
            tmp_output_file.unlink(missing_ok=True)
            elapsed = time.time() - start_time
            state[state_key] = {
                "status": "error",
                "signature": signature,
                "source_id": source_id,
                "input_folder": str(folder),
                "output_file": output_file.name,
                "error": str(e),
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "elapsed_sec": round(elapsed, 2),
            }
            save_state(state)
            print(f"[error] {source_id}: {e}")

    con.close()
    if had_errors:
        raise SystemExit("02.py failed. Check the error records in .etl_02_state.json.")
    print("[done] 02.py processing complete.")


if __name__ == "__main__":
    main()
