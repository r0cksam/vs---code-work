"""Persistent disk cache and lightweight metadata database for pDash.

This layer complements Streamlit's in-memory cache. Streamlit cache is fast while
one app process is alive, but it is usually lost when Streamlit restarts. The
helpers here persist expensive Global Behavior results and file metadata under
.pdash_cache so repeated launches can reuse prior work safely.
"""
from __future__ import annotations

import hashlib
import json
import os
import pickle
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from pdash.config import CONFIG
from pdash.utils.logging_utils import log_warning

CACHE_ROOT = Path(os.getenv("PDASH_CACHE_DIR", ".pdash_cache"))
GLOBAL_CACHE_DIR = CACHE_ROOT / "global_bundles"
META_DB_PATH = CACHE_ROOT / "metadata.sqlite"
CACHE_VERSION = "v4_global_bundle_1"


def ensure_cache_dirs() -> None:
    GLOBAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def _json_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_fingerprint(files: list[str]) -> list[dict[str, Any]]:
    """Return a stable fingerprint for selected parquet files.

    Uses path + size + mtime. If a file changes, the cache key changes and the
    app recomputes automatically.
    """
    rows: list[dict[str, Any]] = []
    for f in sorted(map(str, files)):
        try:
            st = Path(f).stat()
            rows.append({"path": f, "size": int(st.st_size), "mtime_ns": int(st.st_mtime_ns)})
        except OSError as e:
            log_warning(f"Could not stat file for cache fingerprint: {f}", e)
            rows.append({"path": f, "size": 0, "mtime_ns": 0})
    return rows


def global_bundle_cache_key(files: list[str], col_map: dict, start_date: str, end_date: str,
                            daypart_mode: str, preload_top_n: int) -> str:
    payload = {
        "version": CACHE_VERSION,
        "files": file_fingerprint(files),
        "col_map": col_map,
        "start_date": start_date,
        "end_date": end_date,
        "daypart_mode": daypart_mode,
        "preload_top_n": int(preload_top_n),
    }
    return _json_hash(payload)


def _cache_path(cache_key: str) -> Path:
    ensure_cache_dirs()
    return GLOBAL_CACHE_DIR / f"{cache_key}.pkl"


def load_global_bundle_from_disk(cache_key: str, max_age_seconds: int | None = None) -> dict | None:
    path = _cache_path(cache_key)
    if not path.exists():
        return None
    try:
        if max_age_seconds is not None:
            age = time.time() - path.stat().st_mtime
            if age > max_age_seconds:
                return None
        with path.open("rb") as f:
            obj = pickle.load(f)
        if isinstance(obj, dict):
            obj["_disk_cache_hit"] = True
            obj["_disk_cache_path"] = str(path)
            return obj
    except Exception as e:
        log_warning(f"Could not load global bundle disk cache: {path}", e)
    return None


def save_global_bundle_to_disk(cache_key: str, bundle: dict) -> None:
    path = _cache_path(cache_key)
    tmp = path.with_suffix(".tmp")
    try:
        payload = dict(bundle)
        payload.pop("_disk_cache_hit", None)
        payload.pop("_disk_cache_path", None)
        with tmp.open("wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.replace(path)
    except Exception as e:
        log_warning(f"Could not save global bundle disk cache: {path}", e)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def clear_global_disk_cache() -> int:
    ensure_cache_dirs()
    count = 0
    for p in GLOBAL_CACHE_DIR.glob("*.pkl"):
        try:
            p.unlink()
            count += 1
        except Exception as e:
            log_warning(f"Could not delete cache file: {p}", e)
    return count


def _connect_meta() -> sqlite3.Connection:
    ensure_cache_dirs()
    con = sqlite3.connect(META_DB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS parquet_files (
            path TEXT PRIMARY KEY,
            folder TEXT,
            size INTEGER,
            mtime_ns INTEGER,
            rows INTEGER,
            columns_json TEXT,
            schema_json TEXT,
            updated_at REAL
        )
        """
    )
    con.commit()
    return con


def refresh_metadata_db(files: list[str]) -> dict[str, Any]:
    """Refresh and reuse metadata for parquet files in a lightweight SQLite DB."""
    con = _connect_meta()
    changed = 0
    reused = 0
    failed = 0

    for f in sorted(map(str, files)):
        try:
            p = Path(f)
            stat = p.stat()
            row = con.execute("SELECT size, mtime_ns FROM parquet_files WHERE path=?", (f,)).fetchone()
            if row and int(row[0]) == int(stat.st_size) and int(row[1]) == int(stat.st_mtime_ns):
                reused += 1
                continue

            schema = pq.read_schema(f)
            columns = list(schema.names)
            schema_map = {name: str(schema.field(name).type) for name in schema.names}
            rows = int(pq.read_metadata(f).num_rows)
            con.execute(
                """
                INSERT OR REPLACE INTO parquet_files
                (path, folder, size, mtime_ns, rows, columns_json, schema_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (f, str(p.parent), int(stat.st_size), int(stat.st_mtime_ns), rows,
                 json.dumps(columns), json.dumps(schema_map), time.time()),
            )
            changed += 1
        except Exception as e:
            failed += 1
            log_warning(f"Could not refresh metadata DB for {f}", e)
    con.commit()
    con.close()
    return {"changed": changed, "reused": reused, "failed": failed, "db_path": str(META_DB_PATH)}


def load_metadata_db_df(files: list[str]) -> pd.DataFrame:
    con = _connect_meta()
    try:
        paths = list(map(str, files))
        if not paths:
            return pd.DataFrame()
        qmarks = ",".join("?" for _ in paths)
        df = pd.read_sql_query(f"SELECT * FROM parquet_files WHERE path IN ({qmarks})", con, params=paths)
        return df
    finally:
        con.close()
