"""Compatibility data API used by app.py.

The heavy work now delegates to metadata.py and query_engine.py. This keeps the
existing Streamlit UI stable while giving the project a cleaner architecture.
"""
from __future__ import annotations
import os
from pathlib import Path
import urllib.parse
import time
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from pdash.common import *  # keeps existing globals/imports available to app.py
from pdash.config import CONFIG
from pdash.core.metadata import collect_files, get_file_columns, load_schema, count_stats, build_metadata_index
from pdash.core.query_engine import get_engine, apply_all_filters, pyarrow_stream_query
from pdash.core.sql import dq as _dq
from pdash.utils.logging_utils import log_warning


def scan_root(root: str) -> list:
    import os as _os
    root_path = Path(root)
    results = []

    def _walk(p: Path, depth: int):
        if depth > CONFIG.folder_scan_depth:
            return
        try:
            entries = list(_os.scandir(p))
            pq_count = sum(1 for e in entries if e.is_file(follow_symlinks=False) and e.name.endswith(".parquet"))
            subdirs = [Path(e.path) for e in entries if e.is_dir(follow_symlinks=False) and not e.name.startswith(".")]
            if pq_count > 0:
                try:
                    rel = str(p.relative_to(root_path))
                except Exception:
                    rel = p.name
                results.append({"path": str(p), "name": p.name, "n_files": pq_count, "rel": "(root)" if rel == "." else rel})
            for sub in sorted(subdirs, key=lambda x: x.name):
                _walk(sub, depth + 1)
        except (PermissionError, OSError) as e:
            log_warning(f"Skipping folder during scan: {p}", e)

    _walk(root_path, 0)
    return results


def _parse_query_string(raw_val: str) -> dict:
    try:
        parsed = dict(urllib.parse.parse_qsl(str(raw_val), keep_blank_values=True))
    except Exception:
        parsed = {}
    parsed["_raw"] = raw_val
    parsed["_count"] = 1
    return parsed


def _iter_query_string_batches(parquet_file: Path, qrystr_col: str, batch_size: int = CONFIG.small_batch_size):
    pf = pq.ParquetFile(parquet_file)
    for batch in pf.iter_batches(columns=[qrystr_col], batch_size=batch_size):
        arr = batch.column(0)
        raws = ["" if x is None else str(x) for x in arr.to_pylist()]
        if raws:
            yield pd.DataFrame.from_records([_parse_query_string(raw) for raw in raws]).fillna("")


@st.cache_data(show_spinner=False, ttl=CONFIG.cache_ttl_seconds)
def column_completeness_profile(folder_key: str, profile_cols: list, mode: str = "Exact selected columns", sample_rows: int = CONFIG.sample_rows) -> pd.DataFrame:
    if not profile_cols:
        return pd.DataFrame(columns=["Column", "Filled %", "Empty %", "Filled rows", "Empty/null-like rows", "Total rows checked", "Mode"])
    progress = st.progress(10, text="Profiling columns with DuckDB ...")
    try:
        df = get_engine(folder_key).completeness(profile_cols, mode, sample_rows)
        progress.progress(100, text="✅ Column profile complete")
        time.sleep(0.15)
        return df
    finally:
        progress.empty()


def unique_values(folder_key: str, column: str) -> pd.DataFrame:
    """Exact value counts using DuckDB over the full selected parquet dataset."""
    with st.spinner(f"Counting unique values for `{column}` using DuckDB ..."):
        try:
            return get_engine(folder_key).value_counts(column).rename(columns={"value": "value"})
        except Exception as e:
            log_warning("DuckDB unique_values failed; falling back to PyArrow", e)
            # Minimal fallback via old streaming approach.
            files = collect_files(folder_key.split("|"))
            file_cols = get_file_columns(folder_key)
            counter = {}
            bar = st.progress(0, text=f"Fallback scan ... 0 / {len(files):,} files")
            for i, f in enumerate(files):
                bar.progress(int((i + 1) / max(len(files), 1) * 100), text=f"Fallback scan ... {i+1:,} / {len(files):,} files")
                try:
                    if column not in file_cols.get(str(f), set()):
                        continue
                    pf = pq.ParquetFile(f)
                    for batch in pf.iter_batches(columns=[column], batch_size=CONFIG.batch_size):
                        import pyarrow as pa
                        import pyarrow.compute as pc
                        arr = pa.array(batch.column(0)).cast(pa.string())
                        vc = pc.value_counts(arr)
                        for item in vc:
                            val = item["values"].as_py()
                            val = str(val) if val not in (None, "") else "(blank)"
                            counter[val] = counter.get(val, 0) + int(item["counts"].as_py())
                except Exception as ee:
                    log_warning(f"Skipped file during unique value fallback: {f}", ee)
            bar.empty()
            if not counter:
                return pd.DataFrame(columns=["value", "count", "% of rows"])
            df = pd.DataFrame(counter.items(), columns=["value", "count"]).sort_values("count", ascending=False).reset_index(drop=True)
            total = int(df["count"].sum())
            df["% of rows"] = (df["count"] / max(total, 1) * 100).round(2)
            return df


def build_mask(tbl, filters: dict):
    from pdash.core.query_engine import _build_mask
    return _build_mask(tbl, filters)


def run_query(folder_key: str, sel_cols: list, filters: dict, dual: dict, max_rows=None, progress_label: str = "Scanning files") -> pd.DataFrame:
    total_est = None
    if max_rows is None:
        try:
            total_est = get_engine(folder_key).row_count(filters, dual)
            if total_est > CONFIG.export_warning_rows:
                st.warning(f"This query matches about {total_est:,} rows. CSV creation may use a lot of RAM. Consider narrowing filters or exporting fewer columns.")
        except Exception as e:
            log_warning("Could not estimate row count", e)
    with st.spinner(f"{progress_label} with query engine ..."):
        return get_engine(folder_key).select_rows(sel_cols, filters, dual, max_rows=max_rows)


def full_filtered_value_counts(folder_key: str, target_col: str, filters: dict, dual: dict, top_n: int = 20, include_other: bool = True, progress_label: str = "Building full-dataset chart") -> pd.DataFrame:
    with st.spinner(f"{progress_label} using DuckDB exact aggregation ..."):
        df = get_engine(folder_key).value_counts(target_col, filters, dual, top_n=int(top_n), include_other=bool(include_other))
    if df.empty:
        return pd.DataFrame(columns=[target_col, "count"])
    out = df.rename(columns={"value": target_col})
    if "% of rows" in out.columns:
        out = out.rename(columns={"% of rows": "% of filtered rows"})
    return out.reset_index(drop=True)


def estimate_query_rows(folder_key: str, filters: dict | None = None, dual: dict | None = None) -> int:
    return get_engine(folder_key).row_count(filters or {}, dual or {})
