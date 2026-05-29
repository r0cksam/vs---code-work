"""Central query engine for pDash.

Purpose:
- keep heavy data access out of UI code
- prefer DuckDB for full-dataset aggregation
- keep PyArrow streaming fallback for difficult row materialization
- provide query-level caching and safe SQL helpers
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import streamlit as st

from pdash.common import DUCKDB_OK, duckdb
from pdash.config import CONFIG
from pdash.core.metadata import build_metadata_index, collect_files, get_file_columns
from pdash.core.sql import dq, build_filter_where, make_cache_key
from pdash.utils.logging_utils import log_warning


def _parquet_list(folder_key: str) -> list[str]:
    return build_metadata_index(folder_key).files


@st.cache_resource
def get_duckdb_conn():
    if not DUCKDB_OK:
        raise RuntimeError("DuckDB is required. Install with: pip install duckdb")
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={int(CONFIG.duckdb_threads)}")
    except Exception:
        pass
    return con


@dataclass
class QueryEngine:
    folder_key: str

    @property
    def files(self) -> list[str]:
        return _parquet_list(self.folder_key)

    @property
    def con(self):
        return get_duckdb_conn()

    def relation_sql(self) -> str:
        return f"read_parquet({self.files!r}, union_by_name=true)"

    def value_counts(self, column: str, filters: dict | None = None, dual: dict | None = None,
                     top_n: int | None = None, include_other: bool = False) -> pd.DataFrame:
        where = build_filter_where(filters, dual)
        col = dq(column)
        limit_sql = "" if top_n is None or include_other else f"LIMIT {int(top_n)}"
        query = f"""
        SELECT
            COALESCE(NULLIF(TRY_CAST({col} AS VARCHAR), ''), '(blank)') AS value,
            COUNT(*) AS count
        FROM {self.relation_sql()}
        {where}
        GROUP BY 1
        ORDER BY count DESC
        {limit_sql}
        """
        df = self.con.execute(query).df()
        if df.empty:
            return pd.DataFrame(columns=["value", "count", "% of rows"])
        total = int(df["count"].sum())
        if include_other and top_n is not None and len(df) > int(top_n):
            head = df.head(int(top_n)).copy()
            other = int(df.iloc[int(top_n):]["count"].sum())
            df = pd.concat([head, pd.DataFrame([{"value": "Other", "count": other}])], ignore_index=True)
        df["% of rows"] = (df["count"] / max(total, 1) * 100).round(2)
        return df

    def row_count(self, filters: dict | None = None, dual: dict | None = None) -> int:
        where = build_filter_where(filters, dual)
        query = f"SELECT COUNT(*) FROM {self.relation_sql()} {where}"
        return int(self.con.execute(query).fetchone()[0] or 0)

    def completeness(self, profile_cols: list[str], mode: str, sample_rows: int) -> pd.DataFrame:
        profile_cols = list(dict.fromkeys([c for c in profile_cols if c]))
        if not profile_cols:
            return pd.DataFrame(columns=["Column", "Filled %", "Empty %", "Filled rows", "Empty/null-like rows", "Total rows checked", "Mode"])
        sample_sql = ""
        display_mode = mode
        if mode == "Fast estimate (sample)":
            sample_sql = f" USING SAMPLE {int(sample_rows or CONFIG.sample_rows)} ROWS"
            display_mode = f"Fast estimate ({int(sample_rows or CONFIG.sample_rows):,} sampled rows)"
        results = []
        for i in range(0, len(profile_cols), 50):
            chunk = profile_cols[i:i+50]
            select_parts = ["COUNT(*) AS total_rows"]
            for idx, col in enumerate(chunk):
                select_parts.append(
                    f"SUM(CASE WHEN {dq(col)} IS NULL OR TRIM(TRY_CAST({dq(col)} AS VARCHAR)) IN ('', '-', '^') THEN 1 ELSE 0 END) AS empty_{idx}"
                )
            query = f"SELECT {', '.join(select_parts)} FROM {self.relation_sql()}{sample_sql}"
            row = self.con.execute(query).fetchone()
            total = int(row[0] or 0) if row else 0
            for idx, col in enumerate(chunk):
                empty = int(row[idx + 1] or 0) if row else 0
                filled = max(total - empty, 0)
                results.append({
                    "Column": col,
                    "Filled %": round((filled / total * 100), 2) if total else 0.0,
                    "Empty %": round((empty / total * 100), 2) if total else 0.0,
                    "Filled rows": filled,
                    "Empty/null-like rows": empty,
                    "Total rows checked": total,
                    "Mode": display_mode,
                })
        return pd.DataFrame(results)

    def select_rows(self, sel_cols: list[str], filters: dict | None = None, dual: dict | None = None, max_rows=None) -> pd.DataFrame:
        if not sel_cols:
            return pd.DataFrame()
        where = build_filter_where(filters, dual)
        select_exprs = [f"{dq(c)} AS {dq(c)}" for c in sel_cols]
        # Preserve the original convenience column from older versions.
        select_sql = ", ".join(select_exprs)
        limit_sql = "" if max_rows is None else f"LIMIT {int(max_rows)}"
        query = f"""
        SELECT {select_sql}
        FROM {self.relation_sql()}
        {where}
        {limit_sql}
        """
        try:
            df = self.con.execute(query).df()
            return df
        except Exception as e:
            # If a column exists only in some files or an edge schema fails, fall back to PyArrow streaming.
            log_warning("DuckDB select_rows failed; falling back to PyArrow streaming", e)
            return pyarrow_stream_query(self.folder_key, sel_cols, filters or {}, dual or {}, max_rows=max_rows)


def get_engine(folder_key: str) -> QueryEngine:
    return QueryEngine(folder_key)


@st.cache_data(show_spinner=False, ttl=CONFIG.cache_ttl_seconds)
def cached_value_counts(folder_key: str, column: str) -> pd.DataFrame:
    return get_engine(folder_key).value_counts(column)


def _build_mask(tbl, filters: dict):
    mask = None
    for col, vals in (filters or {}).items():
        if not vals or col not in tbl.schema.names:
            continue
        col_arr = tbl.column(col).cast(pa.string())
        sub_mask = None
        for v in vals:
            eq = pc.equal(col_arr, pa.scalar(str(v), pa.string()))
            sub_mask = eq if sub_mask is None else pc.or_(sub_mask, eq)
        if sub_mask is not None:
            mask = sub_mask if mask is None else pc.and_(mask, sub_mask)
    return mask


def apply_all_filters(tbl, filters: dict, dual: dict):
    mask = None
    dual = dual or {}
    if dual and (dual.get("vals_a") or dual.get("vals_b")):
        dm = None
        if dual.get("vals_a") and dual.get("col_a") in tbl.schema.names:
            arr = tbl.column(dual["col_a"]).cast(pa.string())
            for v in dual.get("vals_a", []):
                eq = pc.equal(arr, pa.scalar(str(v), pa.string()))
                dm = eq if dm is None else pc.or_(dm, eq)
        if dual.get("vals_b") and dual.get("col_b") in tbl.schema.names:
            arr = tbl.column(dual["col_b"]).cast(pa.string())
            for v in dual.get("vals_b", []):
                eq = pc.equal(arr, pa.scalar(str(v), pa.string()))
                dm = eq if dm is None else pc.or_(dm, eq)
        if dm is not None:
            mask = dm
    std = _build_mask(tbl, filters or {})
    if std is not None:
        mask = std if mask is None else pc.and_(mask, std)
    return tbl.filter(mask) if mask is not None else tbl


def pyarrow_stream_query(folder_key: str, sel_cols: list, filters: dict, dual: dict, max_rows=None) -> pd.DataFrame:
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    frames = []
    collected = 0
    needed_cols = list(dict.fromkeys(list(sel_cols) + list((filters or {}).keys()) + [dual.get("col_a"), dual.get("col_b")]))
    needed_cols = [c for c in needed_cols if c]
    for f in files:
        if max_rows is not None and collected >= max_rows:
            break
        try:
            available = file_cols.get(str(f), set())
            avail = [c for c in needed_cols if c in available]
            if not any(c in available for c in sel_cols) or not avail:
                continue
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=avail, batch_size=CONFIG.batch_size):
                tbl = pa.Table.from_batches([batch])
                tbl = apply_all_filters(tbl, filters, dual)
                if len(tbl) == 0:
                    continue
                present = [c for c in sel_cols if c in tbl.schema.names]
                if not present:
                    continue
                tbl = tbl.select(present)
                need = (max_rows - collected) if max_rows is not None else len(tbl)
                if need <= 0:
                    break
                chunk = tbl.slice(0, need).to_pandas()
                chunk.insert(0, "_folder", Path(str(f)).parent.name)
                frames.append(chunk)
                collected += len(chunk)
                if max_rows is not None and collected >= max_rows:
                    break
        except Exception as e:
            log_warning(f"Skipped file during PyArrow query: {f}", e)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
