"""Metadata index for selected parquet folders.

This avoids re-reading schema/row metadata in multiple tabs and gives the app one
source of truth for file inventory, columns, and folder stats.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from pdash.config import CONFIG
from pdash.utils.logging_utils import log_warning
from pdash.core.disk_cache import refresh_metadata_db


@dataclass
class MetadataIndex:
    folders: list[str]
    files: list[str]
    columns: list[str]
    column_types: dict[str, str]
    file_columns: dict[str, set[str]]
    row_counts: dict[str, int]

    @property
    def total_rows(self) -> int:
        return int(sum(self.row_counts.values()))

    @property
    def n_folders(self) -> int:
        return len({str(Path(f).parent) for f in self.files})

    def as_files_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "file": self.files,
            "folder": [str(Path(f).parent) for f in self.files],
            "rows": [self.row_counts.get(f, 0) for f in self.files],
            "n_columns": [len(self.file_columns.get(f, set())) for f in self.files],
        })


@st.cache_data(show_spinner=False, ttl=CONFIG.cache_ttl_seconds)
def collect_files(folders: list[str]) -> list[Path]:
    files: list[Path] = []
    for f in folders:
        p = Path(str(f).strip())
        if not p.is_dir():
            continue
        try:
            files.extend(sorted(p.glob("*.parquet")))
        except Exception as e:
            log_warning(f"Could not list parquet files in {p}", e)
    return files


@st.cache_data(show_spinner="Building parquet metadata index ...", ttl=CONFIG.cache_ttl_seconds)
def build_metadata_index(folder_key: str) -> MetadataIndex:
    folders = [f for f in folder_key.split("|") if f]
    files = [str(f) for f in collect_files(folders)]
    try:
        refresh_metadata_db(files)
    except Exception as e:
        log_warning("Could not refresh persistent metadata DB", e)
    all_cols: dict[str, str] = {}
    file_cols: dict[str, set[str]] = {}
    row_counts: dict[str, int] = {}

    for f in files:
        try:
            schema = pq.read_schema(f)
            names = set(schema.names)
            file_cols[f] = names
            for name in schema.names:
                all_cols.setdefault(name, str(schema.field(name).type))
            row_counts[f] = int(pq.read_metadata(f).num_rows)
        except Exception as e:
            log_warning(f"Could not read parquet metadata for {f}", e)
            file_cols[f] = set()
            row_counts[f] = 0

    return MetadataIndex(
        folders=folders,
        files=files,
        columns=list(all_cols.keys()),
        column_types=all_cols,
        file_columns=file_cols,
        row_counts=row_counts,
    )


@st.cache_data(show_spinner=False, ttl=CONFIG.cache_ttl_seconds)
def get_file_columns(folder_key: str) -> dict:
    return build_metadata_index(folder_key).file_columns


@st.cache_data(show_spinner="Reading column schema from parquet files ...", ttl=CONFIG.cache_ttl_seconds)
def load_schema(folder_key: str):
    idx = build_metadata_index(folder_key)
    return idx.columns, idx.column_types


@st.cache_data(show_spinner="Counting total rows across all files ...", ttl=CONFIG.cache_ttl_seconds)
def count_stats(folder_key: str):
    idx = build_metadata_index(folder_key)
    return len(idx.files), idx.total_rows, idx.n_folders
