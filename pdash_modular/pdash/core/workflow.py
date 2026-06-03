"""Workflow helper messages for the Streamlit app."""
from __future__ import annotations


def next_action(valid_folders: list[str], columns: list[str] | None, has_preview: bool, has_qsa: bool) -> str:
    if not valid_folders:
        return "Start by selecting one or more folders with parquet files."
    if not columns:
        return "Schema could not be loaded. Check that selected folders contain valid parquet files."
    if not has_preview:
        return "Next: use Filter & Export to preview rows or build a full-dataset chart."
    if not has_qsa:
        return "Next: parse queryStr in Query String Analyzer to unlock behavior dashboards."
    return "You are ready for User Behavior and Global Behavior analysis."
