"""
lib/utils.py — Shared utility functions: numeric coercion, geo helpers, ASN normalisation.
"""

from __future__ import annotations

import json
from urllib.parse import unquote_plus

import pandas as pd

from .constants import COUNTRY_LABELS, CHUNK_DURATION_HOURS


# ── Numeric helpers ───────────────────────────────────────────────────────────

def numeric(df: pd.DataFrame, column: str) -> pd.Series:
    """Coerce a DataFrame column to float, filling NaN with 0."""
    if df.empty or column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def total(df: pd.DataFrame, column: str) -> float:
    """Sum a column safely, returning 0.0 if absent or empty."""
    if df.empty or column not in df.columns:
        return 0.0
    return float(numeric(df, column).sum())


def pct(part: float, whole: float) -> float:
    """Safe percentage: returns 0.0 when whole is zero."""
    return float(part / whole * 100.0) if whole else 0.0


def with_numbers(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return a copy of df with the given columns coerced to float."""
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = numeric(out, col)
    return out


# ── DataFrame serialisation ───────────────────────────────────────────────────

def records(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Convert a DataFrame (or subset) to a list of plain dicts."""
    if df.empty:
        return []
    out = df.copy()
    if columns:
        out = out[[col for col in columns if col in out.columns]]
    if limit:
        out = out.head(limit)
    return json.loads(out.to_json(orient="records", date_format="iso"))


def one_row(df: pd.DataFrame) -> dict:
    """Return the first row as a dict, or {} if empty."""
    rows = records(df, limit=1)
    return rows[0] if rows else {}


# ── String / encoding helpers ─────────────────────────────────────────────────

def decode_cols(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """URL-decode the given string columns in a copy of df."""
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).map(unquote_plus)
    return out


# ── ASN normalisation ─────────────────────────────────────────────────────────

def normalize_asn(value: object) -> str:
    """Strip 'AS' prefix, trailing '.0', and non-digit chars from an ASN value."""
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if text.startswith("AS"):
        text = text[2:].strip()
    if text.endswith(".0"):
        text = text[:-2]
    return "".join(ch for ch in text if ch.isdigit())


# ── Geo helpers ───────────────────────────────────────────────────────────────

def country_label(value: object) -> str:
    """Map a 2-letter country code to a human-readable label."""
    code = "" if pd.isna(value) else str(value).strip().upper()
    if not code:
        return "Unknown"
    if code == "IN":
        return "India"
    name = COUNTRY_LABELS.get(code)
    return f"{name} ({code})" if name else code


def geo_text(value: object, fallback: str) -> str:
    """Return stripped string or fallback if blank/null."""
    if pd.isna(value):
        return fallback
    text = str(value).strip()
    return text if text else fallback


# ── Watch-hours helpers ───────────────────────────────────────────────────────

def add_watch_hours_from_ts(df: pd.DataFrame) -> pd.DataFrame:
    """Add a watch_hours column derived from ts_rows * CHUNK_DURATION_HOURS."""
    out = df.copy()
    if not out.empty and "ts_rows" in out.columns:
        out["watch_hours"] = numeric(out, "ts_rows") * CHUNK_DURATION_HOURS
    return out
