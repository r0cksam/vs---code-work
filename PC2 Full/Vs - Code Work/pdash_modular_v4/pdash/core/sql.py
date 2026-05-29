"""DuckDB SQL helpers."""
from __future__ import annotations
import hashlib
import json
from typing import Any


def dq(identifier: str) -> str:
    """DuckDB-safe double-quoted identifier."""
    return '"' + str(identifier).replace('"', '""') + '"'


def sql_literal(value: Any) -> str:
    """Very small literal escaper for values we embed in generated SQL."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def make_cache_key(*parts: Any) -> str:
    payload = json.dumps(parts, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_filter_where(filters: dict | None = None, dual: dict | None = None) -> str:
    """Build WHERE conditions for exact string matching.

    Standard filters are AND-ed. Values inside one column are OR-ed via IN.
    Dual filter is OR between two column/value sets, then AND-ed with standard filters.
    """
    clauses: list[str] = []
    filters = filters or {}
    dual = dual or {}

    for col, vals in filters.items():
        vals = [v for v in (vals or [])]
        if not col or not vals:
            continue
        vals_sql = ", ".join(sql_literal(v) for v in vals)
        clauses.append(f"TRY_CAST({dq(col)} AS VARCHAR) IN ({vals_sql})")

    dual_parts: list[str] = []
    if dual:
        if dual.get("col_a") and dual.get("vals_a"):
            vals = ", ".join(sql_literal(v) for v in dual.get("vals_a", []))
            dual_parts.append(f"TRY_CAST({dq(dual['col_a'])} AS VARCHAR) IN ({vals})")
        if dual.get("col_b") and dual.get("vals_b"):
            vals = ", ".join(sql_literal(v) for v in dual.get("vals_b", []))
            dual_parts.append(f"TRY_CAST({dq(dual['col_b'])} AS VARCHAR) IN ({vals})")
    if dual_parts:
        clauses.append("(" + " OR ".join(dual_parts) + ")")

    return "WHERE " + " AND ".join(clauses) if clauses else ""
