"""
lib/aggregations.py — Domain-specific aggregation and transformation functions.
"""

from __future__ import annotations

import pandas as pd

from .constants import CHUNK_DURATION_HOURS, STATUS_CODE_MEANINGS
from .utils import (
    add_watch_hours_from_ts,
    country_label,
    geo_text,
    numeric,
    pct,
    records,
    total,
    with_numbers,
)


# ── Status codes ──────────────────────────────────────────────────────────────

def add_status_meanings(status_df: pd.DataFrame) -> pd.DataFrame:
    """Add a human-readable 'meaning' column to a status-code DataFrame."""
    if status_df.empty or "statusCode" not in status_df.columns:
        return status_df
    out = status_df.copy()
    out["statusCode"] = (
        out["statusCode"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    )
    out["meaning"] = out["statusCode"].map(
        lambda code: STATUS_CODE_MEANINGS.get(
            code, "Status code was present in logs but is not in the built-in reference list."
        )
    )
    return out


# ── Cache rollup ──────────────────────────────────────────────────────────────

def build_cache_rollup(cache: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cache rows per host and compute hit percentages."""
    if cache.empty:
        return cache
    out = with_numbers(cache, ["rows", "approx_unique_ips"])
    out["cacheStatus"] = out["cacheStatus"].astype(str)
    grouped = out.groupby("reqHost", as_index=False).agg(rows=("rows", "sum"))
    hits = (
        out[out["cacheStatus"] == "1"]
        .groupby("reqHost", as_index=False)
        .agg(cache_hit_rows=("rows", "sum"))
    )
    grouped = grouped.merge(hits, on="reqHost", how="left").fillna({"cache_hit_rows": 0})
    grouped["cache_hit_pct"] = grouped.apply(
        lambda r: pct(r["cache_hit_rows"], r["rows"]), axis=1
    )
    return grouped.sort_values("rows", ascending=False)


# ── Query review ──────────────────────────────────────────────────────────────

def build_query_review(
    query_channels: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Summarise query-string channel review status.
    Returns (summary_df, non_ok_df, stats_dict).
    """
    if query_channels.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    out = with_numbers(query_channels, ["requests", "sessions", "devices", "unique_viewers"])
    summary = (
        out.groupby("review_status", as_index=False)
        .agg(
            rows=("review_status", "size"),
            requests=("requests", "sum"),
            sessions=("sessions", "sum"),
            devices=("devices", "sum"),
        )
        .sort_values("requests", ascending=False)
    )
    non_ok = out[out["review_status"] != "ok"].sort_values("requests", ascending=False)
    ok_requests  = total(out[out["review_status"] == "ok"], "requests")
    all_requests = total(out, "requests")
    stats = {
        "review_rows":            int(len(out)),
        "query_review_requests":  all_requests,
        "query_ok_requests":      ok_requests,
        "query_ok_request_pct":   pct(ok_requests, all_requests),
        "non_ok_rows":            int(len(non_ok)),
    }
    return summary, non_ok, stats


# ── Geo breakdown ─────────────────────────────────────────────────────────────

def _aggregate_geo_rows(
    df: pd.DataFrame,
    group_cols: list[str],
    total_watch_hours: float,
    limit: int | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(group_cols, as_index=False, dropna=False)
        .agg(
            watch_hours=("watch_hours", "sum"),
            approx_unique_ips=("approx_unique_ips", "sum"),
        )
        .sort_values("watch_hours", ascending=False)
    )
    grouped["share_pct"] = grouped["watch_hours"].map(
        lambda v: pct(v, total_watch_hours)
    )
    return grouped.head(limit) if limit else grouped


def build_geo_breakdown(geo: pd.DataFrame) -> dict:
    """Return a structured geo breakdown dict ready for JSON serialisation."""
    _EMPTY = {
        "country_split": [],
        "india_regions":  [],
        "india_cities":   [],
        "other_regions":  [],
        "other_cities":   [],
    }
    if geo.empty:
        return _EMPTY

    out = geo.copy()
    for col in ["country", "state", "city", "watch_hours", "approx_unique_ips"]:
        if col not in out.columns:
            out[col] = 0 if col in ("watch_hours", "approx_unique_ips") else ""

    out["country_code"]  = out["country"].fillna("").astype(str).str.strip().str.upper()
    out["country_label"] = out["country_code"].map(country_label)
    out["state_region"]  = out["state"].map(lambda v: geo_text(v, "Unknown Region"))
    out["city_name"]     = out["city"].map(lambda v: geo_text(v, ""))

    total_hours = total(out, "watch_hours")
    india = out[out["country_code"] == "IN"].copy()
    other = out[out["country_code"] != "IN"].copy()

    country_split = pd.DataFrame([
        {
            "segment": "India",
            "watch_hours":        total(india, "watch_hours"),
            "approx_unique_ips":  total(india, "approx_unique_ips"),
        },
        {
            "segment": "Other Countries",
            "watch_hours":        total(other, "watch_hours"),
            "approx_unique_ips":  total(other, "approx_unique_ips"),
        },
    ])
    country_split["share_pct"] = country_split["watch_hours"].map(
        lambda v: pct(v, total_hours)
    )

    india_total = total(india, "watch_hours")
    other_total = total(other, "watch_hours")

    return {
        "country_split": records(
            country_split.sort_values("watch_hours", ascending=False),
            ["segment", "watch_hours", "share_pct", "approx_unique_ips"],
        ),
        "india_regions": records(
            _aggregate_geo_rows(india, ["state_region"], india_total, 30),
            ["state_region", "watch_hours", "share_pct", "approx_unique_ips"],
        ),
        "india_cities": records(
            _aggregate_geo_rows(india[india["city_name"] != ""], ["city_name", "state_region"], india_total, 30),
            ["city_name", "state_region", "watch_hours", "share_pct", "approx_unique_ips"],
        ),
        "other_regions": records(
            _aggregate_geo_rows(other, ["country_label", "state_region"], other_total, 40),
            ["country_label", "state_region", "watch_hours", "share_pct", "approx_unique_ips"],
        ),
        "other_cities": records(
            _aggregate_geo_rows(
                other[other["city_name"] != ""],
                ["country_label", "city_name", "state_region"],
                other_total, 40,
            ),
            ["country_label", "city_name", "state_region", "watch_hours", "share_pct", "approx_unique_ips"],
        ),
    }


# ── Channel helpers ───────────────────────────────────────────────────────────

def build_channel_daily_series(
    channel_daily: pd.DataFrame,
    top_channels: list[str],
) -> list[dict]:
    """Build per-channel daily time-series for the top N channels."""
    if channel_daily.empty or not top_channels:
        return []
    out = with_numbers(channel_daily, ["raw_watch_hours", "raw_ts_chunks"])
    out = out[out["channel_name"].isin(top_channels)].copy()
    out["log_date"] = pd.to_datetime(out["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["log_date"])
    return [
        {
            "channel": ch,
            "points": records(
                out[out["channel_name"] == ch].sort_values("log_date"),
                ["log_date", "raw_watch_hours"],
            ),
        }
        for ch in top_channels
    ]
