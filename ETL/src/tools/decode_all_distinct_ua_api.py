#!/usr/bin/env python3
"""Fill a full WhatMyUserAgent API cache for every distinct valid UA.

This builds the reusable API reference layer. Large log datasets should join
to the final UA lookup by ua_hash instead of calling the API per log row.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
THIS_DIR = Path(__file__).resolve().parent
DECODER_PATH = THIS_DIR / "decode_distinct_ua_lookup.py"
DEFAULT_INPUT = ETL_ROOT / "distinct_UA_Both_All.csv"
DEFAULT_UA_DAILY = ETL_ROOT / "output" / "watch_hours" / "daily_tables" / "user_agents_daily.parquet"
DEFAULT_CACHE = ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_all_distinct_ua_cache.parquet"
DEFAULT_OUT_DIR = ETL_ROOT / "output" / "device_decode"


def load_decoder() -> Any:
    spec = importlib.util.spec_from_file_location("decode_distinct_ua_lookup", DECODER_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load decoder module from {DECODER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


decoder = load_decoder()


def log(message: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {message}", flush=True)


def build_impact(ua_daily_path: Path) -> pd.DataFrame:
    if not ua_daily_path.exists():
        return pd.DataFrame(columns=["ua_hash", "rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"])
    daily = pd.read_parquet(ua_daily_path)
    if daily.empty or "userAgent" not in daily.columns:
        return pd.DataFrame(columns=["ua_hash", "rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"])
    daily = daily.copy()
    daily["ua_hash"] = daily["userAgent"].map(decoder.normalize_ua).map(decoder.ua_hash)
    for col in ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"]:
        if col not in daily.columns:
            daily[col] = 0
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0)
    return (
        daily.groupby("ua_hash", dropna=False)[["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"]]
        .sum()
        .reset_index()
    )


def load_cache(path: Path) -> pd.DataFrame:
    return decoder.load_api_cache(path)


def save_cache(cache: pd.DataFrame, path: Path) -> None:
    decoder.save_api_cache(cache, path)


def select_candidates(distinct: pd.DataFrame, combined_cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    candidates = distinct.copy()
    candidates["malformed_reason"] = candidates["ua_norm"].map(decoder.malformed_reason)
    if not args.include_malformed:
        candidates = candidates[candidates["malformed_reason"].eq("")].copy()

    cached = set(combined_cache["ua_hash"].astype(str)) if not combined_cache.empty else set()
    candidates = candidates[~candidates["ua_hash"].astype(str).isin(cached)].copy()

    impact = build_impact(args.ua_daily)
    if not impact.empty:
        candidates = candidates.merge(impact, on="ua_hash", how="left")
    for col in ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"]:
        if col not in candidates.columns:
            candidates[col] = 0
        candidates[col] = pd.to_numeric(candidates[col], errors="coerce").fillna(0)

    candidates["_score"] = (
        candidates["raw_ts_rows"] * 100
        + candidates["approx_unique_ips"]
        + candidates["rows"] * 0.01
    )
    candidates = candidates.sort_values(["_score", "ua_norm"], ascending=[False, True]).drop(columns=["_score"])
    if args.api_limit > 0:
        candidates = candidates.head(args.api_limit)
    return candidates


def run_api(candidates: pd.DataFrame, own_cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    if args.api_limit == 0 or candidates.empty:
        return own_cache
    rows: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(candidates.iterrows(), start=1):
        ua = decoder.safe_text(row["ua_norm"])
        ua_hash = decoder.safe_text(row["ua_hash"])
        log(f"API full-fill {idx}/{len(candidates)} ua_hash={ua_hash[:12]} ua={ua[:100]}")
        try:
            decoded = decoder.api_decode(ua, args)
            rows.append({"ua_hash": ua_hash, **decoded})
        except Exception as exc:
            rows.append(decoder.api_error_row(ua_hash, str(exc)))
            log(f"API error: {exc}")
            if args.stop_on_rate_limit and "rate limit" in str(exc).lower():
                log("Stopping because API appears rate-limited. Cache is resumable.")
                break

        if args.api_flush_every > 0 and len(rows) >= args.api_flush_every:
            own_cache = pd.concat([own_cache, pd.DataFrame(rows)], ignore_index=True)
            save_cache(own_cache, args.api_cache)
            rows = []

        if idx < len(candidates):
            time.sleep(random.uniform(args.api_sleep_min_seconds, args.api_sleep_max_seconds))

    if rows:
        own_cache = pd.concat([own_cache, pd.DataFrame(rows)], ignore_index=True)
        save_cache(own_cache, args.api_cache)
    return load_cache(args.api_cache)


def write_manifest(args: argparse.Namespace, distinct: pd.DataFrame, valid_count: int, combined_before: pd.DataFrame, own_after: pd.DataFrame, selected_count: int, started: float) -> None:
    combined_after = decoder.combine_api_caches(
        load_cache(decoder.DEFAULT_CACHE),
        load_cache(decoder.DEFAULT_LOCAL_VERIFIED_CROSSCHECK_CACHE),
        own_after,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": str(args.input),
        "api_url": args.api_url,
        "api_cache": str(args.api_cache),
        "stats": {
            "distinct_ua_rows": int(len(distinct)),
            "valid_distinct_ua_rows": int(valid_count),
            "combined_api_cached_before": int(len(combined_before)),
            "new_candidates_this_run": int(selected_count),
            "own_full_cache_rows": int(len(own_after)),
            "combined_api_cached_after": int(len(combined_after)),
            "remaining_valid_after": int(max(valid_count - len(combined_after), 0)),
            "elapsed_seconds": round(time.time() - started, 2),
        },
    }
    path = args.out_dir / "ua_api_all_distinct_manifest.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log(f"Manifest written: {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="API-decode every distinct valid UA into a reusable cache.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--column", default=None)
    parser.add_argument("--ua-daily", type=Path, default=DEFAULT_UA_DAILY)
    parser.add_argument("--api-cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--api-limit", type=int, default=100, help="0 status/manifest only; positive checks N new UAs; -1 checks all remaining valid UAs.")
    parser.add_argument("--api-key", default="NOTREQUIED")
    parser.add_argument("--api-url", default=decoder.DEFAULT_API_URL)
    parser.add_argument("--api-timeout", type=float, default=20.0)
    parser.add_argument("--api-sleep-min-seconds", type=float, default=2.0)
    parser.add_argument("--api-sleep-max-seconds", type=float, default=5.0)
    parser.add_argument("--api-flush-every", type=int, default=5)
    parser.add_argument("--stop-on-rate-limit", action="store_true", default=True)
    parser.add_argument("--include-malformed", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.input = args.input.expanduser().resolve()
    args.ua_daily = args.ua_daily.expanduser().resolve()
    args.api_cache = args.api_cache.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()

    started = time.time()
    distinct = decoder.read_distinct_ua_csv(args.input, args.column)
    valid_count = int(distinct["ua_norm"].map(decoder.malformed_reason).eq("").sum())
    own_cache = load_cache(args.api_cache)
    combined_before = decoder.combine_api_caches(
        load_cache(decoder.DEFAULT_CACHE),
        load_cache(decoder.DEFAULT_LOCAL_VERIFIED_CROSSCHECK_CACHE),
        own_cache,
    )
    candidates = select_candidates(distinct, combined_before, args)
    log(f"Distinct UA rows: {len(distinct):,}; valid: {valid_count:,}; combined API cached: {len(combined_before):,}; selected this run: {len(candidates):,}")
    own_after = run_api(candidates, own_cache, args)
    write_manifest(args, distinct, valid_count, combined_before, own_after, len(candidates), started)


if __name__ == "__main__":
    main()
