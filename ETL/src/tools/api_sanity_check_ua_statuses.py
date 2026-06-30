#!/usr/bin/env python3
"""Fresh API sanity check for selected UA decode statuses.

This audit tool intentionally writes separate outputs and does not update the
production UA decode lookup. It is useful when local/unknown/malformed rows need
one independent WhatMyUserAgent verification pass.
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
DEFAULT_LOOKUP = ETL_ROOT / "output" / "device_decode" / "ua_decode_lookup_both_all.parquet"
DEFAULT_OUT_DIR = ETL_ROOT / "output" / "device_decode" / "api_sanity"
DEFAULT_CACHE = ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_status_sanity_cache.parquet"


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


def read_lookup(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Lookup not found: {path}")
    frame = pd.read_parquet(path)
    for column in ["UA", "ua_norm", "ua_hash", "decode_status"]:
        if column not in frame.columns:
            frame[column] = ""
    frame["ua_norm"] = frame["ua_norm"].fillna("").astype(str)
    frame.loc[frame["ua_norm"].str.strip().eq(""), "ua_norm"] = frame.loc[
        frame["ua_norm"].str.strip().eq(""),
        "UA",
    ].map(decoder.normalize_ua)
    frame["ua_hash"] = frame["ua_hash"].fillna("").astype(str)
    frame.loc[frame["ua_hash"].str.strip().eq(""), "ua_hash"] = frame.loc[
        frame["ua_hash"].str.strip().eq(""),
        "ua_norm",
    ].map(decoder.ua_hash)
    return frame


def load_cache(path: Path) -> pd.DataFrame:
    return decoder.load_api_cache(path)


def save_cache(cache: pd.DataFrame, path: Path) -> None:
    decoder.save_api_cache(cache, path)


def select_candidates(lookup: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    statuses = {item.strip() for item in args.statuses.split(",") if item.strip()}
    candidates = lookup[lookup["decode_status"].fillna("").isin(statuses)].copy()
    candidates = candidates[candidates["ua_norm"].fillna("").astype(str).str.strip().ne("")]
    if not args.include_malformed and "is_malformed" in candidates.columns:
        candidates = candidates[~candidates["is_malformed"].astype(str).str.lower().isin(["true", "1"])]
    cached = set(cache["ua_hash"].astype(str)) if not cache.empty and "ua_hash" in cache.columns else set()
    if not args.force:
        candidates = candidates[~candidates["ua_hash"].astype(str).isin(cached)]
    sort_cols = [col for col in ["decode_status", "confidence", "ua_norm"] if col in candidates.columns]
    candidates = candidates.sort_values(sort_cols)
    if args.api_limit > 0:
        candidates = candidates.head(args.api_limit)
    return candidates


def run_api(candidates: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    if args.api_limit == 0 or candidates.empty:
        return cache
    rows: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(candidates.iterrows(), start=1):
        ua_hash = str(row.get("ua_hash", "")).strip()
        ua = str(row.get("ua_norm", "")).strip()
        log(f"API sanity {idx}/{len(candidates)} status={row.get('decode_status')} ua_hash={ua_hash[:12]} ua={ua[:100]}")
        try:
            decoded = decoder.api_decode(ua, args)
            rows.append({"ua_hash": ua_hash, **decoded})
        except Exception as exc:
            rows.append(decoder.api_error_row(ua_hash, str(exc)))
            log(f"API error: {exc}")
            if args.stop_on_rate_limit and "rate limit" in str(exc).lower():
                log("Stopping because API appears rate-limited. Cache/report are resumable.")
                break

        if args.api_flush_every > 0 and len(rows) >= args.api_flush_every:
            cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
            save_cache(cache, args.api_cache)
            rows = []
        if idx < len(candidates):
            time.sleep(random.uniform(args.api_sleep_min_seconds, args.api_sleep_max_seconds))

    if rows:
        cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
        save_cache(cache, args.api_cache)
    return load_cache(args.api_cache)


def build_report(lookup: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    statuses = {item.strip() for item in args.statuses.split(",") if item.strip()}
    report = lookup[lookup["decode_status"].fillna("").isin(statuses)].copy()
    if cache.empty:
        for column in decoder.API_COLUMNS:
            if column not in report.columns:
                report[column] = ""
        return report
    api = cache.drop_duplicates("ua_hash", keep="last")
    report = report.merge(api, on="ua_hash", how="left", suffixes=("", "_sanity"))
    for column in decoder.API_COLUMNS:
        sanity_col = f"{column}_sanity"
        if sanity_col in report.columns:
            report[column] = report[sanity_col]
            report = report.drop(columns=[sanity_col])
        elif column not in report.columns:
            report[column] = ""
    report["sanity_has_api_identity"] = (
        report["api_status"].fillna("").eq("decoded_api")
        & (
            report["api_device_type"].fillna("").astype(str).str.strip().ne("")
            | report["api_brand"].fillna("").astype(str).str.strip().ne("")
            | report["api_model"].fillna("").astype(str).str.strip().ne("")
            | report["api_os_name"].fillna("").astype(str).str.strip().ne("")
            | report["api_browser_name"].fillna("").astype(str).str.strip().ne("")
        )
    )
    return report


def write_outputs(report: pd.DataFrame, args: argparse.Namespace, started: float, candidates_count: int) -> None:
    args.out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.out_dir / f"{args.output_prefix}.csv"
    parquet_path = args.out_dir / f"{args.output_prefix}.parquet"
    summary_path = args.out_dir / f"{args.output_prefix}_summary.csv"
    manifest_path = args.out_dir / f"{args.output_prefix}_manifest.json"

    report.to_csv(csv_path, index=False, encoding="utf-8-sig")
    report.to_parquet(parquet_path, index=False)
    summary = (
        report.groupby(["decode_status", "api_status", "sanity_has_api_identity"], dropna=False)
        .size()
        .reset_index(name="distinct_ua_count")
        .sort_values(["decode_status", "api_status", "sanity_has_api_identity"])
    )
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lookup": str(args.lookup),
        "statuses": args.statuses,
        "api_url": args.api_url,
        "api_cache": str(args.api_cache),
        "api_limit": args.api_limit,
        "new_api_candidates_this_run": int(candidates_count),
        "report_rows": int(len(report)),
        "elapsed_seconds": round(time.time() - started, 2),
        "outputs": {
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "summary": str(summary_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log(f"Report CSV written     : {csv_path}")
    log(f"Report Parquet written : {parquet_path}")
    log(f"Summary written        : {summary_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fresh API sanity check for selected UA decode statuses.")
    parser.add_argument("--lookup", type=Path, default=DEFAULT_LOOKUP)
    parser.add_argument("--statuses", default="decoded_local,unknown,malformed")
    parser.add_argument("--api-cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--output-prefix", default="ua_status_api_sanity")
    parser.add_argument("--api-limit", type=int, default=25, help="0 report only; positive checks N rows; -1 checks all uncached rows.")
    parser.add_argument("--api-key", default="NOTREQUIED")
    parser.add_argument("--api-url", default=decoder.DEFAULT_API_URL)
    parser.add_argument("--api-timeout", type=float, default=20.0)
    parser.add_argument("--api-sleep-min-seconds", type=float, default=2.0)
    parser.add_argument("--api-sleep-max-seconds", type=float, default=5.0)
    parser.add_argument("--api-flush-every", type=int, default=5)
    parser.add_argument("--stop-on-rate-limit", action="store_true", default=True)
    parser.add_argument("--include-malformed", action="store_true")
    parser.add_argument("--force", action="store_true", help="Re-check even if the sanity cache already has the UA hash.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.lookup = args.lookup.expanduser().resolve()
    args.api_cache = args.api_cache.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()

    started = time.time()
    lookup = read_lookup(args.lookup)
    cache = load_cache(args.api_cache)
    candidates = select_candidates(lookup, cache, args)
    log(f"Selected statuses: {args.statuses}")
    log(f"Lookup rows in selected statuses: {lookup['decode_status'].fillna('').isin(set(args.statuses.split(','))).sum():,}")
    log(f"API candidates this run: {len(candidates):,}")
    cache = run_api(candidates, cache, args)
    report = build_report(lookup, cache, args)
    write_outputs(report, args, started, len(candidates))


if __name__ == "__main__":
    main()
