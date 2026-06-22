#!/usr/bin/env python3
"""Cross-check locally verified UA/device decodes with WhatMyUserAgent.

This is an audit tool. It does not overwrite the production UA lookup. Local
rules remain the dashboard source of truth; API responses are written as
evidence so mismatches can be reviewed safely.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

import decode_distinct_ua_lookup as decoder  # noqa: E402


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOOKUP = ETL_ROOT / "output" / "device_decode" / "ua_decode_lookup_both_all.parquet"
DEFAULT_UA_DAILY = ETL_ROOT / "output" / "watch_hours" / "daily_tables" / "user_agents_daily.parquet"
DEFAULT_CACHE = ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_local_verified_crosscheck_cache.parquet"
DEFAULT_OUT_DIR = ETL_ROOT / "output" / "device_decode"


def log(message: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {message}", flush=True)


def clean_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def lower_clean(value: Any) -> str:
    return clean_text(value).lower()


def compatible_device_type(local_value: str, api_value: str) -> bool:
    local = lower_clean(local_value)
    api = lower_clean(api_value)
    if not local or not api:
        return False
    groups = [
        {"smart_tv", "tv", "desktop"},  # WhatMyUserAgent often labels TV app UAs as desktop.
        {"streaming_device", "smart_tv", "tv", "desktop"},
        {"smartphone", "mobile", "phone"},
        {"tablet", "ipad"},
        {"desktop", "computer", "pc"},
        {"bot", "crawler"},
    ]
    for group in groups:
        if any(token in local for token in group) and any(token in api for token in group):
            return True
    return local == api


def compatible_text(local_value: str, api_value: str) -> bool:
    local = lower_clean(local_value)
    api = lower_clean(api_value)
    if not local or not api:
        return False
    if local == api or local in api or api in local:
        return True
    local_parts = compare_parts(local)
    api_parts = compare_parts(api)
    return bool(local_parts & api_parts)


def compare_parts(value: str) -> set[str]:
    text = re.sub(r"\([^)]*\)", "", lower_clean(value)).strip()
    parts = {text}
    for separator in ["/", "|", ",", ";"]:
        parts.update(part.strip() for part in text.split(separator))
    return {part for part in parts if len(part) >= 4}


def compatible_os(local_value: str, api_value: str) -> bool:
    local = lower_clean(local_value)
    api = lower_clean(api_value)
    if compatible_text(local, api):
        return True
    # Fire OS UAs often expose Android in the raw UA string, while external
    # decoders correctly label the product OS as Fire OS.
    android_family = {"android", "fire os", "android tv"}
    return local in android_family and api in android_family


def api_has_detail(row: pd.Series) -> bool:
    return any(
        clean_text(row.get(col))
        for col in [
            "api_device_type",
            "api_brand",
            "api_model",
            "api_os_name",
            "api_browser_name",
        ]
    )


def classify_crosscheck(row: pd.Series) -> tuple[str, str]:
    api_status = clean_text(row.get("api_status"))
    api_error = clean_text(row.get("api_error"))
    if api_status == "api_error":
        return "api_error", api_error or "API returned an error"
    if api_status != "decoded_api":
        return "not_checked", "No API response cached for this UA yet"
    if not api_has_detail(row):
        return "api_weak", "API decoded but did not expose useful device identity"

    checks: list[str] = []
    conflicts: list[str] = []

    local_brand = clean_text(row.get("brand"))
    api_brand = clean_text(row.get("api_brand"))
    if local_brand and api_brand:
        if compatible_text(local_brand, api_brand):
            checks.append("brand")
        else:
            conflicts.append(f"brand local={local_brand} api={api_brand}")

    local_model = clean_text(row.get("model"))
    api_model = clean_text(row.get("api_model"))
    if local_model and api_model:
        if compatible_text(local_model, api_model):
            checks.append("model")
        else:
            conflicts.append(f"model local={local_model} api={api_model}")

    local_os = clean_text(row.get("os_name"))
    api_os = clean_text(row.get("api_os_name"))
    if local_os and api_os:
        if compatible_os(local_os, api_os):
            checks.append("os")
        else:
            conflicts.append(f"os local={local_os} api={api_os}")

    local_device = clean_text(row.get("device_type"))
    api_device = clean_text(row.get("api_device_type"))
    if local_device and api_device:
        if compatible_device_type(local_device, api_device):
            checks.append("device_type")
        else:
            conflicts.append(f"device_type local={local_device} api={api_device}")

    if conflicts and not checks:
        return "review_mismatch", "; ".join(conflicts)
    if conflicts:
        return "partial_match_review", "Matched " + ", ".join(checks) + "; review " + "; ".join(conflicts)
    if checks:
        return "match", "Matched " + ", ".join(checks)
    return "api_basic_support", "API provided generic detail but no direct comparable identity"


def read_lookup(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Lookup not found: {path}")
    frame = pd.read_parquet(path)
    for col in decoder.OUTPUT_COLUMNS:
        if col not in frame.columns:
            frame[col] = ""
    return frame


def build_impact(ua_daily_path: Path) -> pd.DataFrame:
    if not ua_daily_path.exists():
        return pd.DataFrame(columns=["ua_hash", "rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips", "source_count"])
    daily = pd.read_parquet(ua_daily_path)
    if daily.empty or "userAgent" not in daily.columns:
        return pd.DataFrame(columns=["ua_hash", "rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips", "source_count"])
    daily = daily.copy()
    daily["ua_hash"] = daily["userAgent"].map(decoder.normalize_ua).map(decoder.ua_hash)
    for col in ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"]:
        if col not in daily.columns:
            daily[col] = 0
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0)
    source_count = daily.groupby("ua_hash", dropna=False)["source"].nunique().rename("source_count") if "source" in daily.columns else None
    impact = (
        daily.groupby("ua_hash", dropna=False)[["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"]]
        .sum()
        .reset_index()
    )
    if source_count is not None:
        impact = impact.merge(source_count.reset_index(), on="ua_hash", how="left")
    else:
        impact["source_count"] = 0
    return impact


def load_api_cache(path: Path) -> pd.DataFrame:
    return decoder.load_api_cache(path)


def save_api_cache(cache: pd.DataFrame, path: Path) -> None:
    decoder.save_api_cache(cache, path)


def select_candidates(local: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    local = local[local["decode_status"].fillna("").eq("decoded_local")].copy()
    local = local[~local["is_malformed"].astype(str).str.lower().isin(["true", "1"])].copy()
    if not args.include_bots and "is_bot" in local.columns:
        local = local[~local["is_bot"].astype(str).str.lower().isin(["true", "1"])].copy()
    if args.min_confidence:
        allowed = {item.strip().lower() for item in args.min_confidence.split(",") if item.strip()}
        local = local[local["confidence"].fillna("").str.lower().isin(allowed)].copy()

    cached = set(cache["ua_hash"].astype(str)) if not cache.empty else set()
    local = local[~local["ua_hash"].astype(str).isin(cached)].copy()
    local["_score"] = (
        pd.to_numeric(local.get("raw_ts_rows", 0), errors="coerce").fillna(0) * 100
        + pd.to_numeric(local.get("approx_unique_ips", 0), errors="coerce").fillna(0)
        + pd.to_numeric(local.get("rows", 0), errors="coerce").fillna(0) * 0.01
    )
    local = local.sort_values(["_score", "ua_norm"], ascending=[False, True]).drop(columns=["_score"])
    if args.api_limit > 0:
        local = local.head(args.api_limit)
    return local


def run_api_crosscheck(candidates: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    if args.api_limit == 0 or candidates.empty:
        return cache
    api_rows: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(candidates.iterrows(), start=1):
        ua = clean_text(row["ua_norm"])
        ua_hash = clean_text(row["ua_hash"])
        log(f"API cross-check {idx}/{len(candidates)} ua_hash={ua_hash[:12]} ua={ua[:90]}")
        try:
            decoded = decoder.api_decode(ua, args)
            api_rows.append({"ua_hash": ua_hash, **decoded})
        except Exception as exc:
            api_rows.append(decoder.api_error_row(ua_hash, str(exc)))
            log(f"API error: {exc}")
            if args.stop_on_rate_limit and "rate limit" in str(exc).lower():
                log("Stopping because API appears rate-limited.")
                break
        if args.api_flush_every > 0 and len(api_rows) >= args.api_flush_every:
            cache = pd.concat([cache, pd.DataFrame(api_rows)], ignore_index=True)
            save_api_cache(cache, args.api_cache)
            api_rows = []
        if idx < len(candidates):
            time.sleep(random.uniform(args.api_sleep_min_seconds, args.api_sleep_max_seconds))
    if api_rows:
        cache = pd.concat([cache, pd.DataFrame(api_rows)], ignore_index=True)
        save_api_cache(cache, args.api_cache)
    return load_api_cache(args.api_cache)


def build_report(local: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    # The production lookup already contains api_* columns from the original
    # decode pass. For this audit report we want the cross-check cache to be the
    # evidence source, so drop overlapping API fields before merging.
    local = local.drop(columns=[col for col in decoder.API_COLUMNS if col != "ua_hash" and col in local.columns])
    merged = local.merge(cache, on="ua_hash", how="left")
    for col in decoder.API_COLUMNS:
        if col == "ua_hash":
            continue
        if col not in merged.columns:
            merged[col] = ""
    classifications = merged.apply(classify_crosscheck, axis=1, result_type="expand")
    merged["crosscheck_status"] = classifications[0]
    merged["crosscheck_note"] = classifications[1]
    columns = [
        "crosscheck_status",
        "crosscheck_note",
        "ua_hash",
        "rows",
        "raw_ts_rows",
        "status_200_ts_rows",
        "approx_unique_ips",
        "source_count",
        "confidence",
        "device_type",
        "form_factor",
        "brand",
        "model",
        "model_code",
        "os_name",
        "os_version",
        "browser_name",
        "api_status",
        "api_device_type",
        "api_brand",
        "api_model",
        "api_os_name",
        "api_os_version",
        "api_browser_name",
        "api_browser_version",
        "api_error",
        "ua_norm",
    ]
    for col in columns:
        if col not in merged.columns:
            merged[col] = ""
    return merged[columns].sort_values(["crosscheck_status", "raw_ts_rows", "approx_unique_ips"], ascending=[True, False, False])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="API cross-check locally verified UA/device decodes.")
    parser.add_argument("--lookup", type=Path, default=DEFAULT_LOOKUP)
    parser.add_argument("--ua-daily", type=Path, default=DEFAULT_UA_DAILY)
    parser.add_argument("--api-cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--output-prefix", default="ua_local_api_crosscheck")
    parser.add_argument("--api-limit", type=int, default=25, help="0 writes report from cache only; positive checks N new local rows; -1 checks all uncached local rows.")
    parser.add_argument("--api-key", default="NOTREQUIED")
    parser.add_argument("--api-url", default=decoder.DEFAULT_API_URL)
    parser.add_argument("--api-timeout", type=float, default=20.0)
    parser.add_argument("--api-sleep-min-seconds", type=float, default=2.0)
    parser.add_argument("--api-sleep-max-seconds", type=float, default=5.0)
    parser.add_argument("--api-flush-every", type=int, default=5)
    parser.add_argument("--stop-on-rate-limit", action="store_true", default=True)
    parser.add_argument("--include-bots", action="store_true")
    parser.add_argument("--min-confidence", default="high,medium")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.lookup = args.lookup.expanduser().resolve()
    args.ua_daily = args.ua_daily.expanduser().resolve()
    args.api_cache = args.api_cache.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    log(f"Reading lookup: {args.lookup}")
    lookup = read_lookup(args.lookup)
    impact = build_impact(args.ua_daily)
    if not impact.empty:
        lookup = lookup.merge(impact, on="ua_hash", how="left")
    for col in ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips", "source_count"]:
        if col not in lookup.columns:
            lookup[col] = 0
        lookup[col] = pd.to_numeric(lookup[col], errors="coerce").fillna(0)

    api_cache = load_api_cache(args.api_cache)
    candidates = lookup.iloc[0:0].copy() if args.api_limit == 0 else select_candidates(lookup, api_cache, args)
    log(f"Local verified candidates for API cross-check this run: {len(candidates):,}")
    api_cache = run_api_crosscheck(candidates, api_cache, args)

    # After API evidence is promoted into production, the checked rows become
    # decoded_api. Keep them in this audit report so the evidence does not
    # disappear just because the production lookup accepted the API value.
    local_verified = lookup[
        lookup["decode_status"].fillna("").isin(["decoded_local", "decoded_api"])
        & lookup["decoder_method"].fillna("").astype(str).str.contains("local_rule", case=False, na=False)
    ].copy()
    if not args.include_bots and "is_bot" in local_verified.columns:
        local_verified = local_verified[~local_verified["is_bot"].astype(str).str.lower().isin(["true", "1"])].copy()
    report = build_report(local_verified, api_cache)

    csv_path = args.out_dir / f"{args.output_prefix}.csv"
    parquet_path = args.out_dir / f"{args.output_prefix}.parquet"
    summary_path = args.out_dir / f"{args.output_prefix}_summary.csv"
    manifest_path = args.out_dir / f"{args.output_prefix}_manifest.json"

    report.to_csv(csv_path, index=False, encoding="utf-8-sig")
    report.to_parquet(parquet_path, index=False)
    summary = report.groupby("crosscheck_status", dropna=False).size().reset_index(name="distinct_ua_count")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lookup": str(args.lookup),
        "ua_daily": str(args.ua_daily),
        "api_url": args.api_url,
        "api_limit": args.api_limit,
        "api_cache": str(args.api_cache),
        "outputs": {
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "summary": str(summary_path),
        },
        "stats": {
            "local_verified_rows": int(len(local_verified)),
            "new_api_candidates_this_run": int(len(candidates)),
            "api_cache_rows": int(len(api_cache)),
            "report_rows": int(len(report)),
            "elapsed_seconds": round(time.time() - started, 2),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log(f"Cross-check CSV written    : {csv_path}")
    log(f"Cross-check Parquet written: {parquet_path}")
    log(f"Summary written            : {summary_path}")


if __name__ == "__main__":
    main()
