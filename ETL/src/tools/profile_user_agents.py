#!/usr/bin/env python3
"""Build a distinct-UA profile and optional local decode cache.

The production pattern is:
raw lake -> distinct UA profile -> local decode cache -> dashboard/report use.
Network API calls are opt-in and operate only on distinct UA strings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LAKE = ETL_ROOT / "data" / "lake"
DEFAULT_OUT = ETL_ROOT / "output" / "device_decode"
DEFAULT_CACHE = ETL_ROOT / "data" / "cache" / "device_decode" / "ua_decode_cache.parquet"
IST_OFFSET_SECONDS = 19_800
CHUNK_DURATION_HOURS = 6 / 3600
UA_COLUMNS = [
    "ua_hash",
    "ua_norm_key",
    "ua_sample",
    "decoder",
    "decode_status",
    "device_type",
    "brand",
    "model",
    "os_name",
    "os_version",
    "os_family",
    "browser_name",
    "browser_version",
    "browser_engine",
    "bot_name",
    "decoded_at_utc",
    "error",
]


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def ist_date_expr(epoch_expr: str) -> str:
    return (
        "CAST(epoch_ms(CAST(FLOOR(("
        f"TRY_CAST({epoch_expr} AS DOUBLE) + {IST_OFFSET_SECONDS}"
        ") * 1000) AS BIGINT)) AS DATE)"
    )


def suffix_for(args: argparse.Namespace) -> str:
    return "_".join(
        part
        for part in [
            args.source or "all_sources",
            args.start or "all_start",
            "to",
            args.end or "all_end",
        ]
        if part
    )


def date_filter(args: argparse.Namespace) -> str:
    parts = []
    day_expr = ist_date_expr("reqTimeSec")
    if args.start:
        parts.append(f"{day_expr} >= DATE {sql_text(args.start)}")
    if args.end:
        parts.append(f"{day_expr} <= DATE {sql_text(args.end)}")
    return " AND ".join(parts) if parts else "1=1"


def source_filter(source: str | None) -> str:
    if not source:
        return "1=1"
    return f"lower(COALESCE(CAST(source AS VARCHAR), 'stream')) = lower({sql_text(source)})"


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, int(args.threads))}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={sql_text(q(args.temp_dir))}")
    return con


def normalize_ua(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    for _ in range(3):
        decoded = urllib.parse.unquote_plus(text)
        if decoded == text:
            break
        text = decoded
    text = re.sub(r"\s+", " ", text).strip()
    return text


def ua_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def safe_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def build_profile_sql(args: argparse.Namespace) -> str:
    lake_glob = q(args.lake / "**" / "*.parquet")
    where_sql = f"({date_filter(args)}) AND ({source_filter(args.source)})"
    return f"""
    WITH base AS (
        SELECT
            CAST({ist_date_expr("reqTimeSec")} AS VARCHAR) AS log_date,
            lower(COALESCE(CAST(source AS VARCHAR), 'stream')) AS source,
            UA AS ua_raw,
            cliIP,
            lower(reqHost) AS reqHost,
            reqPath,
            regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', '') AS statusCode,
            lower(reqPath) LIKE '%.ts' AS is_ts
        FROM read_parquet('{lake_glob}', hive_partitioning=1, union_by_name=1)
        WHERE {where_sql}
          AND NULLIF(UA, '') IS NOT NULL
    )
    SELECT
        source,
        ua_raw,
        COUNT(*)::BIGINT AS rows,
        SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
        SUM(CASE WHEN is_ts AND statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_ts_rows,
        COUNT(DISTINCT NULLIF(cliIP, ''))::BIGINT AS approx_ips,
        MIN(log_date) AS first_date,
        MAX(log_date) AS last_date,
        ANY_VALUE(reqHost) AS sample_reqHost,
        ANY_VALUE(reqPath) AS sample_reqPath
    FROM base
    GROUP BY source, ua_raw
    ORDER BY rows DESC
    """


def collapse_profile(raw: pd.DataFrame, limit: int = 0) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "ua_hash",
                "ua_norm_key",
                "ua_sample",
                "rows",
                "ts_rows",
                "status_200_ts_rows",
                "watch_hours",
                "status_200_watch_hours",
                "approx_ips",
                "first_date",
                "last_date",
                "sample_reqHost",
                "sample_reqPath",
            ]
        )

    df = raw.copy()
    df["ua_norm_key"] = df["ua_raw"].map(normalize_ua)
    df = df[df["ua_norm_key"] != ""].copy()
    df["ua_hash"] = df["ua_norm_key"].map(ua_hash)
    for col in ["rows", "ts_rows", "status_200_ts_rows", "approx_ips"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    grouped = (
        df.groupby(["source", "ua_hash", "ua_norm_key"], as_index=False)
        .agg(
            ua_sample=("ua_raw", "first"),
            rows=("rows", "sum"),
            ts_rows=("ts_rows", "sum"),
            status_200_ts_rows=("status_200_ts_rows", "sum"),
            approx_ips=("approx_ips", "max"),
            first_date=("first_date", "min"),
            last_date=("last_date", "max"),
            sample_reqHost=("sample_reqHost", "first"),
            sample_reqPath=("sample_reqPath", "first"),
        )
        .sort_values("rows", ascending=False)
    )
    grouped["watch_hours"] = grouped["ts_rows"] * CHUNK_DURATION_HOURS
    grouped["status_200_watch_hours"] = grouped["status_200_ts_rows"] * CHUNK_DURATION_HOURS
    columns = [
        "source",
        "ua_hash",
        "ua_norm_key",
        "ua_sample",
        "rows",
        "ts_rows",
        "status_200_ts_rows",
        "watch_hours",
        "status_200_watch_hours",
        "approx_ips",
        "first_date",
        "last_date",
        "sample_reqHost",
        "sample_reqPath",
    ]
    out = grouped[columns]
    return out.head(limit) if limit and limit > 0 else out


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp.unlink(missing_ok=True)
    df.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError, ImportError):
        return pd.DataFrame()


def ensure_cache_shape(cache: pd.DataFrame) -> pd.DataFrame:
    out = cache.copy()
    for col in UA_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out[UA_COLUMNS]


def dedupe_cache(cache: pd.DataFrame) -> pd.DataFrame:
    cache = ensure_cache_shape(cache)
    if cache.empty:
        return cache

    priority = {
        "whatmyuseragent": 0,
        "whatmyuseragent_error": 1,
        "local_rule": 9,
    }
    out = cache.copy()
    out["_decoder_priority"] = out["decoder"].map(priority).fillna(9)
    out["_decoded_sort"] = pd.to_datetime(out["decoded_at_utc"], errors="coerce", utc=True)
    out = (
        out.sort_values(
            ["ua_hash", "_decoder_priority", "_decoded_sort"],
            ascending=[True, True, False],
            na_position="last",
        )
        .drop_duplicates("ua_hash", keep="first")
        .drop(columns=["_decoder_priority", "_decoded_sort"])
    )
    return ensure_cache_shape(out)


def browser_from_ua(ua: str) -> tuple[str, str, str]:
    patterns = [
        ("Chrome", r"(?:Chrome|CriOS)/([0-9.]+)", "Blink"),
        ("Samsung Internet", r"SamsungBrowser/([0-9.]+)", "Blink"),
        ("Firefox", r"(?:Firefox|FxiOS)/([0-9.]+)", "Gecko"),
        ("Safari", r"Version/([0-9.]+).*Safari/", "WebKit"),
        ("AppleCoreMedia", r"AppleCoreMedia/([0-9.]+)", "Apple"),
        ("Dalvik", r"Dalvik/([0-9.]+)", "Android runtime"),
    ]
    for name, pattern, engine in patterns:
        match = re.search(pattern, ua, flags=re.I)
        if match:
            return name, match.group(1), engine
    return "", "", ""


def infer_brand(model: str, ua: str) -> str:
    upper = model.upper()
    lower = ua.lower()
    if upper.startswith("SM-") or "samsung" in lower or "tizen" in lower:
        return "Samsung"
    if upper.startswith("AFT") or "fire tv" in lower:
        return "Amazon"
    if "iphone" in lower or "ipad" in lower:
        return "Apple"
    if "roku" in lower:
        return "Roku"
    if "webos" in lower:
        return "LG"
    if upper.startswith("CPH"):
        return "OPPO"
    if upper.startswith("RMX"):
        return "Realme"
    if upper.startswith("ONEPLUS"):
        return "OnePlus"
    return ""


def local_decode(row: pd.Series) -> dict:
    ua = normalize_ua(row.get("ua_sample") or row.get("ua_norm_key") or "")
    lower = ua.lower()
    device_type = ""
    brand = ""
    model = ""
    os_name = ""
    os_version = ""
    os_family = ""

    fire_code = re.search(r"\b(AFT[A-Z0-9]{1,12}|B0[A-Z0-9]{8,12})\b", ua, flags=re.I)
    android = re.search(r"Android[ /]([0-9._]+)", ua, flags=re.I)
    android_model = re.search(r"Android[^;)]*;\s*([^;)]+?)(?:\s+Build/|\)|;)", ua, flags=re.I)

    if "iphone" in lower:
        device_type, brand, model, os_name, os_family = "smartphone", "Apple", "iPhone", "iOS", "iOS"
    elif "ipad" in lower:
        device_type, brand, model, os_name, os_family = "tablet", "Apple", "iPad", "iPadOS", "iOS"
    elif "tizen" in lower:
        device_type, brand, model, os_name, os_family = "smart_tv", "Samsung", "Samsung/Tizen TV", "Tizen", "Tizen"
    elif "webos" in lower:
        device_type, brand, model, os_name, os_family = "smart_tv", "LG", "LG/webOS TV", "webOS", "webOS"
    elif "roku" in lower:
        device_type, brand, model, os_name, os_family = "streaming_device", "Roku", "Roku device", "Roku OS", "Roku"
    elif "appletv" in lower or "apple tv" in lower:
        device_type, brand, model, os_name, os_family = "streaming_device", "Apple", "Apple TV", "tvOS", "iOS"
    elif fire_code:
        device_type, brand, model, os_name, os_family = "streaming_device", "Amazon", fire_code.group(1).upper(), "Android", "Android"
    elif android:
        os_name, os_version, os_family = "Android", android.group(1).replace("_", "."), "Android"
        model = android_model.group(1).strip() if android_model else ""
        brand = infer_brand(model, ua)
        if "tv" in lower or "smart" in lower:
            device_type = "smart_tv"
        elif "tablet" in lower or "pad" in model.lower():
            device_type = "tablet"
        else:
            device_type = "smartphone"

    browser_name, browser_version, browser_engine = browser_from_ua(ua)
    bot_name = "bot" if re.search(r"bot|crawler|spider|slurp|monitor", lower) else ""
    status = "decoded_local" if any([device_type, brand, model, os_name, browser_name, bot_name]) else "unknown"

    return {
        "ua_hash": row["ua_hash"],
        "ua_norm_key": row["ua_norm_key"],
        "ua_sample": ua,
        "decoder": "local_rule",
        "decode_status": status,
        "device_type": device_type,
        "brand": brand,
        "model": model,
        "os_name": os_name,
        "os_version": os_version,
        "os_family": os_family,
        "browser_name": browser_name,
        "browser_version": browser_version,
        "browser_engine": browser_engine,
        "bot_name": bot_name,
        "decoded_at_utc": datetime.now(timezone.utc).isoformat(),
        "error": "",
    }


def decode_api(ua: str, args: argparse.Namespace) -> dict:
    params = urllib.parse.urlencode({"ua": ua, "key": args.api_key})
    url = f"{args.api_url}?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": "Veto-ETL-UA-Decoder/1.0"})
    with urllib.request.urlopen(request, timeout=args.api_timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))

    device = payload.get("Device") or {}
    os_info = payload.get("OS") or {}
    browser = payload.get("Browser") or {}
    bot = payload.get("Bot") or []
    bot_name = ""
    if isinstance(bot, list) and bot:
        bot_name = ", ".join(str(x) for x in bot[:3])
    elif isinstance(bot, dict):
        bot_name = str(bot.get("name") or bot.get("type") or "")

    result = {
        "decoder": "whatmyuseragent",
        "decode_status": "decoded_api",
        "device_type": safe_text(device.get("deviceType")),
        "brand": safe_text(device.get("brand")),
        "model": safe_text(device.get("model")),
        "os_name": safe_text(os_info.get("name")),
        "os_version": safe_text(os_info.get("version")),
        "os_family": safe_text(os_info.get("family")),
        "browser_name": safe_text(browser.get("name")),
        "browser_version": safe_text(browser.get("version")),
        "browser_engine": safe_text(browser.get("engine")),
        "bot_name": bot_name,
        "decoded_at_utc": datetime.now(timezone.utc).isoformat(),
        "error": "",
    }
    if not any(
        result.get(key)
        for key in [
            "device_type",
            "brand",
            "model",
            "os_name",
            "browser_name",
            "bot_name",
        ]
    ):
        result["decode_status"] = "unknown"
        result["error"] = "API returned no usable device/OS/browser fields"
    return result


def unknown_api_decode(row: pd.Series, error: str) -> dict:
    ua = normalize_ua(row.get("ua_sample") or row.get("ua_norm_key") or "")
    return {
        "ua_hash": row["ua_hash"],
        "ua_norm_key": row["ua_norm_key"],
        "ua_sample": ua,
        "decoder": "whatmyuseragent_error",
        "decode_status": "unknown",
        "device_type": "",
        "brand": "",
        "model": "",
        "os_name": "",
        "os_version": "",
        "os_family": "",
        "browser_name": "",
        "browser_version": "",
        "browser_engine": "",
        "bot_name": "",
        "decoded_at_utc": datetime.now(timezone.utc).isoformat(),
        "error": error,
    }


def api_sleep_seconds(args: argparse.Namespace) -> float:
    if args.api_sleep_seconds is not None:
        return max(0.0, float(args.api_sleep_seconds))
    low = max(0.0, float(args.api_sleep_min_seconds))
    high = max(low, float(args.api_sleep_max_seconds))
    return random.uniform(low, high)


def update_cache(profile: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    cache = dedupe_cache(cache)
    known_hashes = set(cache["ua_hash"].astype(str))
    local_rows = [local_decode(row) for _, row in profile.iterrows() if str(row["ua_hash"]) not in known_hashes]
    if local_rows:
        cache = pd.concat([cache, pd.DataFrame(local_rows)], ignore_index=True)
        cache = dedupe_cache(cache)

    if args.api_limit == 0:
        return cache

    cache_by_hash = cache.set_index("ua_hash", drop=False)
    candidates = profile[profile["rows"] >= args.min_rows_for_api].sort_values("rows", ascending=False)
    decode_all = args.api_limit < 0
    to_decode = []
    for _, row in candidates.iterrows():
        cached = cache_by_hash.loc[row["ua_hash"]] if row["ua_hash"] in cache_by_hash.index else None
        if cached is None or safe_text(cached.get("decoder")) != "whatmyuseragent":
            to_decode.append(row)
        if not decode_all and len(to_decode) >= args.api_limit:
            break

    api_rows = []

    def flush_api_rows() -> None:
        nonlocal cache, api_rows
        if not api_rows:
            return
        cache = pd.concat([cache, pd.DataFrame(api_rows)], ignore_index=True)
        cache = dedupe_cache(cache)
        write_parquet(cache, args.cache)
        print(f"[api flush] cache rows={len(cache)}")
        api_rows = []

    print(f"API decode candidates: {len(to_decode)}")
    for idx, row in enumerate(to_decode, start=1):
        ua = normalize_ua(row.get("ua_sample") or row.get("ua_norm_key") or "")
        print(f"[api {idx}/{len(to_decode)}] rows={int(row['rows'])} ua_hash={row['ua_hash'][:12]}")
        base = {
            "ua_hash": row["ua_hash"],
            "ua_norm_key": row["ua_norm_key"],
            "ua_sample": ua,
        }
        try:
            decoded = decode_api(ua, args)
            api_rows.append({**base, **decoded})
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            api_rows.append(unknown_api_decode(row, str(exc)))
        if args.api_flush_every > 0 and len(api_rows) >= args.api_flush_every:
            flush_api_rows()
        if idx < len(to_decode):
            time.sleep(api_sleep_seconds(args))

    flush_api_rows()
    return dedupe_cache(cache)


def enrich_profile(profile: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    cache = dedupe_cache(cache)
    enriched = profile.merge(cache, on="ua_hash", how="left", suffixes=("", "_cache"))
    for col in ["ua_norm_key", "ua_sample"]:
        cache_col = f"{col}_cache"
        if cache_col in enriched.columns:
            enriched[col] = enriched[col].fillna(enriched[cache_col])
            enriched = enriched.drop(columns=[cache_col])
    return enriched.sort_values("rows", ascending=False)


def write_unknown_review(enriched: pd.DataFrame, out_path: Path, limit: int = 500) -> None:
    unknown = enriched[
        (enriched["decode_status"].fillna("") == "unknown")
        | (enriched["device_type"].fillna("") == "")
    ].sort_values("rows", ascending=False)
    columns = [
        "source",
        "ua_hash",
        "rows",
        "ts_rows",
        "watch_hours",
        "approx_ips",
        "first_date",
        "last_date",
        "ua_sample",
        "decoder",
        "decode_status",
        "error",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    unknown[[c for c in columns if c in unknown.columns]].head(limit).to_csv(out_path, index=False)


def load_or_build_profile(args: argparse.Namespace, profile_path: Path) -> pd.DataFrame:
    if args.profile_path:
        profile = read_parquet(args.profile_path)
        if profile.empty:
            raise SystemExit(f"Profile path is empty or unreadable: {args.profile_path}")
        return profile

    if args.dry_run:
        print(f"[dry-run] Would scan lake: {args.lake}")
        print(f"[dry-run] Would write profile: {profile_path}")
        return pd.DataFrame()

    if not args.lake.exists():
        raise SystemExit(f"Lake not found: {args.lake}")
    con = connect(args)
    print(f"Scanning distinct UAs from lake: {args.lake}")
    raw = con.execute(build_profile_sql(args)).fetchdf()
    con.close()
    profile = collapse_profile(raw, args.profile_limit)
    write_parquet(profile, profile_path)
    return profile


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile distinct User-Agent strings and maintain a local decode cache.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--profile-path", type=Path, default=None, help="Use an existing ua_distinct_profile parquet instead of scanning lake.")
    parser.add_argument("--start", help="Optional IST start date YYYY-MM-DD")
    parser.add_argument("--end", help="Optional IST end date YYYY-MM-DD")
    parser.add_argument("--source", choices=["stream", "fast"], default=None)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    parser.add_argument("--profile-limit", type=int, default=0, help="Optional top-N distinct UA rows to retain after aggregation.")
    parser.add_argument("--api-limit", type=int, default=0, help="Number of high-impact UAs to decode through whatmyuseragent.com. Use -1 for all candidates. Default: 0.")
    parser.add_argument("--min-rows-for-api", type=int, default=1, help="Minimum rows before a UA is eligible for API decode.")
    parser.add_argument("--api-sleep-seconds", type=float, default=None, help="Fixed sleep between API calls. Overrides random min/max when set.")
    parser.add_argument("--api-sleep-min-seconds", type=float, default=2.0)
    parser.add_argument("--api-sleep-max-seconds", type=float, default=5.0)
    parser.add_argument("--api-flush-every", type=int, default=25, help="Write API cache progress every N decoded UA rows.")
    parser.add_argument("--api-timeout", type=float, default=20.0)
    parser.add_argument("--api-key", default=os.getenv("WHATMYUA_KEY", "NOTREQUIED"))
    parser.add_argument("--api-url", default="https://whatmyuseragent.com/api")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    args.cache = args.cache.expanduser().resolve()
    if args.profile_path:
        args.profile_path = args.profile_path.expanduser().resolve()

    suffix = suffix_for(args)
    profile_path = args.out_dir / f"ua_distinct_profile_{suffix}.parquet"
    enriched_path = args.out_dir / f"ua_decode_enriched_{suffix}.parquet"
    unknown_path = args.out_dir / f"ua_decode_unknown_review_{suffix}.csv"
    manifest_path = args.out_dir / f"ua_decode_manifest_{suffix}.json"

    profile = load_or_build_profile(args, profile_path)
    if args.dry_run:
        return

    cache = read_parquet(args.cache)
    cache = update_cache(profile, cache, args)
    write_parquet(cache, args.cache)

    enriched = enrich_profile(profile, cache)
    write_parquet(enriched, enriched_path)
    write_unknown_review(enriched, unknown_path)

    stats = {
        "profile_rows": int(len(profile)),
        "profile_raw_rows": int(profile["rows"].sum()) if "rows" in profile else 0,
        "cache_rows": int(len(cache)),
        "api_decoded_rows": int((cache["decoder"] == "whatmyuseragent").sum()) if "decoder" in cache else 0,
        "unknown_review_rows": int(pd.read_csv(unknown_path).shape[0]) if unknown_path.exists() else 0,
    }
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake": str(args.lake),
        "source": args.source or "all",
        "start": args.start,
        "end": args.end,
        "api_limit": args.api_limit,
        "outputs": {
            "profile": str(profile_path),
            "cache": str(args.cache),
            "enriched": str(enriched_path),
            "unknown_review": str(unknown_path),
        },
        "stats": stats,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Profile written : {profile_path}")
    print(f"Cache written   : {args.cache}")
    print(f"Enriched written: {enriched_path}")
    print(f"Unknown review  : {unknown_path}")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
