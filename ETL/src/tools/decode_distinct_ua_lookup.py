#!/usr/bin/env python3
"""Decode a distinct User-Agent CSV into a reusable lookup table.

This is designed for files produced by Smart Parquet Lake Distinct Extractor,
for example:

    ETL/distinct_UA_Both_All.csv

The decoder is intentionally layered:

1. Normalize and URL-decode UA strings.
2. Apply deterministic local parsing and local model-code maps.
3. Optionally call WhatMyUserAgent only for unresolved/high-value rows.
4. Cache API results so future runs resume without repeating calls.
5. Write CSV + Parquet outputs for dashboard joins and manual review.
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
from typing import Any

import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ETL_ROOT / "distinct_UA_Both_All.csv"
DEFAULT_MAP = ETL_ROOT / "config" / "device_decode" / "amazon_fire_tv_models.csv"
DEFAULT_OUT_DIR = ETL_ROOT / "output" / "device_decode"
DEFAULT_CACHE = ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_distinct_ua_cache.parquet"
DEFAULT_LOCAL_VERIFIED_CROSSCHECK_CACHE = (
    ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_local_verified_crosscheck_cache.parquet"
)
DEFAULT_ALL_DISTINCT_API_CACHE = (
    ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_all_distinct_ua_cache.parquet"
)
DEFAULT_API_URL = "https://whatmyuseragent.com/api"

OUTPUT_COLUMNS = [
    "UA",
    "ua_norm",
    "ua_hash",
    "decode_status",
    "decoder_method",
    "confidence",
    "device_type",
    "form_factor",
    "brand",
    "model",
    "model_code",
    "product_family",
    "generation",
    "os_name",
    "os_version",
    "os_family",
    "browser_name",
    "browser_version",
    "browser_engine",
    "app_player",
    "is_bot",
    "is_malformed",
    "api_device_type",
    "api_brand",
    "api_model",
    "api_os_name",
    "api_os_version",
    "api_browser_name",
    "api_browser_version",
    "api_error",
    "notes",
    "decoded_at_utc",
]

API_COLUMNS = [
    "ua_hash",
    "decoder_method",
    "api_status",
    "api_device_type",
    "api_brand",
    "api_model",
    "api_os_name",
    "api_os_version",
    "api_os_family",
    "api_browser_name",
    "api_browser_version",
    "api_browser_engine",
    "api_bot_name",
    "api_error",
    "decoded_at_utc",
]


def log(message: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {message}", flush=True)


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value)


def normalize_ua(value: Any) -> str:
    text = safe_text(value).strip()
    for _ in range(5):
        decoded = urllib.parse.unquote_plus(text).strip()
        if decoded == text:
            break
        text = decoded
    text = re.sub(r"\s+", " ", text).strip()
    return text


def ua_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError, ImportError):
        return pd.DataFrame()


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp.unlink(missing_ok=True)
    frame.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp.unlink(missing_ok=True)
    frame.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(path)


def read_distinct_ua_csv(path: Path, column: str | None) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Input CSV not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if df.empty:
        raise SystemExit(f"Input CSV is empty: {path}")
    selected = column or ("UA" if "UA" in df.columns else df.columns[0])
    if selected not in df.columns:
        raise SystemExit(f"Column {selected!r} not found. Available columns: {', '.join(df.columns)}")
    out = pd.DataFrame({"UA": df[selected].map(safe_text)})
    out["ua_norm"] = out["UA"].map(normalize_ua)
    out = out[out["ua_norm"] != ""].copy()
    out["ua_hash"] = out["ua_norm"].map(ua_hash)
    out = out.drop_duplicates("ua_hash", keep="first").reset_index(drop=True)
    return out


def load_fire_tv_map(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    mapping: dict[str, dict[str, str]] = {}
    for _, row in df.iterrows():
        code = safe_text(row.get("device_code")).strip().upper()
        if not code:
            continue
        mapping[code] = {
            "brand": safe_text(row.get("vendor")) or "Amazon",
            "model": safe_text(row.get("decoded_name")),
            "product_family": safe_text(row.get("product_family")),
            "generation": safe_text(row.get("generation")),
            "notes": safe_text(row.get("notes")),
        }
    return mapping


def malformed_reason(ua: str) -> str:
    if not ua:
        return "blank UA"
    if len(ua) > 2000:
        return "extremely long UA"
    if "${" in ua or "$%7b" in ua.lower():
        return "template-injection style payload"
    if re.search(r"(jndi|ldap|rmi|dns):", ua, flags=re.I):
        return "injection/probing payload"
    printable = sum(1 for ch in ua if ch.isprintable())
    if printable < len(ua):
        return "contains non-printable characters"
    alpha = sum(1 for ch in ua if ch.isalpha())
    if len(ua) > 80 and alpha / max(len(ua), 1) < 0.15:
        return "low alphabetic signal"
    return ""


def first_match(patterns: list[tuple[str, str]], ua: str) -> tuple[str, str]:
    for name, pattern in patterns:
        match = re.search(pattern, ua, flags=re.I)
        if match:
            return name, match.group(1) if match.groups() else ""
    return "", ""


def browser_from_ua(ua: str) -> tuple[str, str, str]:
    patterns = [
        ("Samsung Internet", r"SamsungBrowser/([0-9.]+)", "Blink"),
        ("Chrome Mobile", r"(?:Chrome|CriOS)/([0-9.]+).*Mobile", "Blink"),
        ("Chrome", r"(?:Chrome|CriOS)/([0-9.]+)", "Blink"),
        ("Firefox", r"(?:Firefox|FxiOS)/([0-9.]+)", "Gecko"),
        ("Safari", r"Version/([0-9.]+).*Safari/", "WebKit"),
        ("AppleCoreMedia", r"AppleCoreMedia/([0-9.]+)", "Apple"),
        ("Dalvik", r"Dalvik/([0-9.]+)", "Android runtime"),
        ("OkHttp", r"OkHttp/([0-9.]+)", "OkHttp"),
        ("ExoPlayer", r"ExoPlayer(?:Lib)?/([0-9.]+)", "Media3/ExoPlayer"),
    ]
    for name, pattern, engine in patterns:
        match = re.search(pattern, ua, flags=re.I)
        if match:
            return name, match.group(1), engine
    return "", "", ""


def infer_android_brand(model: str, ua: str) -> str:
    upper = model.upper()
    lower = ua.lower()
    if upper.startswith("SM-") or "samsung" in lower:
        return "Samsung"
    if upper.startswith("AFT") or "fire tv" in lower:
        return "Amazon"
    if upper.startswith("CPH"):
        return "OPPO"
    if upper.startswith("RMX"):
        return "Realme"
    if upper.startswith("ONEPLUS") or "oneplus" in lower:
        return "OnePlus"
    if upper.startswith(("MIBOX", "MITV", "MI ", "M201", "M200", "M210", "M220")) or "xiaomi" in lower:
        return "Xiaomi"
    if "bravia" in lower or "sony" in lower:
        return "Sony"
    if upper.startswith(("SWTV", "SW-")) or "skyworth" in lower:
        return "Skyworth"
    if "beyondtv" in lower or "tcl" in lower:
        return "TCL"
    if upper.startswith("PATH_") or "kodak" in lower:
        return "Kodak"
    if upper in {"R3G", "R4G"} or "artel" in lower:
        return "Artel"
    if "sharp" in lower:
        return "Sharp"
    if "hisense" in lower:
        return "Hisense"
    if "tcl" in lower:
        return "TCL"
    if "haier" in lower:
        return "Haier"
    if "vu tv" in lower or "vutv" in lower:
        return "VU"
    return ""


def is_android_ctv_model(model: str, ua: str) -> bool:
    """Identify Android TV / CTV UAs that do not say "mobile".

    Many TV apps use Dalvik-style Android UAs and only expose a model/build
    token. Without these model hints they get misclassified as smartphones.
    """
    text = f"{ua} {model}".lower()
    ctv_tokens = [
        "smart tv",
        "smart-tv",
        "android tv",
        "bravia",
        "mitv",
        "mi tv",
        "mibox",
        "sw-tv",
        "swtv",
        "beyondtv",
        "ai pont",
        "aipont",
        "matrixtv",
        "nstv",
        "vutv",
        "vu tv",
        "hisense",
        "skyworth",
        "path_",
        "hidptandroid",
    ]
    if any(token in text for token in ctv_tokens):
        return True
    model_upper = model.upper().strip()
    return bool(re.match(r"^(R3G|R4G|C00|D00|JHSD\d+|Y SERIES)\b", model_upper))


def normalize_api_device_type(value: Any) -> str:
    device_type = safe_text(value).strip().lower()
    if device_type in {"tv", "smart tv", "smart-tv", "television"}:
        return "smart_tv"
    if device_type in {"phone", "mobile"}:
        return "smartphone"
    if device_type in {"tablet", "desktop", "bot"}:
        return device_type
    return device_type


def os_family_from_api(os_name: str, api_family: str = "") -> str:
    family = safe_text(api_family).strip()
    if family:
        return family
    name = safe_text(os_name).lower()
    if "fire" in name:
        return "Android/FireOS"
    if "android" in name:
        return "Android"
    if "tizen" in name:
        return "Tizen"
    if "webos" in name:
        return "webOS"
    if name in {"ios", "ipados", "tvos"}:
        return "iOS"
    if "mac" in name:
        return "macOS"
    if "windows" in name:
        return "Windows"
    return ""


def classify_form_factor(device_type: str, model: str, ua: str) -> str:
    lower = f"{ua} {model} {device_type}".lower()
    if any(token in lower for token in ["smart_tv", "smart tv", "android tv", "tizen", "webos", "bravia", "mitv", "mi tv"]):
        return "CTV"
    if any(token in lower for token in ["fire tv", "roku", "appletv", "apple tv", "aft"]):
        return "CTV"
    if "tablet" in lower or "ipad" in lower:
        return "Tablet"
    if "mobile" in lower or "iphone" in lower or "smartphone" in lower:
        return "Mobile"
    if "bot" in lower:
        return "Bot"
    if any(token in lower for token in ["windows", "macintosh", "x11", "linux x86"]):
        return "Desktop"
    return "Unknown"


def extract_android_model(ua: str) -> str:
    patterns = [
        r"Android[^;)]*;\s*([^;)]+?)\s+Build/",
        r"Android[^;)]*;\s*([^;)]+?)(?:;|\))",
    ]
    for pattern in patterns:
        match = re.search(pattern, ua, flags=re.I)
        if match:
            model = re.sub(r"\s+", " ", match.group(1)).strip()
            ignored = {"u", "wv", "mobile", "tablet", "linux", "android"}
            if model.lower() not in ignored:
                return model
    return ""


def local_decode_row(row: pd.Series, fire_map: dict[str, dict[str, str]]) -> dict[str, Any]:
    ua = safe_text(row["ua_norm"])
    lower = ua.lower()
    now = datetime.now(timezone.utc).isoformat()
    reason = malformed_reason(ua)

    result: dict[str, Any] = {
        "UA": row["UA"],
        "ua_norm": ua,
        "ua_hash": row["ua_hash"],
        "decode_status": "unknown",
        "decoder_method": "local_rule",
        "confidence": "low",
        "device_type": "",
        "form_factor": "Unknown",
        "brand": "",
        "model": "",
        "model_code": "",
        "product_family": "",
        "generation": "",
        "os_name": "",
        "os_version": "",
        "os_family": "",
        "browser_name": "",
        "browser_version": "",
        "browser_engine": "",
        "app_player": "",
        "is_bot": False,
        "is_malformed": bool(reason),
        "api_device_type": "",
        "api_brand": "",
        "api_model": "",
        "api_os_name": "",
        "api_os_version": "",
        "api_browser_name": "",
        "api_browser_version": "",
        "api_error": "",
        "notes": reason,
        "decoded_at_utc": now,
    }
    if reason:
        result["decode_status"] = "malformed"
        result["decoder_method"] = "local_malformed"
        result["confidence"] = "high"
        return result

    browser_name, browser_version, browser_engine = browser_from_ua(ua)
    result["browser_name"] = browser_name
    result["browser_version"] = browser_version
    result["browser_engine"] = browser_engine

    if re.search(r"bot|crawler|spider|slurp|monitor|uptime|probe", lower):
        result.update(
            decode_status="decoded_local",
            device_type="bot",
            form_factor="Bot",
            is_bot=True,
            confidence="medium",
            notes="Bot/crawler pattern",
        )
        return result

    fire_match = re.search(r"\b(AFT[A-Z0-9]{1,12}|B0[A-Z0-9]{8,12})\b", ua, flags=re.I)
    android_match = re.search(r"Android[ /]([0-9._]+)", ua, flags=re.I)

    if "iphone" in lower:
        result.update(device_type="smartphone", brand="Apple", model="iPhone", os_name="iOS", os_family="iOS", confidence="high")
    elif "ipad" in lower:
        result.update(device_type="tablet", brand="Apple", model="iPad", os_name="iPadOS", os_family="iOS", confidence="high")
    elif "tizen" in lower:
        version = first_match([("Tizen", r"Tizen[ /]([0-9.]+)")], ua)[1]
        result.update(device_type="smart_tv", brand="Samsung", model="Samsung/Tizen TV", os_name="Tizen", os_version=version, os_family="Tizen", confidence="high")
    elif "webos" in lower:
        version = first_match([("webOS", r"webOS[ /]([0-9.]+)")], ua)[1]
        result.update(device_type="smart_tv", brand="LG", model="LG/webOS TV", os_name="webOS", os_version=version, os_family="webOS", confidence="high")
    elif "roku" in lower:
        result.update(device_type="streaming_device", brand="Roku", model="Roku device", os_name="Roku OS", os_family="Roku", confidence="medium")
    elif "appletv" in lower or "apple tv" in lower:
        result.update(device_type="streaming_device", brand="Apple", model="Apple TV", os_name="tvOS", os_family="iOS", confidence="medium")
    elif fire_match:
        code = fire_match.group(1).upper()
        mapped = fire_map.get(code, {})
        result.update(
            device_type="streaming_device" if not mapped.get("product_family", "").lower().endswith("tv") else "smart_tv",
            brand=mapped.get("brand") or "Amazon",
            model=mapped.get("model") or code,
            model_code=code,
            product_family=mapped.get("product_family", "Fire TV"),
            generation=mapped.get("generation", ""),
            os_name="Android",
            os_family="Android/FireOS",
            confidence="high" if mapped else "medium",
            notes=mapped.get("notes", "Amazon Fire TV model code extracted from UA") if mapped else "Unknown Amazon Fire TV model code",
        )
    elif android_match:
        model = extract_android_model(ua)
        brand = infer_android_brand(model, ua)
        version = android_match.group(1).replace("_", ".")
        device_type = "smart_tv" if is_android_ctv_model(model, ua) else "smartphone"
        if "tablet" in lower or "pad" in model.lower():
            device_type = "tablet"
        result.update(
            device_type=device_type,
            brand=brand,
            model=model,
            os_name="Android",
            os_version=version,
            os_family="Android",
            confidence="medium" if model or brand else "low",
        )
    elif "windows" in lower:
        result.update(device_type="desktop", form_factor="Desktop", brand="", model="Windows PC", os_name="Windows", os_family="Windows", confidence="medium")
    elif "macintosh" in lower or "mac os x" in lower:
        result.update(device_type="desktop", form_factor="Desktop", brand="Apple", model="Mac", os_name="macOS", os_family="macOS", confidence="medium")
    elif "linux" in lower:
        result.update(device_type="desktop", form_factor="Desktop", model="Linux device", os_name="Linux", os_family="Linux", confidence="low")

    if "exoplayer" in lower or "media3" in lower:
        result["app_player"] = "ExoPlayer / AndroidX Media3"
    elif "applecoremedia" in lower:
        result["app_player"] = "AppleCoreMedia"
    elif "okhttp" in lower:
        result["app_player"] = "OkHttp"
    elif "afterglowtv" in lower:
        result["app_player"] = "AfterglowTV"

    if result["device_type"] or result["brand"] or result["model"] or result["os_name"] or result["browser_name"] or result["app_player"]:
        result["decode_status"] = "decoded_local"
    result["form_factor"] = classify_form_factor(result["device_type"], result["model"], ua)
    return result


def load_api_cache(path: Path) -> pd.DataFrame:
    cache = read_parquet(path)
    for column in API_COLUMNS:
        if column not in cache.columns:
            cache[column] = ""
    return cache[API_COLUMNS].drop_duplicates("ua_hash", keep="last") if not cache.empty else cache[API_COLUMNS]


def combine_api_caches(*frames: pd.DataFrame) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=API_COLUMNS)
    combined = pd.concat(non_empty, ignore_index=True)
    for column in API_COLUMNS:
        if column not in combined.columns:
            combined[column] = ""
    # Later frames are more recent/specific evidence, so keep the last row.
    return combined[API_COLUMNS].drop_duplicates("ua_hash", keep="last")


def save_api_cache(cache: pd.DataFrame, path: Path) -> None:
    if cache.empty:
        return
    cache = cache[API_COLUMNS].drop_duplicates("ua_hash", keep="last")
    write_parquet(cache, path)


def api_decode(ua: str, args: argparse.Namespace) -> dict[str, Any]:
    params = urllib.parse.urlencode({"ua": ua, "key": args.api_key})
    url = f"{args.api_url}?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": "Veto-ETL-UA-Decoder/1.1"})
    with urllib.request.urlopen(request, timeout=args.api_timeout) as response:
        text = response.read().decode("utf-8", errors="replace").strip()
    payload = json.loads(text)
    if isinstance(payload, dict) and str(payload.get("error", "")).lower().find("rate limit") >= 0:
        raise RuntimeError("Rate limit exceeded")
    if not isinstance(payload, dict):
        raise ValueError("API returned non-object JSON")
    device = payload.get("Device") or {}
    os_info = payload.get("OS") or {}
    browser = payload.get("Browser") or {}
    bot = payload.get("Bot") or []
    bot_name = ""
    if isinstance(bot, list) and bot:
        bot_name = ", ".join(str(item) for item in bot[:3])
    elif isinstance(bot, dict):
        bot_name = safe_text(bot.get("name") or bot.get("type"))
    return {
        "decoder_method": "whatmyuseragent",
        "api_status": "decoded_api",
        "api_device_type": safe_text(device.get("deviceType")),
        "api_brand": safe_text(device.get("brand")),
        "api_model": safe_text(device.get("model")),
        "api_os_name": safe_text(os_info.get("name")),
        "api_os_version": safe_text(os_info.get("version")),
        "api_os_family": safe_text(os_info.get("family")),
        "api_browser_name": safe_text(browser.get("name")),
        "api_browser_version": safe_text(browser.get("version")),
        "api_browser_engine": safe_text(browser.get("engine")),
        "api_bot_name": bot_name,
        "api_error": "",
        "decoded_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def api_error_row(hash_value: str, error: str) -> dict[str, Any]:
    return {
        "ua_hash": hash_value,
        "decoder_method": "whatmyuseragent_error",
        "api_status": "api_error",
        "api_device_type": "",
        "api_brand": "",
        "api_model": "",
        "api_os_name": "",
        "api_os_version": "",
        "api_os_family": "",
        "api_browser_name": "",
        "api_browser_version": "",
        "api_browser_engine": "",
        "api_bot_name": "",
        "api_error": error,
        "decoded_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def should_api_decode(row: pd.Series) -> bool:
    if bool(row.get("is_malformed")):
        return False
    if api_candidate_score(row) <= 0:
        return False
    if safe_text(row.get("decode_status")) == "unknown":
        return True
    if safe_text(row.get("confidence")) == "low" and not safe_text(row.get("brand")):
        return True
    return False


def api_candidate_score(row: pd.Series) -> int:
    """Rank unresolved rows so scarce API calls go to meaningful UA strings first."""
    ua = safe_text(row.get("ua_norm"))
    lower = ua.lower()
    if not ua or bool(row.get("is_malformed")):
        return -100

    score = 0
    high_signal_tokens = [
        "mozilla",
        "dalvik",
        "android",
        "iphone",
        "ipad",
        "applecoremedia",
        "exoplayer",
        "media3",
        "okhttp",
        "tizen",
        "webos",
        "roku",
        "bravia",
        "smart-tv",
        "smarttv",
        "fire tv",
        "libvlc",
        "carplay",
    ]
    if any(token in lower for token in high_signal_tokens):
        score += 100
    if re.search(r"\b[A-Za-z][A-Za-z0-9_.+-]{2,40}/[0-9][A-Za-z0-9_.+-]*", ua):
        score += 40
    if re.search(r"\([^)]+(?:android|linux|windows|mac|ios|tizen|webos)[^)]+\)", lower):
        score += 30
    if 8 <= len(ua) <= 400:
        score += 10
    if re.search(r"^[^A-Za-z]{0,4}[A-Za-z0-9]{1,3}[/-][^ ]{1,6}", ua) and score < 100:
        score -= 50
    alpha = sum(1 for ch in ua if ch.isalpha())
    if len(ua) > 40 and alpha / max(len(ua), 1) < 0.35 and score < 100:
        score -= 40
    return score


def enrich_with_api(local: pd.DataFrame, cache: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    if args.api_limit == 0:
        return cache
    cache = load_api_cache(args.api_cache)
    cached_hashes = set(cache["ua_hash"].astype(str)) if not cache.empty else set()
    candidates = local[local.apply(should_api_decode, axis=1)].copy()
    candidates = candidates[~candidates["ua_hash"].isin(cached_hashes)]
    if not candidates.empty:
        candidates["_api_candidate_score"] = candidates.apply(api_candidate_score, axis=1)
        candidates = candidates.sort_values(
            ["_api_candidate_score", "ua_norm"],
            ascending=[False, True],
        ).drop(columns=["_api_candidate_score"])
    if args.api_limit > 0:
        candidates = candidates.head(args.api_limit)
    log(f"WhatMyUserAgent API candidates this run: {len(candidates):,}")
    api_rows: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(candidates.iterrows(), start=1):
        ua = safe_text(row["ua_norm"])
        hash_value = safe_text(row["ua_hash"])
        log(f"API {idx}/{len(candidates)} ua_hash={hash_value[:12]} ua={ua[:90]}")
        try:
            decoded = api_decode(ua, args)
            api_rows.append({"ua_hash": hash_value, **decoded})
        except Exception as exc:
            error = str(exc)
            api_rows.append(api_error_row(hash_value, error))
            log(f"API error: {error}")
            if "rate limit" in error.lower() and args.stop_on_rate_limit:
                log("Stopping API loop because rate limit was reached. Cache will be saved.")
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


def apply_api_to_local(local: pd.DataFrame, api_cache: pd.DataFrame) -> pd.DataFrame:
    if api_cache.empty:
        for column in API_COLUMNS:
            if column != "ua_hash" and column not in local.columns:
                local[column] = ""
        return local
    merged = local.merge(api_cache, on="ua_hash", how="left", suffixes=("", "_api"))
    for column in API_COLUMNS:
        if column == "ua_hash":
            continue
        api_column = f"{column}_api"
        if api_column not in merged.columns:
            continue
        if column not in merged.columns:
            merged[column] = merged[api_column]
            continue
        blank_local = merged[column].fillna("").astype(str).str.strip() == ""
        has_api_value = merged[api_column].fillna("").astype(str).str.strip() != ""
        merged.loc[blank_local & has_api_value, column] = merged.loc[blank_local & has_api_value, api_column]
    for source_col, target_col in [
        ("api_device_type", "device_type"),
        ("api_brand", "brand"),
        ("api_model", "model"),
        ("api_os_name", "os_name"),
        ("api_os_version", "os_version"),
        ("api_browser_name", "browser_name"),
        ("api_browser_version", "browser_version"),
        ("api_browser_engine", "browser_engine"),
    ]:
        if source_col not in merged.columns:
            continue
        blank_target = merged[target_col].fillna("").astype(str).str.strip() == ""
        has_api = merged[source_col].fillna("").astype(str).str.strip() != ""
        merged.loc[blank_target & has_api, target_col] = merged.loc[blank_target & has_api, source_col]
    has_good_api = merged.get("api_status", "").fillna("").eq("decoded_api") if "api_status" in merged else False
    if not isinstance(has_good_api, bool):
        api_has_identity = has_good_api & (
            merged["api_device_type"].fillna("").astype(str).str.strip().ne("")
            | merged["api_brand"].fillna("").astype(str).str.strip().ne("")
            | merged["api_model"].fillna("").astype(str).str.strip().ne("")
            | merged["api_os_name"].fillna("").astype(str).str.strip().ne("")
        )
        for source_col, target_col in [
            ("api_brand", "brand"),
            ("api_model", "model"),
            ("api_os_name", "os_name"),
            ("api_os_version", "os_version"),
            ("api_browser_name", "browser_name"),
            ("api_browser_version", "browser_version"),
            ("api_browser_engine", "browser_engine"),
        ]:
            has_api = merged[source_col].fillna("").astype(str).str.strip().ne("")
            merged.loc[api_has_identity & has_api, target_col] = merged.loc[api_has_identity & has_api, source_col]
        has_api_device = merged["api_device_type"].fillna("").astype(str).str.strip().ne("")
        merged.loc[api_has_identity & has_api_device, "device_type"] = merged.loc[
            api_has_identity & has_api_device, "api_device_type"
        ].map(normalize_api_device_type)
        if "api_os_family" in merged.columns:
            merged.loc[api_has_identity, "os_family"] = merged.loc[api_has_identity].apply(
                lambda row: os_family_from_api(safe_text(row.get("api_os_name")), safe_text(row.get("api_os_family"))),
                axis=1,
            )
        merged.loc[api_has_identity, "decode_status"] = "decoded_api"
        merged.loc[api_has_identity, "decoder_method"] = "local_rule+whatmyuseragent"
        merged.loc[api_has_identity, "confidence"] = "high"
        unknown_or_low = merged["decode_status"].isin(["unknown"]) | merged["confidence"].eq("low")
        merged.loc[has_good_api & unknown_or_low, "decode_status"] = "decoded_api"
        merged.loc[has_good_api & unknown_or_low, "decoder_method"] = "local_rule+whatmyuseragent"
        merged.loc[has_good_api & unknown_or_low, "confidence"] = "medium"
    merged["form_factor"] = merged.apply(lambda row: classify_form_factor(safe_text(row["device_type"]), safe_text(row["model"]), safe_text(row["ua_norm"])), axis=1)
    return merged


def build_summary(decoded: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["decode_status", "form_factor", "device_type", "brand", "os_name"]
    summary = (
        decoded.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="distinct_ua_count")
        .sort_values("distinct_ua_count", ascending=False)
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decode distinct UA CSV into CSV/parquet lookup.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--column", default=None, help="UA column name. Defaults to UA or first CSV column.")
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--output-prefix", default="ua_decode_lookup_both_all")
    parser.add_argument("--api-cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--api-limit", type=int, default=0, help="0 disables API, positive decodes N candidates, -1 decodes all uncached candidates.")
    parser.add_argument("--api-key", default=os.getenv("WHATMYUA_KEY", "NOTREQUIED"))
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--api-timeout", type=float, default=20.0)
    parser.add_argument("--api-sleep-min-seconds", type=float, default=8.0)
    parser.add_argument("--api-sleep-max-seconds", type=float, default=15.0)
    parser.add_argument("--api-flush-every", type=int, default=5)
    parser.add_argument("--stop-on-rate-limit", action="store_true", default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.input = args.input.expanduser().resolve()
    args.map = args.map.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    args.api_cache = args.api_cache.expanduser().resolve()

    started = time.time()
    log(f"Reading distinct UA CSV: {args.input}")
    distinct = read_distinct_ua_csv(args.input, args.column)
    log(f"Distinct normalized UA rows: {len(distinct):,}")

    fire_map = load_fire_tv_map(args.map)
    log(f"Loaded Fire TV model mappings: {len(fire_map):,}")

    local = pd.DataFrame([local_decode_row(row, fire_map) for _, row in distinct.iterrows()])
    api_cache = load_api_cache(args.api_cache)
    api_cache = enrich_with_api(local, api_cache, args)
    crosscheck_cache = (
        load_api_cache(DEFAULT_LOCAL_VERIFIED_CROSSCHECK_CACHE)
        if DEFAULT_LOCAL_VERIFIED_CROSSCHECK_CACHE.resolve() != args.api_cache.resolve()
        else pd.DataFrame(columns=API_COLUMNS)
    )
    all_distinct_cache = (
        load_api_cache(DEFAULT_ALL_DISTINCT_API_CACHE)
        if DEFAULT_ALL_DISTINCT_API_CACHE.resolve() != args.api_cache.resolve()
        else pd.DataFrame(columns=API_COLUMNS)
    )
    combined_api_cache = combine_api_caches(api_cache, crosscheck_cache, all_distinct_cache)
    decoded = apply_api_to_local(local, combined_api_cache)

    for column in OUTPUT_COLUMNS:
        if column not in decoded.columns:
            decoded[column] = ""
    decoded = decoded[OUTPUT_COLUMNS].sort_values(["decode_status", "form_factor", "brand", "model", "ua_norm"])
    summary = build_summary(decoded)
    unknown = decoded[decoded["decode_status"].isin(["unknown", "malformed"])].copy()

    csv_path = args.out_dir / f"{args.output_prefix}.csv"
    parquet_path = args.out_dir / f"{args.output_prefix}.parquet"
    summary_path = args.out_dir / f"{args.output_prefix}_summary.csv"
    unknown_path = args.out_dir / f"{args.output_prefix}_unknown_review.csv"
    manifest_path = args.out_dir / f"{args.output_prefix}_manifest.json"

    write_csv(decoded, csv_path)
    write_parquet(decoded, parquet_path)
    write_csv(summary, summary_path)
    write_csv(unknown, unknown_path)

    stats = {
        "input_rows": int(len(distinct)),
        "decoded_rows": int((decoded["decode_status"] == "decoded_local").sum() + (decoded["decode_status"] == "decoded_api").sum()),
        "local_decoded_rows": int((decoded["decode_status"] == "decoded_local").sum()),
        "api_decoded_rows": int((decoded["decode_status"] == "decoded_api").sum()),
        "unknown_rows": int((decoded["decode_status"] == "unknown").sum()),
        "malformed_rows": int((decoded["decode_status"] == "malformed").sum()),
        "api_cache_rows": int(len(api_cache)),
        "local_verified_api_crosscheck_rows": int(len(crosscheck_cache)),
        "all_distinct_api_cache_rows": int(len(all_distinct_cache)),
        "combined_api_cache_rows": int(len(combined_api_cache)),
        "elapsed_seconds": round(time.time() - started, 2),
    }
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": str(args.input),
        "map": str(args.map),
        "api_url": args.api_url,
        "api_limit": args.api_limit,
        "outputs": {
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "summary": str(summary_path),
            "unknown_review": str(unknown_path),
            "api_cache": str(args.api_cache),
            "local_verified_crosscheck_cache": str(DEFAULT_LOCAL_VERIFIED_CROSSCHECK_CACHE),
            "all_distinct_api_cache": str(DEFAULT_ALL_DISTINCT_API_CACHE),
        },
        "stats": stats,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log(f"CSV written       : {csv_path}")
    log(f"Parquet written   : {parquet_path}")
    log(f"Summary written   : {summary_path}")
    log(f"Unknown review    : {unknown_path}")
    log(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
