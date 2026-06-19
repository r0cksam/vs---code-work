#!/usr/bin/env python3
"""Generate the Veto Audience Operations static HTML dashboard."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote_plus
from zoneinfo import ZoneInfo

import pandas as pd


HERE = Path(__file__).resolve().parent
SRC_ROOT = HERE.parents[1]
ETL_ROOT = SRC_ROOT.parent
DEFAULT_OUTPUT_ROOT = ETL_ROOT / "output"
DEFAULT_OUT = DEFAULT_OUTPUT_ROOT / "audience_ops" / "veto_audience_operations.html"

IST_ZONE = ZoneInfo("Asia/Kolkata")
# Each HLS .ts segment represents six seconds. 6 / 3600 converts one segment
# row into hours, which is the unit used by the stakeholder dashboard.
HOURS_PER_TS_SEGMENT = 6 / 3600


class DashboardDataError(RuntimeError):
    """Raised when an input required to build a trustworthy dashboard is invalid."""


class ParquetReadError(DashboardDataError):
    """Raised when a parquet file exists but cannot be read."""


def warn(message: str) -> None:
    print(f"[warn] {message}", file=sys.stderr)


def _load_common_module(module_name: str, file_name: str) -> Any:
    """Load local shared helpers without mutating sys.path at import time."""
    path = SRC_ROOT / "common" / file_name
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_chartjs_module = _load_common_module("veto_common_chartjs", "chartjs.py")
_render_module = _load_common_module("veto_common_render", "render.py")

load_chartjs = _chartjs_module.load_chartjs
chartjs_script = _render_module.chartjs_script
json_blob = _render_module.json_blob
render_template = _render_module.render_template


def get_duckdb() -> Any | None:
    """Import duckdb lazily so optional dependency failure has no import-time side effect."""
    try:
        import duckdb  # type: ignore[import-not-found]
    except (ImportError, OSError) as exc:
        warn(f"DuckDB is unavailable; true reqTimeSec ranges will use date fallbacks: {exc}")
        return None
    return duckdb


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        # Missing optional marts are allowed, but a present unreadable parquet is
        # a data-quality failure and must not be converted into a silent empty
        # dashboard section.
        raise ParquetReadError(f"Could not read parquet file {path}: {exc}") from exc


def latest_file(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    skipped_tmp = 0
    for file in files:
        if ".tmp." not in file.name and file.suffix.lower() == ".parquet":
            return file
        skipped_tmp += 1
    if skipped_tmp:
        warn(f"Only temporary files matched {folder / pattern}; ignoring {skipped_tmp} candidate(s).")
    return None


def records(df: pd.DataFrame, limit: int | None = None, tail: bool = False) -> list[dict[str, Any]]:
    if df.empty:
        return []
    if limit is not None:
        if limit < 0:
            raise ValueError("records() limit must be non-negative")
        df = df.tail(limit) if tail else df.head(limit)
    clean = df.copy()
    for col in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[col]):
            clean[col] = clean[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    # json.dumps accepts NaN by default, but JavaScript dashboard math treats it
    # as a value. Convert missing scalars to JSON null before rendering.
    clean = clean.astype(object).where(pd.notna(clean), None)
    return clean.to_dict(orient="records")


def numeric(df: pd.DataFrame, cols: list[str], fill_value: float | None = 0) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in cols:
        if col in out.columns:
            values = pd.to_numeric(out[col], errors="coerce")
            out[col] = values.fillna(fill_value) if fill_value is not None else values
    return out


def numeric_nullable(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Coerce numeric columns while preserving NaN where missing has meaning."""
    return numeric(df, cols, fill_value=None)


def date_string(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def add_watch_hours(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = numeric(df, ["raw_ts_rows", "status_200_ts_rows", "ts_rows", "m3u8_rows", "rows", "approx_unique_ips"])
    if "request_watch_hours" not in out.columns:
        out["request_watch_hours"] = out.get("raw_ts_rows", 0) * HOURS_PER_TS_SEGMENT
    if "status_200_request_hours" not in out.columns:
        out["status_200_request_hours"] = out.get("status_200_ts_rows", 0) * HOURS_PER_TS_SEGMENT
    if "non_200_rows" in out.columns and "rows" in out.columns:
        rows = out["rows"].where(out["rows"].ne(0))
        out["non_200_pct"] = (out["non_200_rows"] * 100 / rows).fillna(0)
    return out


def range_info(df: pd.DataFrame, date_col: str = "log_date") -> dict[str, Any]:
    if df.empty or date_col not in df.columns:
        return {"min_date": "", "max_date": "", "rows": 0, "sources": []}
    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    sources = sorted(str(v) for v in df.get("source", pd.Series(dtype=object)).dropna().unique())
    return {
        "min_date": dates.min().strftime("%Y-%m-%d") if not dates.empty else "",
        "max_date": dates.max().strftime("%Y-%m-%d") if not dates.empty else "",
        "rows": int(len(df)),
        "sources": sources,
    }


def source_ranges(df: pd.DataFrame, date_col: str = "log_date") -> list[dict[str, Any]]:
    if df.empty or "source" not in df.columns:
        return []
    out = []
    for source, part in df.groupby("source", dropna=False):
        info = range_info(part, date_col)
        info["source"] = str(source)
        out.append(info)
    return out


def sql_path(path: Path | str) -> str:
    # The DuckDB call uses parameter binding, so do not SQL-escape here; only
    # normalize Windows separators for the glob pattern.
    return str(path).replace("\\", "/")


def resolve_lake_root() -> Path:
    env = os.getenv("VG_ETL_LAKE_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    return ETL_ROOT / "data" / "lake"


def format_epoch_ist(epoch: float | None) -> str:
    if epoch is None:
        return ""
    # reqTimeSec is a UTC Unix epoch. Convert from UTC to IST with zoneinfo
    # instead of adding a hardcoded offset.
    return datetime.fromtimestamp(float(epoch), timezone.utc).astimezone(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")


def day_partition_folder(lake_root: Path, source: str, date_text: str) -> Path | None:
    try:
        dt = datetime.strptime(str(date_text)[:10], "%Y-%m-%d")
    except ValueError:
        return None
    folder = lake_root / f"source={source.lower()}" / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
    return folder if folder.exists() else None


def reqtime_bounds(folder: Path | None) -> tuple[float | None, float | None]:
    duckdb = get_duckdb()
    if duckdb is None or folder is None:
        return None, None
    if not any(folder.glob("*.parquet")):
        return None, None
    glob = sql_path(folder / "*.parquet")
    con = None
    try:
        con = duckdb.connect()
        row = con.execute(
            """
            SELECT
                MIN(TRY_CAST(reqTimeSec AS DOUBLE)) AS first_epoch,
                MAX(TRY_CAST(reqTimeSec AS DOUBLE)) AS last_epoch
            FROM read_parquet(?, hive_partitioning=1, union_by_name=1)
            WHERE TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL
            """,
            [glob],
        ).fetchone()
    except Exception as exc:
        warn(f"Could not scan true time range for {folder}: {exc}")
        return None, None
    finally:
        if con is not None:
            con.close()
    if not row:
        return None, None
    return row[0], row[1]


def source_true_ranges_from_lake(df: pd.DataFrame, lake_root: Path | None = None) -> list[dict[str, Any]]:
    ranges = source_ranges(df)
    if not ranges:
        return []
    lake_root = lake_root or resolve_lake_root()
    out: list[dict[str, Any]] = []
    for item in ranges:
        source = str(item.get("source", "")).lower()
        min_date = str(item.get("min_date", "") or "")[:10]
        max_date = str(item.get("max_date", "") or "")[:10]
        first_epoch, _ = reqtime_bounds(day_partition_folder(lake_root, source, min_date))
        _, last_epoch = reqtime_bounds(day_partition_folder(lake_root, source, max_date))
        enriched = dict(item)
        enriched.update(
            {
                "first_ist": format_epoch_ist(first_epoch) or (f"{min_date} 00:00:00" if min_date else ""),
                "last_ist": format_epoch_ist(last_epoch) or (f"{max_date} 23:59:59" if max_date else ""),
                "first_epoch": first_epoch,
                "last_epoch": last_epoch,
                "basis": "reqTimeSec min/max from first and last lake partitions",
            }
        )
        out.append(enriched)
    return out


def safe_group_sum(df: pd.DataFrame, keys: list[str], metrics: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=keys + metrics)
    cols = list(dict.fromkeys(c for c in keys + metrics if c in df.columns))
    group_keys = [k for k in keys if k in cols]
    if not group_keys:
        return pd.DataFrame(columns=keys + metrics)
    metric_cols = list(dict.fromkeys(m for m in metrics if m in cols))
    out = numeric(df[cols], metric_cols)
    # groupby(..., as_index=False) preserves non-numeric key columns while
    # numeric_only=True restricts aggregation to metric columns.
    return out.groupby(group_keys, dropna=False, as_index=False, observed=True).sum(numeric_only=True)


def filter_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty or source == "all" or "source" not in df.columns:
        return df
    return df[df["source"].astype(str).str.lower().eq(source)].copy()


def filter_date_window(df: pd.DataFrame, min_date: Any, max_date: Any, date_col: str = "log_date") -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    start = pd.Timestamp(str(min_date))
    end = pd.Timestamp(str(max_date))
    dates = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    return df[dates.ge(start) & dates.le(end)].copy()


def filter_frame_dict(frames: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        name: filter_source(frame, source) if isinstance(frame, pd.DataFrame) else frame
        for name, frame in frames.items()
    }


def filter_frame_dict_dates(
    frames: dict[str, pd.DataFrame],
    min_date: Any,
    max_date: Any,
    date_cols: dict[str, str] | None = None,
) -> dict[str, Any]:
    date_cols = date_cols or {}
    return {
        name: filter_date_window(frame, min_date, max_date, date_cols.get(name, "log_date"))
        if isinstance(frame, pd.DataFrame)
        else frame
        for name, frame in frames.items()
    }


def build_daily(watch_dir: Path) -> pd.DataFrame:
    daily = add_watch_hours(read_parquet(watch_dir / "daily_volume.parquet"))
    if daily.empty:
        return daily
    daily["log_date"] = date_string(daily["log_date"])
    keep = [
        "log_date",
        "source",
        "rows",
        "status_200_rows",
        "non_200_rows",
        "raw_ts_rows",
        "status_200_ts_rows",
        "ts_rows",
        "m3u8_rows",
        "approx_unique_ips",
        "request_watch_hours",
        "status_200_request_hours",
        "non_200_pct",
    ]
    return daily[[c for c in keep if c in daily.columns]].sort_values(["log_date", "source"])


def build_overview_daily(output_root: Path) -> pd.DataFrame:
    path = output_root / "overview" / "overview_source_daily.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        overview = pd.read_csv(path)
    except Exception as exc:
        raise DashboardDataError(f"Could not read overview daily CSV {path}: {exc}") from exc
    if overview.empty or "date" not in overview.columns:
        return pd.DataFrame()
    overview["date"] = date_string(overview["date"])
    metrics = [
        "bytes",
        "rows",
        "ip_rows",
        "dist_ip",
        "dist_ipua",
        "dist_dev",
        "dist_sess",
        "dist_ip_r2",
        "dist_ipua_r2",
        "sess_avail",
        "sess_na",
        "sess_none",
    ]
    overview = numeric(overview, metrics)
    keep = ["date", "source"] + metrics
    return overview[[c for c in keep if c in overview.columns]].sort_values(["date", "source"])


def build_channel(watch_dir: Path) -> pd.DataFrame:
    channel = read_parquet(watch_dir / "channel_audience_daily.parquet")
    if channel.empty:
        return channel
    channel = numeric(
        channel,
        [
            "raw_watch_hours",
            "status_200_watch_hours",
            "raw_ts_chunks",
            "status_200_ts_chunks",
            "approx_unique_ips",
            "approx_sessions",
            "approx_devices",
        ],
    )
    channel["log_date"] = date_string(channel["log_date"])
    return channel.sort_values(["log_date", "source", "channel_name"])


def build_geo(watch_dir: Path) -> pd.DataFrame:
    geo = read_parquet(watch_dir / "geo_daily.parquet")
    if geo.empty:
        return geo
    geo = numeric(geo, ["raw_watch_hours", "status_200_watch_hours", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"])
    geo["log_date"] = date_string(geo["log_date"])
    return geo.sort_values(["log_date", "source", "raw_watch_hours"], ascending=[True, True, False])


def normalize_ua(value: Any) -> str:
    text = str(value or "").strip()
    for _ in range(5):
        decoded = unquote_plus(text).strip()
        if decoded == text:
            break
        text = decoded
    return re.sub(r"\s+", " ", text).strip()


def ua_hash(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8", errors="replace")).hexdigest()


def clean_label(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null", "unknown / na", "unknown"}:
        return fallback
    return text


def load_ua_lookup(device_dir: Path) -> tuple[pd.DataFrame, str]:
    env_path = os.getenv("VG_UA_DECODE_LOOKUP")
    lookup_path = Path(env_path).expanduser().resolve() if env_path else device_dir / "ua_decode_lookup_both_all.parquet"
    if not lookup_path.exists():
        lookup_path = latest_file(device_dir, "ua_decode_lookup_*.parquet") or lookup_path
    lookup = read_parquet(lookup_path)
    if lookup.empty or "ua_hash" not in lookup.columns:
        return pd.DataFrame(), str(lookup_path if lookup_path.exists() else "")

    keep = [
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
        "app_player",
        "api_device_type",
        "api_brand",
        "api_model",
        "api_os_name",
        "api_browser_name",
        "notes",
    ]
    lookup = lookup[[c for c in keep if c in lookup.columns]].drop_duplicates("ua_hash", keep="last").copy()
    return lookup, str(lookup_path)


def enrich_ua_labels(ua: pd.DataFrame) -> pd.DataFrame:
    out = ua.copy()
    for col in [
        "decode_status",
        "device_type",
        "form_factor",
        "brand",
        "model",
        "model_code",
        "os_name",
        "os_version",
        "browser_name",
        "browser_version",
        "app_player",
    ]:
        if col not in out.columns:
            out[col] = ""

    out["decode_status"] = out["decode_status"].fillna("").replace("", "not_in_lookup")
    out["brand_label"] = out.apply(
        lambda r: "Malformed / Noise"
        if str(r.get("decode_status", "")) == "malformed"
        else (
            "Unknown / Needs Review"
            if str(r.get("decode_status", "")) in {"unknown", "not_in_lookup"}
            else clean_label(r.get("brand"), "Brand Not Exposed In UA")
        ),
        axis=1,
    )
    out["model_label"] = out.apply(
        lambda r: "Malformed / Noise"
        if str(r.get("decode_status", "")) == "malformed"
        else (
            "Unknown / Needs Review"
            if str(r.get("decode_status", "")) in {"unknown", "not_in_lookup"}
            else clean_label(r.get("model"), "Model Not Exposed In UA")
        ),
        axis=1,
    )
    out["form_factor_label"] = out["form_factor"].map(lambda v: clean_label(v, "Unknown Form Factor"))
    out["device_type_label"] = out["device_type"].map(lambda v: clean_label(v, "Unknown Device Type"))
    out["os_label"] = (
        out["os_name"].fillna("").astype(str).str.strip()
        + " "
        + out["os_version"].fillna("").astype(str).str.strip()
    ).str.strip().map(lambda v: clean_label(v, "OS Not Exposed In UA"))
    out["browser_label"] = (
        out["browser_name"].fillna("").astype(str).str.strip()
        + " "
        + out["browser_version"].fillna("").astype(str).str.strip()
    ).str.strip().map(lambda v: clean_label(v, "Browser Not Exposed In UA"))
    out["app_player_label"] = out["app_player"].map(lambda v: clean_label(v, "Player Not Exposed In UA"))
    out["device_identity"] = out.apply(
        lambda r: (
            f"{r['brand_label']} / {r['model_label']}"
            if r["brand_label"] not in {"Brand Not Exposed In UA", "Unknown / Needs Review", "Malformed / Noise"}
            else f"{r['form_factor_label']} / {r['device_type_label']}"
        ),
        axis=1,
    )

    has_api_identity = out.get("api_brand", pd.Series("", index=out.index)).fillna("").astype(str).str.strip().ne("") | out.get(
        "api_model", pd.Series("", index=out.index)
    ).fillna("").astype(str).str.strip().ne("")
    has_api_detail = has_api_identity | out.get("api_device_type", pd.Series("", index=out.index)).fillna("").astype(str).str.strip().ne("")
    out["decode_quality"] = out["decode_status"].map(
        {
            "decoded_local": "Verified Local Decode",
            "decoded_api": "API Enriched",
            "unknown": "Unknown / Needs Review",
            "malformed": "Malformed / Noise",
            "not_in_lookup": "Not In UA Lookup",
        }
    ).fillna("Unknown / Needs Review")
    out.loc[out["decode_status"].eq("decoded_api") & ~has_api_detail, "decode_quality"] = "API Generic / Weak"
    out.loc[out["decode_status"].eq("decoded_api") & has_api_identity, "decode_quality"] = "API Brand/Model Enriched"
    return out


UA_PLATFORM_RULES = [
    (r"aft|fire tv", "Amazon Fire TV"),
    (r"bravia|sony", "Sony BRAVIA"),
    (r"tizen|stvplus|samsung", "Samsung / Tizen TV"),
    (r"webos|web0s|lg smart", "LG Smart TV"),
    (r"xiaomi|mitv|mi tv", "Xiaomi Mi TV"),
    (r"sharp", "Sharp TV"),
    (r"hisense", "Hisense Smart TV"),
    (r"haier", "Haier MatrixTV"),
    (r"vu tv|vutv", "VU TV"),
    (r"ai pont|aipont", "AI PONT TV"),
    (r"iphone|ipad", "iPhone/iPad"),
    (r"android|okhttp|exoplayer|dalvik", "ExoPlayer-Android"),
    (r"smart-tv|smart tv", "Smart TV / Other"),
    (r"mac os", "macOS"),
    (r"windows", "Windows"),
]

UA_OS_RULES = [
    (r"aft|fire tv", "FireOS"),
    (r"android|okhttp|dalvik|exoplayer", "Android"),
    (r"webos|web0s", "webOS"),
    (r"tizen", "Tizen"),
    (r"appletv|apple tv", "Apple TV"),
    (r"iphone|ipad|cpu os", "iOS"),
    (r"mac os", "macOS"),
    (r"windows", "Windows"),
    (r"linux", "Linux"),
]


def clean_ua_text(value: Any) -> str:
    return str(value or "").replace("%20", " ").replace("+", " ").strip()


def ua_label(value: Any) -> str:
    return clean_ua_text(value)


def clean_ua_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.replace("%20", " ", regex=False).str.replace("+", " ", regex=False).str.strip()


def classify_ua_series(series: pd.Series, rules: list[tuple[str, str]], default: str) -> pd.Series:
    clean = clean_ua_series(series).str.lower()
    output = pd.Series(default, index=series.index, dtype="object")
    unmatched = pd.Series(True, index=series.index)
    for pattern, label in rules:
        mask = unmatched & clean.str.contains(pattern, regex=True, na=False)
        output.loc[mask] = label
        unmatched &= ~mask
    return output


def derive_ua_platform(ua_value: Any) -> str:
    clean = clean_ua_text(ua_value).lower()
    for pattern, label in UA_PLATFORM_RULES:
        if re.search(pattern, clean):
            return label
    return "Others"


def derive_ua_os(ua_value: Any) -> str:
    clean = clean_ua_text(ua_value).lower()
    for pattern, label in UA_OS_RULES:
        if re.search(pattern, clean):
            return label
    return "Others"


def build_ua_playtime(watch_dir: Path, device_dir: Path) -> dict[str, pd.DataFrame | str]:
    ua = read_parquet(watch_dir / "user_agents_daily.parquet")
    if ua.empty:
        return {
            "platform": pd.DataFrame(),
            "os": pd.DataFrame(),
            "brand": pd.DataFrame(),
            "model": pd.DataFrame(),
            "form_factor": pd.DataFrame(),
            "decode_quality": pd.DataFrame(),
            "device_detail": pd.DataFrame(),
            "lookup_path": "",
        }
    ua = numeric(ua, ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips"])
    ua["log_date"] = date_string(ua["log_date"])
    ua["playtime_hours"] = ua["raw_ts_rows"] * HOURS_PER_TS_SEGMENT
    ua["status_200_playtime_hours"] = ua["status_200_ts_rows"] * HOURS_PER_TS_SEGMENT
    ua["ua_norm"] = ua["userAgent"].map(normalize_ua)
    ua["ua_hash"] = ua["ua_norm"].map(ua_hash)

    lookup, lookup_path = load_ua_lookup(device_dir)
    if not lookup.empty:
        ua = ua.merge(lookup, on="ua_hash", how="left")
    ua = enrich_ua_labels(ua)

    # Keep the legacy regex buckets as a stable platform fallback. The decoded
    # brand/model fields are exposed separately so unknown brand is not confused
    # with an undecoded UA.
    ua["ua_platform"] = classify_ua_series(ua["userAgent"], UA_PLATFORM_RULES, "Others")
    ua["ua_os"] = classify_ua_series(ua["userAgent"], UA_OS_RULES, "Others")
    ua.loc[ua["os_label"].ne("OS Not Exposed In UA"), "ua_os"] = ua.loc[ua["os_label"].ne("OS Not Exposed In UA"), "os_label"]

    metrics = ["rows", "raw_ts_rows", "status_200_ts_rows", "approx_unique_ips", "playtime_hours", "status_200_playtime_hours"]
    platform = safe_group_sum(ua, ["log_date", "source", "ua_platform"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    os = safe_group_sum(ua, ["log_date", "source", "ua_os"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    brand = safe_group_sum(ua, ["log_date", "source", "brand_label"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    model = safe_group_sum(ua, ["log_date", "source", "model_label", "brand_label"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    form_factor = safe_group_sum(ua, ["log_date", "source", "form_factor_label"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    decode_quality = safe_group_sum(ua, ["log_date", "source", "decode_quality"], metrics).sort_values(["log_date", "source", "playtime_hours"])
    device_detail = safe_group_sum(
        ua,
        [
            "log_date",
            "source",
            "device_identity",
            "brand_label",
            "model_label",
            "form_factor_label",
            "device_type_label",
            "os_label",
            "browser_label",
            "decode_quality",
        ],
        metrics,
    ).sort_values(["log_date", "source", "playtime_hours"])
    return {
        "platform": platform,
        "os": os,
        "brand": brand,
        "model": model,
        "form_factor": form_factor,
        "decode_quality": decode_quality,
        "device_detail": device_detail,
        "lookup_path": lookup_path,
    }


def build_latency(latency_dir: Path) -> dict[str, pd.DataFrame]:
    daily = read_parquet(latency_dir / "daily.parquet")
    hourly = read_parquet(latency_dir / "hourly.parquet")
    channel = read_parquet(latency_dir / "channel_daily.parquet")
    host = read_parquet(latency_dir / "host_daily.parquet")
    status = read_parquet(latency_dir / "status_daily.parquet")
    cache = read_parquet(latency_dir / "cache_daily.parquet")
    count_cols = [
        "rows",
        "status_200_rows",
        "non_200_rows",
        "status_5xx_rows",
        "approx_unique_ips",
        "cache_hit_rows",
        "ttfb_rows",
    ]
    value_cols = [
        "ttfb_p50_ms",
        "ttfb_p95_ms",
        "ttfb_p99_ms",
        "turnaround_p95_ms",
        "transfer_p95_ms",
        "throughput_p05",
        "avg_total_bytes",
    ]
    out = {}
    for name, frame in {
        "daily": daily,
        "hourly": hourly,
        "channel": channel,
        "host": host,
        "status": status,
        "cache": cache,
    }.items():
        frame = numeric(frame, count_cols)
        frame = numeric_nullable(frame, value_cols)
        if not frame.empty and "log_date" in frame.columns:
            frame["log_date"] = date_string(frame["log_date"])
        if not frame.empty and "hour_ist" in frame.columns:
            frame["hour_ist"] = pd.to_datetime(frame["hour_ist"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        if not frame.empty and "rows" in frame.columns:
            denom = frame["rows"].where(frame["rows"].ne(0))
            frame["cache_hit_pct"] = (frame.get("cache_hit_rows", 0) * 100 / denom).fillna(0)
            frame["error_5xx_pct"] = (frame.get("status_5xx_rows", 0) * 100 / denom).fillna(0)
            frame["non_200_pct"] = (frame.get("non_200_rows", 0) * 100 / denom).fillna(0)
        out[name] = frame
    return out


def build_concurrency(concurrency_dir: Path) -> dict[str, pd.DataFrame]:
    minute = read_parquet(concurrency_dir / "concurrency_minute.parquet")
    summary = read_parquet(concurrency_dir / "concurrency_summary.parquet")
    status = read_parquet(concurrency_dir / "concurrency_status_minute.parquet")
    if not minute.empty:
        minute = numeric(
            minute,
            [
                "raw_ts_rows",
                "status_200_ts_rows",
                "unique_viewers",
                "unique_ua_viewers",
                "segment_viewers_estimate",
                "status_200_segment_viewers_estimate",
            ],
        )
        minute["log_date"] = date_string(minute["log_date"])
        # minute_ist is already produced as an IST timestamp by the upstream
        # concurrency mart. Keep it timezone-naive but do not reinterpret it as
        # UTC here; just bucket the local wall-clock value.
        minute["minute_ist"] = pd.to_datetime(minute["minute_ist"], errors="coerce")
        minute = minute[minute["minute_ist"].notna()].copy()
        minute["bucket_5min"] = minute["minute_ist"].dt.floor("5min")
        minute5 = (
            minute.groupby(["log_date", "source", "bucket_5min", "platform_name", "channel_name"], dropna=False, as_index=False)
            .agg(
                avg_unique_viewers=("unique_viewers", "mean"),
                peak_unique_viewers=("unique_viewers", "max"),
                avg_unique_ua_viewers=("unique_ua_viewers", "mean"),
                raw_ts_rows=("raw_ts_rows", "sum"),
                status_200_ts_rows=("status_200_ts_rows", "sum"),
            )
            .sort_values(["log_date", "bucket_5min"])
        )
        minute5["bucket_5min"] = minute5["bucket_5min"].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        minute5 = pd.DataFrame()
    if not summary.empty:
        summary = numeric(
            summary,
            [
                "minute_count",
                "raw_ts_rows",
                "status_200_ts_rows",
            ],
        )
        summary = numeric_nullable(
            summary,
            [
                "avg_unique_viewers",
                "peak_unique_viewers",
                "p95_unique_viewers",
                "avg_unique_ua_viewers",
                "peak_unique_ua_viewers",
                "p95_unique_ua_viewers",
                "avg_segment_viewers_estimate",
                "peak_segment_viewers_estimate",
                "avg_status_200_segment_viewers_estimate",
                "peak_status_200_segment_viewers_estimate",
            ],
        )
        summary["log_date"] = date_string(summary["log_date"])
    if not status.empty:
        status = numeric(status, ["status_ts_rows", "status_segment_viewers_estimate"])
        status["log_date"] = date_string(status["log_date"])
    return {"minute5": minute5, "summary": summary, "status": status}


def build_device_decode(device_dir: Path) -> dict[str, pd.DataFrame | str]:
    summary_path = latest_file(device_dir, "device_decode_summary_*.parquet")
    ua_path = latest_file(device_dir, "ua_decode_enriched_*.parquet")
    top_ua_path = latest_file(device_dir, "top_user_agents_*.parquet")
    summary = read_parquet(summary_path) if summary_path else pd.DataFrame()
    ua = read_parquet(ua_path) if ua_path else pd.DataFrame()
    top_ua = read_parquet(top_ua_path) if top_ua_path else pd.DataFrame()
    summary = numeric(summary, ["rows", "ts_rows", "status_200_rows", "status_200_ts_rows", "watch_hours", "status_200_watch_hours", "approx_ips"])
    ua = numeric(ua, ["rows", "ts_rows", "status_200_rows", "status_200_ts_rows", "watch_hours", "status_200_watch_hours", "approx_ips"])
    top_ua = numeric(top_ua, ["rows", "ts_rows", "status_200_rows", "status_200_ts_rows", "watch_hours", "status_200_watch_hours", "approx_ips"])
    return {
        "summary": summary,
        "ua": ua,
        "top_ua": top_ua,
        "summary_path": str(summary_path or ""),
        "ua_path": str(ua_path or ""),
        "top_ua_path": str(top_ua_path or ""),
    }


def build_identity(identity_dir: Path) -> dict[str, pd.DataFrame]:
    metric_cols = [
        "total_devices",
        "total_sessions",
        "total_ipua_sessions",
        "new_devices",
        "returning_devices",
        "device_identity_rows",
        "session_identity_rows",
        "ipua_identity_rows",
        "device_distinct_ip_day_sum",
        "device_distinct_ipua_day_sum",
    ]
    tables = {
        "daily": read_parquet(identity_dir / "identity_daily.parquet"),
        "channel": read_parquet(identity_dir / "identity_channel_daily.parquet"),
        "platform": read_parquet(identity_dir / "identity_platform_daily.parquet"),
        "platform_channel": read_parquet(identity_dir / "identity_platform_channel_daily.parquet"),
    }
    for name, frame in list(tables.items()):
        if frame.empty:
            tables[name] = frame
            continue
        frame = numeric(frame, metric_cols)
        if "log_date" in frame.columns:
            frame["log_date"] = date_string(frame["log_date"])
        tables[name] = frame.sort_values([c for c in ["log_date", "source", "platform_name", "channel_name"] if c in frame.columns])
    return tables


def build_content(content_dir: Path) -> pd.DataFrame:
    content = read_parquet(content_dir / "content_daily.parquet")
    if content.empty:
        return content
    content = numeric(
        content,
        [
            "rows",
            "raw_ts_rows",
            "status_200_ts_rows",
            "m3u8_rows",
            "status_200_m3u8_rows",
            "non_200_rows",
            "raw_watch_hours",
            "status_200_watch_hours",
            "approx_unique_ips",
            "approx_sessions",
            "approx_devices",
        ],
    )
    if "log_date" in content.columns:
        content["log_date"] = date_string(content["log_date"])
    return content.sort_values(["log_date", "source", "m3u8_rows"], ascending=[True, True, False])


def top_table(df: pd.DataFrame, group_cols: list[str], metric: str, limit: int = 25) -> pd.DataFrame:
    if df.empty or metric not in df.columns:
        return pd.DataFrame()
    keys = [c for c in group_cols if c in df.columns]
    if not keys:
        return pd.DataFrame()
    grouped = safe_group_sum(df, keys, [metric, "approx_unique_ips", "raw_watch_hours", "status_200_watch_hours", "rows"])
    return grouped.sort_values(metric, ascending=False).head(limit)


def completed_date_window(primary: pd.DataFrame, fallback_frames: list[pd.DataFrame]) -> tuple[Any, Any]:
    latest_completed = datetime.now(IST_ZONE).date() - timedelta(days=1)

    def collect_dates(frame: pd.DataFrame) -> list[Any]:
        if frame.empty or "log_date" not in frame.columns:
            return []
        dates = pd.to_datetime(frame["log_date"], errors="coerce").dropna().dt.date
        return [d for d in dates.tolist() if d <= latest_completed]

    primary_dates = collect_dates(primary)
    if primary_dates:
        return min(primary_dates), max(primary_dates)

    all_dates: list[Any] = []
    for frame in fallback_frames:
        all_dates.extend(collect_dates(frame))
    if not all_dates:
        return latest_completed, latest_completed
    return min(all_dates), max(all_dates)


def weighted_group(
    df: pd.DataFrame,
    keys: list[str],
    sum_cols: list[str],
    weighted_cols: list[str],
    weight_col: str = "rows",
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=keys + sum_cols + weighted_cols)
    cols = [c for c in dict.fromkeys(keys + sum_cols + weighted_cols + [weight_col]) if c in df.columns]
    out = numeric(df[cols], [c for c in sum_cols + [weight_col] if c in cols])
    out = numeric_nullable(out, [c for c in weighted_cols if c in cols])
    valid_keys = [k for k in keys if k in out.columns]
    if not valid_keys:
        return pd.DataFrame()
    if weight_col not in out.columns:
        out[weight_col] = 0
    sum_cols_present = [c for c in sum_cols if c in out.columns]
    weighted_cols_present = [c for c in weighted_cols if c in out.columns]
    work = out.copy()
    agg_spec: dict[str, tuple[str, str]] = {col: (col, "sum") for col in sum_cols_present}
    agg_spec["_weight_rows"] = (weight_col, "sum")
    numerator_cols: list[str] = []
    for col in weighted_cols_present:
        numerator_col = f"__weighted_{col}"
        numerator_cols.append(numerator_col)
        work[numerator_col] = work[col] * work[weight_col]
        agg_spec[numerator_col] = (numerator_col, "sum")
    result = work.groupby(valid_keys, dropna=False, observed=True).agg(**agg_spec).reset_index()
    for col, numerator_col in zip(weighted_cols_present, numerator_cols):
        if col not in out.columns:
            continue
        result[col] = result[numerator_col] / result["_weight_rows"].where(result["_weight_rows"].ne(0))
    result = result.drop(columns=["_weight_rows"] + numerator_cols)
    return result


def validate_output_root(path: Path) -> Path:
    root = path.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Output root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Output root is not a directory: {root}")
    return root


def validate_output_target(path: Path, dry_run: bool) -> Path:
    target = path.expanduser().resolve()
    if dry_run:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    # Fail fast before expensive parquet reads if the final destination cannot
    # be written.
    with tempfile.NamedTemporaryFile(prefix=".write-test-", suffix=".tmp", dir=target.parent, delete=True):
        pass
    return target


def build_data(args: argparse.Namespace) -> dict[str, Any]:
    output_root = args.output_root.expanduser().resolve()
    watch_dir = output_root / "watch_hours" / "daily_tables"
    concurrency_dir = output_root / "watch_hours" / "concurrency"
    latency_dir = output_root / "latency" / "profile"
    device_dir = output_root / "device_decode"
    identity_dir = output_root / "identity"
    content_dir = output_root / "content"

    daily = build_daily(watch_dir)
    overview_daily = build_overview_daily(output_root)
    channel = build_channel(watch_dir)
    geo = build_geo(watch_dir)
    ua_playtime = build_ua_playtime(watch_dir, device_dir)
    latency = build_latency(latency_dir)
    concurrency = build_concurrency(concurrency_dir)
    devices = build_device_decode(device_dir)
    identity = build_identity(identity_dir)
    content = build_content(content_dir)
    source = args.source.lower()

    daily = filter_source(daily, source)
    overview_daily = filter_source(overview_daily, source)
    channel = filter_source(channel, source)
    geo = filter_source(geo, source)
    ua_playtime = filter_frame_dict(ua_playtime, source)
    latency = filter_frame_dict(latency, source)
    concurrency = filter_frame_dict(concurrency, source)
    identity = filter_frame_dict(identity, source)
    content = filter_source(content, source)
    for name in ("summary", "ua", "top_ua"):
        if isinstance(devices.get(name), pd.DataFrame):
            devices[name] = filter_source(devices[name], source)

    source_true_ranges = source_true_ranges_from_lake(daily)

    min_date, max_date = completed_date_window(
        daily,
        [channel, geo, latency["daily"], concurrency["summary"], identity["daily"], content],
    )

    daily = filter_date_window(daily, min_date, max_date)
    overview_daily = filter_date_window(overview_daily, min_date, max_date, "date")
    channel = filter_date_window(channel, min_date, max_date)
    geo = filter_date_window(geo, min_date, max_date)
    ua_playtime = filter_frame_dict_dates(ua_playtime, min_date, max_date)
    latency = filter_frame_dict_dates(latency, min_date, max_date)
    concurrency = filter_frame_dict_dates(concurrency, min_date, max_date)
    identity = filter_frame_dict_dates(identity, min_date, max_date)
    content = filter_date_window(content, min_date, max_date)
    for name, date_col in {"summary": "log_date", "ua": "first_date", "top_ua": "first_date"}.items():
        if isinstance(devices.get(name), pd.DataFrame):
            devices[name] = filter_date_window(devices[name], min_date, max_date, date_col)

    channel_top = top_table(channel, ["source", "channel_name"], "raw_watch_hours", 60)
    geo_top = top_table(geo, ["source", "country", "state", "city"], "raw_watch_hours", 80)
    platform_top = top_table(concurrency["summary"], ["source", "platform_name"], "raw_ts_rows", 40)
    content_top = top_table(content, ["source", "channel_name", "platform_name", "content_title", "category_name"], "m3u8_rows", 120)
    status_top = top_table(latency["status"], ["source", "extension", "status_code"], "rows", 80)
    host_latency = top_table(latency["host"], ["source", "platform_name", "reqHost"], "rows", 80)
    if not host_latency.empty and not latency["host"].empty:
        # Collapse daily host rows to one row per host/platform. The p95 value
        # is a row-weighted average of daily p95s, which is not a mathematically
        # exact p95 over the whole range but is safer than showing one arbitrary
        # high day as if it were the selected-range host value.
        host_latency = weighted_group(
            latency["host"],
            ["source", "platform_name", "reqHost"],
            ["rows", "cache_hit_rows", "status_5xx_rows", "non_200_rows"],
            ["ttfb_p95_ms"],
        )
        if not host_latency.empty:
            denom = host_latency["rows"].where(host_latency["rows"].ne(0))
            host_latency["cache_hit_pct"] = (host_latency.get("cache_hit_rows", 0) * 100 / denom).fillna(0)
            host_latency["error_5xx_pct"] = (host_latency.get("status_5xx_rows", 0) * 100 / denom).fillna(0)
            host_latency["non_200_pct"] = (host_latency.get("non_200_rows", 0) * 100 / denom).fillna(0)
            host_latency = host_latency.sort_values(["ttfb_p95_ms", "rows"], ascending=[False, False]).head(80)

    device_summary = devices["summary"]
    device_by_name = pd.DataFrame()
    if isinstance(device_summary, pd.DataFrame) and not device_summary.empty:
        key_cols = [c for c in ["source", "decoded_device_name", "platform", "decode_status"] if c in device_summary.columns]
        device_by_name = safe_group_sum(device_summary, key_cols, ["rows", "ts_rows", "status_200_rows", "approx_ips"]).sort_values(
            "rows", ascending=False
        ).head(80)

    availability = [
        {
            "area": "Watch and channel trends",
            "status": "Available",
            "basis": "watch_hours/daily_tables",
            "note": "Request-hour metrics are production-ready. Dedup playback mart is WIP.",
        },
        {
            "area": "FAST concurrency",
            "status": "Available",
            "basis": "watch_hours/concurrency",
            "note": "Minute and 5-minute viewer estimates are available for FAST. STREAM concurrency mart is not available yet.",
        },
        {
            "area": "Latency and CDN reliability",
            "status": "Available",
            "basis": "latency/profile",
            "note": "TTFB, turnaround, transfer, throughput, cache, and status metrics exist.",
        },
        {
            "area": "Device, OS, UA decode",
            "status": "Available",
            "basis": "device_decode/ua_decode_lookup_both_all.parquet joined to user_agents_daily.parquet",
            "note": "Usage-weighted brand, model, form factor, OS, browser, and decode-quality buckets are available. Brand Not Exposed In UA is shown separately from true unknowns.",
        },
        {
            "area": "App device/session IDs",
            "status": "Available for STREAM",
            "basis": "identity mart from queryStr session_id/device_id",
            "note": "STREAM has queryStr device/session evidence. FAST currently does not expose these fields in the CDN mart.",
        },
        {
            "area": "Content-title analytics",
            "status": "Partially available",
            "basis": "content/content_daily.parquet",
            "note": "STREAM has content_title manifest-view analytics. FAST currently has no content_title parameter. True content watch-hours need session attribution beyond CDN manifest rows.",
        },
        {
            "area": "New vs returning users",
            "status": "Available for STREAM",
            "basis": "identity mart first-seen device_id logic",
            "note": "STREAM new/returning is scoped to the selected identity dimension. FAST remains Not Available until device_id appears.",
        },
    ]

    return {
        "meta": {
            "title": args.title,
            "source": source.upper(),
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "min_date": min_date.isoformat(),
            "max_date": max_date.isoformat(),
            "source_true_ranges": source_true_ranges,
            "output_root": str(output_root),
            "notes": [
                "Request Hours = all .ts request rows * 6 seconds.",
                "Playback Hours = dedup unique segment-path estimate. Shared mart is WIP.",
                "This dashboard supports FAST and STREAM source filters.",
                "WIP sections are intentionally shown so stakeholders know which telemetry is not production-ready.",
            ],
        },
        "ranges": {
            "watch_daily": range_info(daily),
            "watch_by_source": source_ranges(daily),
            "channel": range_info(channel),
            "geo": range_info(geo),
            "ua_platform": range_info(ua_playtime["platform"]),
            "ua_os": range_info(ua_playtime["os"]),
            "ua_brand": range_info(ua_playtime["brand"]),
            "ua_model": range_info(ua_playtime["model"]),
            "ua_form_factor": range_info(ua_playtime["form_factor"]),
            "ua_decode_quality": range_info(ua_playtime["decode_quality"]),
            "content": range_info(content),
            "concurrency": range_info(concurrency["summary"]),
            "latency": range_info(latency["daily"]),
            "identity": range_info(identity["daily"]),
            "overview_daily": range_info(overview_daily, "date"),
        },
        "availability": availability,
        "data": {
            "daily": records(daily, 5000),
            "overview_daily": records(overview_daily, 5000),
            "channel_daily": records(channel, 6000),
            "geo_daily": records(geo, 25000),
            "channel_top": records(channel_top, 80),
            "geo_top": records(geo_top, 100),
            "ua_platform_daily": records(ua_playtime["platform"], 5000),
            "ua_os_daily": records(ua_playtime["os"], 5000),
            "ua_brand_daily": records(ua_playtime["brand"], 5000),
            "ua_model_daily": records(ua_playtime["model"], 5000),
            "ua_form_factor_daily": records(ua_playtime["form_factor"], 5000),
            "ua_decode_quality_daily": records(ua_playtime["decode_quality"], 5000),
            "ua_device_detail_daily": records(ua_playtime["device_detail"]),
            "concurrency_5min": records(concurrency["minute5"]),
            "concurrency_summary": records(concurrency["summary"], 2000),
            "platform_top": records(platform_top, 80),
            "content_daily": records(content, 50000),
            "content_top": records(content_top, 120),
            "latency_daily": records(latency["daily"], 3000),
            "latency_hourly": records(latency["hourly"], 12000, tail=True),
            "latency_channel": records(latency["channel"], 6000),
            "latency_host_daily": records(latency["host"], 5000),
            "latency_host_top": records(host_latency, 100),
            "latency_status": records(status_top, 120),
            "device_summary": records(device_by_name, 100),
            "identity_daily": records(identity["daily"], 5000),
            "identity_channel_daily": records(identity["channel"], 12000),
            "identity_platform_daily": records(identity["platform"], 8000),
            "identity_platform_channel_daily": records(identity["platform_channel"], 16000),
        },
        "source_files": {
            "watch_daily_tables": str(watch_dir),
            "concurrency": str(concurrency_dir),
            "latency": str(latency_dir),
            "device_decode": str(device_dir),
            "identity": str(identity_dir),
            "content": str(content_dir),
            "device_summary": str(devices["summary_path"]),
            "ua_decode": str(devices["ua_path"]),
            "ua_lookup": str(ua_playtime.get("lookup_path", "")),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the Veto Audience Operations dashboard.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--title", default="Veto Audience Operations")
    parser.add_argument("--source", default="all", choices=["fast", "stream", "all"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.out = validate_output_target(args.out, args.dry_run)
    args.output_root = validate_output_root(args.output_root)

    data = build_data(args)
    chartjs_cache = args.output_root / "cache" / "chartjs" / "chart.umd.min.js"
    chartjs = load_chartjs(chartjs_cache, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        CHARTJS_TAG=chartjs_script(chartjs),
        DATA_BLOB=json_blob(data),
    )

    if args.dry_run:
        print(f"[dry-run] Audience ops HTML chars: {len(html):,}")
        print(f"Would write: {args.out}")
        return

    args.out.write_text(html, encoding="utf-8")
    print(f"Audience operations dashboard written: {args.out.resolve()}")
    print(f"Size: {args.out.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
