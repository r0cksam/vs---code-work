#!/usr/bin/env python3
import csv
from collections import defaultdict
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote_plus

import openpyxl
from prometheus_client import Gauge, start_http_server


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("kpi-exporter")


PORT = int(os.environ.get("KPI_EXPORTER_PORT", "9108"))
REFRESH_SEC = int(os.environ.get("KPI_EXPORTER_REFRESH_SEC", "60"))

OVERVIEW_SNAPSHOT_JSON = Path(
    os.environ.get("OVERVIEW_SNAPSHOT_JSON", "/data/overview/overview_snapshot.json")
)
OVERVIEW_XLSX_PATH = Path(os.environ.get("OVERVIEW_XLSX_PATH", "/data/overview/overview_report.xlsx"))
DEVICE_DAILY_CSV = Path(os.environ.get("DEVICE_DAILY_CSV", "/data/overview/device_daily.csv"))
DEVICE_SNAPSHOT_CSV = Path(os.environ.get("DEVICE_SNAPSHOT_CSV", "/data/overview/device_snapshot.csv"))
WATCH_SNAPSHOT_JSON = Path(
    os.environ.get("WATCH_SNAPSHOT_JSON", "/data/watch/watch_snapshot.json")
)
WATCH_DAILY_CSV = Path(os.environ.get("WATCH_DAILY_CSV", "/data/profile/daily_volume.csv"))
WATCH_CHANNEL_SUMMARY_CSV = Path(
    os.environ.get("WATCH_CHANNEL_SUMMARY_CSV", "/data/profile/channel_summary.csv")
)
WATCH_CHANNEL_DAILY_CSV = Path(
    os.environ.get("WATCH_CHANNEL_DAILY_CSV", "/data/profile/channel_daily.csv")
)
WATCH_GEO_CSV = Path(os.environ.get("WATCH_GEO_CSV", "/data/profile/geo_top.csv"))
WATCH_DEVICE_TYPE_CSV = Path(
    os.environ.get("WATCH_DEVICE_TYPE_CSV", "/data/profile/device_type_by_channel.csv")
)
WATCH_UA_CSV = Path(os.environ.get("WATCH_UA_CSV", "/data/profile/ua_top.csv"))
WATCH_CONTENT_CSV = Path(
    os.environ.get("WATCH_CONTENT_CSV", "/data/profile/querystr_channel_profile.csv")
)
PROFILE_TOP_N = max(5, int(os.environ.get("PROFILE_TOP_N", "20")))

DATA_START_ROW = 13
COL = {
    "DATE": 2,
    "BYTES": 3,
    "ROWS": 4,
    "DIST_IP": 6,
    "DIST_DEV": 8,
    "DIST_SESS": 9,
}

CHUNK_HOURS = 6.0 / 3600.0
JSON_CACHE: dict[Path, tuple[int, dict]] = {}


def sn(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_date_any(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    s_upper = s.upper()
    if "TOTAL" in s_upper:
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def get_cell(row: tuple, col_num: int) -> object:
    idx = col_num - 1
    return row[idx] if idx < len(row) else None


def date_to_epoch_start(d: date) -> float:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()


def day_key(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def clean_label(value: object, fallback: str = "Unknown") -> str:
    decoded = unquote_plus(str(value or "").strip()).strip()
    return decoded[:160] if decoded else fallback


def display_name(value: object, fallback: str = "Unknown") -> str:
    label = clean_label(value, fallback)
    return label.replace("_", " ").strip().title()


def platform_name(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    label = display_name(raw)
    aliases = {
        "Android Tv": "Android TV",
        "Fire Tv": "Fire TV",
        "Android Mobile Ott": "Android Mobile OTT",
        "Appletv": "Apple TV",
        "Lg Tv": "LG TV",
        "Samsung Tv": "Samsung TV",
        "Cloud Tv": "Cloud TV",
        "Ios": "iOS",
    }
    return aliases.get(label, label)


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def read_json_snapshot(path: Path) -> dict:
    if not path.exists():
        return {}
    mtime_ns = path.stat().st_mtime_ns
    cached = JSON_CACHE.get(path)
    if cached and cached[0] == mtime_ns:
        return cached[1]
    value = json.loads(path.read_text(encoding="utf-8"))
    JSON_CACHE[path] = (mtime_ns, value)
    return value


def load_overview_rows() -> list[dict]:
    snapshot_rows = read_json_snapshot(OVERVIEW_SNAPSHOT_JSON).get("rows", [])
    if snapshot_rows:
        out = []
        for row in snapshot_rows:
            d = parse_date_any(row.get("date"))
            if d is None:
                continue
            out.append(
                {
                    "date": d,
                    "bytes_gib": sn(row.get("bytes")),
                    "rows": sn(row.get("rows")),
                    "dist_ip": sn(row.get("dist_ip")),
                    "dist_dev": sn(row.get("dist_dev")),
                    "dist_sess": sn(row.get("dist_sess")),
                }
            )
        out.sort(key=lambda x: x["date"])
        return out

    if not OVERVIEW_XLSX_PATH.exists():
        return []
    wb = openpyxl.load_workbook(OVERVIEW_XLSX_PATH, read_only=True, data_only=True)
    ws = wb.active
    out: list[dict] = []
    for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
        d = parse_date_any(get_cell(row, COL["DATE"]))
        if d is None:
            continue
        out.append(
            {
                "date": d,
                "bytes_gib": sn(get_cell(row, COL["BYTES"])),
                "rows": sn(get_cell(row, COL["ROWS"])),
                "dist_ip": sn(get_cell(row, COL["DIST_IP"])),
                "dist_dev": sn(get_cell(row, COL["DIST_DEV"])),
                "dist_sess": sn(get_cell(row, COL["DIST_SESS"])),
            }
        )
    wb.close()
    out.sort(key=lambda x: x["date"])
    return out


def used_slice(rows: list[dict]) -> list[dict]:
    if len(rows) > 2:
        return rows[1:-1]
    return rows


def parse_device_first_seen() -> dict[str, date]:
    out: dict[str, date] = {}
    for r in read_csv_rows(DEVICE_SNAPSHOT_CSV):
        device_id = str(r.get("device_id", "")).strip()
        d = parse_date_any(r.get("first_seen_utc_date"))
        if device_id and d:
            out[device_id] = d
    return out


def load_device_daily_sets() -> dict[date, set[str]]:
    by_day: dict[date, set[str]] = defaultdict(set)
    for r in read_csv_rows(DEVICE_DAILY_CSV):
        device_id = str(r.get("device_id", "")).strip()
        d = parse_date_any(r.get("utc_date"))
        if not device_id or not d:
            continue
        by_day[d].add(device_id)
    return by_day


def load_watch_daily_rows() -> list[dict]:
    rows: list[dict] = []
    source_rows = read_json_snapshot(WATCH_SNAPSHOT_JSON).get("daily") or read_csv_rows(
        WATCH_DAILY_CSV
    )
    for r in source_rows:
        d = parse_date_any(r.get("log_date"))
        if d is None:
            continue
        raw_watch = sn(r.get("raw_watch_hours"))
        if raw_watch <= 0:
            raw_watch = sn(r.get("ts_rows")) * CHUNK_HOURS
        status_200_watch = sn(r.get("status_200_watch_hours"))
        if status_200_watch <= 0:
            status_200_watch = sn(r.get("status_200_ts_rows")) * CHUNK_HOURS
            if status_200_watch <= 0:
                status_200_watch = raw_watch
        rows.append(
            {
                "date": d,
                "rows": sn(r.get("rows")),
                "status_200_rows": sn(r.get("status_200_rows")),
                "non_200_rows": sn(r.get("non_200_rows")),
                "ts_rows": sn(r.get("ts_rows")),
                "m3u8_rows": sn(r.get("m3u8_rows")),
                "approx_unique_ips": sn(r.get("approx_unique_ips")),
                "raw_watch_hours": raw_watch,
                "status_200_watch_hours": status_200_watch,
            }
        )
    rows.sort(key=lambda x: x["date"])
    return rows


def load_watch_channel_summary() -> list[dict]:
    rows: list[dict] = []
    source_rows = read_json_snapshot(WATCH_SNAPSHOT_JSON).get("channels") or read_csv_rows(
        WATCH_CHANNEL_SUMMARY_CSV
    )
    for r in source_rows:
        rows.append(
            {
                "channel_name": clean_label(r.get("channel_name")),
                "segments": sn(r.get("raw_ts_chunks")),
                "raw_watch_hours": sn(r.get("raw_watch_hours")),
                "approx_unique_ips": sn(r.get("approx_unique_ips")),
            }
        )
    rows.sort(key=lambda x: x["raw_watch_hours"], reverse=True)
    return rows


def load_watch_channel_daily() -> list[dict]:
    rows: list[dict] = []
    source_rows = read_json_snapshot(WATCH_SNAPSHOT_JSON).get(
        "channel_daily_all"
    ) or read_csv_rows(WATCH_CHANNEL_DAILY_CSV)
    for r in source_rows:
        d = parse_date_any(r.get("log_date"))
        if d is None:
            continue
        rows.append(
            {
                "date": d,
                "channel_name": clean_label(r.get("channel_name")),
                "raw_watch_hours": sn(r.get("raw_watch_hours")),
                "approx_unique_ips": sn(r.get("approx_unique_ips")),
            }
        )
    return rows


def classify_os(raw_ua: object) -> str:
    ua = clean_label(raw_ua).lower()
    if "aft" in ua or "fire tv" in ua:
        return "Fire OS"
    if "android" in ua:
        return "Android"
    if any(token in ua for token in ("iphone", "ipad", "ipod")):
        return "iOS"
    if "windows" in ua:
        return "Windows"
    if "mac os" in ua or "macintosh" in ua:
        return "macOS"
    if "linux" in ua:
        return "Linux"
    return "Other"


def content_title(row: dict) -> str:
    query = str(row.get("sample_queryStr", "")).strip()
    if query:
        title = parse_qs(query, keep_blank_values=True).get("content_title", [""])[0]
        if title:
            return clean_label(title)
    return clean_label(row.get("mapped_channel") or row.get("pure_channel"), "Unknown Content")


def decoded_platform(row: dict) -> str:
    os_name = clean_label(row.get("os_name"), "").lower()
    os_family = clean_label(row.get("os_family"), "").lower()
    device_type = clean_label(row.get("device_type"), "").lower()
    model = clean_label(row.get("model"), "").lower()
    combined = " ".join((os_name, os_family, model))
    if "fire" in combined:
        return "Fire TV"
    if "tizen" in combined:
        return "Samsung TV"
    if "webos" in combined:
        return "LG TV"
    if "android" in combined and device_type == "tv":
        return "Android TV"
    if "android" in combined:
        return "Android"
    if any(token in combined for token in ("ios", "iphone", "ipad")):
        return "iOS"
    if "windows" in combined:
        return "Windows"
    if device_type:
        return display_name(device_type)
    return "Other"


def aggregate_profile_rows() -> dict[str, list[dict]]:
    geo: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"ts_rows": 0.0, "unique_ips": 0.0}
    )
    snapshot = read_json_snapshot(WATCH_SNAPSHOT_JSON)
    snapshot_geo = snapshot.get("geo") or {}
    if snapshot_geo:
        for r in snapshot_geo.get("india_regions", []):
            key = ("IN", clean_label(r.get("state_region"), "Unknown"))
            geo[key]["ts_rows"] += sn(r.get("watch_hours")) / CHUNK_HOURS
            geo[key]["unique_ips"] += sn(r.get("approx_unique_ips"))
        for r in snapshot_geo.get("other_regions", []):
            key = (
                clean_label(r.get("country_label"), "Other"),
                clean_label(r.get("state_region"), "Unknown"),
            )
            geo[key]["ts_rows"] += sn(r.get("watch_hours")) / CHUNK_HOURS
            geo[key]["unique_ips"] += sn(r.get("approx_unique_ips"))
    else:
        for r in read_csv_rows(WATCH_GEO_CSV):
            key = (
                clean_label(r.get("country"), "Unknown"),
                clean_label(r.get("state"), "Unknown"),
            )
            geo[key]["ts_rows"] += sn(r.get("ts_rows"))
            geo[key]["unique_ips"] += sn(r.get("approx_unique_ips"))

    device_types: dict[str, dict[str, float]] = defaultdict(
        lambda: {"ts_rows": 0.0, "unique_ips": 0.0}
    )
    device_source = snapshot.get("device") or read_csv_rows(WATCH_DEVICE_TYPE_CSV)
    for r in device_source:
        key = display_name(r.get("device_type"))
        ts_rows = sn(r.get("ts_rows"))
        if ts_rows <= 0:
            ts_rows = sn(r.get("watch_hours")) / CHUNK_HOURS
        device_types[key]["ts_rows"] += ts_rows
        device_types[key]["unique_ips"] += sn(r.get("approx_unique_ips"))

    operating_systems: dict[str, dict[str, float]] = defaultdict(
        lambda: {"rows": 0.0, "unique_ips": 0.0}
    )
    ua_source = snapshot.get("ua") or read_csv_rows(WATCH_UA_CSV)
    for r in ua_source:
        key = classify_os(r.get("UA"))
        operating_systems[key]["rows"] += sn(r.get("rows"))
        operating_systems[key]["unique_ips"] += sn(r.get("approx_unique_ips"))

    platforms: dict[str, dict[str, float]] = defaultdict(
        lambda: {"requests": 0.0, "sessions": 0.0, "viewers": 0.0}
    )
    content: dict[str, dict[str, float]] = defaultdict(
        lambda: {"requests": 0.0, "sessions": 0.0, "devices": 0.0, "viewers": 0.0}
    )
    decoded_rows = snapshot.get("device_decode_ua_cache") or []
    if decoded_rows:
        for r in decoded_rows:
            platform = decoded_platform(r)
            platforms[platform]["requests"] += sn(r.get("rows"))
            platforms[platform]["sessions"] += sn(r.get("approx_ips"))
            platforms[platform]["viewers"] += sn(r.get("approx_ips"))
    for r in read_csv_rows(WATCH_CONTENT_CSV) if not decoded_rows else []:
        platform = platform_name(r.get("platform"))
        if platform:
            platforms[platform]["requests"] += sn(r.get("requests"))
            platforms[platform]["sessions"] += sn(r.get("sessions"))
            platforms[platform]["viewers"] += sn(r.get("unique_viewers"))

    mapping_rows = snapshot.get("mapping_quality") or []
    if mapping_rows:
        for r in mapping_rows:
            title = clean_label(r.get("channel_name"), "Unknown Content")
            content[title]["requests"] += sn(r.get("raw_ts_chunks"))
            content[title]["viewers"] += sn(r.get("approx_unique_ips"))
    else:
        for r in read_csv_rows(WATCH_CONTENT_CSV):
            title = content_title(r)
            content[title]["requests"] += sn(r.get("requests"))
            content[title]["sessions"] += sn(r.get("sessions"))
            content[title]["devices"] += sn(r.get("devices"))
            content[title]["viewers"] += sn(r.get("unique_viewers"))

    geo_rows = [
        {"country": key[0], "state": key[1], **values}
        for key, values in geo.items()
    ]
    geo_rows.sort(key=lambda row: row["ts_rows"], reverse=True)

    device_rows = [{"device_type": key, **values} for key, values in device_types.items()]
    device_rows.sort(key=lambda row: row["ts_rows"], reverse=True)

    os_rows = [{"os": key, **values} for key, values in operating_systems.items()]
    os_rows.sort(key=lambda row: row["rows"], reverse=True)

    platform_rows = [{"platform": key, **values} for key, values in platforms.items()]
    platform_rows.sort(key=lambda row: row["requests"], reverse=True)

    content_rows = [{"content_title": key, **values} for key, values in content.items()]
    content_rows.sort(key=lambda row: row["requests"], reverse=True)

    return {
        "geo": geo_rows[:PROFILE_TOP_N],
        "device_types": device_rows[:PROFILE_TOP_N],
        "operating_systems": os_rows[:PROFILE_TOP_N],
        "platforms": platform_rows[:PROFILE_TOP_N],
        "content": content_rows[:PROFILE_TOP_N],
    }


KPI_EXPORTER_REFRESH_OK = Gauge(
    "veto_kpi_exporter_refresh_ok",
    "1 if last KPI refresh succeeded, else 0",
)
KPI_EXPORTER_LAST_SUCCESS_TS = Gauge(
    "veto_kpi_exporter_last_success_timestamp_seconds",
    "Unix timestamp of last successful KPI refresh",
)
KPI_EXPORTER_LAST_ERROR_TS = Gauge(
    "veto_kpi_exporter_last_error_timestamp_seconds",
    "Unix timestamp of last failed KPI refresh",
)
KPI_SOURCE_MTIME_TS = Gauge(
    "veto_kpi_source_last_modified_timestamp_seconds",
    "Unix mtime for KPI source files",
    ["source"],
)

OV_TRUE_START_TS = Gauge(
    "veto_overview_true_range_start_timestamp_seconds",
    "Earliest date available in overview source",
)
OV_TRUE_END_TS = Gauge(
    "veto_overview_true_range_end_timestamp_seconds",
    "Latest date available in overview source",
)
OV_USED_START_TS = Gauge(
    "veto_overview_used_range_start_timestamp_seconds",
    "Earliest date used after trimming edge dates",
)
OV_USED_END_TS = Gauge(
    "veto_overview_used_range_end_timestamp_seconds",
    "Latest date used after trimming edge dates",
)
OV_DAYS_USED = Gauge("veto_overview_days_used", "Overview day count in used range")
OV_TOTAL_ROWS = Gauge("veto_overview_total_rows", "Total rows in used overview range")
OV_TOTAL_BYTES_GIB = Gauge("veto_overview_total_bytes_gib", "Total GiB in used overview range")
OV_AVG_ROWS_PER_DAY = Gauge("veto_overview_avg_rows_per_day", "Average rows per day in used range")
OV_AVG_BYTES_GIB_PER_DAY = Gauge(
    "veto_overview_avg_bytes_gib_per_day",
    "Average GiB per day in used range",
)
OV_AVG_DISTINCT_IPS_PER_DAY = Gauge(
    "veto_overview_avg_distinct_ips_per_day",
    "Average distinct IP count per day in used range",
)
OV_AVG_DISTINCT_DEVICES_PER_DAY = Gauge(
    "veto_overview_avg_distinct_devices_per_day",
    "Average distinct device count per day in used range",
)
OV_AVG_SESSIONS_PER_DAY = Gauge(
    "veto_overview_avg_sessions_per_day",
    "Average distinct session count per day in used range",
)
OV_ACTIVE_DEVICES = Gauge("veto_overview_active_devices_total", "Active devices in used overview range")
OV_RETURNING_DEVICES = Gauge(
    "veto_overview_returning_devices_total",
    "Returning devices active in used overview range",
)
OV_NEW_DEVICES = Gauge("veto_overview_new_devices_total", "New devices active in used overview range")
OV_DAY_START_TS = Gauge(
    "veto_overview_day_start_timestamp_seconds",
    "Start timestamp for each used overview day",
    ["day"],
)
OV_DAY_ROWS = Gauge(
    "veto_overview_day_rows",
    "Overview total rows by day",
    ["day"],
)
OV_DAY_BYTES_GIB = Gauge(
    "veto_overview_day_bytes_gib",
    "Overview GiB by day",
    ["day"],
)
OV_DAY_DISTINCT_IPS = Gauge(
    "veto_overview_day_distinct_ips",
    "Overview distinct IP count by day",
    ["day"],
)
OV_DAY_DISTINCT_DEVICES = Gauge(
    "veto_overview_day_distinct_devices",
    "Overview distinct device count by day",
    ["day"],
)
OV_DAY_SESSIONS = Gauge(
    "veto_overview_day_sessions",
    "Overview distinct session count by day",
    ["day"],
)
OV_DAY_ACTIVE_DEVICES = Gauge(
    "veto_overview_day_active_devices",
    "Active devices by day",
    ["day"],
)
OV_DAY_NEW_DEVICES = Gauge(
    "veto_overview_day_new_devices",
    "New devices by day",
    ["day"],
)
OV_DAY_RETURNING_DEVICES = Gauge(
    "veto_overview_day_returning_devices",
    "Returning devices by day",
    ["day"],
)
OV_DEVICE_DAY_START_TS = Gauge(
    "veto_overview_device_day_start_timestamp_seconds",
    "Start timestamp for device first/last-seen day buckets",
    ["day"],
)
OV_DEVICE_FIRST_SEEN_COUNT = Gauge(
    "veto_overview_device_first_seen_count",
    "Count of devices by first-seen day",
    ["day"],
)
OV_DEVICE_LAST_SEEN_COUNT = Gauge(
    "veto_overview_device_last_seen_count",
    "Count of devices by last-seen day",
    ["day"],
)

WH_TRUE_START_TS = Gauge(
    "veto_watch_true_range_start_timestamp_seconds",
    "Earliest date available in watch-hours source",
)
WH_TRUE_END_TS = Gauge(
    "veto_watch_true_range_end_timestamp_seconds",
    "Latest date available in watch-hours source",
)
WH_USED_START_TS = Gauge(
    "veto_watch_used_range_start_timestamp_seconds",
    "Earliest watch-hours date used after trimming edge dates",
)
WH_USED_END_TS = Gauge(
    "veto_watch_used_range_end_timestamp_seconds",
    "Latest watch-hours date used after trimming edge dates",
)
WH_DAYS_USED = Gauge("veto_watch_days_used", "Watch-hours day count in used range")
WH_RAW_WATCH_HOURS_TOTAL = Gauge(
    "veto_watch_raw_watch_hours_total",
    "Raw watch hours in used range",
)
WH_STATUS_200_WATCH_HOURS_TOTAL = Gauge(
    "veto_watch_status_200_watch_hours_total",
    "Status 200 watch hours in used range",
)
WH_ACTIVE_CHANNELS = Gauge("veto_watch_active_channels_total", "Active channels in watch-hours summary")
WH_TOP_CHANNEL_RAW_HOURS = Gauge(
    "veto_watch_top_channel_raw_watch_hours",
    "Top channel watch hours (label carries channel name)",
    ["channel_name"],
)
WH_AVG_RAW_WATCH_HOURS_PER_DAY = Gauge(
    "veto_watch_avg_raw_watch_hours_per_day",
    "Average raw watch hours per day in used range",
)
WH_AVG_ROWS_PER_DAY = Gauge("veto_watch_avg_rows_per_day", "Average rows per day in used range")
WH_TOTAL_ROWS = Gauge("veto_watch_total_rows", "Total rows in used watch range")
WH_STATUS_200_ROWS = Gauge("veto_watch_status_200_rows", "Status 200 rows in used watch range")
WH_NON_200_ROWS = Gauge("veto_watch_non_200_rows", "Non-200 rows in used watch range")
WH_NON_200_PCT = Gauge("veto_watch_non_200_pct", "Non-200 row percentage in used watch range")
WH_TS_ROWS = Gauge("veto_watch_ts_rows", "TS rows in used watch range")
WH_M3U8_ROWS = Gauge("veto_watch_m3u8_rows", "M3U8 rows in used watch range")
WH_APPROX_UNIQUE_IPS = Gauge(
    "veto_watch_approx_unique_ips",
    "Approx unique IPs (max daily) in used watch range",
)
WH_TOP_CHANNEL_INFO = Gauge(
    "veto_watch_top_channel_info",
    "Top channel marker (value=1, label carries channel_name)",
    ["channel_name"],
)
WH_CHANNEL_RAW_HOURS = Gauge(
    "veto_watch_channel_raw_watch_hours",
    "Raw watch hours by channel",
    ["channel_name"],
)
WH_CHANNEL_SEGMENTS = Gauge(
    "veto_watch_channel_segments",
    "Raw TS segment count by channel",
    ["channel_name"],
)
WH_CHANNEL_UNIQUE_IPS = Gauge(
    "veto_watch_channel_unique_ips",
    "Approx unique IP count by channel",
    ["channel_name"],
)
WH_CHANNEL_DAY_RAW_HOURS = Gauge(
    "veto_watch_channel_day_raw_watch_hours",
    "Raw watch hours by day and channel",
    ["day", "channel_name"],
)
WH_CHANNEL_DAY_UNIQUE_IPS = Gauge(
    "veto_watch_channel_day_unique_ips",
    "Approx unique IP count by day and channel",
    ["day", "channel_name"],
)
WH_DAY_START_TS = Gauge(
    "veto_watch_day_start_timestamp_seconds",
    "Start timestamp for each used watch day",
    ["day"],
)
WH_DAY_ROWS = Gauge(
    "veto_watch_day_rows",
    "Watch total rows by day",
    ["day"],
)
WH_DAY_RAW_WATCH_HOURS = Gauge(
    "veto_watch_day_raw_watch_hours",
    "Watch raw hours by day",
    ["day"],
)
WH_DAY_STATUS_200_WATCH_HOURS = Gauge(
    "veto_watch_day_status_200_watch_hours",
    "Watch status-200 hours by day",
    ["day"],
)
WH_DAY_STATUS_200_ROWS = Gauge(
    "veto_watch_day_status_200_rows",
    "Watch status-200 rows by day",
    ["day"],
)
WH_DAY_NON_200_ROWS = Gauge(
    "veto_watch_day_non_200_rows",
    "Watch non-200 rows by day",
    ["day"],
)
WH_DAY_TS_ROWS = Gauge(
    "veto_watch_day_ts_rows",
    "Watch TS rows by day",
    ["day"],
)
PROFILE_GEO_TS_ROWS = Gauge(
    "veto_profile_geo_ts_rows",
    "Profiled TS rows by country and state",
    ["country", "state"],
)
PROFILE_GEO_UNIQUE_IPS = Gauge(
    "veto_profile_geo_unique_ips",
    "Approx unique IP signals by country and state",
    ["country", "state"],
)
PROFILE_DEVICE_TYPE_TS_ROWS = Gauge(
    "veto_profile_device_type_ts_rows",
    "Profiled TS rows by device type",
    ["device_type"],
)
PROFILE_DEVICE_TYPE_UNIQUE_IPS = Gauge(
    "veto_profile_device_type_unique_ips",
    "Approx unique IP signals by device type",
    ["device_type"],
)
PROFILE_OS_ROWS = Gauge(
    "veto_profile_os_rows",
    "Profiled request rows by inferred operating system",
    ["os"],
)
PROFILE_OS_UNIQUE_IPS = Gauge(
    "veto_profile_os_unique_ips",
    "Approx unique IP signals by inferred operating system",
    ["os"],
)
PROFILE_PLATFORM_REQUESTS = Gauge(
    "veto_profile_platform_requests",
    "Profiled requests by application platform",
    ["platform"],
)
PROFILE_PLATFORM_SESSIONS = Gauge(
    "veto_profile_platform_sessions",
    "Profiled sessions by application platform",
    ["platform"],
)
PROFILE_PLATFORM_VIEWERS = Gauge(
    "veto_profile_platform_viewers",
    "Profiled unique viewer signals by application platform",
    ["platform"],
)
PROFILE_CONTENT_INFO = Gauge(
    "veto_profile_content_info",
    "Popular content table; numeric values are carried as labels",
    ["content_title", "requests", "sessions", "devices", "viewers"],
)


def set_source_mtimes() -> None:
    sources = {
        "overview_snapshot_json": OVERVIEW_SNAPSHOT_JSON,
        "overview_xlsx": OVERVIEW_XLSX_PATH,
        "device_daily_csv": DEVICE_DAILY_CSV,
        "device_snapshot_csv": DEVICE_SNAPSHOT_CSV,
        "watch_snapshot_json": WATCH_SNAPSHOT_JSON,
        "watch_daily_csv": WATCH_DAILY_CSV,
        "watch_channel_summary_csv": WATCH_CHANNEL_SUMMARY_CSV,
        "watch_channel_daily_csv": WATCH_CHANNEL_DAILY_CSV,
        "watch_geo_csv": WATCH_GEO_CSV,
        "watch_device_type_csv": WATCH_DEVICE_TYPE_CSV,
        "watch_ua_csv": WATCH_UA_CSV,
        "watch_content_csv": WATCH_CONTENT_CSV,
    }
    for key, path in sources.items():
        if path.exists():
            KPI_SOURCE_MTIME_TS.labels(source=key).set(path.stat().st_mtime)


def refresh_overview_kpis() -> None:
    rows = load_overview_rows()
    if not rows:
        LOG.warning("No overview rows found at %s", OVERVIEW_XLSX_PATH)
        return

    true_start = rows[0]["date"]
    true_end = rows[-1]["date"]
    OV_TRUE_START_TS.set(date_to_epoch_start(true_start))
    OV_TRUE_END_TS.set(date_to_epoch_start(true_end))

    used = used_slice(rows)
    used_start = used[0]["date"]
    used_end = used[-1]["date"]
    OV_USED_START_TS.set(date_to_epoch_start(used_start))
    OV_USED_END_TS.set(date_to_epoch_start(used_end))

    days = float(len(used))
    total_rows = sum(r["rows"] for r in used)
    total_bytes = sum(r["bytes_gib"] for r in used)
    total_ips = sum(r["dist_ip"] for r in used)
    total_devs = sum(r["dist_dev"] for r in used)
    total_sess = sum(r["dist_sess"] for r in used)

    OV_DAYS_USED.set(days)
    OV_TOTAL_ROWS.set(total_rows)
    OV_TOTAL_BYTES_GIB.set(total_bytes)
    OV_AVG_ROWS_PER_DAY.set(total_rows / days if days else 0.0)
    OV_AVG_BYTES_GIB_PER_DAY.set(total_bytes / days if days else 0.0)
    OV_AVG_DISTINCT_IPS_PER_DAY.set(total_ips / days if days else 0.0)
    OV_AVG_DISTINCT_DEVICES_PER_DAY.set(total_devs / days if days else 0.0)
    OV_AVG_SESSIONS_PER_DAY.set(total_sess / days if days else 0.0)

    first_seen = parse_device_first_seen()
    first_seen_by_day: dict[date, int] = defaultdict(int)
    for d in first_seen.values():
        first_seen_by_day[d] += 1

    daily_sets = load_device_daily_sets()
    active: set[str] = set()
    for d, devices in daily_sets.items():
        if used_start <= d <= used_end:
            active.update(devices)

    new_devices = 0
    for device_id in active:
        fs = first_seen.get(device_id)
        if fs and used_start <= fs <= used_end:
            new_devices += 1
    returning_devices = max(0, len(active) - new_devices)

    OV_ACTIVE_DEVICES.set(float(len(active)))
    OV_RETURNING_DEVICES.set(float(returning_devices))
    OV_NEW_DEVICES.set(float(new_devices))

    OV_DAY_START_TS.clear()
    OV_DAY_ROWS.clear()
    OV_DAY_BYTES_GIB.clear()
    OV_DAY_DISTINCT_IPS.clear()
    OV_DAY_DISTINCT_DEVICES.clear()
    OV_DAY_SESSIONS.clear()
    OV_DAY_ACTIVE_DEVICES.clear()
    OV_DAY_NEW_DEVICES.clear()
    OV_DAY_RETURNING_DEVICES.clear()
    OV_DEVICE_DAY_START_TS.clear()
    OV_DEVICE_FIRST_SEEN_COUNT.clear()
    OV_DEVICE_LAST_SEEN_COUNT.clear()
    # Daily series retain partial edge dates so Grafana can show the exact selected range.
    for r in rows:
        d: date = r["date"]
        key = day_key(d)
        OV_DAY_START_TS.labels(day=key).set(date_to_epoch_start(d))
        OV_DAY_ROWS.labels(day=key).set(r["rows"])
        OV_DAY_BYTES_GIB.labels(day=key).set(r["bytes_gib"])
        OV_DAY_DISTINCT_IPS.labels(day=key).set(r["dist_ip"])
        OV_DAY_DISTINCT_DEVICES.labels(day=key).set(r["dist_dev"])
        OV_DAY_SESSIONS.labels(day=key).set(r["dist_sess"])

        day_active = len(daily_sets.get(d, set()))
        day_new = int(first_seen_by_day.get(d, 0))
        day_returning = max(0, day_active - day_new)
        OV_DAY_ACTIVE_DEVICES.labels(day=key).set(float(day_active))
        OV_DAY_NEW_DEVICES.labels(day=key).set(float(day_new))
        OV_DAY_RETURNING_DEVICES.labels(day=key).set(float(day_returning))

    first_seen_day_counts: dict[date, int] = defaultdict(int)
    last_seen_day_counts: dict[date, int] = defaultdict(int)
    for r in read_csv_rows(DEVICE_SNAPSHOT_CSV):
        fs = parse_date_any(r.get("first_seen_utc_date"))
        ls = parse_date_any(r.get("last_seen_utc_date"))
        if fs:
            first_seen_day_counts[fs] += 1
        if ls:
            last_seen_day_counts[ls] += 1

    device_days = sorted(set(first_seen_day_counts.keys()) | set(last_seen_day_counts.keys()))
    for d in device_days:
        key = day_key(d)
        OV_DEVICE_DAY_START_TS.labels(day=key).set(date_to_epoch_start(d))
        OV_DEVICE_FIRST_SEEN_COUNT.labels(day=key).set(float(first_seen_day_counts.get(d, 0)))
        OV_DEVICE_LAST_SEEN_COUNT.labels(day=key).set(float(last_seen_day_counts.get(d, 0)))


def refresh_watch_kpis() -> None:
    daily = load_watch_daily_rows()
    if not daily:
        LOG.warning("No watch daily rows found at %s", WATCH_DAILY_CSV)
        return

    true_start = daily[0]["date"]
    true_end = daily[-1]["date"]
    WH_TRUE_START_TS.set(date_to_epoch_start(true_start))
    WH_TRUE_END_TS.set(date_to_epoch_start(true_end))

    used = used_slice(daily)
    used_start = used[0]["date"]
    used_end = used[-1]["date"]
    WH_USED_START_TS.set(date_to_epoch_start(used_start))
    WH_USED_END_TS.set(date_to_epoch_start(used_end))

    days = float(len(used))
    raw_hours = sum(r["raw_watch_hours"] for r in used)
    s200_hours = sum(r["status_200_watch_hours"] for r in used)
    rows_total = sum(r["rows"] for r in used)
    status_200_rows_total = sum(r["status_200_rows"] for r in used)
    non_200_rows_total = sum(r["non_200_rows"] for r in used)
    ts_rows_total = sum(r["ts_rows"] for r in used)
    m3u8_rows_total = sum(r["m3u8_rows"] for r in used)
    approx_unique_ips = max((r["approx_unique_ips"] for r in used), default=0.0)

    WH_DAYS_USED.set(days)
    WH_TOTAL_ROWS.set(rows_total)
    WH_STATUS_200_ROWS.set(status_200_rows_total)
    WH_NON_200_ROWS.set(non_200_rows_total)
    WH_NON_200_PCT.set((non_200_rows_total / rows_total) if rows_total else 0.0)
    WH_TS_ROWS.set(ts_rows_total)
    WH_M3U8_ROWS.set(m3u8_rows_total)
    WH_APPROX_UNIQUE_IPS.set(approx_unique_ips)
    WH_RAW_WATCH_HOURS_TOTAL.set(raw_hours)
    WH_STATUS_200_WATCH_HOURS_TOTAL.set(s200_hours)
    WH_AVG_RAW_WATCH_HOURS_PER_DAY.set(raw_hours / days if days else 0.0)
    WH_AVG_ROWS_PER_DAY.set(rows_total / days if days else 0.0)

    WH_DAY_START_TS.clear()
    WH_DAY_ROWS.clear()
    WH_DAY_RAW_WATCH_HOURS.clear()
    WH_DAY_STATUS_200_WATCH_HOURS.clear()
    WH_DAY_STATUS_200_ROWS.clear()
    WH_DAY_NON_200_ROWS.clear()
    WH_DAY_TS_ROWS.clear()
    # Daily series retain partial edge dates so Grafana can show today's available ETL data.
    for r in daily:
        d: date = r["date"]
        key = day_key(d)
        WH_DAY_START_TS.labels(day=key).set(date_to_epoch_start(d))
        WH_DAY_ROWS.labels(day=key).set(r["rows"])
        WH_DAY_RAW_WATCH_HOURS.labels(day=key).set(r["raw_watch_hours"])
        WH_DAY_STATUS_200_WATCH_HOURS.labels(day=key).set(r["status_200_watch_hours"])
        WH_DAY_STATUS_200_ROWS.labels(day=key).set(r["status_200_rows"])
        WH_DAY_NON_200_ROWS.labels(day=key).set(r["non_200_rows"])
        WH_DAY_TS_ROWS.labels(day=key).set(r["ts_rows"])

    channels = load_watch_channel_summary()
    active_channels = [c for c in channels if c["channel_name"] and c["channel_name"] != "Other" and c["raw_watch_hours"] > 0]
    WH_ACTIVE_CHANNELS.set(float(len(active_channels)))

    WH_TOP_CHANNEL_RAW_HOURS.clear()
    WH_TOP_CHANNEL_INFO.clear()
    WH_CHANNEL_RAW_HOURS.clear()
    WH_CHANNEL_SEGMENTS.clear()
    WH_CHANNEL_UNIQUE_IPS.clear()
    for channel in active_channels:
        channel_name = channel["channel_name"]
        WH_CHANNEL_RAW_HOURS.labels(channel_name=channel_name).set(channel["raw_watch_hours"])
        WH_CHANNEL_SEGMENTS.labels(channel_name=channel_name).set(channel["segments"])
        WH_CHANNEL_UNIQUE_IPS.labels(channel_name=channel_name).set(channel["approx_unique_ips"])

    if channels:
        top = channels[0]
        channel_name = top["channel_name"] or "unknown"
        WH_TOP_CHANNEL_RAW_HOURS.labels(channel_name=channel_name).set(top["raw_watch_hours"])
        WH_TOP_CHANNEL_INFO.labels(channel_name=channel_name).set(1.0)

    WH_CHANNEL_DAY_RAW_HOURS.clear()
    WH_CHANNEL_DAY_UNIQUE_IPS.clear()
    for row in load_watch_channel_daily():
        labels = {
            "day": day_key(row["date"]),
            "channel_name": row["channel_name"],
        }
        WH_CHANNEL_DAY_RAW_HOURS.labels(**labels).set(row["raw_watch_hours"])
        WH_CHANNEL_DAY_UNIQUE_IPS.labels(**labels).set(row["approx_unique_ips"])


def refresh_profile_kpis() -> None:
    profile = aggregate_profile_rows()

    PROFILE_GEO_TS_ROWS.clear()
    PROFILE_GEO_UNIQUE_IPS.clear()
    for row in profile["geo"]:
        labels = {"country": row["country"], "state": row["state"]}
        PROFILE_GEO_TS_ROWS.labels(**labels).set(row["ts_rows"])
        PROFILE_GEO_UNIQUE_IPS.labels(**labels).set(row["unique_ips"])

    PROFILE_DEVICE_TYPE_TS_ROWS.clear()
    PROFILE_DEVICE_TYPE_UNIQUE_IPS.clear()
    for row in profile["device_types"]:
        PROFILE_DEVICE_TYPE_TS_ROWS.labels(device_type=row["device_type"]).set(row["ts_rows"])
        PROFILE_DEVICE_TYPE_UNIQUE_IPS.labels(device_type=row["device_type"]).set(
            row["unique_ips"]
        )

    PROFILE_OS_ROWS.clear()
    PROFILE_OS_UNIQUE_IPS.clear()
    for row in profile["operating_systems"]:
        PROFILE_OS_ROWS.labels(os=row["os"]).set(row["rows"])
        PROFILE_OS_UNIQUE_IPS.labels(os=row["os"]).set(row["unique_ips"])

    PROFILE_PLATFORM_REQUESTS.clear()
    PROFILE_PLATFORM_SESSIONS.clear()
    PROFILE_PLATFORM_VIEWERS.clear()
    for row in profile["platforms"]:
        platform = row["platform"]
        PROFILE_PLATFORM_REQUESTS.labels(platform=platform).set(row["requests"])
        PROFILE_PLATFORM_SESSIONS.labels(platform=platform).set(row["sessions"])
        PROFILE_PLATFORM_VIEWERS.labels(platform=platform).set(row["viewers"])

    PROFILE_CONTENT_INFO.clear()
    for row in profile["content"]:
        PROFILE_CONTENT_INFO.labels(
            content_title=row["content_title"],
            requests=str(int(row["requests"])),
            sessions=str(int(row["sessions"])),
            devices=str(int(row["devices"])),
            viewers=str(int(row["viewers"])),
        ).set(row["requests"])


def refresh_all() -> None:
    set_source_mtimes()
    refresh_overview_kpis()
    refresh_watch_kpis()
    refresh_profile_kpis()


def main() -> None:
    LOG.info("Starting KPI exporter on :%d", PORT)
    LOG.info("Overview snapshot: %s", OVERVIEW_SNAPSHOT_JSON)
    LOG.info("Overview source: %s", OVERVIEW_XLSX_PATH)
    LOG.info("Watch snapshot: %s", WATCH_SNAPSHOT_JSON)
    LOG.info("Watch daily source: %s", WATCH_DAILY_CSV)
    start_http_server(PORT)

    while True:
        t0 = time.time()
        try:
            refresh_all()
            KPI_EXPORTER_REFRESH_OK.set(1.0)
            KPI_EXPORTER_LAST_SUCCESS_TS.set(time.time())
            LOG.info("KPI refresh succeeded in %.2fs", time.time() - t0)
        except Exception as exc:  # noqa: BLE001
            KPI_EXPORTER_REFRESH_OK.set(0.0)
            KPI_EXPORTER_LAST_ERROR_TS.set(time.time())
            LOG.exception("KPI refresh failed: %s", exc)
        time.sleep(REFRESH_SEC)


if __name__ == "__main__":
    main()
