"""
lib/constants.py — Static lookup tables and project-wide constants.
"""

from pathlib import Path
from zoneinfo import ZoneInfo

# ── Timing ────────────────────────────────────────────────────────────────────
CHUNK_DURATION_HOURS: float = 6 / 3600.0   # 6-second .ts segment → fractional hours
IST = ZoneInfo("Asia/Kolkata")

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent.parent   # project root
ETL_ROOT = HERE.parents[2]
CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"

# ── Profile CSV filenames ─────────────────────────────────────────────────────
PROFILE_FILES: dict[str, str] = {
    "asn":             "asn_top.csv",
    "cache":           "cache_by_host.csv",
    "channel_daily":   "channel_daily.csv",
    "channel_summary": "channel_summary.csv",
    "cmcd":            "cmcd_presence.csv",
    "column_fill":     "column_fill_rate.csv",
    "daily":           "daily_volume.csv",
    "device":          "device_type_by_channel.csv",
    "errors":          "errors_by_host.csv",
    "extensions":      "extensions.csv",
    "files":           "file_inventory.csv",
    "geo":             "geo_top.csv",
    "hosts":           "hosts_overview.csv",
    "mapping_quality": "path_candidate_quality.csv",
    "performance":     "performance_by_host_extension.csv",
    "query_params":    "querystr_param_presence.csv",
    "query_channels":  "querystr_channel_profile.csv",
    "status":          "status_codes.csv",
    "unmapped":        "unmapped_candidates.csv",
    "ua":              "ua_top.csv",
}

# Daily aggregation table names (stored in output/watch_hours/daily_tables/)
DAILY_TABLE_NAMES: list[str] = [
    "daily_volume",
    "status_codes_daily",
    "extensions_daily",
    "hosts_daily",
    "geo_daily",
    "channel_geo_daily",
    "asn_daily",
    "cache_daily",
    "errors_daily",
    "query_params_daily",
    "query_param_keys_daily",
    "query_m_channel_daily",
    "channel_audience_daily",
    "region_channel_audience_daily",
    "cmcd_daily",
    "user_agents_daily",
    "device_type_by_channel_daily",
    "region_channel_device_daily",
    "mapping_quality_daily",
    "unmapped_candidates_daily",
]

# ── Geo / reference labels ────────────────────────────────────────────────────
COUNTRY_LABELS: dict[str, str] = {
    "AE": "United Arab Emirates",
    "AU": "Australia",
    "BD": "Bangladesh",
    "BH": "Bahrain",
    "BR": "Brazil",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "GB": "United Kingdom",
    "IN": "India",
    "JP": "Japan",
    "NL": "Netherlands",
    "NP": "Nepal",
    "OM": "Oman",
    "SG": "Singapore",
    "US": "United States",
}

STATUS_CODE_MEANINGS: dict[str, str] = {
    "200": "OK: request succeeded.",
    "206": "Partial Content: byte-range response, commonly used for media segment delivery.",
    "301": "Moved Permanently: client should use the redirected URL.",
    "302": "Found: temporary redirect.",
    "304": "Not Modified: cache validation response; payload not sent again.",
    "400": "Bad Request: malformed or invalid request.",
    "401": "Unauthorized: authentication is required or failed.",
    "403": "Forbidden: server understood the request but refused access.",
    "404": "Not Found: requested object was not available.",
    "408": "Request Timeout: client did not complete the request in time.",
    "429": "Too Many Requests: rate limit or throttling response.",
    "500": "Internal Server Error: origin/server-side failure.",
    "501": "Not Implemented: method or feature not supported by server.",
    "502": "Bad Gateway: upstream server returned an invalid response.",
    "503": "Service Unavailable: server or upstream temporarily unavailable.",
    "504": "Gateway Timeout: upstream did not respond in time.",
}

REPORT_DEFINITIONS: list[str] = [
    "Raw watch hours = all .ts segment rows multiplied by 6 seconds, independent of HTTP status.",
    "Status 200 watch hours = HTTP 200 .ts segment rows multiplied by 6 seconds; this is shown separately from raw hours.",
    ".m3u8 rows are playlist and evidence rows; they are not counted as watch hours.",
    "HTTP status code meanings are listed in Reliability > Status Codes.",
    "queryStr channel values are used as mapping QA evidence, not as the primary watch-hour source.",
    "FAST queryStr uses media marker parameters such as m/cmcd instead of STREAM-style channel/device/session fields.",
    "FAST platform breakdown is derived from reqHost labels such as indiatv-tcl, indiatv-samsung, indiatv-cloudtv, and indiatv-vi.",
    "Concurrency active viewers = exact distinct cliIP count per minute from FAST .ts traffic; segment estimates are shown separately for QA.",
    "State/region drilldown uses pre-aggregated channel + geography rows; it is not inferred by joining separate channel and geography totals in the browser.",
    "Sensitive queryStr values such as token and hdnts are redacted in dashboard evidence tables.",
    "Approx unique IP metrics use pre-aggregated profile counts and can overlap across channels or days.",
    "Channel average cards use selected range watch hours divided by range max approximate IP/session/device counts, displayed in minutes; missing session/device IDs are shown as n/a.",
]
