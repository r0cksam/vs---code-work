from pathlib import Path

# ─────────────────────────────────────────────
# ROOT PATH — verified from your machine
# ─────────────────────────────────────────────
ROOT = Path(r"D:\VETO Logs")

# Day folders available
# Add new folders here as new days arrive e.g. "06", "07"
DAY_FOLDERS = ["01"]   # currently only 01 — add "02","03" etc. as they arrive

# Auto-generate raw and parquet paths for each day
# RAW["01"]     = D:\VETO Logs\01           ← source .gz files
# PARQUET["01"] = D:\VETO Logs\01_parquet   ← output, partitioned by date inside
RAW     = {day: ROOT / day              for day in DAY_FOLDERS}
PARQUET = {day: ROOT / f"{day}_parquet" for day in DAY_FOLDERS}

# ─────────────────────────────────────────────
# PROCESSING SETTINGS
# ─────────────────────────────────────────────
BATCH_SIZE           = 500   # number of .gz files per batch (lower to 200 if RAM issues)
SEGMENT_DURATION_SEC = 6     # confirmed from .ts filename timestamps
TIMEZONE             = "Asia/Kolkata"  # IST

# ─────────────────────────────────────────────
# OUTPUT PARTITIONING
# Parquet files inside 01_parquet\ are split by date automatically
# e.g. 01_parquet\date=2026-04-01\part-0.parquet
#      01_parquet\date=2026-04-02\part-0.parquet
# Streamlit/DuckDB reads only the dates it needs — very fast
# ─────────────────────────────────────────────
PARTITION_BY = "date"

# ─────────────────────────────────────────────
# FOLDER PATHS
# ─────────────────────────────────────────────
ARCHIVE_DIR  = ROOT / "archive"                  # processed .gz files moved here
LOG_DIR      = ROOT / "pipeline_logs"            # all run logs saved here
PROGRESS_LOG = ROOT / "conversion_progress.log"  # tracks completed day folders

# ─────────────────────────────────────────────
# COLUMNS TO EXTRACT
# Verified from actual .gz file (68 raw fields total)
# Skipped 16: accLang, cookie, customField, overheadBytes,
#   breadcrumbs, cmcd, contentProtectionInfo, ewExecutionInfo,
#   ewUsageInfo, securityRules, tlsEarlyData, xForwardedFor,
#   referer, queryStr, range, edgeIP
# ─────────────────────────────────────────────
COLUMNS = [
    # ── Identity & Network ──
    "cliIP",                   # client IP — viewer identifier
    "asn",                     # autonomous system number
    "country",                 # country code
    "state",                   # Indian state
    "city",                    # city
    "serverCountry",           # CDN server country

    # ── Request Info ──
    "reqTimeSec",              # epoch timestamp (float) — used to derive timestamp
    "reqId",                   # unique request ID
    "reqHost",                 # CDN hostname → identifies app/channel
    "reqPath",                 # path → .m3u8 playlist or .ts segment
    "reqMethod",               # GET / POST
    "reqPort",                 # port (443 = HTTPS)
    "reqEndTimeMSec",          # request end time offset ms

    # ── Response Info ──
    "statusCode",              # HTTP status (200, 404, etc.)
    "rspContentType",          # content type (mpegURL, MP2T, etc.)
    "rspContentLen",           # response content length bytes
    "errorCode",               # CDN error code

    # ── Size & Bytes ──
    "bytes",                   # bytes served to client
    "objSize",                 # object size in bytes
    "totalBytes",              # total bytes transferred
    "uncompressedSize",        # uncompressed size
    "fileSizeBucket",          # size bucket (0-1KB, 1MB-10MB, etc.)

    # ── Timing & Performance (QoE metrics) ──
    "timeToFirstByte",         # TTFB in ms — key quality of experience metric
    "downloadTime",            # total download time ms
    "transferTimeMSec",        # transfer time ms
    "turnAroundTimeMSec",      # turnaround time ms
    "tlsOverheadTimeMSec",     # TLS handshake overhead ms
    "dnsLookupTimeMSec",       # DNS lookup time ms
    "throughput",              # throughput in Kbps

    # ── Cache ──
    "cacheStatus",             # 1 = cache hit, 0 = miss
    "cacheable",               # is content cacheable
    "maxAgeSec",               # cache max age seconds

    # ── Delivery & Stream ──
    "streamId",                # stream identifier
    "deliveryFormat",          # delivery format
    "deliveryType",            # delivery type
    "deliveryPolicyId",        # delivery policy ID
    "deliveryPolicyReqStatus", # delivery policy request status
    "proto",                   # HTTP protocol (HTTPS/1.1, HTTP/2)
    "tlsVersion",              # TLS version (TLSv1.3, etc.)
    "mediaEncryption",         # media encryption flag
    "lastByte",                # last byte delivered flag

    # ── Download Tracking ──
    "downloadInitiated",       # download initiated flag
    "downloadsCompleted",      # download completed flag
    "startupError",            # startup error flag

    # ── CDN / Billing ──
    "arlid",                   # Akamai resource list ID
    "cp",                      # content provider ID
    "billingRegion",           # billing region
    "edgeAttempts",            # edge retry attempts
    "edgeIPBinding",           # edge IP binding
    "prefetchMidgressHits",    # prefetch midgress hits
    "version",                 # log version

    # ── Device ──
    "UA",                      # User Agent → Android version, device info
]

# ─────────────────────────────────────────────
# DERIVED COLUMNS (added during gz_to_parquet.py)
# These do NOT exist in raw .gz — computed on the fly
# ─────────────────────────────────────────────
# timestamp    → reqTimeSec converted to IST datetime
# date         → date part of timestamp ← used for folder partitioning
# hour         → hour of day (0-23)
# minute       → minute (0-59)
# is_segment   → True if .ts in reqPath
# is_playlist  → True if .m3u8 in reqPath
# quality      → 360p/480p/576p/720p/1080p parsed from reqPath
# channel      → channel name parsed from reqHost
# android_ver  → Android version number parsed from UA
# is_error     → True if statusCode >= 400
# cache_hit    → True if cacheStatus == "1"

# ─────────────────────────────────────────────
# SANITY CHECK — run: python config.py
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  Pipeline Configuration Check")
    print("=" * 55)
    print(f"  Root path     : {ROOT}")
    print(f"  Days loaded   : {DAY_FOLDERS}")
    print(f"  Batch size    : {BATCH_SIZE} files at a time")
    print(f"  Segment dur.  : {SEGMENT_DURATION_SEC}s per .ts file")
    print(f"  Timezone      : {TIMEZONE}")
    print(f"  Partition by  : {PARTITION_BY}")
    print(f"  Raw columns   : {len(COLUMNS)} fields")
    print(f"  Derived cols  : 11 (added during conversion)")
    print(f"  Total columns : {len(COLUMNS) + 11} in final parquet")
    print(f"  Archive dir   : {ARCHIVE_DIR}")
    print(f"  Log dir       : {LOG_DIR}")
    print(f"  Progress log  : {PROGRESS_LOG}")
    print()
    print("  Folder Status:")
    for day in DAY_FOLDERS:
        raw_ok     = "✅ found"  if RAW[day].exists()     else "❌ NOT FOUND — check path"
        parquet_ok = "✅ found"  if PARQUET[day].exists() else "📁 will be created"
        print(f"    Day {day} → raw: {raw_ok}  |  parquet: {parquet_ok}")
    print()
    print("  Expected output structure:")
    print(f"    {ROOT}\\01_parquet\\")
    print(f"        date=2026-04-01\\part-0.parquet")
    print(f"        date=2026-04-02\\part-0.parquet")
    print(f"        ...")
    print("=" * 55)