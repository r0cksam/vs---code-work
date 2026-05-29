# ============================================================
#  VETO LOGS SYNC — CONFIG
# ============================================================

# ── Credentials ──────────────────────────────────────────────
ENDPOINT_URL  = "https://in-maa-1.linodeobjects.com"
ACCESS_KEY_ID = "W5LRU06PYI9F13JSIWAB"
SECRET_KEY    = "F5YVjfb0FLtenYCQWtWPtWCJVpE1BLE5ZhXYJYXs"      # <── only thing to fill
BUCKET_NAME   = "veto-stream-logs"

# ── Drive base paths ─────────────────────────────────────────
NETWORK_DRIVE = r"Y:\Veto Logs Backup"      # full sync + missing check
LOCAL_DRIVE   = r"D:\Veto Logs Backup"      # yesterday's data only

# ── Source definitions ───────────────────────────────────────
#
#   Each source defines:
#     s3_prefix   : prefix inside the bucket
#     date_type   : "mmdd"   -> files live under MM/DD/ subfolders
#                   "mmddx"  -> files live under MM/DD/SUBDIR/ (3 levels)
#     net_subdir  : subfolder under NETWORK_DRIVE
#     local_subdir: subfolder under LOCAL_DRIVE
#
SOURCES = [
    {
        "name"        : "veto-stream-logs",
        "s3_prefix"   : "veto-stream-logs/",
        "date_type"   : "mmdd",             # MM/DD/file
        "net_subdir"  : "Veto Stream Logs", # Y:\Veto Logs Backup\Veto Stream Logs\04\01\
        "local_subdir": "Veto Stream Logs", # D:\Veto Logs Backup\Veto Stream Logs\04\01\
    },
    {
        "name"        : "veto-fast-logs",
        "s3_prefix"   : "veto-fast-logs/",
        "date_type"   : "mmddx",            # MM/DD/SUBDIR/file (3-level)
        "net_subdir"  : "veto fast logs",   # Y:\Veto Logs Backup\veto fast logs\05\13\110280\
        "local_subdir": "veto fast logs",   # D:\Veto Logs Backup\veto fast logs\05\13\110280\
    },
]

# ── Performance ───────────────────────────────────────────────
MAX_WORKERS       = 128    # parallel download threads (try 200-256 on fast net)
LIST_WORKERS      = 32     # parallel listing threads
DOWNLOAD_CHUNK_MB = 8
RETRY_ATTEMPTS    = 5
RETRY_BACKOFF     = 2      # seconds (exponential)

# ── Logging ───────────────────────────────────────────────────
LOG_DIR   = "./logs"
LOG_LEVEL = "INFO"
