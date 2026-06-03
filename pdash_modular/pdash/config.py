"""Central configuration for pDash.

Edit values here instead of hunting hard-coded constants across the app.
"""
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class AppConfig:
    batch_size: int = int(os.getenv("PDASH_BATCH_SIZE", "100000"))
    small_batch_size: int = int(os.getenv("PDASH_SMALL_BATCH_SIZE", "50000"))
    sample_rows: int = int(os.getenv("PDASH_SAMPLE_ROWS", "100000"))
    watch_gap_cap_seconds: int = int(os.getenv("PDASH_WATCH_GAP_CAP_SECONDS", "60"))
    duckdb_threads: int = int(os.getenv("PDASH_DUCKDB_THREADS", "4"))
    export_warning_rows: int = int(os.getenv("PDASH_EXPORT_WARNING_ROWS", "1000000"))
    max_preview_rows: int = int(os.getenv("PDASH_MAX_PREVIEW_ROWS", "10000"))
    cache_ttl_seconds: int = int(os.getenv("PDASH_CACHE_TTL_SECONDS", "1800"))
    folder_scan_depth: int = int(os.getenv("PDASH_FOLDER_SCAN_DEPTH", "4"))

CONFIG = AppConfig()
