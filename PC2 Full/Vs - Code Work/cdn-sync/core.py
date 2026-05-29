"""
core.py — Engine: S3 listing, smart path mapping, parallel download.

Handles two source types:
  "mmdd" → veto-stream-logs/MM/DD/file.gz  (date-folder structure)
  "flat" → veto-fast-logs/file.gz           (flat, filter by last_modified)
"""

import os
import time
import logging
import threading
from pathlib import Path, PurePosixPath
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import tqdm

import config

# ── Logging ──────────────────────────────────────────────────────────────────
os.makedirs(config.LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, f"sync_{datetime.now():%Y%m%d}.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("veto-sync")

# ── Thread-local S3 clients ──────────────────────────────────────────────────
_tls = threading.local()

def get_s3():
    if not hasattr(_tls, "c"):
        _tls.c = boto3.client(
            "s3",
            endpoint_url=config.ENDPOINT_URL,
            aws_access_key_id=config.ACCESS_KEY_ID,
            aws_secret_access_key=config.SECRET_KEY,
            config=Config(
                retries={"max_attempts": config.RETRY_ATTEMPTS, "mode": "adaptive"},
                max_pool_connections=config.MAX_WORKERS + 20,
                tcp_keepalive=True,
                connect_timeout=10,   # ← add this
                read_timeout=30,
            ),
        )
    return _tls.c

XFER = TransferConfig(
    multipart_threshold=16 * 1024 * 1024,
    max_concurrency=4,
    multipart_chunksize=config.DOWNLOAD_CHUNK_MB * 1024 * 1024,
    use_threads=True,
)


# ── S3 listing ───────────────────────────────────────────────────────────────

def list_prefix(prefix: str) -> dict:
    """List all objects under prefix → {key: {size, etag, last_modified}}"""
    s3, objs = get_s3(), {}
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=config.BUCKET_NAME, Prefix=prefix
    ):
        for o in page.get("Contents", []):
            objs[o["Key"]] = {
                "size":          o["Size"],
                "etag":          o["ETag"].strip('"'),
                "last_modified": o["LastModified"],
            }
    return objs


def list_source_for_date(src: dict, mm: str, dd: str) -> dict:
    """List S3 objects for a source on a specific MM/DD."""
    # Both mmdd and mmddx have real MM/DD/ folders in S3
    return list_prefix(f"{src['s3_prefix']}{mm}/{dd}/")


def list_source_date_range(src: dict, start: datetime, end: datetime) -> dict:
    """List S3 objects for a source across a date range (sequential, safe)."""
    all_objs = {}
    cur = start
    while cur <= end:
        prefix = f"{src['s3_prefix']}{cur:%m}/{cur:%d}/"
        log.info(f"    Listing {prefix} ...")
        day_objs = list_prefix(prefix)
        log.info(f"    {cur:%m/%d} → {len(day_objs):,} objects")
        all_objs.update(day_objs)
        cur += timedelta(days=1)
    return all_objs


# ── Path mapping ─────────────────────────────────────────────────────────────

def s3_key_to_path(key: str, src: dict, drive: str) -> Path:
    """
    Map an S3 key to its local filesystem path.

    veto-stream-logs (mmdd):
      veto-stream-logs/04/01/file.gz      -> <drive>/Veto Stream Logs/04/01/file.gz

    veto-fast-logs (mmddx, 3-level):
      veto-fast-logs/05/13/110280/file.gz -> <drive>/veto fast logs/05/13/110280/file.gz
    """
    rel_posix = key[len(src["s3_prefix"]):]   # strip bucket prefix

    subdir = src["net_subdir"] if drive == config.NETWORK_DRIVE else src["local_subdir"]
    base   = Path(drive) / subdir if subdir else Path(drive)

    # Keep the S3 relative path structure as-is under base
    return base / rel_posix.replace("/", os.sep)


# ── Missing-file detection ────────────────────────────────────────────────────

def find_missing(s3_objects: dict, src: dict, drive: str) -> list:
    """Return S3 keys whose local file is absent or wrong size."""
    missing = []
    for key, meta in s3_objects.items():
        local = s3_key_to_path(key, src, drive)
        if not local.exists() or local.stat().st_size != meta["size"]:
            missing.append(key)
    log.info(
        f"  [{src['name']}] Missing/incomplete → {len(missing):,} / {len(s3_objects):,}"
    )
    return missing


# ── Download ─────────────────────────────────────────────────────────────────

def _dl_one(key: str, dest: Path, size: int) -> tuple:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    for attempt in range(1, config.RETRY_ATTEMPTS + 1):
        try:
            get_s3().download_file(config.BUCKET_NAME, key, str(tmp), Config=XFER)
            if tmp.stat().st_size != size:
                raise ValueError(f"size mismatch: {tmp.stat().st_size} ≠ {size}")
            tmp.rename(dest)
            return key, True
        except Exception as e:
            wait = config.RETRY_BACKOFF ** attempt
            log.warning(f"[{attempt}/{config.RETRY_ATTEMPTS}] {key}: {e} — retry {wait}s")
            tmp.unlink(missing_ok=True)
            if attempt < config.RETRY_ATTEMPTS:
                time.sleep(wait)
    log.error(f"FAILED: {key}")
    return key, False


def bulk_download(keys: list, s3_objects: dict, src: dict, drive: str) -> tuple:
    """Download keys in parallel to drive. Returns (ok, failed)."""
    if not keys:
        return 0, 0

    total_mb = sum(s3_objects[k]["size"] for k in keys) / 1024 / 1024
    log.info(f"  [{src['name']}] Downloading {len(keys):,} files ({total_mb:.1f} MB) — {config.MAX_WORKERS} workers")

    ok = failed = 0
    failed_keys = []
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as pool:
        futures = {
            pool.submit(_dl_one, k, s3_key_to_path(k, src, drive), s3_objects[k]["size"]): k
            for k in keys
        }
        bar = tqdm.tqdm(total=len(keys), unit="file", ncols=95,
                        desc=f"  {src['name'][:18]}",
                        bar_format="{desc} {bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining} {rate_fmt}]")
        for fut in as_completed(futures):
            _, success = fut.result()
            with lock:
                if success: ok += 1
                else:
                    failed += 1
                    failed_keys.append(futures[fut])
            bar.update(1)
            bar.set_postfix(ok=ok, fail=failed, refresh=False)
        bar.close()

    if failed_keys:
        p = os.path.join(config.LOG_DIR, f"failed_{src['name']}_{datetime.now():%Y%m%d_%H%M%S}.txt")
        Path(p).write_text("\n".join(failed_keys))
        log.warning(f"  Failed keys → {p}")

    return ok, failed
