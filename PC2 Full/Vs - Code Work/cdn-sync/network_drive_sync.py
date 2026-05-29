"""
network_drive_sync.py
---------------------------------------------------------------------------
Syncs BOTH log sources  ->  Y:\\Veto Logs Backup

  veto-stream-logs/MM/DD/file.gz  ->  Y:\\Veto Logs Backup\\MM Veto Logs\\DD\\file.gz
  veto-fast-logs/file.gz          ->  Y:\\Veto Logs Backup\\veto fast logs\\file.gz

Checks for missing/size-mismatch only -- never re-downloads good files.
Runs both sources sequentially, reports combined totals.

USAGE:
  python network_drive_sync.py                   # last 3 days (default)
  python network_drive_sync.py --days 7          # last 7 days
  python network_drive_sync.py --month 04        # full month April
  python network_drive_sync.py --date 04 30      # specific MM DD
  python network_drive_sync.py --full            # entire bucket (slow)
  python network_drive_sync.py --source stream   # only veto-stream-logs
  python network_drive_sync.py --source fast     # only veto-fast-logs
  python network_drive_sync.py --dry-run         # preview, no download
---------------------------------------------------------------------------
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

import config
from core import log, list_prefix, list_source_date_range, find_missing, bulk_download

DRIVE = config.NETWORK_DRIVE


def parse_args():
    p = argparse.ArgumentParser(description="Network Drive Sync — Both Log Sources")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--days",  type=int,         metavar="N",
                   help="Last N days (default 3)")
    g.add_argument("--month", type=str,         metavar="MM",
                   help="Full month e.g. --month 04")
    g.add_argument("--date",  type=str, nargs=2, metavar=("MM","DD"),
                   help="Specific date e.g. --date 04 30")
    g.add_argument("--full",  action="store_true",
                   help="Full bucket scan")
    p.add_argument("--source", choices=["stream","fast","both"], default="both",
                   help="Which source to sync (default: both)")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def select_sources(source_arg):
    lookup = {"stream": "veto-stream-logs", "fast": "veto-fast-logs"}
    if source_arg == "both":
        return config.SOURCES
    return [s for s in config.SOURCES if s["name"] == lookup[source_arg]]


def sync_source(src: dict, args) -> tuple:
    """Sync one source. Returns (ok, failed, s3_total, missing_total)."""
    today = datetime.now(timezone.utc)
    log.info(f"\n{'-'*68}")
    log.info(f"  SOURCE : {src['name']}")
    log.info(f"  S3     : s3://{config.BUCKET_NAME}/{src['s3_prefix']}")
    log.info(f"  Drive  : {DRIVE}\\{src['net_subdir'] or ''}")
    log.info(f"{'-'*68}")

    # ── List S3 ──────────────────────────────────────────────────────────────
    if args.full:
        s3_objs = list_prefix(src["s3_prefix"])
    elif args.month:
        mm = args.month.zfill(2)
        # Both mmdd and mmddx have real MM/ folders in S3
        s3_objs = list_prefix(f"{src['s3_prefix']}{mm}/")
    elif args.date:
        mm, dd = args.date[0].zfill(2), args.date[1].zfill(2)
        # Both mmdd and mmddx have real MM/DD/ folders in S3
        s3_objs = list_prefix(f"{src['s3_prefix']}{mm}/{dd}/")
    else:
        days  = args.days or 3
        end   = today
        start = today - timedelta(days=days - 1)
        s3_objs = list_source_date_range(src, start, end)

    if not s3_objs:
        log.info("  No objects found — skipping.")
        return 0, 0, 0, 0

    missing   = find_missing(s3_objs, src, DRIVE)
    miss_mb   = sum(s3_objs[k]["size"] for k in missing) / 1024 / 1024
    total_mb  = sum(v["size"] for v in s3_objs.values()) / 1024 / 1024

    log.info(f"  S3 total   : {len(s3_objs):>10,}  ({total_mb:.1f} MB)")
    log.info(f"  In sync    : {len(s3_objs)-len(missing):>10,}  [OK]")
    log.info(f"  To fetch   : {len(missing):>10,}  ({miss_mb:.1f} MB)")

    if args.dry_run:
        for k in missing[:30]:
            log.info(f"    MISSING: {k}")
        if len(missing) > 30:
            log.info(f"    ... and {len(missing)-30} more")
        return 0, 0, len(s3_objs), len(missing)

    if not missing:
        log.info("  Fully in sync.")
        return 0, 0, len(s3_objs), 0

    ok, fail = bulk_download(missing, s3_objs, src, DRIVE)
    return ok, fail, len(s3_objs), len(missing)


def run():
    args    = parse_args()
    t0      = time.time()
    sources = select_sources(args.source)

    log.info("=" * 68)
    log.info("  VETO LOGS — NETWORK DRIVE SYNC")
    log.info(f"  Drive    : {DRIVE}")
    log.info(f"  Sources  : {', '.join(s['name'] for s in sources)}")
    log.info("=" * 68)

    grand_ok = grand_fail = 0
    for src in sources:
        ok, fail, _, _ = sync_source(src, args)
        grand_ok   += ok
        grand_fail += fail

    elapsed = time.time() - t0
    log.info("\n" + "=" * 68)
    log.info("  ALL SOURCES COMPLETE")
    log.info(f"  OK   Total downloaded : {grand_ok:,}")
    log.info(f"  FAIL Total failed     : {grand_fail:,}")
    log.info(f"  Time                  : {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    if elapsed > 0 and grand_ok > 0:
        log.info(f"  Speed                 : {grand_ok/elapsed:.0f} files/sec")
    log.info("=" * 68)

    sys.exit(1 if grand_fail else 0)


if __name__ == "__main__":
    run()
