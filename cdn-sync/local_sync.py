"""
local_sync.py
───────────────────────────────────────────────────────────────────────────
Downloads YESTERDAY's logs from BOTH sources  →  D:\Veto Logs Backup

  veto-stream-logs/MM/DD/file.gz  →  D:\Veto Logs Backup\veto-stream-logs\MM\DD\file.gz
  veto-fast-logs/file.gz          →  D:\Veto Logs Backup\veto fast logs\file.gz

Skips already-downloaded files (size check). Run daily at 01:00 AM.

USAGE:
  python local_sync.py                          # yesterday (default)
  python local_sync.py --date 04 30             # specific MM DD
  python local_sync.py --today                  # today's files so far
  python local_sync.py --range 04 01 04 30      # date range
  python local_sync.py --source fast            # only veto-fast-logs
  python local_sync.py --source stream          # only veto-stream-logs
  python local_sync.py --dry-run                # preview only
  python local_sync.py --redownload             # force re-fetch all
───────────────────────────────────────────────────────────────────────────
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

import config
from core import log, list_prefix, list_source_date_range, find_missing, bulk_download

DRIVE = config.LOCAL_DRIVE


def parse_args():
    p = argparse.ArgumentParser(description="Local PC — Daily Log Download (Both Sources)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--yesterday", action="store_true",
                   help="Yesterday's files (default)")
    g.add_argument("--today",     action="store_true",
                   help="Today's files so far")
    g.add_argument("--date",      type=str, nargs=2, metavar=("MM","DD"),
                   help="Specific date  e.g. --date 04 30")
    g.add_argument("--range",     type=str, nargs=4,
                   metavar=("MM_S","DD_S","MM_E","DD_E"),
                   help="Date range  e.g. --range 04 01 04 30")
    p.add_argument("--source",    choices=["stream","fast","both"], default="both")
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--redownload",action="store_true",
                   help="Re-download even if file exists")
    return p.parse_args()


def resolve_dates(args):
    now = datetime.utcnow()
    if args.date:
        mm, dd = int(args.date[0]), int(args.date[1])
        start = end = datetime(now.year, mm, dd)
        label = f"{mm:02d}/{dd:02d}"
    elif args.range:
        mm1,dd1,mm2,dd2 = [int(x) for x in args.range]
        start = datetime(now.year, mm1, dd1)
        end   = datetime(now.year, mm2, dd2)
        label = f"{mm1:02d}/{dd1:02d} → {mm2:02d}/{dd2:02d}"
    elif args.today:
        start = end = now
        label = f"today ({now:%m/%d})"
    else:
        yest  = now - timedelta(days=1)
        start = end = yest
        label = f"yesterday ({yest:%m/%d})"
    return start, end, label


def select_sources(source_arg):
    lookup = {"stream": "veto-stream-logs", "fast": "veto-fast-logs"}
    if source_arg == "both":
        return config.SOURCES
    return [s for s in config.SOURCES if s["name"] == lookup[source_arg]]


def download_source(src: dict, start: datetime, end: datetime, args) -> tuple:
    """Download one source for date range. Returns (ok, failed)."""
    log.info(f"\n{'─'*68}")
    log.info(f"  SOURCE : {src['name']}")
    log.info(f"  Drive  : {DRIVE}\\{src['local_subdir'] or ''}")
    log.info(f"{'─'*68}")

    # ── List S3 for target dates ──────────────────────────────────────────────
    if src["date_type"] == "mmdd":
        if start == end:
            s3_objs = list_prefix(f"{src['s3_prefix']}{start:%m}/{start:%d}/")
        else:
            s3_objs = list_source_date_range(src, start, end)
    else:
        # flat source: list all, filter by last_modified
        all_o       = list_prefix(src["s3_prefix"])
        start_utc   = start.replace(tzinfo=timezone.utc)
        end_utc     = (end + timedelta(days=1)).replace(tzinfo=timezone.utc)
        s3_objs     = {k: v for k, v in all_o.items()
                       if start_utc <= v["last_modified"] < end_utc}

    if not s3_objs:
        log.info("  No objects found for this date range — skipping.")
        return 0, 0

    total_mb = sum(v["size"] for v in s3_objs.values()) / 1024 / 1024
    log.info(f"  S3 objects : {len(s3_objs):,}  ({total_mb:.1f} MB)")

    # ── Find what's missing locally ───────────────────────────────────────────
    if args.redownload:
        to_dl = list(s3_objs.keys())
        log.info("  --redownload: fetching all files")
    else:
        to_dl = find_missing(s3_objs, src, DRIVE)

    already  = len(s3_objs) - len(to_dl)
    dl_mb    = sum(s3_objs[k]["size"] for k in to_dl) / 1024 / 1024

    log.info(f"  Already local : {already:>10,}  ✅")
    log.info(f"  To download   : {len(to_dl):>10,}  ({dl_mb:.1f} MB)")

    if args.dry_run:
        for k in to_dl[:30]:
            log.info(f"    {k}")
        if len(to_dl) > 30:
            log.info(f"    ... and {len(to_dl)-30} more")
        return 0, 0

    if not to_dl:
        log.info(f"  ✅ Already have all files locally.")
        return 0, 0

    return bulk_download(to_dl, s3_objs, src, DRIVE)


def run():
    args              = parse_args()
    t0                = time.time()
    start, end, label = resolve_dates(args)
    sources           = select_sources(args.source)

    log.info("=" * 68)
    log.info("  VETO LOGS — LOCAL PC DAILY DOWNLOAD")
    log.info(f"  Date     : {label}")
    log.info(f"  Drive    : {DRIVE}")
    log.info(f"  Sources  : {', '.join(s['name'] for s in sources)}")
    log.info("=" * 68)

    grand_ok = grand_fail = 0
    for src in sources:
        ok, fail = download_source(src, start, end, args)
        grand_ok   += ok
        grand_fail += fail

    elapsed = time.time() - t0
    log.info("\n" + "=" * 68)
    log.info("  ALL SOURCES COMPLETE")
    log.info(f"  ✅ Total downloaded : {grand_ok:,}")
    log.info(f"  ❌ Total failed     : {grand_fail:,}")
    log.info(f"  ⏱  Time             : {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    if elapsed > 0 and grand_ok > 0:
        log.info(f"  🚀 Speed            : {grand_ok/elapsed:.0f} files/sec")
    log.info("=" * 68)

    sys.exit(1 if grand_fail else 0)


if __name__ == "__main__":
    run()
