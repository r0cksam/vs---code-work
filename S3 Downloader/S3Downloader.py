#!/usr/bin/env python3
"""
===============================================================
  AWS S3 Daily Sync Scheduler
  Syncs yesterday's S3 logs folder to local disk every day.

  Script : D:\Vs - Code Work\S3 Downloader\S3Downloader.py
  Logs   : D:\Vs - Code Work\S3 Downloader\sync_logs\
===============================================================
"""

# ============================================================
#  CONFIGURATION — Edit these values once, leave rest alone
# ============================================================
RUN_TIME        = "09:30"                                        # 24-hr HH:MM

S3_BUCKET       = "veto-stream-logs"
S3_PREFIX       = "veto-stream-logs"                             # path inside bucket before /MM/DD
ENDPOINT_URL    = "https://in-maa-1.linodeobjects.com"
EXTRA_AWS_ARGS  = ["--size-only"]                                # valid aws s3 sync flags

AWS_EXE         = r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"   # full path — no PATH issues

LOCAL_BASE      = r"\\192.168.50.126\Veto Logs Backup\05 Veto Logs"

# How long (seconds) to keep retrying if the network path isn't reachable yet.
NETWORK_WAIT_TIMEOUT  = 120   # wait up to 2 minutes for network to come up
NETWORK_WAIT_INTERVAL = 10    # check every 10 seconds
# ============================================================

import datetime
import logging
import subprocess
import sys
import time
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR    = SCRIPT_DIR / "sync_logs"
STATE_FILE = SCRIPT_DIR / ".last_run_date"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging helpers ────────────────────────────────────────────────────────────
def _daily_log_path() -> Path:
    return LOG_DIR / f"sync_{datetime.date.today().strftime('%Y-%m-%d')}.log"

def setup_logging():
    """Refresh logging so messages always go to today's log file + console."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in root.handlers[:]:
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(_daily_log_path(), encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(ch)

# ── State helpers ──────────────────────────────────────────────────────────────
def _today_iso() -> str:
    return datetime.date.today().isoformat()

def has_run_today() -> bool:
    try:
        return STATE_FILE.read_text().strip() == _today_iso()
    except FileNotFoundError:
        return False

def mark_ran_today():
    STATE_FILE.write_text(_today_iso())

# ── Network path helper ────────────────────────────────────────────────────────
def wait_for_network_path() -> bool:
    base = Path(LOCAL_BASE)
    if not str(LOCAL_BASE).startswith("\\\\"):
        return True
    if NETWORK_WAIT_TIMEOUT == 0:
        return base.exists()
    waited = 0
    logging.info(f"Waiting for network path: {LOCAL_BASE}")
    while waited < NETWORK_WAIT_TIMEOUT:
        try:
            if base.exists():
                logging.info(f"Network path is reachable (waited {waited}s).")
                return True
        except OSError:
            pass
        logging.info(f"  Not reachable yet — retrying in {NETWORK_WAIT_INTERVAL}s "
                     f"({waited}/{NETWORK_WAIT_TIMEOUT}s elapsed)")
        time.sleep(NETWORK_WAIT_INTERVAL)
        waited += NETWORK_WAIT_INTERVAL
    logging.error(f"NETWORK_TIMEOUT — Could not reach {LOCAL_BASE!r} "
                  f"after {NETWORK_WAIT_TIMEOUT}s. Sync aborted.")
    return False

# ── Core sync job ──────────────────────────────────────────────────────────────
def run_sync():
    setup_logging()

    if has_run_today():
        logging.info("Sync already completed today — skipping duplicate run.")
        return

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    month_str = yesterday.strftime("%m")
    day_str   = yesterday.strftime("%d")

    s3_source  = f"s3://{S3_BUCKET}/{S3_PREFIX}/{month_str}/{day_str}"
    local_dest = str(Path(LOCAL_BASE) / day_str)

    if not wait_for_network_path():
        return

    try:
        Path(local_dest).mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logging.error(f"Cannot create local folder {local_dest!r}: {exc}")
        return

    cmd = (
        [AWS_EXE, "s3", "sync", s3_source, local_dest, "--endpoint-url", ENDPOINT_URL]
        + EXTRA_AWS_ARGS
    )

    sep = "=" * 64
    logging.info(sep)
    logging.info("SYNC_STARTED")
    logging.info(f"  Syncing data for : {yesterday.strftime('%d/%m/%Y')}  (yesterday)")
    logging.info(f"  S3 source        : {s3_source}")
    logging.info(f"  Local dest       : {local_dest}")
    logging.info(f"  AWS exe          : {AWS_EXE}")
    logging.info(sep)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                logging.info(line)
        proc.wait()

        if proc.returncode == 0:
            logging.info(sep)
            logging.info("SYNC_COMPLETED — SUCCESS ✓")
            logging.info(sep)
            mark_ran_today()
        else:
            logging.error(sep)
            logging.error(f"SYNC_FAILED — exit code {proc.returncode}")
            logging.error(sep)

    except FileNotFoundError:
        logging.error(f"SYNC_ERROR — aws.exe not found at: {AWS_EXE}")
        logging.error("Check the AWS_EXE path in config at top of script.")
    except Exception as exc:
        logging.error(f"SYNC_ERROR — Unexpected error: {exc}")

# ── Startup check ──────────────────────────────────────────────────────────────
def startup_check():
    setup_logging()
    sep = "=" * 64
    logging.info(sep)
    logging.info("SCHEDULER STARTED  (script is now running)")
    logging.info(f"  Script   : {__file__}")
    logging.info(f"  AWS exe  : {AWS_EXE}")
    logging.info(f"  Run time : {RUN_TIME} daily")
    logging.info(f"  Today    : {_today_iso()}")

    # Verify aws.exe exists right at startup — instant feedback
    try:
        result = subprocess.run([AWS_EXE, "--version"],
                                capture_output=True, text=True)
        logging.info(f"  AWS CLI  : {result.stdout.strip() or result.stderr.strip()}")
    except FileNotFoundError:
        logging.error(f"  AWS CLI NOT FOUND at {AWS_EXE} — sync will fail!")

    logging.info(sep)

    if has_run_today():
        logging.info("Today's sync already completed. Waiting for tomorrow.")
        return

    now = datetime.datetime.now()
    run_h, run_m = map(int, RUN_TIME.split(":"))
    target = now.replace(hour=run_h, minute=run_m, second=0, microsecond=0)

    if now >= target:
        logging.warning("Started AFTER scheduled time and sync has NOT run yet — "
                        "running missed sync NOW.")
        run_sync()
    else:
        mins_left = int((target - now).total_seconds() / 60)
        logging.info(f"Sync not due yet. Will run at {RUN_TIME} "
                     f"(~{mins_left} minute(s) from now).")

# ── Scheduler loop with hourly heartbeat ───────────────────────────────────────
def _next_run_datetime() -> datetime.datetime:
    run_h, run_m = map(int, RUN_TIME.split(":"))
    now   = datetime.datetime.now()
    today = now.replace(hour=run_h, minute=run_m, second=0, microsecond=0)
    if now < today:
        return today
    return today + datetime.timedelta(days=1)

def main():
    startup_check()
    next_run = _next_run_datetime()
    logging.info(f"Next scheduled sync: {next_run.strftime('%Y-%m-%d %H:%M')}")

    last_heartbeat = datetime.datetime.now()

    while True:
        setup_logging()   # swap log file cleanly after midnight
        now = datetime.datetime.now()

        # ── Heartbeat every 60 minutes — visible proof script is alive ─────────
        if (now - last_heartbeat).total_seconds() >= 3600:
            mins_to_run = int((next_run - now).total_seconds() / 60)
            sync_status = "Done today ✓" if has_run_today() else "Pending"
            logging.info(f"[HEARTBEAT] Script alive | "
                         f"Next sync: {next_run.strftime('%Y-%m-%d %H:%M')} "
                         f"(~{mins_to_run} min away) | "
                         f"Today's sync: {sync_status}")
            last_heartbeat = now

        # ── Run sync at scheduled time ─────────────────────────────────────────
        if now >= next_run:
            run_sync()
            next_run = _next_run_datetime()
            logging.info(f"Next scheduled sync: {next_run.strftime('%Y-%m-%d %H:%M')}")

        time.sleep(30)

if __name__ == "__main__":
    main()