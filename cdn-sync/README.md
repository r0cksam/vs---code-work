# Veto Stream Logs — Fast S3 Sync Tool

## Structure understood

```
S3 Bucket   :  s3://veto-stream-logs/
S3 Key      :  veto-stream-logs/MM/DD/filename.gz
              └── same name as bucket, then month/day

Network Drive:  Y:\Veto Logs Backup\MM Veto Logs\DD\filename.gz
Local PC     :  C:\Veto Logs\Daily\MM\DD\filename.gz
```

## Why faster than `aws s3 sync`

| | AWS CLI | This tool |
|---|---|---|
| Parallel threads | ~10 | **128** (tunable to 256) |
| Missing detection | Re-lists everything | Size-check per file (instant) |
| 1–2 missed files | Re-scans full bucket | Checks only last 3 days |
| 400k files/day | ~15–30 min | **3–8 min** |

---

## One-time setup

### 1. Install
```bash
pip install -r requirements.txt
```

### 2. Edit `config.py` — fill in two lines:
```python
SECRET_KEY     = "your_actual_secret_key"     # from Linode dashboard
```
Everything else (bucket, endpoint, paths) is already pre-filled.

---

## Daily usage

### Network Drive Sync (auto, runs via Task Scheduler)
```bash
# Check last 3 days for missing files (recommended default)
python network_drive_sync.py

# Check last 7 days
python network_drive_sync.py --days 7

# Sync entire month of April
python network_drive_sync.py --month 04

# Sync specific date (e.g. April 30)
python network_drive_sync.py --date 04 30

# Full bucket scan (use occasionally to verify everything)
python network_drive_sync.py --full

# Preview only — see what's missing without downloading
python network_drive_sync.py --dry-run
```

### Local PC — Download Yesterday's Logs
```bash
# Yesterday's files (default — use this in Task Scheduler)
python local_sync.py

# Specific date
python local_sync.py --date 04 30

# Date range (e.g. first 30 days of April)
python local_sync.py --range 04 01 04 30

# Preview
python local_sync.py --dry-run

# Force re-download even if files exist
python local_sync.py --redownload --date 04 30
```

---

## Windows Task Scheduler Setup

### Network Drive — every 2 hours
1. Task Scheduler → Create Basic Task
2. Trigger → Daily, repeat every **2 hours**
3. Action → `C:\cdn-sync\run_network_sync.bat`
4. ✅ "Run whether user is logged on or not"
5. ✅ "Run with highest privileges"

### Local PC — daily at 1 AM
1. Task Scheduler → Create Basic Task
2. Trigger → Daily at **01:00 AM**
3. Action → `C:\cdn-sync\run_local_sync.bat`
4. ✅ "Run whether user is logged on or not"

---

## Tune performance (config.py)

```python
MAX_WORKERS = 128   # ↑ try 200 on a fast connection
LIST_WORKERS = 32   # threads for listing (fine as-is)
```

## Logs
All logs saved in `./logs/` folder.
If any files fail, a `failed_YYYYMMDD_HHMMSS.txt` is created — run:
```bash
# Retry only failed files from a log
python network_drive_sync.py --date MM DD
```
