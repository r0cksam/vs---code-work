# Veto Watch Hours ETL Runbook

## Goal

Build the stakeholder-ready Veto Watch Hours dashboard without rescanning the full lake unless the lake changed.

## Main Pipeline

Script:

```powershell
.\venv\Scripts\python.exe .\vglive_incremental_etl.py --threads 6 --memory-limit 12GB --top-n 1000
```

What it does:

- Scans Parquet metadata under `D:\Veto Logs Backup\lake`.
- Detects changed dates from file path, size, mtime, and Parquet row count.
- Refreshes only changed dates into `vglive_channel_profile\etl_store`.
- Materializes dashboard Parquet tables into `vglive_channel_profile\deep_profile_full`.

## Fast Safety Check

Run this first when you only want to know whether anything changed:

```powershell
.\venv\Scripts\python.exe .\vglive_incremental_etl.py --dry-run --threads 6 --memory-limit 12GB
```

Expected clean output:

```text
[dirty] no changed lake dates; profile is already current
```

## Regenerate HTML

```powershell
.\venv\Scripts\python.exe .\ExcelGenerator\vglive_share_report.py --profile .\vglive_channel_profile\deep_profile_full --out .\vglive_channel_profile\veto_watch_hours.html
```

## Force One Date

Use when a prior date receives an extra Parquet file or has been corrected:

```powershell
.\venv\Scripts\python.exe .\vglive_incremental_etl.py --force-dates 2026-05-27 --process-dates 2026-05-27 --threads 6 --memory-limit 12GB
```

## Full Rebuild

Use only when logic changes or the aggregate store needs to be rebuilt:

```powershell
.\venv\Scripts\python.exe .\vglive_incremental_etl.py --full-refresh --threads 6 --memory-limit 12GB --top-n 1000
```

## Approved Watch-Hour Definitions

- Raw watch hours = all `reqPath LIKE '%.ts'` rows multiplied by 6 seconds.
- Status 200 watch hours = `statusCode = 200` and `reqPath LIKE '%.ts'` rows multiplied by 6 seconds.
- `.m3u8` rows are playlist/evidence rows and are not watch hours.

## Latest Baseline

- Lake files: 132 Parquet files.
- Lake rows: 1,360,187,031.
- Raw `.ts` rows: 517,637,274.
- Status 200 `.ts` rows: 513,650,439.
- Raw watch hours: 862,728.790.
- Status 200 watch hours: 856,084.065.
- Raw minus status 200 difference: 6,644.725 watch hours.
- Full refresh runtime on this PC with `--threads 6 --memory-limit 12GB`: about 21.5 minutes.
- No-change dry run runtime: about 6 seconds.
