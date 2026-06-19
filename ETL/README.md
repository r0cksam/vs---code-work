# ETL

Single-folder workflow for your Veto watch-hours pipeline and dashboards.

## Folder layout

- `run.py` : main command you run
- `run_daily_pipeline.ps1` : rclone-yesterday helper
- `src/` : all Python code
- `tools/` : optional bundled command-line tools, such as rclone
- `config/` : optional portable config files, such as `rclone.conf`
- `src/tools/asn/` : optional ASN lookup/refresh tools
- `data/` : local lake, ASN lookup data, and raw backup folders
- `output/` : generated watch-hours dashboard, overview dashboard, logs, and state
- `extras/` : old reports, archived generated HTML, and non-runtime clutter

## Quick run

```powershell
cd "<path-to-your-ETL-folder>"
python run.py
```

For portability, keep this folder self-contained:
- `ETL/run.py`
- `ETL/src/*`
- `ETL/config/rclone.conf` (if using bundled rclone)
- `ETL/output/state/gz_parquet_prefs.json` (001.py approved column list)
- `ETL/data/asn/*`
- `ETL/data/lake/*`
- `ETL/requirements.txt`

Preferred local data layout:
- `ETL\data\lake`
- `ETL\data\asn` (CSV/JSON lookup data only)
- `ETL\data\raw`

This runs:

1) `001.py`  (raw `.gz` to parquet; defaults to `data\raw\Veto Logs Backup` when present)
2) `02.py`   (dedupe `_final_clean`)
3) `03.py`   (lake partitioning)
4) watch-hours dashboard
5) overview dashboard

## Main commands

```powershell
python run.py                 # full pipeline
python run.py etl             # only 001/02/03
python run.py dashboards      # rebuild profile + both dashboards from existing lake
python run.py watch           # rebuild watch-hours profile + dashboard
python run.py overview        # rebuild overview data + dashboard
python run.py sync-yesterday  # rclone yesterday, then pipeline
```

Pass advanced pipeline options after `--`:

```powershell
python run.py dashboards -- --dry-run
python run.py all -- --base ".\veto Stream Logs"
python run.py all -- --base ".\data"
```

## Common options

- `--skip-etl`, `--skip-watch`, `--skip-overview`
- `--base` (defaults to env `VG_ETL_BASE`)
- `--output-root` (defaults to `output`)
- `--watch-profile` (defaults to `output\watch_hours\profile`)
- `--watch-out` (defaults to `output\watch_hours\veto_watch_hours.html`)
- `--overview-data-dir` (defaults to `output\overview`)
- `--overview-html` (defaults to `output\overview\overview_dashboard.html`)
- `--dry-run` to validate dashboards without writing
- `--etl1-prefs-file` to choose the `001.py` column preference JSON

## Path controls

Default Linode/rclone download location:

```powershell
python run.py sync-yesterday
# downloads both Linode folders, verifies local counts, then builds .\data\lake once
```

Default daily raw download layout:

```text
ETL\data\raw\Veto Logs Backup\Veto Stream Backup\MM\DD
ETL\data\raw\Veto Logs Backup\Veto fast Backup\MM\DD
```

Default remotes:

```text
veto:veto-stream-logs/veto-stream-logs/MM/DD
veto:veto-stream-logs/veto-fast-logs/MM/DD
```

After both source folders are verified, the single ETL run writes reusable outputs under `ETL\data`, including `*_parquet`, `*_final_clean.parquet`, and `ETL\data\lake`.

Choose a different local download/base folder:

```powershell
python run.py sync-yesterday -- -RawRoot "Y:\Veto Logs Backup\Raw"
python run.py sync-yesterday -- -LocalRoot "D:\Veto Logs Backup\Vs - Code Work\ETL\data"
```

Use the old one-remote behavior only when needed:

```powershell
python run.py sync-yesterday -- -SingleSourceMode -RemoteRoot "veto:veto-stream-logs/veto-stream-logs"
```

Daily download check flow:

- remote file count is captured once at start
- each source folder is synced separately
- after each sync, local file count must match that source's starting remote count
- verification retries `-VerifyRetries 3`; each mismatch reruns sync
- after verified, ETL waits `-PostVerifyDelaySeconds 60`

Optional remote stability wait for cautious scheduled runs:

```powershell
python run.py sync-yesterday -- -WaitForRemoteStable -StableChecks 2 -StableWaitMinutes 10
```

Fast/manual run switches:

```powershell
python run.py sync-yesterday -- -SkipVerifyAfterSync -SkipPostVerifyDelay
```

Choose dashboard/profile output locations:

```powershell
python run.py dashboards -- --output-root ".\output"
python run.py dashboards -- --watch-out ".\output\watch_hours\veto_watch_hours.html"
python run.py dashboards -- --overview-html ".\output\overview\overview_dashboard.html"
```

## Environment overrides used by dashboard scripts

- `VG_ETL_BASE`
- `VG_DASH_PROFILE_DIR`
- `VG_DASH_WATCH_OUT`
- `VG_DASH_OVERVIEW_BASE`

