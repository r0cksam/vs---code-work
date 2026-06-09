# ETLlive

Near-live Veto monitoring layer for stream and FAST logs.

This folder is intentionally separate from the historical `ETL` folder. The historical ETL remains the source of truth for full-day dashboards. `ETLlive` is for current-day monitoring, small rolling aggregates, and Grafana-ready metrics.

## Architecture

```text
Linode/rclone source folders
  -> local raw archive for recent UTC days
  -> normalized detail parquet with only live-needed columns
  -> exact minute aggregates
  -> Prometheus text metrics and health JSON
  -> Grafana dashboard
```

The live worker reuses channel and host mapping from `ETL/src/profile/vglive_core.py`, so stream and FAST names stay consistent with the historical dashboards.

## First target

Build a reliable microbatch worker:

- download current/recent UTC day folders for `stream` and `fast`
- process only new stable `.gz` files
- write normalized detail parquet
- rebuild current-day minute aggregates exactly
- write `output/live_metrics.prom` for Prometheus/Grafana
- write `output/live_health.json` for monitoring pipeline health

## Run Once

From `D:\Veto Logs Backup\Vs - Code Work`:

```powershell
.\venv\Scripts\python.exe ETLlive\src\live_worker.py --once
```

Dry run without downloading or writing detail parquet:

```powershell
.\venv\Scripts\python.exe ETLlive\src\live_worker.py --once --skip-download --dry-run
```

Process only one source:

```powershell
.\venv\Scripts\python.exe ETLlive\src\live_worker.py --once --source fast
.\venv\Scripts\python.exe ETLlive\src\live_worker.py --once --source stream
```

Controlled first download:

```powershell
.\ETLlive\run_live_once.ps1 -Source fast -RemoteLookbackDays 1 -DownloadMaxAge 2h -MaxFiles 100
```

## Environment

Machine-specific values belong in:

```text
ETLlive\.env
```

The template is:

```text
ETLlive\.env.example
```

`.env` is ignored by git. If it is not present, the worker uses `config/live_config.json`, bundled rclone from `ETL/tools/rclone/rclone.exe`, and `ETL/config/rclone.conf` when available.

## Run Loop

```powershell
.\ETLlive\run_live_loop.ps1
```

Default loop interval is controlled by `config/live_config.json`.

## Metrics Server

For Grafana/Prometheus testing:

```powershell
.\venv\Scripts\python.exe ETLlive\src\live_metrics_server.py
```

Endpoints:

- `http://127.0.0.1:9108/metrics`
- `http://127.0.0.1:9108/health`

## Grafana Preview

The local Grafana and Prometheus binaries live under:

```text
ETLlive\dependency
```

Start Prometheus and Grafana:

```powershell
.\ETLlive\run_live_stack.ps1
```

Open:

```text
http://127.0.0.1:3000/d/veto-live/veto-live
```

Login:

```text
admin / admin
```

This dashboard uses Prometheus scraping `http://127.0.0.1:9108/metrics`. The metrics server must be running, and charts update when `live_worker.py` or `run_live_loop.ps1` updates `live_metrics.prom`.

## Important Accuracy Notes

- Live output is near-live, not final audited data.
- Files are processed only after they are older than `min_file_age_seconds`.
- If a file changes after it was processed, the worker skips the changed copy to avoid double counting. The historical daily ETL remains the final correction path.
- Estimated viewers use the same convention as the current concurrency dashboard: `.ts rows / 10` per minute, because `.ts` chunks are treated as 6 seconds.
