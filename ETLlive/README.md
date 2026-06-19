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

For production, prefer the server-side path:

```text
.gz log folders on server
  -> ETLlive server_gz_worker.py on the same server
  -> ClickHouse detail table for date/channel/platform/status/device queries
  -> Prometheus metrics for live counters
  -> Grafana over network
```

That means the office PC does not download raw files for live dashboards. It only opens Grafana.

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

## FAST First With ClickHouse

For near-live analytics, start with FAST only:

```powershell
.\ETLlive\run_live_once.ps1 -Source fast -RemoteLookbackDays 1 -DownloadMaxAge 2h -MaxFiles 100
```

When ClickHouse is ready, set this in `ETLlive\config\live_config.json` or a local config file:

```json
"clickhouse": {
  "enabled": true,
  "url": "http://127.0.0.1:8123",
  "database": "veto_live",
  "user": "default",
  "password": "",
  "detail_table": "live_ts_detail"
}
```

Create the schema first:

```bash
clickhouse-client --multiquery < ETLlive/sql/clickhouse_schema.sql
```

Then run a controlled FAST insert:

```powershell
.\ETLlive\run_live_once.ps1 -Source fast -RemoteLookbackDays 1 -DownloadMaxAge 2h -MaxFiles 100
```

Run continuously after the controlled test:

```powershell
.\ETLlive\run_live_loop.ps1 -Source fast -RemoteLookbackDays 1
```

Useful Grafana/ClickHouse starter queries are in:

```text
ETLlive\sql\grafana_fast_queries.sql
```

Prometheus can remain for worker health and freshness. ClickHouse should power the actual analytics charts.

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

## Server-Side `.gz` Worker

Use this when the code is deployed on the server/VM that can read the raw `.gz` log folders directly.

1. Create a local config from the template:

```powershell
Copy-Item ETLlive\config\server_live_config.example.json ETLlive\config\server_live_config.json
```

2. Edit these values in `ETLlive\config\server_live_config.json`:

```text
server_sources.stream.root
server_sources.fast.root
clickhouse.url
clickhouse.user
clickhouse.password
clickhouse.enabled
```

3. Create the ClickHouse database/tables:

```bash
clickhouse-client --multiquery < ETLlive/sql/clickhouse_schema.sql
```

4. Test one small cycle:

```powershell
.\ETLlive\run_server_gz_once.ps1 -Source all -MaxFiles 50
```

5. Run continuously:

```powershell
.\ETLlive\run_server_gz_loop.ps1
```

On Linux/Linode, use:

```bash
ETLlive/run_server_gz_once.sh all
ETLlive/run_server_gz_loop.sh
```

For a real service, adapt:

```text
ETLlive/deploy/systemd/veto-etllive-server.service
```

The server worker reads only stable `.gz` files, stores processed-file state in `ETLlive\state\server_gz_state.json`, and skips files it has already processed.

ClickHouse is the proper store for "any date over network" questions. Example query:

```sql
SELECT
    minute_ist,
    channel_name,
    platform_name,
    estimated_viewers_all_status,
    estimated_viewers_http_200
FROM veto_live.live_minute_view
WHERE log_date = '2026-06-09'
ORDER BY minute_ist, channel_name, platform_name;
```

## Important Accuracy Notes

- Live output is near-live, not final audited data.
- Files are processed only after they are older than `min_file_age_seconds`.
- If a file changes after it was processed, the worker skips the changed copy to avoid double counting. The historical daily ETL remains the final correction path.
- Estimated viewers use the same convention as the current concurrency dashboard: `.ts rows / 10` per minute, because `.ts` chunks are treated as 6 seconds.
