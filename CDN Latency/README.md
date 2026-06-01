# CDN Latency Live Stack

This stack streams CDN logs from object storage into Prometheus and Grafana, and now also exports KPI snapshots from your processed Overview + Watch-Hours outputs:

`Object Storage -> consumer -> Pushgateway -> Prometheus -> Grafana`

`Processed KPI files -> kpi-exporter -> Prometheus -> Grafana`

## 1) Configure credentials

Edit `.env` and fill real values:

- `ACCESS_KEY_ID`
- `SECRET_KEY`
- `GF_SECURITY_ADMIN_PASSWORD`

Optional:

- `S3_PREFIX` to limit scan scope
- `LOG_KEY_SUFFIXES` (default `.gz`)
- `POLL_INTERVAL_SEC` (default `30`)
- `KPI_EXPORTER_REFRESH_SEC` (default `60`)

## 2) Start stack

```powershell
docker compose up -d --build
```

## 3) Open UIs

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Pushgateway: `http://localhost:9091`

Grafana auto-loads:

- Prometheus datasource (`uid=prometheus`)
- Dashboard: `CDN Live Overview`
- Dashboard: `CDN Unified Executive`

## KPI exporter sources

The `kpi-exporter` service reads:

- `./kpi_sources/overview` (local cache of overview_report.xlsx + device CSVs)
- `D:\Vs - Code Work\PC2 Full\Vs - Code Work\vglive_channel_profile\deep_profile_full` (watch-hours profile CSVs)

These mounts are configured in `docker-compose.yml`.

### Refresh overview KPI cache

Because mapped network drives (like `Y:`) are often not directly readable by Docker bind mounts on Windows, run this before `docker compose up` (or anytime source files change):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_kpi_sources.ps1
```

## Notes

- Consumer checkpoint is persisted at `/state/processed_keys.txt` via Docker volume.
- Rotate credentials immediately if any old keys were exposed.
