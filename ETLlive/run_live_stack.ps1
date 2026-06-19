param()

$ErrorActionPreference = "Stop"
$LiveRoot = $PSScriptRoot

& (Join-Path $LiveRoot "run_metrics_server.ps1")
& (Join-Path $LiveRoot "run_prometheus.ps1")
& (Join-Path $LiveRoot "run_grafana.ps1")

Write-Host ""
Write-Host "Veto Live Grafana:"
Write-Host "  http://127.0.0.1:3000/d/veto-live/veto-live"
Write-Host "Login:"
Write-Host "  admin / admin"
