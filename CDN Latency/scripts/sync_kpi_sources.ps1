param(
    [string]$EtlRoot = "Z:\Vs - Code Work\ETL",
    [string]$Destination = "D:\Vs - Code Work\CDN Latency\kpi_sources"
)

$ErrorActionPreference = "Stop"

Write-Host "Syncing KPI source files..." -ForegroundColor Cyan
Write-Host "  ETL root: $EtlRoot"
Write-Host "  Dest    : $Destination"

if (-not (Test-Path -LiteralPath $EtlRoot)) {
    throw "ETL root folder does not exist: $EtlRoot"
}

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python launcher 'py' was not found."
}

$script = Join-Path $PSScriptRoot "sync_kpi_sources.py"
& py $script --etl-root $EtlRoot --destination $Destination
if ($LASTEXITCODE -ne 0) {
    throw "KPI source sync failed with exit code $LASTEXITCODE"
}

Write-Host "Done." -ForegroundColor Cyan
