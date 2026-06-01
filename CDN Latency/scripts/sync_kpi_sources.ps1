param(
    [string]$OverviewSource = "Y:\Veto Logs Backup\Dashboards\OverView",
    [string]$OverviewDest = "D:\Vs - Code Work\CDN Latency\kpi_sources\overview"
)

$ErrorActionPreference = "Stop"

Write-Host "Syncing KPI source files..." -ForegroundColor Cyan
Write-Host "  Source: $OverviewSource"
Write-Host "  Dest  : $OverviewDest"

if (-not (Test-Path -LiteralPath $OverviewSource)) {
    throw "Overview source folder does not exist: $OverviewSource"
}

New-Item -ItemType Directory -Force -Path $OverviewDest | Out-Null

$files = @(
    "overview_report.xlsx",
    "device_daily.csv",
    "device_snapshot.csv"
)

foreach ($file in $files) {
    $src = Join-Path $OverviewSource $file
    $dst = Join-Path $OverviewDest $file
    if (Test-Path -LiteralPath $src) {
        Copy-Item -LiteralPath $src -Destination $dst -Force
        Write-Host ("  Copied: {0}" -f $file) -ForegroundColor Green
    } else {
        Write-Warning "Missing source file: $src"
    }
}

Write-Host "Done." -ForegroundColor Cyan
