param(
    [ValidateSet("all", "stream", "fast")]
    [string]$Source = "all",
    [switch]$SkipDownload,
    [switch]$DryRun,
    [switch]$SkipClickHouse,
    [int]$MaxFiles = 0,
    [int]$RemoteLookbackDays = 0,
    [string]$DownloadMaxAge = ""
)

$ErrorActionPreference = "Stop"
$LiveRoot = $PSScriptRoot
$WorkspaceRoot = Split-Path $LiveRoot -Parent
$Python = Join-Path $WorkspaceRoot "venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = "python"
}

$argsList = @(
    (Join-Path $LiveRoot "src\live_worker.py"),
    "--once",
    "--source", $Source
)
if ($SkipDownload) { $argsList += "--skip-download" }
if ($DryRun) { $argsList += "--dry-run" }
if ($SkipClickHouse) { $argsList += "--skip-clickhouse" }
if ($MaxFiles -gt 0) { $argsList += @("--max-files", [string]$MaxFiles) }
if ($RemoteLookbackDays -gt 0) { $argsList += @("--remote-lookback-days", [string]$RemoteLookbackDays) }
if ($DownloadMaxAge) { $argsList += @("--download-max-age", $DownloadMaxAge) }

& $Python @argsList
exit $LASTEXITCODE
