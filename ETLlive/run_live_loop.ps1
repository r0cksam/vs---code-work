param(
    [ValidateSet("all", "stream", "fast")]
    [string]$Source = "all",
    [switch]$SkipDownload,
    [switch]$SkipClickHouse,
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
    "--loop",
    "--source", $Source
)
if ($SkipDownload) { $argsList += "--skip-download" }
if ($SkipClickHouse) { $argsList += "--skip-clickhouse" }
if ($RemoteLookbackDays -gt 0) { $argsList += @("--remote-lookback-days", [string]$RemoteLookbackDays) }
if ($DownloadMaxAge) { $argsList += @("--download-max-age", $DownloadMaxAge) }

& $Python @argsList
exit $LASTEXITCODE
