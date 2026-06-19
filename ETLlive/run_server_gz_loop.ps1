param(
    [ValidateSet("all", "stream", "fast")]
    [string]$Source = "all",
    [string]$Config = "",
    [switch]$SkipClickHouse,
    [int]$MaxFiles = 0,
    [int]$LookbackDays = 0
)

$ErrorActionPreference = "Stop"
$LiveRoot = $PSScriptRoot
$WorkspaceRoot = Split-Path $LiveRoot -Parent
$Python = Join-Path $WorkspaceRoot "venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = "python"
}
if (-not $Config) {
    $Config = Join-Path $LiveRoot "config\server_live_config.json"
}

$argsList = @(
    (Join-Path $LiveRoot "src\server_gz_worker.py"),
    "--loop",
    "--config", $Config,
    "--source", $Source
)
if ($SkipClickHouse) { $argsList += "--skip-clickhouse" }
if ($MaxFiles -gt 0) { $argsList += @("--max-files", [string]$MaxFiles) }
if ($LookbackDays -gt 0) { $argsList += @("--lookback-days", [string]$LookbackDays) }

& $Python @argsList
exit $LASTEXITCODE
