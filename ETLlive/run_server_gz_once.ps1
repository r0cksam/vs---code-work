param(
    [ValidateSet("all", "stream", "fast")]
    [string]$Source = "all",
    [string]$Config = "",
    [switch]$DryRun,
    [switch]$SkipClickHouse,
    [int]$MaxFiles = 0,
    [int]$LookbackDays = 0,
    [string]$Date = ""
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
    "--once",
    "--config", $Config,
    "--source", $Source
)
if ($DryRun) { $argsList += "--dry-run" }
if ($SkipClickHouse) { $argsList += "--skip-clickhouse" }
if ($MaxFiles -gt 0) { $argsList += @("--max-files", [string]$MaxFiles) }
if ($LookbackDays -gt 0) { $argsList += @("--lookback-days", [string]$LookbackDays) }
if ($Date) { $argsList += @("--date", $Date) }

& $Python @argsList
exit $LASTEXITCODE
