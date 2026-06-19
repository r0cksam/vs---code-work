param(
    [int]$Port = 3000
)

$ErrorActionPreference = "Stop"
$LiveRoot = $PSScriptRoot
$GrafanaHome = Join-Path $LiveRoot "dependency\grafana"
$GrafanaExe = Join-Path $GrafanaHome "bin\grafana.exe"
$Config = Join-Path $LiveRoot "config\grafana.ini"
$LogDir = Join-Path $LiveRoot "logs"
$DataDir = Join-Path $LiveRoot "runtime\grafana-data"
$PluginDir = Join-Path $LiveRoot "runtime\grafana-plugins"

if (-not (Test-Path $GrafanaExe)) { throw "Grafana not found: $GrafanaExe" }
if (-not (Test-Path $Config)) { throw "Grafana config not found: $Config" }

New-Item -ItemType Directory -Path $LogDir, $DataDir, $PluginDir -Force | Out-Null

$existing = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($existing) {
    $pids = ($existing | Select-Object -ExpandProperty OwningProcess -Unique) -join ","
    Write-Host "Grafana port $Port already listening. PID(s): $pids"
    exit 0
}

$outLog = Join-Path $LogDir "grafana.out.log"
$errLog = Join-Path $LogDir "grafana.err.log"
$argText = @(
    "server",
    "--homepath", "`"$GrafanaHome`"",
    "--config", "`"$Config`""
) -join " "

$p = Start-Process -FilePath $GrafanaExe -ArgumentList $argText -WorkingDirectory $GrafanaHome -WindowStyle Hidden -RedirectStandardOutput $outLog -RedirectStandardError $errLog -PassThru
Start-Sleep -Seconds 8
$check = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($check) {
    Write-Host "Grafana started. PID: $($p.Id)"
    Write-Host "URL: http://127.0.0.1:$Port"
    Write-Host "Login: admin / admin"
} else {
    Write-Host "Grafana failed to listen on $Port. PID was: $($p.Id)"
    if (Test-Path $errLog) { Get-Content $errLog -Tail 80 }
    exit 1
}
