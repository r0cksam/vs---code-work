param(
    [int]$Port = 9108
)

$ErrorActionPreference = "Stop"
$LiveRoot = $PSScriptRoot
$WorkspaceRoot = Split-Path $LiveRoot -Parent
$Python = Join-Path $WorkspaceRoot "venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }
$Script = Join-Path $LiveRoot "src\live_metrics_server.py"
$LogDir = Join-Path $LiveRoot "logs"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$existing = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($existing) {
    $pids = ($existing | Select-Object -ExpandProperty OwningProcess -Unique) -join ","
    Write-Host "Metrics server port $Port already listening. PID(s): $pids"
    exit 0
}

$outLog = Join-Path $LogDir "metrics_server.out.log"
$errLog = Join-Path $LogDir "metrics_server.err.log"
$argText = "`"$Script`" --host 127.0.0.1 --port $Port"

$p = Start-Process -FilePath $Python -ArgumentList $argText -WorkingDirectory $WorkspaceRoot -WindowStyle Hidden -RedirectStandardOutput $outLog -RedirectStandardError $errLog -PassThru
Start-Sleep -Seconds 3
$check = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($check) {
    Write-Host "Metrics server started. PID: $($p.Id)"
    Write-Host "Metrics: http://127.0.0.1:$Port/metrics"
    Write-Host "Health : http://127.0.0.1:$Port/health"
} else {
    Write-Host "Metrics server failed to listen on $Port. PID was: $($p.Id)"
    if (Test-Path $errLog) { Get-Content $errLog -Tail 40 }
    exit 1
}
