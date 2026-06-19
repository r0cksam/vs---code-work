param(
    [int]$Port = 9090
)

$ErrorActionPreference = "Stop"
$LiveRoot = $PSScriptRoot
$PromDir = Join-Path $LiveRoot "dependency\prometheus\prometheus-3.12.0.windows-amd64"
$PromExe = Join-Path $PromDir "prometheus.exe"
$Config = Join-Path $LiveRoot "config\prometheus.yml"
$DataDir = Join-Path $LiveRoot "runtime\prometheus-data"
$LogDir = Join-Path $LiveRoot "logs"

if (-not (Test-Path $PromExe)) { throw "Prometheus not found: $PromExe" }
if (-not (Test-Path $Config)) { throw "Prometheus config not found: $Config" }

New-Item -ItemType Directory -Path $DataDir, $LogDir -Force | Out-Null

$existing = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($existing) {
    $pids = ($existing | Select-Object -ExpandProperty OwningProcess -Unique) -join ","
    Write-Host "Prometheus port $Port already listening. PID(s): $pids"
    exit 0
}

$outLog = Join-Path $LogDir "prometheus.out.log"
$errLog = Join-Path $LogDir "prometheus.err.log"
$argText = @(
    "--config.file=`"$Config`"",
    "--storage.tsdb.path=`"$DataDir`"",
    "--web.listen-address=127.0.0.1:$Port",
    "--web.enable-lifecycle"
) -join " "

$p = Start-Process -FilePath $PromExe -ArgumentList $argText -WorkingDirectory $PromDir -WindowStyle Hidden -RedirectStandardOutput $outLog -RedirectStandardError $errLog -PassThru
Start-Sleep -Seconds 3
$check = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($check) {
    Write-Host "Prometheus started. PID: $($p.Id)"
    Write-Host "URL: http://127.0.0.1:$Port"
} else {
    Write-Host "Prometheus failed to listen on $Port. PID was: $($p.Id)"
    if (Test-Path $errLog) { Get-Content $errLog -Tail 40 }
    exit 1
}
