param(
    [int]$ApiLimit = -1,
    [double]$SleepMinSeconds = 2,
    [double]$SleepMaxSeconds = 5,
    [switch]$SkipAudienceOpsRefresh
)

$ErrorActionPreference = "Stop"

$EtlRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $EtlRoot
$LogDir = Join-Path $EtlRoot "output\logs\ua_decode"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$TranscriptPath = Join-Path $LogDir "full_ua_api_decode_$Stamp.log"

Start-Transcript -Path $TranscriptPath -Force | Out-Null
try {
    Set-Location -LiteralPath $RepoRoot
    Write-Host "ETL root: $EtlRoot"
    Write-Host "Log     : $TranscriptPath"
    Write-Host "Step 1  : Fill all-distinct UA API cache"
    & ".\venv\Scripts\python.exe" "ETL\src\tools\decode_all_distinct_ua_api.py" `
        --api-limit $ApiLimit `
        --api-sleep-min-seconds $SleepMinSeconds `
        --api-sleep-max-seconds $SleepMaxSeconds
    if ($LASTEXITCODE -ne 0) {
        throw "decode_all_distinct_ua_api.py failed with exit code $LASTEXITCODE"
    }

    Write-Host "Step 2  : Regenerate production UA lookup from all available caches"
    & ".\venv\Scripts\python.exe" "ETL\src\tools\decode_distinct_ua_lookup.py" --api-limit 0
    if ($LASTEXITCODE -ne 0) {
        throw "decode_distinct_ua_lookup.py failed with exit code $LASTEXITCODE"
    }

    if (-not $SkipAudienceOpsRefresh) {
        Write-Host "Step 3  : Refresh Veto Audience Operations dashboard"
        & ".\venv\Scripts\python.exe" "ETL\src\dashboards\audienceOpsDashboard\generate_audience_ops.py"
        if ($LASTEXITCODE -ne 0) {
            throw "generate_audience_ops.py failed with exit code $LASTEXITCODE"
        }
    }

    Write-Host "Full UA API decode wrapper completed."
}
finally {
    Stop-Transcript | Out-Null
}
