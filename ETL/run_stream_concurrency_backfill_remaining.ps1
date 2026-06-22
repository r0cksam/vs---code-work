param(
    [string]$StartFrom = "2026-05-20"
)

$ErrorActionPreference = "Stop"

$EtlRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $EtlRoot
$LogDir = Join-Path $EtlRoot "output\logs\concurrency"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$TranscriptPath = Join-Path $LogDir "stream_concurrency_backfill_remaining_$Stamp.log"

$Chunks = @(
    @("2026-05-20", "2026-05-26"),
    @("2026-05-27", "2026-06-02"),
    @("2026-06-03", "2026-06-09"),
    @("2026-06-10", "2026-06-16"),
    @("2026-06-17", "2026-06-20")
)

Start-Transcript -Path $TranscriptPath -Force | Out-Null
try {
    Set-Location -LiteralPath $RepoRoot
    Write-Host "ETL root: $EtlRoot"
    Write-Host "Log     : $TranscriptPath"
    Write-Host "StartFrom: $StartFrom"

    foreach ($Chunk in $Chunks) {
        $Start = $Chunk[0]
        $End = $Chunk[1]
        if ($End -lt $StartFrom) {
            Write-Host "[skip] $Start to $End before StartFrom."
            continue
        }

        Write-Host "[run] STREAM concurrency $Start to $End"
        & ".\venv\Scripts\python.exe" "ETL\src\tools\build_concurrency.py" `
            --source stream `
            --start $Start `
            --end $End `
            --threads 6 `
            --memory-limit 16GB
        if ($LASTEXITCODE -ne 0) {
            throw "STREAM concurrency backfill failed for $Start to $End with exit code $LASTEXITCODE"
        }
    }

    Write-Host "STREAM concurrency remaining backfill completed."
}
finally {
    Stop-Transcript | Out-Null
}
