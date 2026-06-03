param(
    [string]$RemoteRoot = "veto:veto-stream-logs/veto-stream-logs",
    [string]$StreamRemoteRoot = "veto:veto-stream-logs/veto-stream-logs",
    [string]$FastRemoteRoot = "veto:veto-stream-logs/veto-fast-logs",
    [string]$LocalRoot = "",
    [string]$RawRoot = "",
    [string]$StreamLocalName = "Veto Stream Backup",
    [string]$FastLocalName = "Veto fast Backup",
    [string]$PrefsFile = "",
    [string]$VenvPython = "",
    [Nullable[datetime]]$Date = $null,
    [int]$LookbackDays = 1,
    [int]$StableChecks = 2,
    [int]$StableWaitMinutes = 10,
    [int]$VerifyRetries = 3,
    [int]$VerifyWaitMinutes = 0,
    [switch]$WaitForRemoteStable,
    [int]$PostVerifyDelaySeconds = 60,
    [switch]$SkipRemoteStableCheck,
    [switch]$SkipVerifyAfterSync,
    [switch]$SkipPostVerifyDelay,
    [switch]$SkipWatch,
    [switch]$SkipOverview,
    [switch]$SingleSourceMode
)

$ErrorActionPreference = "Stop"
$WorkspaceRoot = $PSScriptRoot
if ($LocalRoot) {
    $DefaultLocalRoot = $LocalRoot
} elseif (Test-Path (Join-Path $WorkspaceRoot "data\lake")) {
    $DefaultLocalRoot = Join-Path $WorkspaceRoot "data"
} elseif (Test-Path (Join-Path $WorkspaceRoot "lake")) {
    $DefaultLocalRoot = $WorkspaceRoot
} else {
    $DefaultLocalRoot = Join-Path $WorkspaceRoot "data"
}
$RawBaseRoot = if ($RawRoot) {
    $RawRoot
} else {
    Join-Path $DefaultLocalRoot "raw\Veto Logs Backup"
}
$DefaultVenvPython = if ($VenvPython) {
    $VenvPython
} elseif (Test-Path (Join-Path $WorkspaceRoot "venv\Scripts\python.exe")) {
    Join-Path $WorkspaceRoot "venv\Scripts\python.exe"
} elseif (Test-Path (Join-Path (Split-Path $WorkspaceRoot -Parent) "venv\Scripts\python.exe")) {
    Join-Path (Split-Path $WorkspaceRoot -Parent) "venv\Scripts\python.exe"
} else {
    "python"
}
$env:VG_ETL_BASE = $DefaultLocalRoot
if (($DefaultVenvPython -ne "python") -and (-not (Test-Path $DefaultVenvPython))) { $DefaultVenvPython = "python" }
$BundledRclone = Join-Path $WorkspaceRoot "tools\rclone\rclone.exe"
$RcloneExe = if (Test-Path $BundledRclone) { $BundledRclone } else { "rclone" }
$PortableRcloneConfig = Join-Path $WorkspaceRoot "config\rclone.conf"
if (Test-Path $PortableRcloneConfig) {
    $env:RCLONE_CONFIG = $PortableRcloneConfig
}

function Invoke-Rclone {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [string]$StepName
    )

    Write-Host "[$(Get-Date -Format o)] rclone ${StepName}: $($Arguments -join ' ')"
    & $RcloneExe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "rclone $StepName failed with exit code $LASTEXITCODE"
    }
}

function Get-RcloneSize {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    Write-Host "[$(Get-Date -Format o)] Checking $Label size/count: $Path"
    $jsonLines = & $RcloneExe size $Path --json
    if ($LASTEXITCODE -ne 0) {
        throw "rclone size failed for $Label with exit code $LASTEXITCODE"
    }

    $jsonText = ($jsonLines | Out-String).Trim()
    if (-not $jsonText) {
        throw "rclone size returned empty output for $Label"
    }

    $stats = $jsonText | ConvertFrom-Json
    return [pscustomobject]@{
        Count = [int64]$stats.count
        Bytes = [int64]$stats.bytes
    }
}

function Wait-RemoteStable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RemotePath,
        [Parameter(Mandatory = $true)]
        [string]$SourceName
    )

    if ($SkipRemoteStableCheck) {
        Write-Host "[$(Get-Date -Format o)] ${SourceName}: remote stability check skipped."
        return
    }

    $needed = [Math]::Max(1, $StableChecks)
    $sameSeen = 0
    $previous = $null

    while ($true) {
        $current = Get-RcloneSize -Path $RemotePath -Label "$SourceName remote"
        Write-Host "[$(Get-Date -Format o)] ${SourceName}: remote count=$($current.Count), bytes=$($current.Bytes)"

        if ($previous -and $previous.Count -eq $current.Count -and $previous.Bytes -eq $current.Bytes) {
            $sameSeen += 1
        } else {
            $sameSeen = 1
            $previous = $current
        }

        if ($sameSeen -ge $needed) {
            Write-Host "[$(Get-Date -Format o)] ${SourceName}: remote stable after $sameSeen matching check(s)."
            return
        }

        $waitSeconds = [Math]::Max(1, $StableWaitMinutes) * 60
        Write-Host "[$(Get-Date -Format o)] ${SourceName}: remote not stable yet. Waiting $StableWaitMinutes minute(s) before next check."
        Start-Sleep -Seconds $waitSeconds
    }
}

function Sync-RemoteDay {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RemotePath,
        [Parameter(Mandatory = $true)]
        [string]$LocalPath,
        [Parameter(Mandatory = $true)]
        [string]$SourceName
    )

    Invoke-Rclone -StepName "sync" -Arguments @(
        "sync",
        $RemotePath,
        $LocalPath,
        "--size-only",
        "--transfers", "16",
        "--checkers", "32",
        "--multi-thread-streams", "4",
        "--buffer-size", "16M",
        "--retries", "5",
        "--low-level-retries", "10",
        "--stats", "30s",
        "-P"
    )
}

function Test-LocalMatchesExpectedCount {
    param(
        [Parameter(Mandatory = $true)]
        [int64]$ExpectedCount,
        [Parameter(Mandatory = $true)]
        [string]$LocalPath,
        [Parameter(Mandatory = $true)]
        [string]$RemotePath,
        [Parameter(Mandatory = $true)]
        [string]$SourceName
    )

    if ($SkipVerifyAfterSync) {
        Write-Host "[$(Get-Date -Format o)] ${SourceName}: post-sync verification skipped."
        return
    }

    $maxRetries = [Math]::Max(1, $VerifyRetries)
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        Write-Host "[$(Get-Date -Format o)] ${SourceName}: local file-count verification attempt $attempt/$maxRetries"
        $localStats = Get-RcloneSize -Path $LocalPath -Label "$SourceName local"

        Write-Host "[$(Get-Date -Format o)] ${SourceName}: expected remote count from start=$ExpectedCount"
        Write-Host "[$(Get-Date -Format o)] ${SourceName}: local count=$($localStats.Count), bytes=$($localStats.Bytes)"

        if ($localStats.Count -eq $ExpectedCount) {
            Write-Host "[$(Get-Date -Format o)] ${SourceName}: local folder verified by file count."
            return
        }

        if ($attempt -eq $maxRetries) {
            throw "local verification failed after $maxRetries attempt(s): expected remote count $ExpectedCount, local count $($localStats.Count)"
        }

        Write-Host "[$(Get-Date -Format o)] ${SourceName}: file-count mismatch. Re-running sync."
        Sync-RemoteDay -RemotePath $RemotePath -LocalPath $LocalPath -SourceName $SourceName
        if ($VerifyWaitMinutes -gt 0) {
            Write-Host "[$(Get-Date -Format o)] ${SourceName}: waiting $VerifyWaitMinutes minute(s) before next verification."
            Start-Sleep -Seconds ($VerifyWaitMinutes * 60)
        }
    }
}

# Set local date to fetch. Default = yesterday (-1), so if it runs today it copies yesterday.
if ($Date) {
    $TargetDate = $Date.Date
} else {
    $TargetDate = (Get-Date).AddDays(-1 * [Math]::Abs($LookbackDays))
}
$month = $TargetDate.ToString("MM")
$day = $TargetDate.ToString("dd")

$sources = @()
if ($SingleSourceMode) {
    $sources += [pscustomobject]@{
        Name = "single"
        RemoteRoot = $RemoteRoot
        LocalRoot = $DefaultLocalRoot
    }
} else {
    $sources += [pscustomobject]@{
        Name = "stream"
        RemoteRoot = $StreamRemoteRoot
        LocalRoot = Join-Path $RawBaseRoot $StreamLocalName
    }
    $sources += [pscustomobject]@{
        Name = "fast"
        RemoteRoot = $FastRemoteRoot
        LocalRoot = Join-Path $RawBaseRoot $FastLocalName
    }
}

$logFolder = Join-Path $WorkspaceRoot "output\logs"
$logFile = Join-Path $logFolder ("run_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))

New-Item -ItemType Directory -Path $logFolder -Force | Out-Null
foreach ($source in $sources) {
    $sourceLocalPath = Join-Path $source.LocalRoot (Join-Path $month $day)
    New-Item -ItemType Directory -Path $sourceLocalPath -Force | Out-Null
}

Start-Transcript -Path $logFile -Append | Out-Null

try {
    Write-Host "[$(Get-Date -Format o)] Target date : $($TargetDate.ToString('yyyy-MM-dd'))"
    Write-Host "[$(Get-Date -Format o)] ETL base    : $DefaultLocalRoot"
    if (-not $SingleSourceMode) {
        Write-Host "[$(Get-Date -Format o)] Raw root    : $RawBaseRoot"
    }

    foreach ($source in $sources) {
        $sourceRemoteRoot = $source.RemoteRoot.TrimEnd("/")
        $remotePath = "$sourceRemoteRoot/$month/$day"
        $localPath = Join-Path $source.LocalRoot (Join-Path $month $day)

        Write-Host "[$(Get-Date -Format o)] [$($source.Name)] Target remote: $remotePath"
        Write-Host "[$(Get-Date -Format o)] [$($source.Name)] Target local : $localPath"

        if ($WaitForRemoteStable) {
            Wait-RemoteStable -RemotePath $remotePath -SourceName $source.Name
        } else {
            Write-Host "[$(Get-Date -Format o)] [$($source.Name)] Remote stability wait skipped by default. Capturing remote file count once."
        }

        $remoteStartStats = Get-RcloneSize -Path $remotePath -Label "$($source.Name) remote"
        if ($remoteStartStats.Count -le 0) {
            throw "remote file count is zero for $remotePath"
        }
        Write-Host "[$(Get-Date -Format o)] [$($source.Name)] Start remote count=$($remoteStartStats.Count), bytes=$($remoteStartStats.Bytes)"

        Sync-RemoteDay -RemotePath $remotePath -LocalPath $localPath -SourceName $source.Name
        Write-Host "[$(Get-Date -Format o)] [$($source.Name)] rclone sync done."
        Test-LocalMatchesExpectedCount -ExpectedCount $remoteStartStats.Count -LocalPath $localPath -RemotePath $remotePath -SourceName $source.Name
    }

    if (-not $SkipPostVerifyDelay) {
        Write-Host "[$(Get-Date -Format o)] All source folders verified. Waiting $PostVerifyDelaySeconds second(s) before ETL."
        Start-Sleep -Seconds ([Math]::Max(0, $PostVerifyDelaySeconds))
    } else {
        Write-Host "[$(Get-Date -Format o)] Post-verify delay skipped."
    }

    $env:VG_ETL_BASE = $DefaultLocalRoot
    $pipeline = Join-Path $PSScriptRoot "src\orchestrator\run_pipeline.py"
    $watchArgs = @()
    if ($SkipWatch) { $watchArgs += "--skip-watch" }
    if ($SkipOverview) { $watchArgs += "--skip-overview" }

    $pipelineArgs = @("--base", $DefaultLocalRoot)
    if (-not $SingleSourceMode) {
        $pipelineArgs += @(
            "--etl1-daily-date", $TargetDate.ToString("yyyy-MM-dd"),
            "--etl1-daily-raw-root", $RawBaseRoot,
            "--etl1-stream-name", $StreamLocalName,
            "--etl1-fast-name", $FastLocalName
        )
    }
    if ($PrefsFile) {
        $pipelineArgs += @("--etl1-prefs-file", $PrefsFile)
    }
    $pipelineArgs += $watchArgs

    $cmd = @($DefaultVenvPython, $pipeline) + $pipelineArgs
    Write-Host "[$(Get-Date -Format o)] Running: $cmd"
    & $DefaultVenvPython $pipeline @pipelineArgs
    if ($LASTEXITCODE -ne 0) { throw "run_pipeline.py failed with exit code $LASTEXITCODE" }

    Write-Host "[$(Get-Date -Format o)] Pipeline completed."
} catch {
    Write-Error "[FAILED] $($_.Exception.Message)"
    throw
} finally {
    Stop-Transcript | Out-Null
}
