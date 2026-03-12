param(
    [string]$TaskName = "AirlineIntel_TrainingEnrichment",
    [string]$StartTime = "01:30",
    [int]$RepeatMinutes = 1440,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_training_enrichment_once.bat"
if (-not (Test-Path $batchPath)) {
    throw "Training enrichment wrapper not found: $batchPath"
}
if ($RepeatMinutes -lt 60) {
    throw "RepeatMinutes must be >= 60"
}

function Parse-Time {
    param([string]$Value)
    try {
        return [datetime]::ParseExact($Value, "HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        throw "Invalid time format '$Value'. Expected HH:mm."
    }
}

function Register-TrainingTask {
    param(
        [string]$Name,
        [string]$TargetBatch,
        [datetime]$At
    )
    $now = Get-Date
    $anchor = Get-Date -Hour $At.Hour -Minute $At.Minute -Second 0
    if ($anchor -lt $now) {
        $anchor = $anchor.AddDays(1)
    }

    if ($WhatIf) {
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $Name (initial one-shot at $($anchor.ToString('yyyy-MM-dd HH:mm')))"
        return
    }

    $arg = "/c `"$TargetBatch`""
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg
    $trigger = New-ScheduledTaskTrigger -Once -At $anchor

    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 16)

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
    Write-Host "Training enrichment task ensured: $Name"
}

function Show-TaskSummary {
    param([string]$Name)
    if ($WhatIf) {
        return
    }
    Write-Host ""
    & schtasks.exe /Query /TN $Name /FO LIST /V | Select-String -Pattern "TaskName:|Status:|Next Run Time:|Repeat: Every:|Task To Run:|Run As User:|Logon Mode:" | ForEach-Object {
        Write-Host "  $($_.Line.Trim())"
    }
}

$startAt = Parse-Time $StartTime
Register-TrainingTask -Name $TaskName -TargetBatch $batchPath -At $startAt
Show-TaskSummary -Name $TaskName

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. Training enrichment autorun is installed for current user context."
    Write-Host "This task is finish-driven: the wrapper reschedules the next run after completion + buffer."
    Write-Host "Main command:"
    Write-Host "  $batchPath"
}
