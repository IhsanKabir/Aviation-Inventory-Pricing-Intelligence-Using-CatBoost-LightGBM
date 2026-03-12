param(
    [Parameter(Mandatory = $true)]
    [string]$TaskName,
    [Parameter(Mandatory = $true)]
    [string]$BatchPath,
    [int]$DelayMinutes = 90,
    [int]$ExecutionTimeLimitHours = 8,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

if ($DelayMinutes -lt 1) {
    throw "DelayMinutes must be >= 1"
}

$batchFullPath = (Resolve-Path $BatchPath).Path
$now = Get-Date
$nextRun = $now.AddMinutes($DelayMinutes)

if ($WhatIf) {
    Write-Host "[WhatIf] Reschedule task '$TaskName' for $($nextRun.ToString('yyyy-MM-dd HH:mm:ss'))"
    exit 0
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batchFullPath`""
$trigger = New-ScheduledTaskTrigger -Once -At $nextRun
$settings = New-ScheduledTaskSettingsSet `
    -WakeToRun `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours $ExecutionTimeLimitHours)

$task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null

Write-Host "Rescheduled task '$TaskName' for $($nextRun.ToString('yyyy-MM-dd HH:mm:ss'))"
