param(
    [string]$RepoRoot = "",
    [string]$PythonExe = "",
    [string]$ResultOut = "",
    [string[]]$RunAllArgs = @("--limit-dates", "1", "--ingest", "--non-interactive", "--stop-on-error")
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

if (-not $PythonExe) {
    $venvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPy) {
        $PythonExe = $venvPy
    } else {
        $PythonExe = "python"
    }
}

$runAll = Join-Path $RepoRoot "tools\run_all_manual_assisted.py"
if (-not (Test-Path $runAll)) {
    throw "run_all_manual_assisted.py not found at $runAll"
}

$queueRuns = Join-Path $RepoRoot "output\manual_sessions\queue_runs"
New-Item -ItemType Directory -Path $queueRuns -Force | Out-Null

if (-not $ResultOut) {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss_UTC")
    $ResultOut = Join-Path $queueRuns "run_all_manual_assisted_pad_$stamp.json"
}

$cmd = @($runAll, "--result-out", $ResultOut) + $RunAllArgs

Write-Host "[pad-runner] repo_root=$RepoRoot"
Write-Host "[pad-runner] python=$PythonExe"
Write-Host "[pad-runner] result_out=$ResultOut"
Write-Host "[pad-runner] args=$($RunAllArgs -join ' ')"

Push-Location $RepoRoot
try {
    & $PythonExe @cmd
    $rc = $LASTEXITCODE
} finally {
    Pop-Location
}

if (-not (Test-Path $ResultOut)) {
    Write-Error "[pad-runner] Result summary not found: $ResultOut"
    exit 3
}

$summary = Get-Content $ResultOut -Raw | ConvertFrom-Json
$failed = @($summary.results | Where-Object { $_.status -eq "failed" }).Count
$ran = @($summary.results | Where-Object { $_.status -in @("ok", "failed") }).Count

[pscustomobject]@{
    ok = [bool]$summary.ok
    return_code = [int]$rc
    result_out = $ResultOut
    families_ran = [int]$ran
    families_failed = [int]$failed
} | ConvertTo-Json -Compress | Write-Output

exit $rc
