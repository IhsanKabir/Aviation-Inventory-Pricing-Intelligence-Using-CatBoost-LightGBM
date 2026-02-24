@echo off
setlocal
set "ROOT=%~dp0.."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0verify_accumulation_runtime.ps1" -Root "%ROOT%" %*
endlocal

