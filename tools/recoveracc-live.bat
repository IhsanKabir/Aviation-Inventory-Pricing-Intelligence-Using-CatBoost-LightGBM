@echo off
setlocal
set "ROOT=%~dp0.."
set "PYEXE=%ROOT%\.venv\Scripts\python.exe"

if not exist "%PYEXE%" (
  echo python exe not found: %PYEXE%
  exit /b 1
)

echo [recoveracc-live] LIVE recovery mode (can launch a new accumulation cycle if stale/interrupted and no active pipeline is running)
"%PYEXE%" "%~dp0recover_interrupted_accumulation.py" --mode recover --python-exe "%PYEXE%" --root "%ROOT%" --reports-dir "%ROOT%\output\reports" %*
endlocal

