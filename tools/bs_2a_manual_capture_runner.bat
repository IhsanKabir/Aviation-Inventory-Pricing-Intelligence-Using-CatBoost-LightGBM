@echo off
setlocal
set "ROOT=%~dp0.."
set "PYEXE=%ROOT%\.venv\Scripts\python.exe"

if not exist "%PYEXE%" (
  set "PYEXE=python"
)

"%PYEXE%" "%~dp0bs_2a_manual_capture_runner.py" %*
set "RC=%ERRORLEVEL%"
endlocal & exit /b %RC%
