@echo off
setlocal
set "ROOT=%~dp0.."
set "PYEXE=%ROOT%\.venv\Scripts\python.exe"

if not exist "%PYEXE%" (
  set "PYEXE=python"
)

"%PYEXE%" "%~dp0build_manual_capture_queues.py" %*
set "RC=%ERRORLEVEL%"
endlocal & exit /b %RC%

