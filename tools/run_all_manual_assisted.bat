@echo off
setlocal
set "ROOT=%~dp0.."
set "PYEXE=%ROOT%\.venv\Scripts\python.exe"

if not exist "%PYEXE%" (
  set "PYEXE=python"
)

"%PYEXE%" "%~dp0run_all_manual_assisted.py" %*
set "RC=%ERRORLEVEL%"
endlocal & exit /b %RC%
