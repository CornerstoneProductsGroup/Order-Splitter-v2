@echo off
setlocal

rem Always run from this script's folder
cd /d "%~dp0"

set "LOG=%~dp0csv_only_watcher.log"
echo ================================================== >> "%LOG%"
echo [%date% %time%] Starting CSV-only launcher >> "%LOG%"
echo [%date% %time%] Working dir: %cd% >> "%LOG%"

set "PY=%~dp0.venv\Scripts\python.exe"
if not exist "%PY%" (
	set "PY=%~dp0venv\Scripts\python.exe"
)

if not exist "%PY%" (
	echo [%date% %time%] ERROR: No project venv python found. Expected .venv\Scripts\python.exe or venv\Scripts\python.exe >> "%LOG%"
	echo [%date% %time%] ERROR: Create venv and install requirements before running scheduled task. >> "%LOG%"
	exit /b 2
)

echo [%date% %time%] Python: %PY% >> "%LOG%"

"%PY%" csv_only_watcher.py >> "%LOG%" 2>&1
set "EC=%ERRORLEVEL%"
echo [%date% %time%] csv_only_watcher.py exited with code %EC% >> "%LOG%"
exit /b %EC%
