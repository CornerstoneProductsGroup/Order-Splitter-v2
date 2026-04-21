@echo off
setlocal

rem CSV watcher only: Depot + Lowe's FedEx CSV (outputs + archives).
rem PDF watcher (packing slips + WorldShip labels) stays off for this process.

cd /d "%~dp0"

set "LOG=%~dp0watcher_csv_only.log"
echo ================================================== >> "%LOG%"
echo [%date% %time%] Starting CSV-only watcher launcher >> "%LOG%"
echo [%date% %time%] Working dir: %cd% >> "%LOG%"

set "PY=%~dp0.venv\Scripts\python.exe"
if not exist "%PY%" (
	set "PY=%~dp0venv\Scripts\python.exe"
)

if not exist "%PY%" (
	echo [%date% %time%] ERROR: No project venv python found. Expected .venv\Scripts\python.exe or venv\Scripts\python.exe >> "%LOG%"
	exit /b 2
)

echo [%date% %time%] Python: %PY% >> "%LOG%"

set "ORDER_SPLITTER_DISABLE_PDF_WATCH=1"
set "ORDER_SPLITTER_DISABLE_LABEL_WATCH=1"
"%PY%" watcher.py --pdf-off >> "%LOG%" 2>&1
set "EC=%ERRORLEVEL%"
echo [%date% %time%] watcher.py exited with code %EC% >> "%LOG%"
exit /b %EC%
