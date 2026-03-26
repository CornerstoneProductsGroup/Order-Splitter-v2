@echo off
setlocal

cd /d "%~dp0"

set "LOG=%~dp0watcher_pdf_only.log"
echo ================================================== >> "%LOG%"
echo [%date% %time%] Starting PDF-only watcher launcher >> "%LOG%"
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
set "ORDER_SPLITTER_DISABLE_CSV_WATCH=1"
"%PY%" watcher.py >> "%LOG%" 2>&1
set "EC=%ERRORLEVEL%"
echo [%date% %time%] watcher.py exited with code %EC% >> "%LOG%"
exit /b %EC%
