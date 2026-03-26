@echo off
setlocal

rem Always run from this script's folder
cd /d "%~dp0"

set "LOG=%~dp0watcher.log"
echo ================================================== >> "%LOG%"
echo [%date% %time%] Starting watcher launcher >> "%LOG%"
echo [%date% %time%] Working dir: %cd% >> "%LOG%"

set "PY="
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "%~dp0venv\Scripts\python.exe" set "PY=%~dp0venv\Scripts\python.exe"
if not defined PY set "PY=C:\Windows\py.exe"

echo [%date% %time%] Python: %PY% >> "%LOG%"

"%PY%" -3 watcher.py >> "%LOG%" 2>&1
set "EC=%ERRORLEVEL%"
echo [%date% %time%] watcher.py exited with code %EC% >> "%LOG%"
exit /b %EC%
