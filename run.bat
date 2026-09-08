@echo off
cd /d "%~dp0"
call venv\Scripts\activate.bat

python collection.py
if errorlevel 1 exit /b 1

python loadbq.py
if errorlevel 1 exit /b 1

cd steam
dbt build