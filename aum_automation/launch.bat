@echo off
REM AUM Automation Tool launcher (Windows)

cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
    echo Error: python not found. Install Python 3.9+ from python.org
    pause
    exit /b 1
)

where az >nul 2>&1
if errorlevel 1 (
    echo Error: Azure CLI not found.
    echo Install from: https://docs.microsoft.com/cli/azure/install-azure-cli
    pause
    exit /b 1
)

if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

if not exist logs mkdir logs
if not exist reports mkdir reports
if not exist state mkdir state

if not exist config\config.yaml (
    echo No config\config.yaml found.
    echo Copy config\config.example.yaml to config\config.yaml and fill in your subscription IDs.
    pause
    exit /b 1
)

python main_gui.py %*
