@echo off
setlocal

set VENV_DIR=.venv

if not exist "%VENV_DIR%" (
    echo Virtual environment not found. Run setup.bat first.
    exit /b 1
)

call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
    echo Failed to activate virtual environment.
    exit /b 1
)

echo Starting FastAPI server...
uvicorn app.main:app --host 0.0.0.0 --port 8000

