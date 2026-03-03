@echo off
echo ============================================================
echo   AntiGravity SAP Report Portal - Starting...
echo ============================================================
echo.
echo   Make sure Python is installed with openpyxl and reportlab:
echo     pip install openpyxl reportlab
echo.
echo   Opening browser in 3 seconds...
echo.

cd /d "%~dp0"
start "" /b timeout /t 3 /nobreak >nul & start http://localhost:8080
python portal.py

pause
