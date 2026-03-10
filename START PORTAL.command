#!/bin/bash
echo "============================================================"
echo "  AntiGravity SAP Report Portal - Starting..."
echo "============================================================"
echo ""

# Navigate to the folder where this script lives
cd "$(dirname "$0")"

echo "  Checking Python packages..."
pip3 install openpyxl reportlab uvicorn fastapi pandas python-multipart --quiet 2>/dev/null || pip install openpyxl reportlab uvicorn fastapi pandas python-multipart --quiet 2>/dev/null

echo ""
echo "  Starting portal at http://localhost:8001"
echo "  Other users on your network: http://192.168.8.128:8001"
echo ""
echo "  Tabs after login:"
echo "    SAP Reports    → /sap-reports"
echo "    Payroll        → /payroll"
echo "    Orders Backlog → /orders"
echo ""
echo "  Press Ctrl+C to stop the server"
echo "============================================================"
echo ""

# Open browser after a short delay
(sleep 2 && open "http://localhost:8001") &

# Start the unified server (run.py)
python3 run.py
