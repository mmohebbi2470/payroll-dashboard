#!/bin/bash
echo "============================================================"
echo "  AntiGravity SAP Report Portal - Starting..."
echo "============================================================"
echo ""

# Navigate to the folder where this script lives
cd "$(dirname "$0")"

echo "  Checking Python packages..."
pip3 install openpyxl reportlab --quiet 2>/dev/null || pip install openpyxl reportlab --quiet 2>/dev/null

echo ""
echo "  Starting portal at http://localhost:8080"
echo "  Other users on your network: http://192.168.8.75:8080"
echo ""
echo "  Press Ctrl+C to stop the server"
echo "============================================================"
echo ""

# Open browser after a short delay
(sleep 2 && open "http://localhost:8080") &

# Start the portal
python3 portal.py

