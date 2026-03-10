"""
AntiGravity Unified Server
==========================
Usage:  python3 run.py
Starts: Single server on port 8001 serving all 3 modules.

Modules:
  SAP Reports    →  http://localhost:8001/
  Payroll        →  http://localhost:8001/payroll
  Orders Backlog →  http://localhost:8001/orders
"""
import os, sys, subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

print("=" * 60)
print("  AntiGravity SAP Portal")
print("  Open:  http://localhost:8001")
print("")
print("  Tabs available after login:")
print("    SAP Reports    → http://localhost:8001/")
print("    Payroll        → http://localhost:8001/payroll")
print("    Orders Backlog → http://localhost:8001/orders")
print("")
print("  Press Ctrl+C to stop")
print("=" * 60)

try:
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        "backend.payroll:app",
        "--host", "0.0.0.0",
        "--port", "8001",
        "--reload"
    ], cwd=SCRIPT_DIR)
except KeyboardInterrupt:
    print("\nShutting down...")
