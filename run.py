import os
import sys
import subprocess
import argparse
import time

def main():
    parser = argparse.ArgumentParser(description="Run AntiGravity Unified Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host IP to bind to")
    parser.add_argument("--port", default="8001", help="Port for the unified server")
    args = parser.parse_args()

    # Pass the arguments to the environment variables
    env = os.environ.copy()
    env["AG_HOST"] = args.host
    env["AG_PAYROLL_PORT"] = args.port

    print("=" * 60)
    print("  Starting AntiGravity Unified Server")
    print("=" * 60)
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print("=" * 60)
    
    try:
        print("[run.py] Starting Unified Backend...")
        subprocess.run(
            [sys.executable, "backend/payroll.py"], 
            env=env
        )
            
    except KeyboardInterrupt:
        print("\nShutting down service...")
    finally:
        print("Done.")

if __name__ == "__main__":
    main()
