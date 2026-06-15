#!/bin/bash
# Deploy script for AntiGravity-SAP report (Python FastAPI) to Ubuntu server
# Usage: ./deploy.sh
#

set -e

# Server config
SERVER_HOST="192.168.13.75"
SERVER_PORT="2222"
SERVER_USER="sdhs"
SERVER_PASS='P@$$word@2018'
REMOTE_DIR='AntiGravity-SAP-report'
CHECKSUM_DIR='AntiGravity-SAP-report/.deploy-checksums'
APP_NAME="AntiGravity-SAP-report"
APP_PORT="8002"

echo "🚀 Deploying $APP_NAME to $SERVER_HOST..."

# 1. Sync files
echo ""
echo "📦 Syncing files..."
sshpass -p "$SERVER_PASS" rsync -avz --progress \
  -e "ssh -o StrictHostKeyChecking=no -p $SERVER_PORT" \
  --exclude='venv' \
  --exclude='__pycache__' \
  --exclude='.git' \
  --exclude='.DS_Store' \
  --exclude='.env' \
  --exclude='Orders/' \
  --exclude='deploy.sh' \
  --exclude='.deploy-checksums' \
  "./" "$SERVER_USER@$SERVER_HOST:~/$REMOTE_DIR/"

# 2. Smart build & restart on server
echo ""
echo "🔨 Building and restarting on server..."
sshpass -p "$SERVER_PASS" ssh -o StrictHostKeyChecking=no -p "$SERVER_PORT" "$SERVER_USER@$SERVER_HOST" bash <<'REMOTE_SCRIPT'
  set -e
  cd ~/AntiGravity-SAP-report
  mkdir -p .deploy-checksums

  # --- Ensure virtual environment exists ---
  if [ ! -d "venv" ]; then
    echo '🐍 Creating Python virtual environment...'
    python3 -m venv venv
  fi

  # --- Smart pip install: only if requirements.txt changed ---
  NEW_REQ_HASH=$(md5sum requirements.txt 2>/dev/null | cut -d' ' -f1)
  OLD_REQ_HASH=$(cat .deploy-checksums/req.md5 2>/dev/null || echo 'none')

  if [ "$NEW_REQ_HASH" != "$OLD_REQ_HASH" ]; then
    echo '📥 requirements.txt changed — installing dependencies...'
    ./venv/bin/pip install --upgrade pip 2>&1
    ./venv/bin/pip install -r requirements.txt 2>&1
    echo "$NEW_REQ_HASH" > .deploy-checksums/req.md5
  else
    echo '✅ Dependencies unchanged — skipping pip install'
  fi

  # --- Always restart ---
  echo ''
  echo '🔄 Restarting app with PM2...'

  # Delete old process if it exists (clean slate for env vars)
  pm2 delete AntiGravity-SAP-report 2>/dev/null || true

  # Start with environment variables baked in via PM2
  USE_POSTGRES=true \
  AG_ORDERS_PORT=8002 \
  AG_DATABASE_URL="postgresql://postgres:Regency1@localhost:5432/Backlog" \
  pm2 start ./venv/bin/python \
    --name "AntiGravity-SAP-report" \
    --interpreter none \
    -- backend/orders.py 2>&1

  pm2 save 2>&1

  echo ''
  echo '✅ Deployment complete!'
  pm2 status
REMOTE_SCRIPT
