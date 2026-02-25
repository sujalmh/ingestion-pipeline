#!/bin/bash
# Deploy ingestion pipeline to VM
# Run this from your Mac: ./deploy.sh

VM="clouduser@100.104.12.231"
REMOTE_DIR="/data/ishaan-feature/ingestion-pipeline"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Deploying Ingestion Pipeline to VM ==="
echo "Local:  $LOCAL_DIR"
echo "Remote: $VM:$REMOTE_DIR"
echo ""

# Copy updated files
echo "Copying app files..."
scp "$LOCAL_DIR/app/main.py" \
    "$LOCAL_DIR/app/dashboard.html" \
    "$LOCAL_DIR/app/approve.html" \
    "$LOCAL_DIR/app/upload.html" \
    "$LOCAL_DIR/app/models/db.py" \
    "$VM:$REMOTE_DIR/app/"

echo "Copying services..."
scp "$LOCAL_DIR/app/services/vector_adapter.py" \
    "$LOCAL_DIR/app/services/sql_adapter.py" \
    "$LOCAL_DIR/app/services/pdf_splitter.py" \
    "$VM:$REMOTE_DIR/app/services/"

echo "Copying config..."
scp "$LOCAL_DIR/app/config/settings.py" "$VM:$REMOTE_DIR/app/config/"
scp "$LOCAL_DIR/requirements.txt" "$VM:$REMOTE_DIR/"

echo ""
echo "Installing new dependency (pymupdf)..."
ssh "$VM" "cd $REMOTE_DIR && source venv/bin/activate 2>/dev/null || source .venv/bin/activate 2>/dev/null; pip install pymupdf"

echo ""
echo "Restarting server..."
ssh "$VM" "cd $REMOTE_DIR && lsof -ti:8073 | xargs kill -9 2>/dev/null; sleep 2; source venv/bin/activate 2>/dev/null || source .venv/bin/activate 2>/dev/null; nohup python -m uvicorn app.main:app --host 0.0.0.0 --port 8073 > server.log 2>&1 &"

sleep 3
echo ""
echo "Checking health..."
curl -s http://100.104.12.231:8073/health | python3 -m json.tool
echo ""
echo "=== Done ==="
echo "Dashboard:     http://100.104.12.231:8073/dashboard"
echo "Upload Page:   http://100.104.12.231:8073/upload"
echo "Approval Page: http://100.104.12.231:8073/approve"
