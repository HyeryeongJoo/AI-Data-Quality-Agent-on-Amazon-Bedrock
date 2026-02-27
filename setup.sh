#!/bin/bash
# One-command setup for Bedrock DQ Agent
# Usage: ./setup.sh [--skip-aws]
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKIP_AWS=false

for arg in "$@"; do
    case $arg in
        --skip-aws) SKIP_AWS=true ;;
    esac
done

echo "============================================"
echo " Bedrock DQ Agent — Setup"
echo "============================================"
echo ""

# ---- Step 1: Prerequisites ----
echo "[1/5] Checking prerequisites..."
bash "$SCRIPT_DIR/scripts/check-prereqs.sh" || {
    echo ""
    echo "Please fix the prerequisites above and re-run ./setup.sh"
    exit 1
}
echo ""

# ---- Step 2: .env ----
echo "[2/5] Checking .env configuration..."
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
    echo ""
    echo "  Created .env from .env.example"
    echo "  >>> Please edit .env with your AWS settings, then re-run ./setup.sh <<<"
    echo ""
    exit 0
fi
echo "  .env exists"
echo ""

# ---- Step 3: Agent Python setup ----
echo "[3/5] Setting up agent (Python)..."
cd "$SCRIPT_DIR/agent"
if [ ! -d ".venv" ]; then
    python3.12 -m venv .venv
    echo "  Created virtual environment"
fi
source .venv/bin/activate
pip install -q -e ".[dev]"
echo "  Agent dependencies installed"
cd "$SCRIPT_DIR"
echo ""

# ---- Step 4: Frontend build ----
echo "[4/5] Building frontend (React)..."
cd "$SCRIPT_DIR/web/frontend"
npm install --silent 2>&1 | tail -1
npm run build 2>&1 | tail -1
echo "  Frontend built successfully"
cd "$SCRIPT_DIR"
echo ""

# ---- Step 5: AWS resources ----
if [ "$SKIP_AWS" = true ]; then
    echo "[5/5] Skipping AWS resource creation (--skip-aws)"
else
    echo "[5/5] Creating AWS resources..."
    bash "$SCRIPT_DIR/scripts/setup-aws.sh"
    echo ""

    # Upload sample data
    echo "  Uploading sample data..."
    bash "$SCRIPT_DIR/scripts/upload-sample-data.sh"
fi

echo ""
echo "============================================"
echo " Setup Complete!"
echo "============================================"
echo ""
echo " Start the web dashboard:"
echo "   ./web/start.sh"
echo ""
echo " Then open http://localhost:8001"
echo ""
