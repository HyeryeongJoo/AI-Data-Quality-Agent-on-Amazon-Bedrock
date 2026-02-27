#!/bin/bash
# Start the DQ Agent Web application
# Backend: FastAPI on port 8001
# Frontend: served from backend as static files

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="${AGENT_DIR:-$(cd "$DIR/../agent" && pwd)}"
export AGENT_CODE_PATH="$AGENT_DIR"

echo "Starting DQ Agent Web backend on port 8001..."
cd "$DIR/backend"

# Activate agent venv if available (for ai_dq_agent imports)
if [ -f "$AGENT_DIR/.venv/bin/activate" ]; then
    source "$AGENT_DIR/.venv/bin/activate"
fi

exec python -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
