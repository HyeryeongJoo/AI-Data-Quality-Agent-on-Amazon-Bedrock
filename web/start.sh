#!/bin/bash
# Start the DQ Agent Web application
# Backend: FastAPI on port 8001
# Frontend: served from backend as static files

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting DQ Agent Web backend on port 8001..."
cd "$DIR/backend"

# Activate agent venv if available (for ai_dq_agent imports)
AGENT_VENV="${AGENT_VENV_PATH:-$(dirname "$DIR")/agent/.venv/bin/activate}"
if [ -f "$AGENT_VENV" ]; then
    source "$AGENT_VENV"
fi

exec python -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
