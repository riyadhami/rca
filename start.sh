#!/usr/bin/env bash
# Starts FastAPI backend and Streamlit frontend, piping each to a named log.
# Usage: bash start.sh
#
# Tip: if either service fails to start, check the log files:
#   tail -f backend.log
#   tail -f frontend.log

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo "Starting FastAPI backend on http://localhost:8000 ..."
uv run uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000 > backend.log 2>&1 &
BACKEND_PID=$!

# Give the backend a moment before starting the frontend
sleep 1

echo "Starting Streamlit frontend on http://localhost:8501 ..."
uv run streamlit run frontend/app.py --server.port 8501 > frontend.log 2>&1 &
FRONTEND_PID=$!

echo ""
echo "  Backend  → http://localhost:8000"
echo "  Frontend → http://localhost:8501"
echo ""
echo "Logs: backend.log | frontend.log"
echo "Press Ctrl+C to stop both services."

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0" INT TERM
wait
