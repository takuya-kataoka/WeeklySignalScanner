#!/usr/bin/env bash
set -eu

VENV_ROOT="/workspaces/WeeklySignalScanner-main/.venv"
STREAMLIT="$VENV_ROOT/bin/streamlit"
# default app (original)
APP="/workspaces/WeeklySignalScanner-main/WeeklySignalScanner-main/app_streamlit.py"
LOG="/workspaces/WeeklySignalScanner-main/streamlit.log"
PIDFILE="/workspaces/WeeklySignalScanner-main/streamlit.pid"
# default port (can be overridden with STREAMLIT_PORT env var)
PORT=${STREAMLIT_PORT:-8501}

usage() {
  echo "Usage: $0 {start|stop|restart|status|tail}"
  exit 1
}

start() {
  if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE") 2>/dev/null; then
    echo "Already running (PID $(cat $PIDFILE))"
    return
  fi
  if [ ! -x "$STREAMLIT" ]; then
    echo "Streamlit not found: $STREAMLIT"
    exit 1
  fi
  if [ ! -f "$APP" ]; then
    echo "App not found: $APP"
    exit 1
  fi
    nohup env STREAMLIT_BROWSER_GUESSING=false STREAMLIT_DISABLE_TELEMETRY=1 "$STREAMLIT" run "$APP" --server.port $PORT --server.headless true > "$LOG" 2>&1 & 
    echo $! > "$PIDFILE"
  echo "Started PID $(cat $PIDFILE), waiting for port $PORT..."
  for i in {1..15}; do
    ss -ltnp 2>/dev/null | grep -q ":$PORT " && break || sleep 1
  done
  if ss -ltnp 2>/dev/null | grep -q ":$PORT "; then
    echo "Listening on port $PORT"
  else
    echo "Port $PORT not open yet — check $LOG"
  fi
}

stop() {
  if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill "$PID" 2>/dev/null; then
      echo "Stopped PID $PID"
      rm -f "$PIDFILE"
      return
    fi
  fi
  if pkill -f "$STREAMLIT" 2>/dev/null; then
    echo "Stopped streamlit processes."
  else
    echo "No streamlit process found."
  fi
  rm -f "$PIDFILE" || true
}

status() {
  if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE") 2>/dev/null; then
    echo "Running PID $(cat $PIDFILE)"
  else
    echo "Not running (no pidfile or process)"
  fi
  ss -ltnp 2>/dev/null | grep -E ":$PORT\\b" || true
  echo "Log: $LOG"
}

taillog() { tail -n 200 -f "$LOG"; }

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; sleep 1; start ;;
  status) status ;;
  tail) taillog ;;
  *) usage ;;
esac
