#!/usr/bin/env bash
set -e
cd .
nohup /workspaces/WeeklySignalScanner-main/.venv/bin/python WeeklySignalScanner-main/fetch_all_full_runner.py > fetch_all_full.log 2>&1 &
echo $! > fetch_all_full.pid
