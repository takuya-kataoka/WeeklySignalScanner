#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/commit_with_bump.sh "commit message" [paths...]
# If no paths are provided, stages all changes. If no message provided, uses "chore(release): verX.XX".

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUMP_PY="$REPO_ROOT/scripts/bump_version.py"
VERSION_FILE="$REPO_ROOT/VERSION"

if [ ! -x "$(command -v python3)" ]; then
  echo "python3 is required" >&2
  exit 1
fi

shift 0 || true

MSG=""
if [ $# -ge 1 ]; then
  MSG="$1"
  shift
fi

# Bump version and stage it
if [ -f "$BUMP_PY" ]; then
  python3 "$BUMP_PY"
  git -C "$REPO_ROOT" add "$VERSION_FILE" || true
fi

if [ $# -gt 0 ]; then
  echo "Staging: $*"
  git -C "$REPO_ROOT" add -- "$@"
else
  echo "Staging all changes (including VERSION)"
  git -C "$REPO_ROOT" add -A
fi

if [ -z "$MSG" ]; then
  if [ -f "$VERSION_FILE" ]; then
    ver=$(cat "$VERSION_FILE" | tr -d '\n')
    MSG="chore(release): ver${ver}"
  else
    MSG="chore(release): bump version"
  fi
fi

echo "Committing: $MSG"
if git -C "$REPO_ROOT" commit -m "$MSG"; then
  echo "Pushing to origin main..."
  git -C "$REPO_ROOT" push origin main
else
  echo "No changes to commit";
fi
