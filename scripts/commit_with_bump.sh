#!/usr/bin/env bash
set -euo pipefail

# Usage:
#  scripts/commit_with_bump.sh "commit message" [paths...]
# If no paths provided, stages all changes.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION_FILE="$REPO_ROOT/VERSION"
PY_BUMP="$REPO_ROOT/scripts/bump_version.py"

if [ ! -x "$(command -v python3)" ]; then
  echo "python3 is required" >&2
  exit 1
fi

# bump version
echo "Bumping VERSION..."
python3 "$PY_BUMP"

# stage files
shift_msg=false
if [ $# -ge 1 ]; then
  # first arg may be the commit message; we detect if it's quoted by caller
  COMMIT_MSG="$1"
  shift
  if [ $# -gt 0 ]; then
    echo "Staging provided paths: $*"
    git add -- "$@"
  else
    echo "Staging all changes (including VERSION)"
    git add -A
  fi
else
  COMMIT_MSG="chore(release): bump version"
  echo "Staging all changes (including VERSION)"
  git add -A
fi

# ensure VERSION is staged
git add -- "$VERSION_FILE" || true

# read version for message if not custom
if [ "$COMMIT_MSG" = "chore(release): bump version" ]; then
  if [ -f "$VERSION_FILE" ]; then
    ver=$(cat "$VERSION_FILE" | tr -d '\n' )
    COMMIT_MSG="chore(release): ver${ver}"
  fi
fi

echo "Committing: $COMMIT_MSG"
git commit -m "$COMMIT_MSG" || { echo "No changes to commit"; exit 0; }

echo "Pushing to origin main..."
git push origin main

echo "Done."
