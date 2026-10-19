#!/usr/bin/env bash
set -euo pipefail

uv run ruff check
uv run ruff format --check .
uv run mypy --strict src tests scripts
uv run pytest

echo "Checking for absolute local paths in markdown files..."
shopt -s nullglob
markdown_files=(./*.md ./docs/*.md ./.docs/*.md)
shopt -u nullglob
# grep exits 1 only when nothing matched; a match (0) or a read error (2) fails.
scan_status=0
grep -n '/Users/' /dev/null "${markdown_files[@]}" || scan_status=$?
if [ "$scan_status" -ne 1 ]; then
  echo "ERROR: Found absolute local paths in markdown files, or could not read them"
  exit 1
fi
echo "No absolute local paths found in markdown files."
