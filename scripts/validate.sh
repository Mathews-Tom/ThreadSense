#!/usr/bin/env bash
set -euo pipefail

uv run ruff check
uv run ruff format --check .
uv run mypy --strict src tests
uv run pytest

echo "Checking for absolute local paths in markdown files..."
if grep -rn '/Users/' ./*.md ./docs/*.md ./.docs/*.md 2>/dev/null; then
  echo "ERROR: Found absolute local paths in markdown files"
  exit 1
fi
echo "No absolute local paths found in markdown files."
