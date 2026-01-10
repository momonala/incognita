#!/bin/bash
# Pre-commit hook to run tests, black, and ruff

set -e

echo "🧪 Running tests..."
uv run pytest

echo "🧪 Running isort..."
uv run isort incognita

echo "🖤 Running black..."
uv run black incognita

echo "🧼 Running ruff check..."
uv run ruff check incognita

echo "✅ Pre-commit checks passed!"
