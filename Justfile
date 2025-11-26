# Install dependencies
install:
    uv sync --all-groups

# Setup pre-commit hooks
pre-commit:
    uv run prek install

# Run ruff linter
lint:
    uv run ruff check .

# Run ruff formatter
format:
    uv run ruff format .

# Run mypy type checker
mypy:
    uv run mypy src/

# Run pytest tests
test:
    uv run pytest tests/ -v

# Run pytest tests on a specific Python version
test-python VERSION:
    uv run --python {{VERSION}} pytest tests/ -v

# Run pytest tests on all supported Python versions
test-all:
    #!/usr/bin/env bash
    set -e
    echo "Testing on Python 3.11..."
    uv run --python 3.11 pytest tests/ -v
    echo "\nTesting on Python 3.12..."
    uv run --python 3.12 pytest tests/ -v
    echo "\nTesting on Python 3.13..."
    uv run --python 3.13 pytest tests/ -v
    echo "\n✅ All Python versions passed!"

# Run all checks (lint + mypy)
check: lint mypy

# Run all tasks (install + pre-commit + check)
all: install pre-commit check

# Clean cache files
clean:
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
