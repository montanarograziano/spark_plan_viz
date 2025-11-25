.PHONY: install pre-commit lint type-check format check test all clean

# Install dependencies
install:
	uv sync --all-groups && uv pip install -e .

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

# Run all checks (lint + type-check)
check: lint mypy

# Run all tasks (install + pre-commit + check)
all: install pre-commit check
