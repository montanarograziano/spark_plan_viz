# Contributing

Contributions are welcome! This guide covers the development workflow.

## Development Setup

```bash
git clone https://github.com/montanarograziano/spark_plan_viz.git
cd spark_plan_viz
just install       # Install all dependencies
just pre-commit    # Set up pre-commit hooks
```

## Development Workflow

### Running Tests

```bash
just test                  # Run tests on current Python
just test-python 3.12      # Run on specific version
just test-all              # Run on Python 3.11, 3.12, 3.13
```

### Code Quality

```bash
just lint    # ruff check
just format  # ruff format
just mypy    # Type checking
just check   # lint + mypy
```

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation
- `test:` — tests
- `refactor:` — code restructuring
- `chore:` — maintenance

## Pull Request Process

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Run `just check` and `just test`
5. Push and open a PR

### PR Checklist

- [ ] Tests added/updated
- [ ] Lint and type checks pass
- [ ] Conventional commit messages used
- [ ] Breaking changes documented

## Project Structure

```
src/spark_plan_viz/
  __init__.py        # Public API exports
  _constants.py      # Named constants
  _types.py          # TypedDict definitions
  _extractors.py     # Regex-based extraction functions
  _parser.py         # JVM plan traversal via Py4J
  _renderer.py       # HTML generation and visualize_plan()
  _rules.py          # 15 optimization rules
  _analyzer.py       # Analysis engine
  template.html      # D3.js visualization template

tests/
  test_parsing.py           # Extraction function tests
  test_visualize_plan.py    # Parser, renderer, integration tests
  test_rules.py             # One test per optimization rule

docs/                       # Zensical documentation source
```

## Adding a New Optimization Rule

1. Create a class in `_rules.py` implementing the `Rule` protocol
2. Add it to the `ALL_RULES` list
3. Add tests in `test_rules.py`
4. Document it in `docs/optimization-reference/index.md`
