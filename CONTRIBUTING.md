# Contributing to Spark Plan Viz

Thank you for your interest in contributing to Spark Plan Viz! 🎉

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment.

## How Can I Contribute?

### 🐛 Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- Clear and descriptive title
- Exact steps to reproduce the problem
- Expected vs actual behavior
- Code samples
- Your environment (Python version, PySpark version, OS)

Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.yml).

### ✨ Suggesting Features

Feature suggestions are welcome! Please:

- Check if the feature has already been suggested
- Provide a clear use case
- Explain why this feature would be useful
- Consider implementation complexity

Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.yml).

### 📝 Improving Documentation

Documentation improvements are always appreciated:

- Fix typos or clarify existing docs
- Add examples
- Improve code comments
- Update README

Use the [documentation template](.github/ISSUE_TEMPLATE/documentation.yml).

### 💻 Code Contributions

#### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/spark_plan_viz.git
   cd spark_plan_viz
   ```

2. **Install dependencies using uv**
   ```bash
   # Install uv if you haven't already
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Install project dependencies
   uv sync --all-extras
   ```

3. **Create a branch**
   ```bash
   git checkout -b feature/my-new-feature
   # or
   git checkout -b fix/issue-123
   ```

#### Development Workflow

1. **Make your changes**
   - Write clear, readable code
   - Add docstrings to functions and classes
   - Follow existing code style

2. **Run tests**
   ```bash
   # Run all tests
   uv run pytest tests/ -v

   # Run specific test
   uv run pytest tests/test_visualize_plan.py -v

   # Test on specific Python version
   uv run --python 3.11 pytest tests/ -v

   # Test on all supported Python versions (3.11-3.13)
   just test-all
   ```

3. **Run code quality checks**
   ```bash
   # Format code
   uv run ruff format src/ tests/

   # Check for issues
   uv run ruff check src/ tests/

   # Type checking
   uv run mypy src/
   ```

4. **Test your changes manually**
   ```bash
   # Test in a notebook or script
   python -c "from spark_plan_viz import visualize_plan; print('OK')"
   ```

#### Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Format your commits as:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `test:` - Test changes
- `chore:` - Build/tooling changes

**Examples:**
```bash
git commit -m "feat(visualization): add broadcast join indicator"
git commit -m "fix(template): correct arrow direction in inverted tree"
git commit -m "docs: update installation instructions"
```

#### Submitting a Pull Request

1. **Push your changes**
   ```bash
   git push origin feature/my-new-feature
   ```

2. **Create a Pull Request**
   - Use a clear title following Conventional Commits
   - Fill out the PR template completely
   - Link related issues
   - Add screenshots/examples if applicable

3. **Code Review Process**
   - Address review comments
   - Keep your PR up to date with `main`
   - All tests must pass before merging

#### PR Checklist

- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] Code follows project style
- [ ] Commit messages follow Conventional Commits
- [ ] No breaking changes (or clearly documented)

## Development Guidelines

### Code Style

- Use type hints for function parameters and return values
- Write descriptive variable names
- Keep functions focused and small
- Add docstrings to public functions

### Testing

- Write tests for new features
- Ensure existing tests pass on all Python versions (3.11-3.13)
- Use `just test-all` to verify compatibility across versions
- Aim for good test coverage
- Test edge cases

### Documentation

- Update README if adding features
- Add docstrings to new functions
- Include code examples
- Keep documentation up to date

## Project Structure

```
spark_plan_viz/
├── src/
│   └── spark_plan_viz/
│       ├── __init__.py
│       ├── visualize_plan.py  # Main visualization logic
│       └── template.html      # D3.js visualization template
├── tests/
│   └── test_visualize_plan.py # Test suite
├── docs/                      # Documentation
├── .github/                   # GitHub templates and workflows
└── pyproject.toml            # Project configuration
```

## Getting Help

- 💬 [Start a discussion](https://github.com/montanarograziano/spark_plan_viz/discussions)
- 📖 Check the [README](README.md)
- 🐛 [Open an issue](https://github.com/montanarograziano/spark_plan_viz/issues)

## Recognition

Contributors will be recognized in:
- Repository contributors list
- Release notes (for significant contributions)

Thank you for contributing! 🙏
