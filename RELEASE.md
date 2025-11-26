# Release Process

This project uses automated releases via GitHub Actions.

## Creating a New Release

1. **Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)**:
   ```bash
   git commit -m "feat: add new feature"
   git commit -m "fix: resolve bug in visualization"
   git commit -m "docs: update README"
   ```

2. **Create and push a new version tag**:
   ```bash
   # Create a new tag (e.g., v1.2.3)
   git tag v1.2.3

   # Push the tag to GitHub
   git push origin v1.2.3
   ```

3. **Automated workflow**:
   - The `release.yml` GitHub Action will trigger automatically
   - It will:
     1. Generate a changelog from conventional commits using git-cliff
     2. Create a GitHub Release with the changelog
     3. Build and publish the package to PyPI

## Conventional Commit Types

The changelog is automatically organized by commit type:

- `feat:` - New features (🚀 Features)
- `fix:` - Bug fixes (🐛 Bug Fixes)
- `docs:` - Documentation changes (📚 Documentation)
- `perf:` - Performance improvements (⚡ Performance)
- `refactor:` - Code refactoring (🚜 Refactor)
- `style:` - Code style changes (🎨 Styling)
- `test:` - Test changes (🧪 Testing)
- `chore:` - Maintenance tasks (⚙️ Miscellaneous Tasks)

## Example Commit Messages

```bash
# Feature with scope
git commit -m "feat(visualization): add broadcast join indicator"

# Breaking change
git commit -m "feat!: change API structure" -m "BREAKING CHANGE: API now uses new format"

# Bug fix with scope
git commit -m "fix(template): correct arrow direction in tree"

# Documentation
git commit -m "docs: update installation instructions"
```

## Manual Release (if needed)

If you need to create a release manually:

```bash
# Build the package
uv build

# Publish to PyPI
uv publish
```
