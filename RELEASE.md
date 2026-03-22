# Release Process

This project uses **fully automated releases** via [python-semantic-release](https://python-semantic-release.readthedocs.io/) and GitHub Actions. No manual tagging or version bumping is needed.

## How It Works

Every push to `main` triggers the release pipeline:

1. **Tests** run on Python 3.11, 3.12, and 3.13
2. **python-semantic-release** analyzes commits since the last tag:
   - `feat:` → **minor** bump (0.2.3 → 0.3.0)
   - `fix:` → **patch** bump (0.2.3 → 0.2.4)
   - `feat!:` or `BREAKING CHANGE:` → **major** bump (0.2.3 → 1.0.0)
   - No releasable commits → **no release**
3. If a release is needed, PSR:
   - Bumps `version` in `pyproject.toml`
   - Syncs `uv.lock`
   - Creates a release commit and git tag (`v0.3.0`)
   - Builds the package with `uv build`
4. **git-cliff** generates the changelog from conventional commits
5. A **GitHub Release** is created with the changelog
6. The package is **published to PyPI** via Trusted Publisher (OIDC)

```
Push to main
      ↓
┌─────────────────┐
│  Run Tests      │ ← Python 3.11–3.13 matrix
│  (lint + mypy   │
│   + pytest)     │
└────────┬────────┘
         ↓ (on success)
┌─────────────────┐
│  Semantic       │ ← Analyzes conventional commits
│  Release        │ ← Bumps version, creates tag, builds
└────────┬────────┘
         ↓ (if released)
    ┌────┴────┐
    ↓         ↓
┌────────┐ ┌──────────┐
│ git-   │ │ Publish  │
│ cliff  │ │ to PyPI  │
│ + GH   │ │ (OIDC)   │
│ Release│ │          │
└────────┘ └──────────┘
```

## Conventional Commit Types

Use [Conventional Commits](https://www.conventionalcommits.org/) in all commit messages:

| Type | Changelog Group | Version Bump |
|------|----------------|--------------|
| `feat:` | Features | Minor |
| `fix:` | Bug Fixes | Patch |
| `docs:` | Documentation | — |
| `perf:` | Performance | Patch |
| `refactor:` | Refactor | — |
| `style:` | Styling | — |
| `test:` | Testing | — |
| `chore:` | Miscellaneous | — |

Only `feat` and `fix` (and breaking changes) trigger a release. Other types are included in the changelog when a release does occur.

## Breaking Changes

```bash
# Breaking change via commit type
git commit -m "feat!: change visualize_plan return type"

# Breaking change via footer
git commit -m "feat: new API" -m "BREAKING CHANGE: analyze_plan now returns Suggestion objects"
```

## Example Commits

```bash
git commit -m "feat(rules): add new SpillToDiskRule"
git commit -m "fix(parser): handle missing verboseStringWithSuffix"
git commit -m "docs: update optimization reference"
git commit -m "refactor: simplify extractor functions"
```

## Manual Release (emergency only)

If you need to bypass automation:

```bash
uv build
uv publish
```

## Configuration

Release behavior is configured in `pyproject.toml` under `[tool.semantic_release]`. Changelog formatting is configured in `cliff.toml`.
