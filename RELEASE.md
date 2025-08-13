
# 🧙‍♂️ SorcererDB Release Checklist

> Use this every time you push a new version (alpha, beta, or stable)  
> You can drop this into a `RELEASE.md` or GitHub release guide too

---

## 📦 1. Prep the Code

- [ ] All features for this release are **merged into `main`**
- [ ] Version is updated in `pyproject.toml` (e.g. `0.1.0a1` → `0.1.0a2`)
- [ ] Code is **well-tested**
- [ ] Logging is configured properly with no hardcoded paths
- [ ] No development/test folders (e.g. `logs/`, `tmp/`) are present in the root

---

## ✅ 2. Clean Environment (optional but recommended)

```bash
rm -rf dist/ build/ *.egg-info
pip install --upgrade build twine
```

---

## 🔢 3. Update Changelog

- [ ] Add an entry to `CHANGELOG.md` under the new version
- [ ] Use format:
  ```markdown
  ## [0.1.0a2] - 2025-08-07
  ### Added
  - Feature: Added MagicalCursor
  - Improvement: Spell now logs query durations

  ### Fixed
  - Bug: Cursor not closing in error scenarios
  ```

---

## 🧪 4. Test the Build Locally

```bash
python -m build
```

✅ Output should create a `dist/` folder with `.whl` and `.tar.gz` files

---

## 🧪 5. Upload to **TestPyPI** (optional)

```bash
twine upload --repository testpypi dist/*
```

Test install with:

```bash
pip install --index-url https://test.pypi.org/simple/ sorcererdb
```

---

## 🚀 6. Upload to **Real PyPI**

```bash
twine upload dist/*
```

(uses your `~/.pypirc` for auth)

---

## 🏷️ 7. Tag the Release in Git

```bash
git tag v0.1.0a2
git push origin v0.1.0a2
```

---

## 📝 8. Create GitHub Release (Optional)

On GitHub:
- Go to **Releases**
- Click “Draft a new release”
- Tag: `v0.1.0a2`
- Title: `sorcererdb 0.1.0a2`
- Description: Copy from `CHANGELOG.md`

---

## 🧹 9. Post-Release Cleanup

- [ ] Bump version in `pyproject.toml` to next dev version (`0.1.0a3-dev`)
- [ ] Create or reset `dev` branch from `main` if needed
- [ ] Update README, badges, links if version changed

---

## ✅ Final Tips

| Task | Command |
|------|---------|
| Validate PyPI metadata | `twine check dist/*` |
| Install package locally | `pip install dist/sorcererdb-*.whl` |
| View coverage locally   | `pytest --cov=sorcererdb` |
