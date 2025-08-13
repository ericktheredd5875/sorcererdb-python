
# 🧙‍♂️ Contributing to SorcererDB

Thank you for your interest in contributing to **SorcererDB**!  
Whether you're fixing a bug, improving documentation, or proposing a new feature — your help is appreciated.

---

## ✨ How to Contribute

1. **Fork** the repo and create your branch from `main` or `dev`:
   ```bash
   git checkout -b feature/my-awesome-spell
   ```

2. **Install dependencies** using Pipenv or your preferred virtualenv:
   ```bash
   pipenv install --dev
   ```

3. **Run tests** to ensure stability:
   ```bash
   pipenv run pytest
   ```

4. **Write clear commit messages** and make sure your code is clean and covered.

5. **Submit a pull request** against the `dev` branch with a clear description of your changes.

---

## 📦 Project Structure

```
sorcererdb/
├── config.py
├── core.py
├── spell.py
├── logging.py
tests/
pyproject.toml
```

---

## ✅ Style Guidelines

- Follow [PEP8](https://pep8.org/) for code style.
- Use [loguru](https://github.com/Delgan/loguru) for logging.
- Keep test coverage high (`pytest --cov=sorcererdb`).
- All public methods should have docstrings.

---

## 💬 Questions or Help?

- Open a [discussion](https://github.com/ericktheredd5875/sorcererdb-python/discussions)
- Create an issue for bugs or enhancement ideas

---

Thank you for helping make SorcererDB better!
