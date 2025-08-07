# 🔮 SorcererDB – A Smart SQL Abstraction Layer

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Status: In Development](https://img.shields.io/badge/status-alpha-orange)
[![codecov](https://codecov.io/gh/ericktheredd5875/sorcererdb-python/graph/badge.svg?token=579MCVZ7P3)](https://codecov.io/gh/ericktheredd5875/sorcererdb-python)

**SorcererDB** is a lightweight, extensible database abstraction layer designed to simplify raw SQL access while offering advanced features like multi-connection management, query caching, and reusable query definitions.

## ✨ Features

- Named connections and connection pooling
- Safe, type-aware prepared statements
- Optional query result caching (e.g., Redis or in-memory)
- Stored query registration and reuse
- Lightweight CRUD helpers: `insert`, `update`, `delete`
- Query profiling and memory diagnostics
- Full transaction support

## ⚡️ Quick Start

```bash
pip install sorcererdb
```

```python
from sorcererdb import SorcererDB, DBConfig

config = DBConfig(user="root", password="pw", database="test")
db = SorcererDB().connect("default", config)

rows = db.execute("SELECT * FROM users WHERE role = %s", ("admin",))
```

---

## 📦 Installation

```bash
pip install sorcererdb
```

Or install directly from source:

```bash
git clone https://github.com/ericktheredd5875/sorcererdb-python.git
cd sorcererdb-python
pip install -e .
```

---

## 🧭 Roadmap / TODO

- [x] Basic DB connection pool
- [x] Simple query execution interface
- [ ] Caching layer (Memcache/Redis backends)
- [ ] Query profiler with timing and trace support
- [ ] Named prepared queries (alias/shortcut system)
- [ ] SQLite and PostgreSQL drivers
- [ ] Type hinting and full doc coverage
- [ ] `sorcerer` CLI for testing and diagnostics
- [x] PyPI release
- [ ] Connection Pooling (with thread-local binding)
- [ ] Multi-Threading Support
- [ ] ??

---

## 📜 License

MIT License © 2025 Eric Harris
