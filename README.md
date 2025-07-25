# 🔮 SorcererDB – A Smart SQL Abstraction Layer

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Status: In Development](https://img.shields.io/badge/status-alpha-orange)

**SorcererDB** is a lightweight, extensible database abstraction layer designed to simplify raw SQL access while offering advanced features like multi-connection management, query caching, and reusable query definitions.

## ✨ Features

- Named connections and connection pooling
- Safe, type-aware prepared statements
- Optional query result caching (e.g., Redis or in-memory)
- Stored query registration and reuse
- Lightweight CRUD helpers: `insert`, `update`, `delete`
- Query profiling and memory diagnostics
- Full transaction support

## 📦 Installation

```bash
pip install sorcererdb
```

## 🧪 Example Usage

```python
from sorcererdb.core import SorcererDB

db = SorcererDB(engine='sqlite', dsn=':memory:')
db.connect()

db.set_query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
db.execute()

db.insert("test", {"name": "Alice"})
result = db.set_query("SELECT * FROM test").execute()
print(result)
```

## 📜 License

MIT License © 2025 Eric Harris
