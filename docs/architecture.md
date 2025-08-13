# SorcererDB Architecture

## Before: Thin Wrapper (Basic)

```
+------------------+         +-------------------+         +------------------+
|   Your code      |  SQL    |  SorcererClient   |  SQL    |   DB-API Driver  |
|  (app/service)   |-------> |  (thin wrapper)   |-------> | (conn + cursor)  |
|                  | <-------|  returns rows     |<------- |                  |
+------------------+         +-------------------+         +------------------+

Details:
- SorcererClient exposes .cursor (raw) and calls cursor.execute(...)
- Spell = lightweight container (string + params), often bypassed
- No unified spot to normalize SQL or inject cross-cutting behavior
```

### Request flow (before)
```
Your code
  └─> SorcererClient.cursor.execute(sql, params)
        └─> DB-API cursor.execute(sql, params)
```

---

## After: Dialect + Middleware (Core DB Module)

```
+------------------+         +-------------------+         +------------------+         +------------------+
|   Your code      |  SQL/   |  SorcererClient   |  SQL/   | DialectMiddleware|  SQL    |   DB-API Driver  |
|  (app/service)   | Spell   | (connection mgr)  | Spell   | (cursor wrapper) |%s+bound | (conn + cursor)  |
|                  |-------> | .query/.run/.one  |-------> |  uses Dialect    |-------> |                  |
|                  | <-------| returns rows      |<------- |  + telemetry etc |<------- |                  |
+------------------+         +-------------------+         +------------------+         +------------------+
                                              \
                                               \ uses
                                                v
                                        +------------------+
                                        |     Dialect      |
                                        |  (MySQL/SQLite)  |
                                        | normalize/quote  |
                                        +------------------+
```

### What each piece does
- **SorcererClient**: orchestrates a connection; exposes friendly methods (`query`, `one`, `run`) and a *wrapped* cursor (not raw).
- **DialectMiddleware**: single choke‑point for *all* SQL. Detects Spell vs string, calls `Dialect.normalize(...)`, handles `executemany`, and is the hook for logging/tracing/retries/caching.
- **Dialect (e.g., MySQLDialect)**: converts `:name` / `%(name)s` / `%s` to driver’s `%s`, orders params, quotes identifiers, renders `LIMIT/OFFSET`, etc.

### Request flows

**A) Raw SQL with named params**
```
Your code
  └─> SorcererClient.query("SELECT * FROM t WHERE id = :id", {"id": 7})
        └─> DialectMiddleware.execute(...)
              └─> Dialect.normalize(":id" → "%s", params → [7])
                    └─> DB-API cursor.execute("... id = %s", [7])
```

**B) Spell**
```
Your code
  └─> SorcererClient.run(Spell("UPDATE t SET n=%(n)s WHERE id=:id", {"n":"Hermione","id":42}))
        └─> DialectMiddleware.execute(Spell)
              └─> Spell.compile(dialect) ──> Dialect.normalize(...) → ("%s/%s", ["Hermione", 42])
                    └─> DB-API cursor.execute("UPDATE t SET n=%s WHERE id=%s", ["Hermione", 42])
```

---

## Side-by-side

```
BEFORE                                           AFTER
------                                           -----
Your code                                        Your code
  |                                                |
  v                                                v
SorcererClient (thin)                            SorcererClient (orchestrator)
  |    (calls cursor directly)                     |      (always uses wrapper)
  v                                                v
DB-API Cursor                                    DialectMiddleware (cursor wrapper)
                                                  | \
                                                  |  \ calls
                                                  |   v
                                                  |  Dialect (normalize/quote)
                                                  v
                                              DB-API Cursor
```

---

## Golden Path Examples

**1. Raw SQL**

```python
client.query("SELECT * FROM users WHERE id = :id", {"id": 42})
```

**2. Spell**

```python
spell = Spell("UPDATE users SET name = %(n)s WHERE id = :id", {"n": "Hermione", "id": 42})
client.run(spell)
```

**3. Executemany with dicts**

```python
rows = [{"id": 1, "n": "A"}, {"id": 2, "n": "B"}]
client.cursor.executemany("UPDATE users SET n = :n WHERE id = :id", rows)
```
