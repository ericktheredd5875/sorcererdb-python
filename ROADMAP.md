# SorcererDB — From Simple to Standout

A pragmatic plan to evolve SorcererDB into a developer‑loved Python DB toolkit with powerful dialects, a rock‑solid placeholder engine, and a clean, extensible core.

---

## Product Vision

**Make DB work feel magical, not mysterious.** SorcererDB provides a tiny ergonomic core (connections + execution) and pluggable capabilities (dialects, placeholder normalization, middleware, and lightweight CRUD "Spells").

### Design Principles

* **Small core, sharp edges hidden**: batteries-included where it counts (placeholders, dialect quirks), minimal global state.
* **Predictable API**: explicit, typed, and composable. No magic side effects.
* **Extensible by default**: adapters, middleware, and hooks are first-class.
* **Observability-first**: logs, metrics, and traces built in—not bolted on.
* **Backwards-compatibility policy**: SemVer; deprecations announced one minor ahead.

### Primary Personas

* **Backend engineers**: want simple execution + named placeholders + portability.
* **Infra/Platform**: want consistent connection lifecycle and observability hooks.
* **Library builders**: want a stable adapter/middleware surface.

### Non-Goals (for v1.0)

* Full ORM, migrations, schema management.
* Query builders beyond minimal helpers (Spells stay lightweight).

---

## High-Level Milestones

> Version numbers are directional; ship when acceptance criteria pass. Each milestone includes scope, acceptance criteria, tests, and docs.

### M1 — Core Cleanup & Contracts (v0.2)

**Goal:** Lock foundational contracts so we can iterate safely.

* **Scope**

  * Stabilize `Sorcerer`/`SorcererClient` minimal API: `connect()`, `execute()`, `executemany()`, `close()`.
  * Reintroduce **named connections** and **connection swapping** (parity with original vision).
  * Define `Dialect` and `BaseDialect` protocol (sync) with contract for: `paramstyle`, `normalize_sql()`, `normalize_params()`, `error_map()`.
  * Define **Error Taxonomy**: `SorcererError` base + `ConnectionError`, `TimeoutError`, `IntegrityError`, `ProgrammingError`, `PlaceHolderError`.
* **Acceptance Criteria**

  * Public types and docstrings finalized; mypy/pyright clean.
  * 95%+ unit coverage on core; golden tests for API.
  * CHANGELOG and MIGRATION notes from 0.1 → 0.2.

---

### M2 — Placeholder Normalization Engine (v0.3)

**Goal:** Accept *any* reasonable placeholder style and emit target dialect paramstyle safely.

* **Scope**

  * Support input forms: `?`, `%s`, `:name`, `%(name)s` (mixed allowed).
  * Output forms per dialect: e.g., MySQL/psycopg paramstyles.
  * Deterministic mapping: positional↔named, collision handling, missing/extra params detection.
  * Edge cases: repeated names, tuple expansion for `IN (:ids)` (auto-expand), dictionary order stability.
  * Secure substitution (no string interpolation); always use DB driver binding.
* **Acceptance Criteria**

  * Test matrix:

    * Mixed placeholders → MySQL/SQLite/Postgres.
    * Dict params with positional placeholders yields helpful error.
    * `IN` auto-expansion with 0/1/N elements.
  * Performance: normalization adds **< 50µs** per statement at P95 on modern hardware.
  * Comprehensive docs with cookbook examples.

---

### M3 — Dialects: MySQL, Postgres, SQLite (v0.4)

**Goal:** First-class adapters for the big three.

* **Scope**

  * **MySQL** (mysqlclient/aiomysql compatible paramstyles).
  * **Postgres** (psycopg/asyncpg nuances; `RETURNING` handling).
  * **SQLite** (stdlib `sqlite3` quirks and thread-safety note).
  * Shared conformance suite run across all dialects.
* **Acceptance Criteria**

  * Dialect conformance tests green; placeholder engine integrated.
  * Error mapping from driver exceptions to Sorcerer taxonomy.
  * Examples: one end‑to‑end snippet per dialect in `examples/`.

---

### M4 — Middleware v1 & Observability Hooks (v0.5)

**Goal:** Clean, minimal middleware lifecycle with built‑in logging/metrics/tracing.

* **Scope**

  * Lifecycle hooks: `before_connect`, `after_connect`, `before_execute`, `after_execute`, `before_close`, `after_close` (keyword-only args, typed context object).
  * Provide official middleware:

    * **SQL Logger** (redacts secrets, logs SQL + shaped params + timings).
    * **Metrics** (operation counters + latency histograms).
    * **Tracing** (OpenTelemetry spans with DB system/statement attributes).
  * ExecutionContext spec (stable, serializable `extra` + `meta`).
* **Acceptance Criteria**

  * Middleware composition order deterministic and documented.
  * New Relic / OTEL cookbook examples.
  * Benchmark: middleware overhead P95 **< 200µs** per call with logging off.

---

### M5 — Lightweight CRUD "Spells" (v0.6)

**Goal:** Ergonomic helpers that remain transparent and opt‑in.

* **Scope**

  * `insert(table, data: dict|list[dict], *, returning=None)`
  * `update(table, data: dict, where: dict|Clause, *, returning=None)`
  * `delete(table, where: dict|Clause, *, returning=None)`
  * `select(table, columns: list[str]|"*", where: dict|Clause, *, limit=None, order_by=None)`
  * Powered by placeholder engine + dialect quoting rules; not a query builder.
* **Acceptance Criteria**

  * Spells produce explainable SQL (debug mode shows final SQL+params).
  * Round‑trip tests across dialects.
  * Docs: “From raw SQL to Spells” migration guidance.

---

### M6 — Connection Management, Pools & Timeouts (v0.7)

**Goal:** Production‑ready connection lifecycle and pooling interfaces.

* **Scope**

  * Named connections registry; `use(name)` swap at runtime.
  * Pluggable pools (wrappers around driver pools; sensible defaults): min/max size, idle TTL, health checks.
  * Coherent timeout story (connect/query) + cancellation.
* **Acceptance Criteria**

  * Load tests with pooled vs non‑pooled throughput.
  * Failure injection tests (network flaps, refused connections).

---

### M7 — Async API (v0.8)

**Goal:** Mirror of sync API with `AsyncSorcerer` + async middleware.

* **Scope**

  * Async variants for core, dialects, middleware, and Spells.
  * Ensure zero blocking paths; thread executors only where necessary.
* **Acceptance Criteria**

  * Conformance suite replicated for async.
  * Competitive benchmarks vs driver baselines.

---

### M8 — Docs, Tutorials, and Examples Site (v0.9)

**Goal:** Make it dead‑simple to adopt.

* **Scope**

  * `docs/` with mkdocs-material (or similar), versioned docs.
  * Quickstarts per dialect, observability how‑tos, troubleshooting.
  * `examples/` runnable scripts + `examples/wiring.py` refreshed.
* **Acceptance Criteria**

  * API reference auto‑generated from type hints/docstrings.
  * Copy‑paste ready snippets for common tasks.

---

### M9 — Hardening for v1.0

**Goal:** Stabilize, deprecate, and declare support policy.

* **Scope**

  * Final API review, deprecations resolved or documented.
  * Platform matrix: Python 3.9–3.13, Linux/macOS/Windows, manylinux wheels.
  * Security review: input validation, redaction, safe logging guarantees.
* **Acceptance Criteria**

  * RC series with zero breaking changes between RCs.
  * Adoption signals: at least two external projects using SorcererDB.

---

## Engineering Workstreams & Checklists

### Contracts & Types

* [ ] Protocols: `Dialect`, `BaseDialect`, middleware interfaces (sync/async)
* [ ] `ExecutionContext` dataclass (stable fields: `sql`, `params`, `dialect`, `extra`, `meta`, timers)
* [ ] Error taxonomy and mapping tables per dialect

### Placeholder Engine

* [ ] Parser for input SQL to identify placeholder tokens
* [ ] Normalizer: build param map, expand `IN` tuples
* [ ] Renderer: emit target paramstyle + positional list
* [ ] Safety: forbid string interpolation, enforce param count
* [ ] Perf harness + P95 budget

### Dialects

* [ ] MySQL adapter (mysqlclient, pymysql; paramstyle `%s`)
* [ ] Postgres adapter (psycopg; paramstyle `%s`/`%({})s` mapping)
* [ ] SQLite adapter (`?` paramstyle, quirks)
* [ ] Conformance tests + error mapping

### Middleware

* [ ] Adapter skeleton + composition engine
* [ ] SQL Logger (redaction rules + sampling)
* [ ] Metrics (OpenTelemetry metrics or Prometheus client optional)
* [ ] Tracing (OpenTelemetry spans; attributes: `db.system`, `db.statement`, `db.operation`)

### Spells (CRUD)

* [ ] SQL builders for `insert/update/delete/select`
* [ ] Dialect quoting rules for identifiers
* [ ] RETURNING/lastrowid handling per dialect

### Connection Lifecycle & Pools

* [ ] Registry with named connections and runtime swapping
* [ ] Pool interfaces + adapters (psycopg/asyncpg/aiomysql/sqlite3)
* [ ] Timeouts + cancellation

### Async Surface

* [ ] `AsyncSorcerer`, async middleware, async dialects, async Spells
* [ ] Parity tests with sync API

### Tooling, CI/CD, and Quality Gates

* [ ] `pyproject.toml` with PEP 621 metadata; universal wheels
* [ ] Ruff, black (or Ruff fmt), isort, mypy/pyright; pre-commit hooks
* [ ] GitHub Actions: lint + typecheck + tests on 3.9–3.13; coverage gate
* [ ] Manylinux & macOS wheels via cibuildwheel
* [ ] `uv` project templates and lockfile
* [ ] Release workflow: tag → build → publish (TestPyPI → PyPI)

### Docs & DevX

* [ ] `README`: quickstart + badges + value prop
* [ ] `CHANGELOG` (Keep a Changelog) and `RELEASE.md`
* [ ] `MIGRATIONS.md` for breaking changes
* [ ] `docs/` site: architecture diagrams (core, middleware, placeholder engine)

---

## Acceptance Gates & KPIs

* **API Stability:** No breaking changes within a minor after v1.
* **Perf Budgets:**

  * Placeholder normalization P95 < 50µs
  * Middleware overhead P95 < 200µs (logging off)
* **Quality:**

  * 95% unit coverage on core & engine; 85% overall
  * 100% typed public API
* **DevX:** Quickstart to first successful query in <5 steps.

---

## Versioning & Compatibility

* Semantic Versioning (SemVer). Breaking changes only in majors.
* Deprecated APIs carry warnings for one minor before removal.
* Supported Python: 3.9–3.13. Drivers pinned with compatible ranges.

---

## Risk Log & Mitigations

* **Driver Fragmentation:** Maintain thin adapters; lean on conformance suite.
* **Placeholder Edge Cases:** Treat as a compiler stage with a strict test matrix.
* **Async/Sync Divergence:** Generate tests from shared scenarios.
* **Observability Overhead:** Sampling + lazy formatting; redact early.

---

## Project Management

* **Labels:** `kind/bug`, `kind/feature`, `area/dialect`, `area/placeholder`, `area/middleware`, `kind/docs`, `good-first-issue`.
* **Boards:** Milestone swimlanes tied to versions above.
* **Definition of Done:** code + tests + docs + examples + changelog entry.

---

## Appendix A — API Sketches (non-binding)

```py
# Core
client = Sorcerer(dialect=MySQL(), conn=dsn_or_pool)
cur = client.execute("SELECT * FROM users WHERE status = :status AND id IN (:ids)", {
    "status": "active",
    "ids": [1, 2, 3],
})

# Spell
from sorcererdb.spells import insert, select
insert("users", {"email": "a@b.com", "status": "active"}, returning=["id"])  # -> SQL+params
rows = select("users", ["id", "email"], where={"status": "active"}, limit=50)

# Middleware wiring
client = Sorcerer(
  dialect=Postgres(),
  conn=pool,
  middleware=[SqlLogger(sample=0.1), Metrics(), Tracing(service_name="crm-api")]
)
```

---

## Appendix B — Test Matrix (excerpt)

* Placeholders

  * Input: mixed `:name`, `%s`, `?` → Output (MySQL `%s`, PG `%s`, SQLite `?`)
  * Dict with positional; repeated names; IN (); zero/one/many
* Dialects

  * Error mapping: integrity violation, duplicate key, syntax error
* Middleware

  * Context propagation across nested calls; timing accuracy <= 1ms
* Spells

  * Generated SQL equivalence across dialects; returning behavior

---

**Ready to implement M1.** When we flip this to Issues & PRs, use the checklists above as acceptance criteria.