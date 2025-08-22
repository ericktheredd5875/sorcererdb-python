"""
Quick smoke test for Sorcerer façade + Client using SQLite.

Why SQLite?
- Zero setup, uses the standard library `sqlite3` DB-API
- Proves out: named connections, middleware hooks, CRUD helpers, txns

It also shows how to plug in a tiny dialect to map our CRUD `%s` placeholders
into SQLite's `?` style.

Run:
    uv run python examples/quickcheck.py
or
    python examples/quickcheck.py

Expected output (abridged):
    [MW] before_connect(mem)
    [MW] after_connect(mem)
    [MW] before_execute: CREATE TABLE ...
    ...
    after insert count: 2
    active now: [(1, 'a@example.com', 'active'), (2, 'b@example.com', 'active')]
    after update->inactive: [('inactive', 1)]
    rollback worked: active still 2
    delete count: 1
"""

from __future__ import annotations

import sqlite3
from typing import Any, Mapping, Sequence, Tuple

from loguru import logger

from sorcererdb.sorcerer import DialectLike, Sorcerer


# --- Mini dialect for SQLite -------------------------------------------------
class MiniSqliteDialect(DialectLike):
    """Translate Sorcerer CRUD's `%s` placeholders to SQLite `?` style.

    This is intentionally tiny—just enough to run the sample.
    """

    def normalize(self, sql: str, params: Any) -> Tuple[str, Sequence[Any]]:
        # If the SQL contains %s, replace with ? (one-for-one). Otherwise pass-through.
        if "%s" in sql:
            # Count replacements to avoid overshooting when %s appears in strings.
            # For this demo we assume well-formed CRUD SQL that only uses placeholders
            # in values, never in string literals.
            if params is None:
                n = 0
            elif isinstance(params, (list, tuple)):
                n = len(params)
            elif isinstance(params, Mapping):
                n = len(params)
                params = list(params.values())
            else:
                n = 1
                params = [params]
            pieces = sql.split("%s")
            sql = "?".join(pieces[: n + 1]) + "".join(pieces[n + 1 :])
        # normalize params to a flat list
        if params is None:
            return sql, []
        if isinstance(params, (list, tuple)):
            return sql, list(params)
        if isinstance(params, Mapping):
            return sql, list(params.values())
        return sql, [params]

    def quote_ident(self, name: str) -> str:
        return f'"{name.replace("\"", "\"\"")}"'


# --- Tiny demo middleware ----------------------------------------------------
class PrintSQLMiddleware:
    def before_connect(self, ctx):
        print(f"[MW] before_connect({ctx.connection_name})")

    def after_connect(self, ctx, conn):
        print(f"[MW] after_connect({ctx.connection_name})")

    def before_execute(self, ctx):
        print(f"[MW] before_execute: {ctx.sql} :: params={ctx.params}")

    def after_execute(self, ctx, result):
        print("[MW] after_execute: rowcount=", getattr(result, "rowcount", "?"))

    def before_commit(self, ctx):
        print("[MW] before_commit")

    def after_commit(self, ctx):
        print("[MW] after_commit")

    def before_rollback(self, ctx):
        print("[MW] before_rollback")

    def after_rollback(self, ctx):
        print("[MW] after_rollback")


# --- Main demo ---------------------------------------------------------------


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    sorc = Sorcerer(dialect=MiniSqliteDialect(), middlewares=[PrintSQLMiddleware()])

    # Named connection 'mem' using sqlite3
    sorc.add_connection("mem", sqlite3.connect, database=":memory:")

    with sorc.use("mem") as db:
        # DDL
        cur = db.execute(
            """
            CREATE TABLE users (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                email   TEXT NOT NULL,
                status  TEXT NOT NULL
            )
            """
        )
        cur.close()

        # CRUD: insert two rows
        n1 = db.insert("users", {"email": "a@example.com", "status": "active"})
        n2 = db.insert("users", {"email": "b@example.com", "status": "active"})
        print("after insert count:", n1 + n2)

        # Query active
        active = db.query_all(
            "SELECT id, email, status FROM users WHERE status = ?", ("active",)
        )
        print("active now:", active)

        # Transaction demo: update one to inactive and then rollback
        try:
            with db.transaction():
                n = db.update("users", {"status": "inactive"}, {"id": 1})
                print("updated rows in txn:", n)
                # Force an error to trigger rollback
                raise RuntimeError("demo error -> rollback")
        except RuntimeError:
            pass

        # Verify rollback worked
        still_active = db.query_all(
            "SELECT COUNT(1) FROM users WHERE status = ?", ("active",)
        )
        print("rollback worked: active still", still_active[0][0])

        # Delete one row
        d = db.delete("users", {"id": 2})
        print("delete count:", d)

        # Final snapshot
        rows = db.query_all("SELECT id, email, status FROM users ORDER BY id")
        print("final rows:", rows)


if __name__ == "__main__":
    main()
