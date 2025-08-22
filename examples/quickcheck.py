"""
Quick smoke test for Sorcerer façade + Client using SQLite and MySQL.

- SQLite for zero-setup testing (in-memory)
- MySQL for real-world connectivity (requires pymysql + running server)
- Uses loguru instead of stdlib logging

Run:
    python examples/quickcheck.py sqlite
    python examples/quickcheck.py mysql

"""

from __future__ import annotations

import sqlite3
import sys
from typing import Any, Mapping, Sequence, Tuple

from loguru import logger

from sorcererdb.sorcerer import DialectLike, Sorcerer


# --- Mini dialects -----------------------------------------------------------
class MiniSqliteDialect(DialectLike):
    def normalize(self, sql: str, params: Any) -> Tuple[str, Sequence[Any]]:
        if "%s" in sql:
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
        if "AUTO_INCREMENT" in sql:
            sql = sql.replace("AUTO_INCREMENT", "AUTOINCREMENT")
        if params is None:
            return sql, []
        if isinstance(params, (list, tuple)):
            return sql, list(params)
        if isinstance(params, Mapping):
            return sql, list(params.values())
        return sql, [params]

    def quote_ident(self, name: str) -> str:
        return f'"{name.replace("\"", "\"\"")}"'


class MiniMysqlDialect(DialectLike):
    """Pass-through %s placeholders, backtick quoting."""

    def normalize(self, sql: str, params: Any) -> Tuple[str, Sequence[Any]]:
        if params is None:
            return sql, []
        if isinstance(params, (list, tuple)):
            return sql, list(params)
        if isinstance(params, Mapping):
            return sql, list(params.values())
        return sql, [params]

    def quote_ident(self, name: str) -> str:
        return f"`{name.replace('`', '``')}`"


# --- Tiny demo middleware ----------------------------------------------------
class PrintSQLMiddleware:
    def before_connect(self, ctx):
        logger.info(f"[MW] before_connect({ctx.connection_name})")

    def after_connect(self, ctx, conn):
        logger.info(f"[MW] after_connect({ctx.connection_name})")

    def before_execute(self, ctx):
        logger.info(f"[MW] before_execute: {ctx.sql} :: params={ctx.params}")

    def after_execute(self, ctx, result):
        logger.info("[MW] after_execute: rowcount={}", getattr(result, "rowcount", "?"))

    def before_commit(self, ctx):
        logger.info("[MW] before_commit")

    def after_commit(self, ctx):
        logger.info("[MW] after_commit")

    def before_rollback(self, ctx):
        logger.info("[MW] before_rollback")

    def after_rollback(self, ctx):
        logger.info("[MW] after_rollback")


# --- Common demo function ----------------------------------------------------
def run_demo(sorc: Sorcerer):
    with sorc.use("main") as db:
        cur = db.execute(
            """
            CREATE TABLE users (
                id      INTEGER PRIMARY KEY AUTO_INCREMENT,
                email   VARCHAR(200) NOT NULL,
                status  VARCHAR(50) NOT NULL
            )
            """
        )
        cur.close()

        db.insert("users", {"email": "a@example.com", "status": "active"})
        db.insert("users", {"email": "b@example.com", "status": "active"})

        active = db.query_all(
            "SELECT id, email, status FROM users WHERE status = %s", ("active",)
        )
        logger.info("active now: {}", active)

        try:
            with db.transaction():
                db.update("users", {"status": "inactive"}, {"id": 1})
                raise RuntimeError("demo error -> rollback")
        except RuntimeError:
            pass

        still_active = db.query_all(
            "SELECT COUNT(1) FROM users WHERE status = %s", ("active",)
        )
        logger.info("rollback worked: active still {}", still_active[0][0])

        db.delete("users", {"id": 2})
        rows = db.query_all("SELECT id, email, status FROM users ORDER BY id")
        logger.info("final rows: {}", rows)


# --- Entrypoint --------------------------------------------------------------
def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python examples/quickcheck.py [sqlite|mysql]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "sqlite":
        sorc = Sorcerer(
            dialect=MiniSqliteDialect(),
            middlewares=[PrintSQLMiddleware()],
        )
        sorc.add_connection("main", sqlite3.connect, database=":memory:")
        run_demo(sorc)
    elif mode == "mysql":
        import pymysql  # ensure installed

        sorc = Sorcerer(
            dialect=MiniMysqlDialect(),
            middlewares=[PrintSQLMiddleware()],
        )
        sorc.add_connection(
            "main",
            pymysql.connect,
            host="localhost",
            user="root",
            password="root",
            database="testdb",
        )
        run_demo(sorc)
    else:
        print("Unknown mode, use sqlite or mysql")
        sys.exit(1)


if __name__ == "__main__":
    main()
