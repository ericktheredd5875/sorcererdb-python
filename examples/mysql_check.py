"""
MySQL smoke test for Sorcerer using PyMySQL and a tiny MySQL dialect.

Requirements:
    pip install pymysql loguru

Environment vars used (with sensible defaults for local dev):
    DB_HOST (default: 127.0.0.1)
    DB_PORT (default: 3306)
    DB_USER (default: root)
    DB_PASS (default: "")
    DB_NAME (default: sorcererdb_demo)

Run:
    python examples/mysql_check.py
"""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence, Tuple

import pymysql
from loguru import logger

from sorcererdb.sorcerer import DialectLike, Sorcerer


class MiniMySQLDialect(DialectLike):
    """Minimal MySQL dialect: backtick quoting, `%s` passthrough, param flattening."""

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


class PrintSQLMiddleware:
    def before_connect(self, ctx):
        logger.info("[MW] before_connect: {}", ctx.connection_name)

    def after_connect(self, ctx, conn):
        logger.info("[MW] after_connect: {}", ctx.connection_name)

    def before_execute(self, ctx):
        logger.info("[MW] before_execute: {} :: params={} ", ctx.sql, ctx.params)

    def after_execute(self, ctx, result):
        rc = getattr(result, "rowcount", "?")
        logger.info("[MW] after_execute: rowcount={}", rc)

    def before_commit(self, ctx):
        logger.info("[MW] before_commit")

    def after_commit(self, ctx):
        logger.info("[MW] after_commit")

    def before_rollback(self, ctx):
        logger.info("[MW] before_rollback")

    def after_rollback(self, ctx):
        logger.info("[MW] after_rollback")


def main() -> None:
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "sorcerer")
    password = os.getenv("DB_PASS", "sorcererpw")
    database = os.getenv("DB_NAME", "sorcererdb_test2")

    # Create the database if it doesn't exist (connect to server then run CREATE)
    server_conn = pymysql.connect(
        host=host, port=port, user=user, password=password, autocommit=True
    )
    with server_conn.cursor() as c:
        c.execute(
            f"CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        )
    server_conn.close()

    sorc = Sorcerer(dialect=MiniMySQLDialect(), middlewares=[PrintSQLMiddleware()])

    sorc.add_connection(
        "mysql",
        pymysql.connect,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.Cursor,  # default tuple rows
        autocommit=False,
    )

    with sorc.use("mysql") as db:
        # DDL (drop/create for a clean run)
        db.execute("DROP TABLE IF EXISTS `users`")
        db.execute(
            """
            CREATE TABLE `users` (
                `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
                `email` VARCHAR(255) NOT NULL,
                `status` VARCHAR(32) NOT NULL
            ) ENGINE=InnoDB
            """
        ).close()

        # Inserts
        n1 = db.insert("users", {"email": "a@example.com", "status": "active"})
        n2 = db.insert("users", {"email": "b@example.com", "status": "active"})
        logger.info("inserted rows: {}", n1 + n2)

        # Query active
        rows = db.query_all(
            "SELECT id, email, status FROM `users` WHERE `status` = %s", ("active",)
        )
        logger.info("active now: {}", rows)

        # Transaction demo: update then rollback
        try:
            with db.transaction():
                n = db.update("users", {"status": "inactive"}, {"id": 1})
                logger.info("updated in txn: {}", n)
                raise RuntimeError("demo rollback")
        except RuntimeError:
            logger.info("rollback complete")

        # Verify rollback
        cnt = db.query_one(
            "SELECT COUNT(1) FROM `users` WHERE `status` = %s", ("active",)
        )
        logger.info("active count after rollback: {}", cnt[0] if cnt else None)

        # Delete one
        d = db.delete("users", {"id": 2})
        logger.info("deleted: {}", d)

        # Final snapshot
        final = db.query_all("SELECT id, email, status FROM `users` ORDER BY id")
        logger.info("final rows: {}", final)


if __name__ == "__main__":
    main()
