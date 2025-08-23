from __future__ import annotations

import sqlite3
import typing as t

import pytest

from sorcererdb.sorcerer import DialectLike, Sorcerer


class MiniSqliteDialect(DialectLike):
    def normalize(self, sql: str, params: t.Any) -> tuple[str, list[t.Any]]:
        # translate %s -> ? when present and flatten params
        if "%s" in sql:
            if params is None:
                n = 0
                params_list: list[t.Any] = []
            elif isinstance(params, (list, tuple)):
                n = len(params)
                params_list = list(params)
            elif isinstance(params, dict):
                n = len(params)
                params_list = list(params.values())
            else:
                n = 1
                params_list = [params]
            pieces = sql.split("%s")
            sql = "?".join(pieces[: n + 1]) + "".join(pieces[n + 1 :])
        else:
            if params is None:
                params_list = []
            elif isinstance(params, (list, tuple)):
                params_list = list(params)
            elif isinstance(params, dict):
                params_list = list(params.values())
            else:
                params_list = [params]
        return sql, params_list

    def quote_ident(self, name: str) -> str:
        return f'"{name.replace("\"", "\"\"")}"'


class SpyMiddleware:
    def __init__(self) -> None:
        self.events: list[str] = []

    def before_connect(self, ctx):
        self.events.append("before_connect")

    def after_connect(self, ctx, conn):
        self.events.append("after_connect")

    def before_execute(self, ctx):
        self.events.append("before_execute")

    def after_execute(self, ctx, result):
        self.events.append("after_execute")

    def before_commit(self, ctx):
        self.events.append("before_commit")

    def after_commit(self, ctx):
        self.events.append("after_commit")

    def before_rollback(self, ctx):
        self.events.append("before_rollback")

    def after_rollback(self, ctx):
        self.events.append("after_rollback")

    def before_close(self, ctx):
        self.events.append("before_close")

    def after_close(self, ctx):
        self.events.append("after_close")


@pytest.fixture()
def sorc_and_middleware():
    spy = SpyMiddleware()
    sorc = Sorcerer(dialect=MiniSqliteDialect(), middlewares=[spy])
    sorc.add_connection("mem", sqlite3.connect, database=":memory:")
    return sorc, spy


@pytest.fixture()
def db(sorc_and_middleware):
    sorc, _ = sorc_and_middleware
    try:
        with sorc.use("mem") as client:
            # table for tests
            client.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT NOT NULL,
                    status TEXT NOT NULL
                )
                """
            ).close()
            yield client
    finally:
        sorc.close_all()
