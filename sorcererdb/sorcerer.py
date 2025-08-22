"""
SorcererDB facade + client

Design Goals:
--------------
- Keep your original `sorcerer.py` entry-point while folding the client
(so imports stay ergonomic: `from sorcererdb.sorcerer import Sorcerer`).
- Named connections and easy connection swapping (`sorcerer.use("reporting")`).
- Pluggable dialect (placeholder normalization, quoting) without hard dependency.
- Middleware hooks (before/after connect/execute/commit/rollback/close).
- Lightweight CRUD helpers: insert, update, delete.
- Transaction context manager.


Assumptions
-----------
- A DB-API 2.0 driver (e.g., PyMySQL, mysqlclient, mysql-connector, etc.).
- Optional `sorcererdb.middleware.base` providing `ExecutionContext` and a
middleware stack. If not present, no-op fallbacks are used so this file
works standalone.
"""

from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from loguru import logger

from .errors import (
    ConnectionNotFound,
    DialectError,
    DriverError,
    DuplicateConnection,
    MiddlewareError,
    NoActiveConnection,
    PlaceholderError,
    QueryError,
    TransactionError,
)

# ---------------------------------------------------------------------------
# Optional imports from your middleware package. We provide fallbacks if missing.
# ---------------------------------------------------------------------------
try:
    # Expected to exist in your tree based on prior work
    from .middleware.base import ExecutionContext as _ExecCtx  # type: ignore
    from .middleware.base import MiddlewareStack as _MWStack  # type: ignore
except Exception:  # pragma: no cover - fallback if middleware not wired yet

    @dataclass
    class _ExecCtx:  # minimal shape used by this module
        sql: str
        params: Any
        connection_name: str
        extra: Dict[str, Any] = field(default_factory=dict)
        start_ts: float = field(default_factory=time.perf_counter)

    class _MWStack:
        """No-op middleware stack fallback."""

        def __init__(self, middlewares: Optional[Sequence[Any]] = None) -> None:
            self._mws = list(middlewares or [])

        # connect lifecycle hooks
        def before_connect(self, ctx: _ExecCtx) -> None:  # type: ignore[empty-body]
            for m in self._mws:
                fn = getattr(m, "before_connect", None)
                if fn:
                    fn(ctx)

        def after_connect(self, ctx: _ExecCtx, conn: Any) -> None:  # type: ignore[empty-body]
            for m in self._mws:
                fn = getattr(m, "after_connect", None)
                if fn:
                    fn(ctx, conn)

        # execute lifecycle
        def before_execute(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "before_execute", None)
                if fn:
                    fn(ctx)

        def after_execute(self, ctx: _ExecCtx, result: Any) -> None:
            for m in self._mws:
                fn = getattr(m, "after_execute", None)
                if fn:
                    fn(ctx, result)

        # txn lifecycle
        def before_commit(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "before_commit", None)
                if fn:
                    fn(ctx)

        def after_commit(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "after_commit", None)
                if fn:
                    fn(ctx)

        def before_rollback(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "before_rollback", None)
                if fn:
                    fn(ctx)

        def after_rollback(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "after_rollback", None)
                if fn:
                    fn(ctx)

        # close lifecycle
        def before_close(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "before_close", None)
                if fn:
                    fn(ctx)

        def after_close(self, ctx: _ExecCtx) -> None:
            for m in self._mws:
                fn = getattr(m, "after_close", None)
                if fn:
                    fn(ctx)


# ---------------------------------------------------------------------------
# Dialect protocol (loose) + a tiny default dialect
# ---------------------------------------------------------------------------
class DialectLike(Protocol):
    """Minimal surface we need for a dialect implementation."""

    def normalize(
        self, sql: str, params: Any
    ) -> Tuple[str, Sequence[Any]]:  # placeholder engine
        ...

    def quote_ident(self, name: str) -> str:  # identifier quoting for CRUD
        ...


class _DefaultDialect:
    """Fallback dialect that does no placeholder conversion and naive quoting."""

    def normalize(self, sql: str, params: Any) -> Tuple[str, Sequence[Any]]:
        # Assume DB-API 2.0 paramstyle already matches driver. If dict, convert to tuple
        if isinstance(params, Mapping):
            # Preserve mapping order where possible, but since SQL likely contains named
            # placeholders for real dialects, this is best-effort only.
            return sql, list(params.values())

        if params is None:
            return sql, []

        if isinstance(params, (list, tuple)):
            return sql, list(params)

        return sql, [params]

    def quote_ident(self, name: str) -> str:
        # Barebones quoting - your MySQL dialect will likely use backticks.
        return f'"{name.replace("\"", "\"\"")}"'


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------
@dataclass
class _ConnConfig:
    name: str
    factory: Callable[..., Any]
    kwargs: Dict[str, Any] = field(default_factory=dict)
    connection: Any | None = None  # Live handle once connected

    def connect(self, mw: _MWStack) -> Any:
        if self.connection is not None:
            return self.connection

        ctx = _ExecCtx(sql="<connect>", params=None, connection_name=self.name)
        try:
            mw.before_connect(ctx)
            self.connection = self.factory(**self.kwargs)
            mw.after_connect(ctx, self.connection)
            return self.connection
        except Exception as exc:
            raise MiddlewareError(
                f"Connect hook failed for '{self.name}': {exc}"
            ) from exc

    def close(self, mw: _MWStack) -> None:
        if self.connection is None:
            return

        ctx = _ExecCtx(sql="<close>", params=None, connection_name=self.name)
        try:
            mw.before_close(ctx)
        except Exception as exc:
            raise MiddlewareError(f"before_close failed: {exc}") from exc

        try:
            with contextlib.suppress(Exception):
                self.connection.close()
        finally:
            try:
                mw.after_close(ctx)
            finally:
                self.connection = None


# ---------------------------------------------------------------------------
# Sorcerer façade
# ---------------------------------------------------------------------------
class Sorcerer:
    """High-level façade that manages connections and returns bound clients.

    Example
    -------
    >>> import pymysql
    >>> sorc = Sorcerer(dialect=None) # your MySQL dialect instance here
    >>> sorc.add_connection("default", pymysql.connect, host="...", user="...", password="...", db="...")
    >>> with sorc.use("default") as client:
    ... rows = client.query_all("SELECT 1")
    ... print(rows)
    """

    def __init__(
        self,
        *,
        dialect: Optional[DialectLike] = None,
        middlewares: Optional[Sequence[Any]] = None,
        # logger: Optional[logger.Logger] = None,
    ) -> None:
        self._dialect: DialectLike = dialect or _DefaultDialect()
        self._mw = _MWStack(middlewares)
        self._conns: Dict[str, _ConnConfig] = {}
        self._active: Optional[str] = None
        self.log = logger.bind(component="sorcererdb.sorcerer")

    # --- connection management ------------------------------------------------
    def add_connection(
        self, name: str, factory: Callable[..., Any], /, **kwargs: Any
    ) -> None:
        if self._conns.get(name):
            raise DuplicateConnection(f"Connection '{name}' already exists")

        self._conns[name] = _ConnConfig(name=name, factory=factory, kwargs=dict(kwargs))
        if self._active is None:
            self._active = name

    def set_active(self, name: str) -> None:
        if name not in self._conns:
            raise ConnectionNotFound(f"Connection '{name}' not found")

        self._active = name

    def get_active_name(self) -> str:
        if not self._active:
            raise NoActiveConnection(
                "No active connection; call set_active() or add_connection() first"
            )
        return self._active

    def get_connection(self, name: Optional[str] = None) -> Any:
        nm = name or self.get_active_name()
        cfg = self._conns.get(nm)
        if not cfg:
            raise ConnectionNotFound(f"Connection '{nm}' not found")

        return cfg.connect(self._mw)

    @contextlib.contextmanager
    def use(self, name: str):
        """Temporarily switch active connection and yield a bound client."""
        if name not in self._conns:
            raise ConnectionNotFound(f"Connection '{name}' not found")

        prev = self._active
        try:
            self._active = name
            yield self.client()
        finally:
            self._active = prev

    def client(self, *, name: Optional[str] = None) -> "Client":
        nm = name or self.get_active_name()
        conn = self.get_connection(nm)
        return Client(
            connection=conn,
            connection_name=nm,
            dialect=self._dialect,
            mw=self._mw,
            # logger=self.log,
        )

    def closs_all(self) -> None:
        for cfg in self._conns.values():
            cfg.close(self._mw)


# ---------------------------------------------------------------------------
# Client: DB-API executor with CRUD helpers and transactions
# ---------------------------------------------------------------------------
class Client:
    def __init__(
        self,
        *,
        connection: Any,
        connection_name: str,
        dialect: DialectLike,
        mw: _MWStack,
        # logger: Optional[logger.Logger] = None,
    ) -> None:
        self.connection = connection
        self.connection_name = connection_name
        self.dialect = dialect
        self.mw = mw
        self.log = logger.bind(component="sorcererdb.client", conn=connection_name)

    # --- core execute/query ---------------------------------------------------
    def execute(self, sql: str, params: Any | None = None) -> Any:
        sql2, params2 = self.dialect.normalize(sql, params)
        ctx = _ExecCtx(sql=sql2, params=params2, connection_name=self.connection_name)
        try:
            self.mw.before_execute(ctx)
            cur = self.connection.cursor()
            cur.execute(sql2, params2)
            self.mw.after_execute(ctx, cur)
            return cur
        except Exception as exc:
            raise QueryError(f"Query failed: {exc}") from exc

    def query_one(
        self, sql: str, params: Any | None = None
    ) -> Optional[Mapping[str, Any]]:
        cur = self.execute(sql, params)
        try:
            row = cur.fetchone()
            return row
        finally:
            with contextlib.suppress(Exception):
                cur.close()

    def query_all(
        self, sql: str, params: Any | None = None
    ) -> Sequence[Mapping[str, Any]]:
        cur = self.execute(sql, params)
        try:
            rows = cur.fetchall()
            return rows
        finally:
            with contextlib.suppress(Exception):
                cur.close()

    # --- transactions ---------------------------------------------------------
    @contextlib.contextmanager
    def transaction(self):
        ctx = _ExecCtx(
            sql="<transaction>", params=None, connection_name=self.connection_name
        )
        try:
            yield self
            try:
                self.mw.before_commit(ctx)
                self.connection.commit()
                self.mw.after_commit(ctx)
            except Exception as exc:
                raise TransactionError(f"Commit failed: {exc}") from exc
        except Exception:
            try:
                self.mw.before_rollback(ctx)
                self.connection.rollback()
                self.mw.after_rollback(ctx)
            except Exception as exc2:
                raise TransactionError(f"Rollback failed: {exc2}") from exc2
            raise

    # --- lightweight CRUD -----------------------------------------------------
    def insert(self, table: str, data: Mapping[str, Any]) -> int:
        if not data:
            raise QueryError("No data to insert")

        qtable = self.dialect.quote_ident(table)
        cols = [self.dialect.quote_ident(c) for c in data.keys()]
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {qtable} ({", ".join(cols)}) VALUES ({placeholders})"
        cur = self.execute(sql, list(data.values()))
        try:
            return getattr(cur, "rowcount", 1)
        finally:
            with contextlib.suppress(Exception):
                cur.close()

    def update(
        self, table: str, data: Mapping[str, Any], where: Mapping[str, Any] | str
    ) -> int:
        if not data:
            raise QueryError("No data to update")

        qtable = self.dialect.quote_ident(table)
        set_exprs = ", ".join(
            [f"{self.dialect.quote_ident(k)} = %s" for k in data.keys()]
        )

        if isinstance(where, Mapping):
            where_exprs = " AND ".join(
                [f"{self.dialect.quote_ident(k)} = %s" for k in where.keys()]
            )
            params = list(data.values()) + list(where.values())
        else:
            where_exprs = str(where)
            params = list(data.values())

        sql = f"UPDATE {qtable} SET {set_exprs} WHERE {where_exprs}"
        cur = self.execute(sql, params)
        try:
            return getattr(cur, "rowcount", 0)
        finally:
            with contextlib.suppress(Exception):
                cur.close()

    def delete(self, table: str, where: Mapping[str, Any] | str) -> int:
        qtable = self.dialect.quote_ident(table)
        if isinstance(where, Mapping):
            where_exprs = " AND ".join(
                [f"{self.dialect.quote_ident(k)} = %s" for k in where.keys()]
            )
            params = list(where.values())
        else:
            where_exprs = str(where)
            params = []

        sql = f"DELETE FROM {qtable} WHERE {where_exprs}"
        cur = self.execute(sql, params)
        try:
            return getattr(cur, "rowcount", 0)
        finally:
            with contextlib.suppress(Exception):
                cur.close()
