# ──────────────────────────────────────────────────────────────────────────────
# File: sorcererdb/client.py (excerpt)
# ──────────────────────────────────────────────────────────────────────────────
# This file contains the base client class for all database operations.
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

from .dialects.base import Dialect
from .middleware.base import ExecutionContext, MiddlewareChain


class SorcererClient:
    """Very small client wrapper to show where middleware and dialect slot in.

    This expects a synchronous PEP-249 connection. Async support can be added 
    later via a parallel AsyncSorcererClient.
    """

    def __init__(self, conn, dialect: Dialect, middlewares: Optional[Sequence] = None) -> None:
        self.conn = conn
        self.dialect = dialect
        self.middleware = MiddlewareChain(middlewares)

    def execute(self, sql: str, params: Optional[Sequence[Any] | Mapping[str, Any]] = None):
        ctx = ExecutionContext(operation="execute", sql=sql, params=params)
        self.middleware.run_before(ctx)

        try:
            cur = self.conn.cursor()
            cur.execute(sql, params or ())
            ctx.end_time = time.time()
            ctx.rowcount = getattr(cur, "rowcount", None)
            self.middleware.run_after(ctx, cur)
            return cur
        except BaseException as exc:
            ctx.end_time = time.time()
            self.middleware.run_error(ctx, exc)
            raise

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any]]):
        ctx = ExecutionContext(operation="executemany", sql=sql, params=seq_of_params)
        self.middleware.run_before(ctx)
        try:
            cur = self.conn.cursor()
            cur.executemany(sql, seq_of_params)
            ctx.end_time = time.time()
            ctx.rowcount = getattr(cur, "rowcount", None)
            self.middleware.run_after(ctx, cur)
            return cur
        except BaseException as exc:
            ctx.end_time = time.time()
            self.middleware.run_error(ctx, exc)
            raise
        
