# ──────────────────────────────────────────────────────────────────────────────
# File: sorcererdb/middleware/examples.py
# ──────────────────────────────────────────────────────────────────────────────
import math
import time

from __future__ import annotations
from typing import Any, Optional

from .base import ExecutionContext, MiddlewareAdapter

class LoggingMiddleware(MiddlewareAdapter):
    def before_execute(self, ctx: ExecutionContext) -> None:
        ctx.extra.setdefault("_t0", time.time())
        print(f"[sql] {ctx.operation}: {ctx.sql} :: params={ctx.params}")

    def after_execute(self, ctx: ExecutionContext, result: Any) -> None:
        t0 = ctx.extra.get("_t0", ctx.start_time)
        dur_ms = int((time.time() - t0) * 1000)
        print(f"[sql] done in {dur_ms}ms :: rowcount={ctx.rowcount}")

    def on_error(self, ctx: ExecutionContext, exc: BaseException) -> None:
        print(f"[sql] ERROR after {ctx.operation}: {exc}")


class RetryMiddleware(MiddlewareAdapter):
    def __init__(self, retries: int = 2, base_delay: float = 0.05) -> None:
        self.retries = retries
        self.base_delay = base_delay

    def on_error(self, ctx: ExecutionContext, exc: BaseException) -> None:
        # naive transient error detection: you can enrich this later
        message = str(exc).lower()
        transient = any(tok in message for tok in ("deadlock", "lock wait timeout", "timeout"))
        if not transient:
            return
        if ctx.attempt <= self.retries:
            delay = self.base_delay * (2 ** (ctx.attempt - 1))
            time.sleep(delay)
            ctx.attempt += 1
            # Signal to caller that we want to retry
            ctx.extra["retry"] = True


class TracingMiddleware(MiddlewareAdapter):
    """Hook point for OpenTelemetry or similar tracing systems.
    No external dependencies, just a hook.
    """

    def before_execute(self, ctx: ExecutionContext) -> None:
        ctx.extra["trace_span"] = f"span:{int(time.time() * 1000)}"

    def after_execute(self, ctx: ExecutionContext, result: Any) -> None:
        span = ctx.extra.get("trace_span")
        # Export span data as needed
        _ = span


class CacheMiddleware(MiddlewareAdapter):
    """Read-through cache skeleton (no backend wired).

    Intended use: wrap SELECTs only; write-through/invalidation are out of
    scope for this sketch. Wire this up to a real cache backend later. 
    IE: Redis, Memcached, etc.
    """

    def __init__(self, backend: Any | None = None, ttl_seconds: int = 30) -> None:
        self.backend = backend
        self.ttl = ttl_seconds

    def _is_select(self, sql: str) -> bool:
        return sql.lstrip().upper().startswith("SELECT")

    def _key(self, sql: str, params: Any) -> str:
        return f"sdb:cache:v1:{hash((sql, self._freeze(params)))}"

    def _freeze(self, x: Any) -> str:
        if isinstance(x, (list, tuple)):
            return tuple(self._freeze(i) for i in x)
        if isinstance(x, dict):
            return tuple(sorted(k, self._freeze(v)) for k, v in x.items())
        return x

    def before_execute(self, ctx: ExecutionContext) -> None:
        if not self.backend:
            return
        if not self._is_select(ctx.sql):
            return

        key = self._key(ctx.sql, ctx.params)
        hit = self.backend.get(key)
        if hit is not None:
            ctx.extra["cache_hit"]  = True
            ctx.extra["cache_key"]  = key
            ctx.extra["cache_rows"] = hit
            ctx.extra["cache_ttl"]  = self.ttl

    def after_execute(self, ctx: ExecutionContext, result: Any) -> None:
        if not self.backend:
            return
        if not self._is_select(ctx.sql):
            return
        if ctx.extra.get("cache_hit"):
            return

        key = self._key(ctx.sql, ctx.params)
        try:
            rows = result.fetchall()
            self.backend.set(key, rows, ex=self.ttl)
            ctx.extra["cache_key"] = key
            ctx.extra["cache_rows"] = rows
        except Exception:
            # If fetchall has already been consumed upstream, skip caching.
            pass




class TransactionMiddleware(MiddlewareAdapter):
    """Wraps execution in a transaction."""
    def before_execute(self, ctx: ExecutionContext) -> None:
        if ctx.operation == "execute":
            ctx.extra["transaction"] = True

    def after_execute(self, ctx: ExecutionContext, result: Any) -> None:
        if ctx.extra.get("transaction"):
            ctx.conn.commit()

    def on_error(self, ctx: ExecutionContext, exc: BaseException) -> None:
        if ctx.extra.get("transaction"):
            ctx.conn.rollback()


class PaginationMiddleware(MiddlewareAdapter):
    """Adds pagination to the result set.
    """
    def __init__(self, page_size: int = 100) -> None:
        self.page_size = page_size

    def after_execute(self, ctx: ExecutionContext, result: Any) -> None:
        if ctx.operation == "execute":
            cur = result
            cur.fetchmany(self.page_size)

    def on_error(self, ctx: ExecutionContext, exc: BaseException) -> None:
        if ctx.operation == "execute":
            cur = result