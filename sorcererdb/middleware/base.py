# ──────────────────────────────────────────────────────────────────────────────
# File: sorcererdb/middleware/base.py
# ──────────────────────────────────────────────────────────────────────────────
# This file contains the base classes for all middleware functionality.
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, MutableMapping, Optional, Protocol, Sequence, Tuple

@dataclass
class ExecutionContext:
    """Mutable bag handed through middleware and execution.
    
    Suggested Keys:
        - operation: str ("execute", "executemany", etc.)
        - sql: str
        - params: Sequence[Any] | Sequence[Sequence[Any]] | Mapping[str, Any]
        - attempt: int
        - start_time: flaot
        - end_time: float | None
        - rowcount: int | None
        - extra: dict
    """

    operation : str
    sql       : str
    params    : tuple | list | dict | None
    attempt   : int = 1
    start_time: float = field(default_factory=time.time)
    end_time  : Optional[float] = None
    rowcount  : Optional[int] = None
    extra     : Dict[str, Any] = field(default_factory=dict)

class Middleware(Protocol):
    """Middleware protocol."""

    def before_execute(self, ctx: ExecutionContext) -> None: ...
    def after_execute(self, ctx: ExecutionContext, result: Any) -> None: ...
    def on_error(self, ctx: ExecutionContext, exc: BaseException) -> None: ...

class MiddlewareAdapter:
    """No-Op adapter you can subclass to implement middleware."""

    # type: ignore[override]
    def before_execute(self, ctx: ExecutionContext) -> None: 
        return None

    # type: ignore[override]
    def after_execute(self, ctx: ExecutionContext, result: Any) -> None:
        return None

    # type: ignore[override]
    def on_error(self, ctx: ExecutionContext, exc: BaseException) -> None:
        return None

class MiddlewareChain:
    """Composable middleware chain: call into it around cursor execution."""
    def __init__(self, middlewares: Optional[Sequence[Middleware]] = None) -> None:
        self._m: List[Middleware] = list(middlewares or [])

    def add(self, *middlewares: Middleware) -> None:
        self._m.extend(middlewares)

    # Runner helpers -----------------------------------------------------------
    def run_before(self, ctx: ExecutionContext) -> None:
        for m in self._m:
            m.before_execute(ctx)

    def run_after(self, ctx: ExecutionContext, result: Any) -> None:
        for m in reversed(self._m):
            m.after_execute(ctx, result)

    def run_error(self, ctx: ExecutionContext, exc: BaseException) -> None:
        for m in reversed(self._m):
            try:
                m.on_error(ctx, exc)
            except Exception:
                # Never allow middleware errors to mask the original exception
                pass