"""
Central error types for SorcererDB

Keeping all exceptions here avoids ciruclar imports and makes it easier for
users to catch 'SorcererError' for the whole family, or specific subclasses 
for granular handling.
"""

from __future__ import annotations


class SorcererError(Exception):
    """Base class for all SorcererDB errors."""


class DuplicateConnection(SorcererError):
    """Raised when attempting to add a connection with an existing name."""


class ConnectionNotFound(SorcererError):
    """Raised when a named connection is not found."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Connection '{name}' was not found")
        self.name = name


class NoActiveConnection(SorcererError):
    """Raised when an operation requires an active connection but none is set."""


class DriverError(SorcererError):
    """Wraps lower-level driver exceptions (connectivity, cursor errors, etc.)"""


class QueryError(SorcererError):
    """Wraps errors during SQL execution or CRUD operations."""


class TransactionError(SorcererError):
    """Wraps errors during database transactions (begin, commit, rollback)."""


class DialectError(SorcererError):
    """Wraps errors related to SQL dialect/placeholder/quoting issues."""


class PlaceholderError(SorcererError):
    """Raised when a placeholder normalization fails (mismatched params, etc.)."""


class MiddlewareError(SorcererError):
    """Raised when middleware hooks throw errors or cannot be invoked."""


__all__ = [
    "SorcererError",
    "DuplicateConnection",
    "ConnectionNotFound",
    "NoActiveConnection",
    "DriverError",
    "QueryError",
    "TransactionError",
    "DialectError",
    "PlaceholderError",
    "MiddlewareError",
]
