# ──────────────────────────────────────────────────────────────────────────────
# File: sorcererdb/dialects/base.py
# ──────────────────────────────────────────────────────────────────────────────
# This file contains the base protocol for all SQL dialects.
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Protocol, Sequence, Tuple

class Dialect(Protocol):
    """Protocol for All SQL Dialects must implement
    
    The goal is to keep this surface area small and focused on things that 
    actually vary by engine: placeholders, identifier quoting, limit/offset,
    and a few convenience compilation helpers.
    """

    name: str

    # Parameterization ------------------------------
    def paramstyle(self) -> str:
        """Return DB-API paramstyle for this dialect (e.g. 'pyformat', 'qmark', etc.)"""

    def placeholder(self, index: int | None = None) -> str:
        """Return a single placeholder token appropriate for this dialect.
        Some dialects (e.g., format) ignore index; others (e.g., numbered) 
        may use it.
        """

    def placeholders(self, n: int) -> str:
        """Return a comma-seperated list of *n* placeholders."""

    # Identifiers ----------------------------------
    def quote_indent(self, indent: str) -> str:
        """Quote a single indentifier (table/column) appropriately for the dialect."""

    def quote_path(self, *parts: str) -> str:
        """Quote a dotted indentifier path, e.g. schema.table or table.column."""

    # SQl Fragments --------------------------------
    def compile_limit_offset(self, limit: Optional[int], offset: Optional[int]) -> str:
        """Return a LIMIT/OFFSET fragment for the given arguments (or '')."""

    def compile_upsert(
        self,
        table: str,
        insert_columns: Sequence[str],
        update_columns: Sequence[str],
        conflict_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Return an engine-appropriate UPSERT statement template.

        The template should contain placeholders for values and be ready to
        execute with parameters in this order:
        - values for insert_columns
        - values for update_columns (in the same order)
        """

    # Introspection --------------------------------
    def sql_list_databases(self) -> str: ...
    def sql_list_tables(self, schema: Optional[str] = None) -> Tuple[str, Tuple[Any, ...]]: ...
    def sql_describe_table(self, table: str, schema: Optional[str] = None) -> Tuple[str, Tuple[Any, ...]]: ...
    def sql_list_indexes(self, table: str, schema: Optional[str] = None) -> Tuple[str, Tuple[Any, ...]]: ...