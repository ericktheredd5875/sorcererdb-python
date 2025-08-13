# ──────────────────────────────────────────────────────────────────────────────
# File: sorcererdb/dialects/mysql.py
# ──────────────────────────────────────────────────────────────────────────────
# This file contains the MySQL dialect implementation.
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
from typing import Any, Optional, Sequence, Tuple

from .base import Dialect

class MySQLDialect:
    """MySQL / MariaDB dialect implementation.

    Defaults assume DB-API drivers that use "%s placeholders 
    (e.g. MysqlClient, PyMySQL). Adjust if the target driver 
    requires "pyformat".
    """

    name = "mysql"

    # ----------------------------- Paramstyle ---------------------------------
    # DB-API 2.0 style compatible with "%s" placeholders.
    def paramstyle(self) -> str: 
        return "format"

    def placeholder(self, index: int | None = None) -> str:
        return "%s"

    def placeholders(self, n: int) -> str:
        return ", ".join(self.placeholder() for i in range(n))

    # ---------------------------- Identifiers ---------------------------------
    def quote_indent(self, indent: str) -> str:
        escaped = indent.replace("`", "``")
        return f"`{escaped}`"

    def quote_path(self, *parts: str) -> str:
        return ".".join(self.quote_indent(part) for part in parts if part)

    # --------------------------- SQL Fragments --------------------------------
    def compile_limit_offset(self, limit: Optional[int], offset: Optional[int]) -> str:
        if limit is None and offset is None:
            return ""
        if limit is None and offset is not None:
            # MySQL requires LIMIT when OFFSET is provided - Use max BIGINT
            return f" LIMIT 18446744073709551615 OFFSET {offset}"
        if limit is not None and offset is None:
            return f" LIMIT {limit}"
        return f" LIMIT {limit} OFFSET {offset}"

    def compile_upsert(
        self,
        table: str,
        insert_columns: Sequence[str],
        update_columns: Sequence[str],
        conflict_columns: Optional[Sequence[str]] = None, # Ignored for MySQL
    ) -> str:
        t = self.quote_indent(table)
        cols = ", ".join(self.quote_indent(c) for c in insert_columns)
        vals = self.placeholders(len(insert_columns))
        updates = ", ".join(
            f"{self.quote.indent(c)} = VALUES({self.quote_indent(c)}" for c in update_columns
        )

        return f"INSERT INTO {t} ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE {updates}"

    # --------------------------- Introspection --------------------------------
    def sql_list_databases(self) -> str:
        return "SHOW DATABASES"

    def sql_list_tables(self, schema: Optional[str] = None) -> Tuple[str, Tuple[Any, ...]]:
        if schema:
            return (
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s",
                (schema,)
            )

        return ("SHOW TABLES", Tuple())

    def sql_describe_table(self, table: str, schema: Optional[str] = None) -> Tuple[str, Tuple[Any, ...]]:
        if schema:
            q = self.quote_path(schema, table)
            return (f"SHOW COLUMNS FROM {q}", Tuple())
        
        return (f"SHOW COLUMNS FROM {self.quote_indent(table)}", Tuple())

    def sql_list_indexes(self, table: str, schema: Optional[str] = None) -> Tuple[str, Tuple[Any, ...]]:
        if schema:
            q = self.quote_path(schema, table)
            return (f"SHOW INDEXES FROM {q}", Tuple())

        return (f"SHOW INDEXES FROM {self.quote_indent(table)}", Tuple())