# ──────────────────────────────────────────────────────────────────────────────
# File: examples/wiring.py (how you might wire it up)
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import mysql.connector  # or PyMySQL / mysqlclient

from sorcererdb.client import SorcererClient
from sorcererdb.dialects.mysql import MySQLDialect
from sorcererdb.middleware.examples import LoggingMiddleware, RetryMiddleware


conn = mysql.connector.connect(
    host="localhost", user="root", password="root", database="sorcererdb_test1"
)
client = SorcererClient(
    conn=conn,
    dialect=MySQLDialect(),
    middlewares=[LoggingMiddleware(), RetryMiddleware()],
)

sql = "SELECT id, email FROM users WHERE status = %s"  # placeholders via dialect
cur = client.execute(sql, ("active",))
rows = cur.fetchall()
print(rows)