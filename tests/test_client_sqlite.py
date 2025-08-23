from __future__ import annotations

from sorcererdb.errors import QueryError


def test_execute_and_query(db):
    db.execute(
        "INSERT INTO users (email, status) VALUES (%s, %s)", ("a@x", "active")
    ).close()
    one = db.query_one("SELECT email FROM users WHERE status = ?", ("active",))
    assert one[0] == "a@x"


def test_query_all_returns_rows(db):
    for i in range(3):
        db.execute(
            "INSERT INTO users (email, status) VALUES (%s, %s)", (f"u{i}@x", "active")
        ).close()
    rows = db.query_all("SELECT id, email FROM users ORDER BY id")
    assert len(rows) == 3
    assert rows[0][1] == "u0@x"


def test_execute_error_is_wrapped(db):
    try:
        db.execute("SELCT broken")  # typo on purpose
    except QueryError as e:
        # assert 'Query failed: near "SELCT": syntax error' in str(e)
        assert "Query failed" in str(e)
    else:
        assert False, "expected QueryError"
