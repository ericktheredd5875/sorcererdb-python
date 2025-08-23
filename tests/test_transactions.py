from __future__ import annotations


def test_transaction_commit_and_rollback(db):
    # commit path
    with db.transaction():
        db.insert("users", {"email": "c@x", "status": "active"})
    cnt = db.query_one("SELECT COUNT(1) FROM users")
    assert cnt[0] == 1

    # rollback path
    try:
        with db.transaction():
            db.insert("users", {"email": "d@x", "status": "active"})
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    cnt2 = db.query_one("SELECT COUNT(1) FROM users")
    assert cnt2[0] == 1  # unchanged
