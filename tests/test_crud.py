from __future__ import annotations


def test_crud_insert_update_delete(db):
    n = db.insert("users", {"email": "a@x", "status": "active"})
    assert n == 1

    n2 = db.update("users", {"status": "inactive"}, {"email": "a@x"})
    assert n2 == 1

    n3 = db.delete("users", {"email": "a@x"})
    assert n3 == 1

    rows = db.query_all("SELECT * FROM users")
    assert rows == []
