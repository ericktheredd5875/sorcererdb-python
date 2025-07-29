import pytest
from sorcererdb.core import SorcererDB
from sorcererdb.config import DBConfig

def test_insert(db):

    db.set_query("INSERT INTO users (name) VALUES (%(name)s)")
    db.set_bindings({"name": "Eric"})
    insert_id = db.get_result_set("last_insert_id")

    # Assert that the record was inserted
    assert insert_id is not None

    next_insert_id = db.insert("users", {"name": "Eric"})
    assert next_insert_id is not None
    assert next_insert_id > insert_id

    db.set_query("SELECT * FROM users WHERE id = %(id)s")
    db.set_bindings({"id": insert_id})
    records = db.get_result_set("all")
    assert len(records) > 0
    assert records[0]['name'] == "Eric"

# - test_update
def test_update(db):
    
    insert_id = db.insert("users", {"name": "Eric"})

    db.set_query("UPDATE users SET name = %(name)s WHERE id = %(id)s")
    db.set_bindings({"name": "Barty", "id": insert_id})
    update_count = db.get_result_set("count")
    assert update_count == 1

    update_count = db.update("users", {"name": "Marvin"}, {"id": insert_id})

    sql = "SELECT * FROM users"
    db.set_query(sql)
    records = db.get_result_set("all")
    assert records[0]['name'] == "Marvin"

# - test_delete
def test_delete(db):
    
    insert_id1 = db.insert("users", {"name": "Eric"})
    db.set_query("DELETE FROM users WHERE id = %(id)s")
    db.set_bindings({"id": insert_id1})
    db.get_result_set("count")

    insert_id2 = db.insert("users", {"name": "Eric"})
    db.delete("users", {"id": insert_id2})
    db.commit()

    sql = "SELECT COUNT(*) AS count FROM users"
    db.set_query(sql)
    record = db.get_result_set("one")
    assert record['count'] == 0

# - test_complex_queries
def test_complex_queries(db):
    
    db.insert("users", {"name": "Eric"})

    db.set_query("SELECT * FROM users WHERE name = %(name)s")
    db.set_bindings({"name": "Eric"})
    record = db.get_result_set("one")
    assert record['name'] == "Eric"


