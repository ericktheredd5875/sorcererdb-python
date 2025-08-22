# tests/test_spell.py

import pytest

from sorcererdb import SorcererDB, DBConfig, Spell

@pytest.fixture(scope="module")
def db():
    config = DBConfig(user="sorcerer", password="sorcererpw", host="127.0.0.1", port=3306, database="sorcererdb_test1", name="test")
    return SorcererDB(config).connect(config.name)

def test_basic_select(db):

    db.query("SELECT 1 AS one")
    spell = db.execute()
    result = spell.fetch("one")
    assert result["one"] == 1
    spell.close()

def test_insert_and_rowcount(db):
    db.query("INSERT INTO users (name) VALUES (%(username)s)")
    db.set_bindings({"username": "tester"})
    spell = db.execute()
    assert spell.rowcount() == 1
    spell.close()

def test_insert_and_last_id(db):
    db.query("INSERT INTO users (name) VALUES (%(username)s)")
    db.set_bindings({"username": "sorcerer"})
    spell = db.execute()
    insert_id = spell.fetch("insert_id")
    assert insert_id > 0
    spell.close()

def test_fetchmany(db):
    db.query("INSERT INTO users (name) VALUES (%(username)s)")
    db.set_bindings({"username": "multi1"})
    spell = db.execute()
    assert spell.rowcount() == 1
    spell.close()
    db.query("INSERT INTO users (name) VALUES (%(username)s)")
    db.set_bindings({"username": "multi2"})
    spell = db.execute()
    assert spell.rowcount() == 1
    spell.close()

    db.query("SELECT * FROM users")
    spell = db.execute()
    rows = spell.fetch("many", size=2)
    assert len(rows) == 2
    spell.close()

def test_fetch_invalid_type(db):
    db.query("SELECT 1")
    spell = db.execute()
    with pytest.raises(ValueError):
        spell.fetch("mystery")
    spell.close()