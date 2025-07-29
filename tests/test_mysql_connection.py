from sorcererdb.core import SorcererDB
from sorcererdb.config import DBConfig

def test_mysql_connection():
    config = DBConfig(engine='mysql')
    db = SorcererDB(config)
    db.connect(config.name)

    db.set_query("CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
    db.execute()

    assert db.get_active_connection() == config.name

    db.set_query("INSERT INTO test (name) VALUES (%(name)s)")
    db.set_bindings({"name": "Eric"})
    print(db.execute())

    db.disconnect(config.name)

    # db.insert("test", {"name": "Eric"})
    # result = db.set_query("SELECT * FROM test").execute()

    # assert len(result) >= 1

def test_mysql_connection_with_dsn():
    config = DBConfig(engine='mysql', name='TestDB')
    db = SorcererDB(config)
    db.connect(config.name)

    db.set_query("CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
    db.execute()

    assert db.get_active_connection() == 'TestDB'
    db.disconnect(config.name)
    
def test_mysql_connection_with_stored_queries():
    config = DBConfig(engine='mysql', name='TestDB')
    db = SorcererDB(config)
    db.connect(config.name)

    db.add_stored_query("test_query", "SELECT * FROM test")
    db.set_stored_query("test_query")
    db.execute()

    assert db.get_query() == "SELECT * FROM test"

    db.disconnect(config.name)

def test_mysql_connection_query_reset():
    config = DBConfig(engine='mysql', name='TestDB')
    db = SorcererDB(config)
    db.connect(config.name)

    db.set_query("SELECT * FROM test")
    assert db.get_query() == "SELECT * FROM test"

    db.reset_query()
    assert db.get_query() == ""

    db.disconnect(config.name)

def test_mysql_connection_query_bindings():
    config = DBConfig(engine='mysql', name='TestDB')
    db = SorcererDB(config)
    db.connect(config.name)

    db.set_query("SELECT * FROM test WHERE name = %(name)s")
    db.set_binding("name", "Eric")
    db.execute()

    assert db.get_bindings() == {"name": "Eric"}

    db.disconnect(config.name)

def test_mysql_connection_build_bindings():
    config = DBConfig(engine='mysql', name='TestDB')
    db = SorcererDB(config)
    fields, values = db.build_bindings({"name": "Eric", "age": 30, "condition": "="})
    print(fields)
    print(values)

    assert fields == {'name': 'name = %(name)s', 'age': 'age = %(age)s', 'condition': 'condition = %(condition)s'}