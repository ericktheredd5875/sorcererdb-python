from sorcererdb.core import SorcererDB
from sorcererdb.config import DBConfig

def test_mysql_connection():
    config = DBConfig(engine='mysql')
    db = SorcererDB(config)
    db.connect()

    db.set_query("CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
    db.execute()

    assert db.get_active_connection() == 'default'

    db.disconnect()

    # db.insert("test", {"name": "Eric"})
    # result = db.set_query("SELECT * FROM test").execute()

    # assert len(result) >= 1