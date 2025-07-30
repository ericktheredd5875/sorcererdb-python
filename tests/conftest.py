# tests/conftest.py
import pytest
from sorcererdb import SorcererDB, DBConfig, Spell

@pytest.fixture(scope="session")
def test_config():
    """Test database configuration"""
    return DBConfig(
        engine='mysql',
        name='TestDB',
        host='localhost',
        port=3306,
        user='sorcerer',
        password='sorcererpw',
        database='sorcererdb_test1',  # Separate test database
        charset='utf8mb4',
        autocommit=True
    )

@pytest.fixture(scope="function")
def db(test_config):

    create_test_db()

    """Database fixture with cleanup"""
    db = SorcererDB(test_config)
    db.connect(test_config.name)
    
    # Setup test tables
    setup_test_tables(db)
    # cleanup_test_tables(db)
    
    yield db
    
    # Cleanup
    cleanup_test_tables(db)
    db.disconnect(test_config.name)

def setup_test_tables(db):
    """Create test tables"""
    tables = [
        """
        CREATE TABLE IF NOT EXISTS `users` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(100) NOT NULL DEFAULT '',
            `email` VARCHAR(100) NOT NULL DEFAULT '',
            `age` INT NOT NULL DEFAULT '0',
            `created_at` TIMESTAMP NOT NULL DEFAULT (now()),
            PRIMARY KEY (`id`) USING BTREE,
            INDEX `email` (`email`) USING BTREE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `posts` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `user_id` INT NOT NULL DEFAULT '0',
            `title` VARCHAR(200) NOT NULL DEFAULT '',
            `content` TEXT NOT NULL,
            `created_at` TIMESTAMP NOT NULL DEFAULT (now()),
            INDEX `user_id` (`user_id`) USING BTREE
        )
        """
    ]
    
    for table_sql in tables:
        db.query(table_sql)
        db.execute()

def cleanup_test_tables(db):
    """Clean test tables"""
    tables = ['posts', 'users']  # Order matters due to foreign keys
    for table in tables:
        db.query(f"TRUNCATE TABLE {table}")
        db.execute()

def create_test_db():
    """Create test database"""
    config = DBConfig(engine='mysql', user="root", password="root")
    db = SorcererDB(config)
    db.connect(config.name)

    db.query("CREATE DATABASE IF NOT EXISTS sorcererdb_test1;")
    db.execute()
    db.query("CREATE DATABASE IF NOT EXISTS sorcererdb_test2;")
    db.execute()

    db.query("GRANT ALL PRIVILEGES ON sorcererdb_test1.* TO 'sorcerer'@'%'")
    db.execute()
    db.query("GRANT ALL PRIVILEGES ON sorcererdb_test2.* TO 'sorcerer'@'%'")
    db.execute()

    db.disconnect(config.name)

    return None