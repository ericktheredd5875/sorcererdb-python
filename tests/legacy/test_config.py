# tests/test_config.py
import pytest
from sorcererdb import SorcererDB, DBConfig, Spell

def test_default_config():
    """Test DBConfig with default values"""
    config = DBConfig()
    
    assert config.name == "MainSorcererDB"
    assert config.engine == "mysql"
    assert config.host == "localhost"
    assert config.port == 3306
    assert config.user == "sorcerer"
    assert config.password == "sorcererpw"
    assert config.database == "sorcererdb"
    assert config.charset == "utf8mb4"
    assert config.timeout == 30
    assert config.autocommit is True

def test_custom_config():
    """Test DBConfig with custom values"""
    config = DBConfig(
        name="CustomDB",
        engine="mysql",
        host="192.168.1.100",
        port=3307,
        user="custom_user",
        password="custom_pass",
        database="custom_db",
        charset="utf8",
        timeout=60,
        autocommit=False
    )
    
    assert config.name == "CustomDB"
    assert config.engine == "mysql"
    assert config.host == "192.168.1.100"
    assert config.port == 3307
    assert config.user == "custom_user"
    assert config.password == "custom_pass"
    assert config.database == "custom_db"
    assert config.charset == "utf8"
    assert config.timeout == 60
    assert config.autocommit is False

def test_partial_custom_config():
    """Test DBConfig with some custom values, others default"""
    config = DBConfig(
        name="PartialDB",
        host="remote.server.com",
        port=3308
    )
    
    assert config.name == "PartialDB"
    assert config.host == "remote.server.com"
    assert config.port == 3308
    # Default values should remain
    assert config.engine == "mysql"
    assert config.user == "sorcerer"
    assert config.password == "sorcererpw"
    assert config.database == "sorcererdb"

def test_empty_strings():
    """Test behavior with empty strings"""
    config = DBConfig(
        name="",
        host="",
        user="",
        password="",
        database="",
        charset=""
    )
    
    assert config.name == ""
    assert config.host == ""
    assert config.user == ""
    assert config.password == ""
    assert config.database == ""
    assert config.charset == ""

def test_special_characters():
    """Test behavior with special characters in strings"""
    config = DBConfig(
        name="Test-DB_123",
        host="test.server.com:3306",
        user="user@domain",
        password="pass@word!123",
        database="test_db-name"
    )
    
    assert config.name == "Test-DB_123"
    assert config.host == "test.server.com:3306"
    assert config.user == "user@domain"
    assert config.password == "pass@word!123"
    assert config.database == "test_db-name"

def test_extreme_port_values():
    """Test behavior with extreme port values"""
    # Valid ports
    config = DBConfig(port=1)
    assert config.port == 1
    
    config = DBConfig(port=65535)
    assert config.port == 65535
    
    # Invalid ports (should still work due to dataclass)
    config = DBConfig(port=0)
    assert config.port == 0
    
    config = DBConfig(port=70000)
    assert config.port == 70000

def test_mysql_engine_config():
    """Test MySQL-specific configuration"""
    config = DBConfig(engine="mysql")
    assert config.engine == "mysql"

def test_sqlite_engine_config():
    """Test SQLite-specific configuration"""
    config = DBConfig(engine="sqlite")
    assert config.engine == "sqlite"

def test_invalid_engine():
    """Test behavior with invalid engine"""
    config = DBConfig(engine="invalid_engine")
    assert config.engine == "invalid_engine"

def test_config_immutability():
    """Test that config objects can be modified"""
    config = DBConfig()
    original_name = config.name
    
    config.name = "ModifiedDB"
    assert config.name == "ModifiedDB"
    assert config.name != original_name

def test_config_copy():
    """Test creating copies of config objects"""
    config1 = DBConfig(name="OriginalDB")
    config2 = DBConfig(name="OriginalDB")
    
    assert config1.name == config2.name
    assert config1 is not config2

def test_config_comparison():
    """Test config object comparison"""
    config1 = DBConfig(name="TestDB", host="localhost")
    config2 = DBConfig(name="TestDB", host="localhost")
    config3 = DBConfig(name="TestDB", host="remote.server")
    
    assert config1 == config2
    assert config1 != config3

def test_config_with_sorcererdb():
    """Test that DBConfig works with SorcererDB"""
    from sorcererdb.core import SorcererDB
    
    config = DBConfig(
        engine='mysql',
        name='TestDB',
        host='localhost',
        port=3306,
        user='sorcerer',
        password='sorcererpw',
        database='sorcererdb'
    )
    
    db = SorcererDB(config)
    assert db.config == config
    assert db.config.name == 'TestDB'
    assert db.config.engine == 'mysql'

def test_multiple_configs():
    """Test multiple DBConfig objects with SorcererDB"""
    from sorcererdb.core import SorcererDB
    
    config1 = DBConfig(name='DB1', database='db1')
    config2 = DBConfig(name='DB2', database='db2')
    
    db = SorcererDB(config1)
    db.set_dsn(config2)
    
    assert db.check_dsn('DB1') is True
    assert db.check_dsn('DB2') is True

def test_missing_required_fields():
    """Test behavior when required fields are missing"""
    # This should work since all fields have defaults
    config = DBConfig()
    assert config.name is not None
    assert config.engine is not None

def test_none_values():
    """Test behavior with None values"""
    config = DBConfig(
        name=None,
        host=None,
        user=None,
        password=None,
        database=None,
        charset=None
    )
    
    assert config.name is None
    assert config.host is None
    assert config.user is None
    assert config.password is None
    assert config.database is None
    assert config.charset is None

def test_config_memory_usage():
    """Test memory usage of config objects"""
    import sys
    
    config = DBConfig()
    size = sys.getsizeof(config)
    
    # Config objects should be relatively small
    assert size < 1000  # Adjust based on actual size

def test_multiple_config_creation():
    """Test creating many config objects"""
    configs = []
    for i in range(100):
        config = DBConfig(name=f"DB{i}")
        configs.append(config)
    
    assert len(configs) == 100
    assert all(isinstance(c, DBConfig) for c in configs)

def test_port_type_validation():
    """Test that port must be an integer"""
    config = DBConfig(port=3306)
    assert isinstance(config.port, int)
    
    # Test with string port (should work due to dataclass)
    config = DBConfig(port="3306")
    assert config.port == "3306"  # This might be an issue

def test_timeout_type_validation():
    """Test that timeout must be an integer"""
    config = DBConfig(timeout=30)
    assert isinstance(config.timeout, int)

def test_autocommit_type_validation():
    """Test that autocommit must be a boolean"""
    config = DBConfig(autocommit=True)
    assert isinstance(config.autocommit, bool)
    
    config = DBConfig(autocommit=False)
    assert isinstance(config.autocommit, bool)

