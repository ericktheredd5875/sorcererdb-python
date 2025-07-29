from sorcererdb.core import SorcererDB
from sorcererdb.config import DBConfig
import pytest

def test_connection_management(db):
    """Test connection management using fixture"""
    assert db.get_active_connection() == 'TestDB'
    assert db.check_connection('TestDB') is True


def test_connection_failure_handling():
    config = DBConfig(engine='mysql', name='TestDB')
    config.host = 'invalid.host'
    db = SorcererDB(config)

    with pytest.raises(ConnectionError):
        db.connect(config.name)

    assert db.get_active_connection() is None
    assert db.check_connection(config.name) is False

def test_multiple_connections():
    # Create multiple database configurations
    config1 = DBConfig(name='TestDB1', database='sorcererdb_test1')
    config2 = DBConfig(name='TestDB2', database='sorcererdb_test2')
    
    db = SorcererDB(config1)
    
    # Add second DSN configuration
    db.set_dsn(config2)
    
    # Verify both DSNs are registered
    assert db.check_dsn('TestDB1') is True
    assert db.check_dsn('TestDB2') is True
    
    # Connect to first database
    db.connect('TestDB1')
    assert db.get_active_connection() == 'TestDB1'
    assert db.check_connection('TestDB1') is True
    assert db.check_connection('TestDB2') is False
    
    # Connect to second database
    db.connect('TestDB2')
    assert db.get_active_connection() == 'TestDB2'
    assert db.check_connection('TestDB1') is True  # First connection should still exist
    assert db.check_connection('TestDB2') is True
    
    # Switch back to first connection using set_active_connection
    db.set_active_connection('TestDB1')
    assert db.get_active_connection() == 'TestDB1'
    
    # Switch to second connection
    db.set_active_connection('TestDB2')
    assert db.get_active_connection() == 'TestDB2'
    
    # Test switching to non-existent connection
    with pytest.raises(ValueError):
        db.set_active_connection('NonExistentDB')
    
    # Disconnect first connection
    db.disconnect('TestDB1')
    assert db.check_connection('TestDB1') is False
    assert db.check_connection('TestDB2') is True
    assert db.get_active_connection() == 'TestDB2'  # Should still be active
    
    # Disconnect second connection
    db.disconnect('TestDB2')
    assert db.check_connection('TestDB1') is False
    assert db.check_connection('TestDB2') is False
    assert db.get_active_connection() is None
    
    # Test reconnecting after disconnect
    db.connect('TestDB1')
    assert db.get_active_connection() == 'TestDB1'
    assert db.check_connection('TestDB1') is True
    
    # Cleanup
    db.disconnect('TestDB1')
    

def test_invalid_dsn_handling():
    config = DBConfig(engine='mysql', name='TestDB')
    config2 = DBConfig(engine='mysql', name='TestDB2')
    db = SorcererDB(config)

    with pytest.raises(ValueError):
        db.set_dsn(config)

    assert db.check_dsn('TestDB2') is False

    db.set_dsn(config2)
    assert db.check_dsn('TestDB') is True