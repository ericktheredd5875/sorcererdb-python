import pytest
from loguru import logger
from sorcererdb.logging import configure_logging
from sorcererdb import Spell

@pytest.fixture(autouse=True)
def test_spell_logging(loguru_caplog):
    class DummyConn:
        def cursor(self, **kwargs):
            class DummyCursor:
                def execute(self, query, bindings): pass
                def close(self): pass
                @property
                def rowcount(self): return 42
            return DummyCursor()

    spell = Spell(DummyConn())
    spell.execute("SELECT * FROM users")

    assert any("Executing query" in r.message for r in loguru_caplog.records)
