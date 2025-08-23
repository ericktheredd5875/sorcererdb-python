from __future__ import annotations

import sqlite3

import pytest

from sorcererdb.errors import ConnectionNotFound


def test_connection_switch_and_middleware_events(sorc_and_middleware):
    sorc, spy = sorc_and_middleware
    # add a second connection to prove swapping works
    sorc.add_connection("alt", sqlite3.connect, database=":memory:")

    with sorc.use("alt") as db:
        db.execute("CREATE TABLE t (id INTEGER)").close()

    # We should have seen connect + execute + close events across at least one connection
    assert "before_connect" in spy.events
    assert "after_connect" in spy.events
    assert "before_execute" in spy.events
    assert "after_execute" in spy.events
    # close_all should trigger close events
    sorc.close_all()
    assert "before_close" in spy.events and "after_close" in spy.events


def test_missing_connection_raises():
    from sorcererdb.sorcerer import Sorcerer

    s = Sorcerer()
    with pytest.raises(ConnectionNotFound):
        s.client(name="nope")
