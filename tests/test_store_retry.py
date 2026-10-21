"""Retry behaviour of EventStore, with psycopg.connect replaced by a scripted fake."""
from contextlib import contextmanager

import psycopg
import pytest

from app import store as store_module
from app.store import EventStore, Source, StoreUnavailable

EVENT = {
    "event_id": "event_001", "event_type": "payment.authorized", "tenant_id": "t",
    "schema_version": "1.0", "occurred_at": "2026-10-18T10:00:00Z", "payload": {},
}
SOURCE = Source("events", 0, 1)


class FakeConnection:
    def __init__(self, script):
        self.script, self.closed = script, False

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql, row):
        outcome = self.script.pop(0) if self.script else 1
        if isinstance(outcome, Exception):
            raise outcome
        return type("Result", (), {"rowcount": outcome})()

    def close(self):
        self.closed = True


@pytest.fixture
def db(monkeypatch):
    """`db.connects` scripts connect() results; `db.executes` scripts execute() results."""
    class Db:
        connects, executes, connect_calls = [], [], 0

    d = Db()
    d.connects, d.executes = [], []

    def connect(url, **kwargs):
        d.connect_calls += 1
        outcome = d.connects.pop(0) if d.connects else None
        if outcome is not None:
            raise outcome
        return FakeConnection(d.executes)

    d.delays = []
    monkeypatch.setattr(store_module.psycopg, "connect", connect)
    monkeypatch.setattr(store_module.time, "sleep", d.delays.append)  # record instead of sleeping
    return d


def make(**kwargs):
    return EventStore("postgresql://x", **kwargs)


def down():
    return psycopg.OperationalError("connection refused")


def test_connect_is_retried_with_exponential_backoff(db):
    db.connects = [down(), down(), down()]
    assert make().save_ready(EVENT, SOURCE) is True
    assert db.connect_calls == 4
    assert db.delays == [0.5, 1.0, 2.0]


def test_backoff_is_capped_at_max_delay(db):
    db.connects = [down()] * 6
    make(max_attempts=8, base_delay=1, max_delay=5).save_ready(EVENT, SOURCE)
    assert db.delays == [1, 2, 4, 5, 5, 5]


def test_dropped_connection_is_replaced_and_the_write_retried(db):
    store = make()
    db.executes[:] = [psycopg.OperationalError("server closed the connection"), 1]
    assert store.save_ready(EVENT, SOURCE) is True
    assert db.connect_calls == 2  # first connection plus one reconnect


def test_retry_after_commit_is_reported_as_duplicate(db):
    store = make()
    db.executes[:] = [psycopg.OperationalError("lost the reply"), 0]  # row is already there
    assert store.save_ready(EVENT, SOURCE) is False


def test_gives_up_after_max_attempts(db):
    store = make(max_attempts=3)
    db.executes[:] = [down(), down(), down()]
    with pytest.raises(StoreUnavailable):
        store.save_ready(EVENT, SOURCE)


def test_permanent_errors_are_not_retried(db):
    store = make()
    db.executes[:] = [psycopg.errors.DataError("bad value")]
    with pytest.raises(psycopg.errors.DataError):
        store.save_ready(EVENT, SOURCE)
    assert db.delays == []
    assert db.connect_calls == 1

