"""Integration tests against a real PostgreSQL (the one from docker compose by default).

Skipped when the database is not reachable. Rows are tagged with a test-only topic and
removed afterwards.
"""
import os

import psycopg
import pytest

from app.store import EventStore, Source

URL = os.getenv("TEST_POSTGRES_URL", "postgresql://events:events@localhost:5432/events")
TOPIC = "store-test"

EVENT = {
    "event_id": "store_test_001",
    "event_type": "payment.authorized",
    "tenant_id": "tenant-001",
    "schema_version": "1.0",
    "occurred_at": "2026-10-18T10:00:00Z",
    "payload": {"transaction_id": "txn_001", "amount": 10, "currency": "USD"},
}


@pytest.fixture
def db():
    try:
        conn = psycopg.connect(URL, autocommit=True, connect_timeout=2)
        conn.execute("SELECT 1 FROM events LIMIT 1")
    except psycopg.Error as e:
        pytest.skip("PostgreSQL with the events table is not available: %s" % e)
    clean = lambda: conn.execute("DELETE FROM events WHERE source_topic = %s OR event_id = %s", (TOPIC, EVENT["event_id"]))
    clean()
    yield conn
    clean()
    conn.close()


@pytest.fixture
def store(db):
    s = EventStore(URL)
    yield s
    s.close()


def rows(db, **where):
    cur = db.execute(
        "SELECT event_id, status, tenant_id, payload, raw_message, validation_errors, "
        "source_partition, source_offset, occurred_at, routing_targets FROM events WHERE source_topic = %s ORDER BY id",
        (TOPIC,),
    )
    return cur.fetchall()


def test_ready_event_is_persisted(store, db):
    assert store.save_ready(EVENT, Source(TOPIC, 2, 10), ["destination-A"]) is True
    [(event_id, status, tenant, payload, raw, errors, partition, offset, occurred_at, targets)] = rows(db)
    assert targets == ["destination-A"]
    assert (event_id, status, tenant) == ("store_test_001", "READY", "tenant-001")
    assert payload == EVENT["payload"]
    assert raw is None and errors is None
    assert (partition, offset) == (2, 10)
    assert occurred_at.year == 2026


def test_duplicate_event_id_is_ignored(store, db):
    assert store.save_ready(EVENT, Source(TOPIC, 0, 1), ["destination-A"]) is True
    assert store.save_ready(EVENT, Source(TOPIC, 0, 2), ["destination-A"]) is False  # redelivery / producer retry
    assert len(rows(db)) == 1
    assert rows(db)[0][7] == 1  # the first write wins


def test_invalid_event_keeps_errors_and_raw_bytes(store, db):
    event = dict(EVENT, event_id="store_test_002", occurred_at="yesterday")
    assert store.save_invalid(["bad date"], Source(TOPIC, 0, 3), b'{"raw": 1}', event) is True
    [(event_id, status, tenant, payload, raw, errors, *_)] = rows(db)
    assert (event_id, status, tenant) == ("store_test_002", "INVALID", "tenant-001")
    assert payload is None and bytes(raw) == b'{"raw": 1}' and errors == ["bad date"]


def test_unparseable_message_uses_position_as_key_and_is_idempotent(store, db):
    source = Source(TOPIC, 1, 42)
    assert store.save_invalid(["not json"], source, b"{oops") is True
    assert store.save_invalid(["not json"], source, b"{oops") is False
    [(event_id, status, tenant, _, raw, *_)] = rows(db)
    assert event_id == "invalid:%s:1:42" % TOPIC
    assert status == "INVALID" and tenant is None and bytes(raw) == b"{oops"


def test_wrongly_typed_envelope_values_do_not_break_the_insert(store, db):
    event = {"event_id": 123, "tenant_id": ["x"], "event_type": None}
    assert store.save_invalid(["bad"], Source(TOPIC, 0, 5), b"{}", event) is True
    assert rows(db)[0][0] == "invalid:%s:0:5" % TOPIC


def test_failed_write_raises_and_stores_nothing(store, db):
    broken = dict(EVENT, occurred_at="not a timestamp")
    with pytest.raises(psycopg.Error):
        store.save_ready(broken, Source(TOPIC, 0, 6), ["destination-A"])
    assert rows(db) == []
    assert store.save_ready(EVENT, Source(TOPIC, 0, 7), ["destination-A"]) is True  # connection still usable
