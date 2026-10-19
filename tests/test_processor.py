import json
import logging

import pytest

from app.processor import EventProcessor

VALID = {
    "event_id": "event_001",
    "event_type": "payment.authorized",
    "tenant_id": "tenant-001",
    "schema_version": "1.0",
    "occurred_at": "2026-10-18T10:00:00Z",
    "payload": {"transaction_id": "txn_001", "amount": 10, "currency": "USD"},
}


class FakeKafkaMessage:
    def __init__(self, value: bytes, key: bytes = b"tenant-001"):
        self._value, self._key = value, key

    def value(self): return self._value
    def key(self): return self._key
    def topic(self): return "events"
    def partition(self): return 1
    def offset(self): return 7


def message(value):
    if not isinstance(value, bytes):
        value = json.dumps(value).encode()
    return FakeKafkaMessage(value)


@pytest.fixture
def process(caplog):
    caplog.set_level(logging.INFO)
    return EventProcessor().process


def test_valid_event_is_ready(process, caplog):
    process(message(VALID))
    assert "READY" in caplog.text
    assert "partition=1 offset=7 key=tenant-001" in caplog.text
    assert "event_id=event_001 event_type=payment.authorized" in caplog.text


def test_invalid_payload_is_logged_with_errors(process, caplog):
    event = json.loads(json.dumps(VALID))
    event["payload"]["amount"] = -5
    process(message(event))
    assert "INVALID" in caplog.text
    assert "payload:" in caplog.text


def test_unknown_event_type(process, caplog):
    process(message(dict(VALID, event_type="order.created")))
    assert "INVALID" in caplog.text
    assert "order.created/1.0" in caplog.text


@pytest.mark.parametrize("raw", [b"[1, 2]", b"null", b'"text"'])
def test_json_that_is_not_an_object_is_invalid(process, caplog, raw):
    process(message(raw))
    assert "INVALID" in caplog.text
