import copy
import logging

import pytest

from app.processor import EventProcessor
from app.store import Source
from app.validator import EventValidator

VALID = {
    "event_id": "event_001",
    "event_type": "payment.authorized",
    "tenant_id": "tenant-001",
    "schema_version": "1.0",
    "occurred_at": "2026-10-18T10:00:00Z",
    "payload": {"transaction_id": "txn_001", "amount": 10, "currency": "USD"},
}
SOURCE = Source("events", 1, 7)


class FakeStore:
    def __init__(self):
        self.ready, self.invalid = [], []

    def save_ready(self, event, source, routing_targets):
        self.ready.append((event, source, routing_targets))

    def save_invalid(self, errors, source, raw, event=None):
        self.invalid.append({"errors": errors, "source": source, "raw": raw, "event": event})


@pytest.fixture
def store():
    return FakeStore()


@pytest.fixture
def processor(store, caplog):
    caplog.set_level(logging.INFO)
    return EventProcessor(EventValidator(), store)


def test_valid_event_is_stored_as_ready(processor, store, caplog):
    processor.process(VALID, SOURCE)
    assert store.ready == [(VALID, SOURCE, ["destination-A"])]
    assert store.invalid == []
    assert "READY event_id=event_001 event_type=payment.authorized" in caplog.text


def test_invalid_payload_is_stored_with_errors(processor, store, caplog):
    event = copy.deepcopy(VALID)
    event["payload"]["amount"] = -5
    processor.process(event, SOURCE)
    assert store.ready == []
    [row] = store.invalid
    assert row["errors"][0].startswith("payload:")
    assert row["event"] == event and row["source"] == SOURCE
    assert b"event_001" in row["raw"]
    assert "INVALID" in caplog.text


def test_unknown_tenant_is_stored_as_invalid_with_a_routing_error(processor, store):
    event = dict(VALID, tenant_id="tenant-999")
    processor.process(event, SOURCE)
    assert store.ready == []
    assert store.invalid[0]["errors"] == ["routing: no route for tenant_id 'tenant-999'"]


def test_unknown_event_type_is_stored_as_invalid(processor, store):
    processor.process(dict(VALID, event_type="order.created"), SOURCE)
    assert "'order.created', '1.0'" in store.invalid[0]["errors"][0]


@pytest.mark.parametrize("parsed", [[1, 2], None, "text"])
def test_json_that_is_not_an_object_is_stored_as_invalid(processor, store, parsed):
    processor.process(parsed, SOURCE)
    assert len(store.invalid) == 1 and store.ready == []


@pytest.mark.parametrize("raw", [b"{not json", b"", None])
def test_unparseable_value_is_stored_as_invalid_with_raw_bytes(processor, store, caplog, raw):
    processor.process_invalid_json(raw, SOURCE)
    assert store.invalid == [
        {"errors": ["value is not valid JSON"], "source": SOURCE, "raw": raw, "event": None}
    ]
    assert "INVALID" in caplog.text


def test_store_failure_propagates_so_the_offset_is_not_committed(processor, store):
    def boom(*args, **kwargs):
        raise RuntimeError("db down")

    store.save_ready = boom
    with pytest.raises(RuntimeError):
        processor.process(VALID, SOURCE)
