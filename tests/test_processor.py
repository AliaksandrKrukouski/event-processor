import json
import logging

import pytest

from app.processor import EventProcessor
from app.validator import EventValidator

VALID = {
    "event_id": "event_001",
    "event_type": "payment.authorized",
    "tenant_id": "tenant-001",
    "schema_version": "1.0",
    "occurred_at": "2026-10-18T10:00:00Z",
    "payload": {"transaction_id": "txn_001", "amount": 10, "currency": "USD"},
}


@pytest.fixture
def processor(caplog):
    caplog.set_level(logging.INFO)
    return EventProcessor(EventValidator())


@pytest.fixture
def process(processor):
    return processor.process


def test_valid_event_is_ready(process, caplog):
    process(VALID)
    assert "READY" in caplog.text
    assert "event_id=event_001 event_type=payment.authorized" in caplog.text


def test_invalid_payload_is_logged_with_errors(process, caplog):
    event = json.loads(json.dumps(VALID))
    event["payload"]["amount"] = -5
    process(event)
    assert "INVALID" in caplog.text
    assert "payload:" in caplog.text


def test_unknown_event_type(process, caplog):
    process(dict(VALID, event_type="order.created"))
    assert "INVALID" in caplog.text
    assert "'order.created', '1.0'" in caplog.text


@pytest.mark.parametrize("parsed", [[1, 2], None, "text"])
def test_json_that_is_not_an_object_is_invalid(process, caplog, parsed):
    process(parsed)
    assert "INVALID" in caplog.text


@pytest.mark.parametrize("raw", [b"{not json", b"", None])
def test_invalid_json_is_logged_with_raw_value(processor, caplog, raw):
    processor.process_invalid_json(raw)
    assert "INVALID" in caplog.text
    assert "not valid JSON" in caplog.text
