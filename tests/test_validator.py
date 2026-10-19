import copy

import pytest

from app.validator import validate_event

PAYMENT = {
    "event_id": "event_001",
    "event_type": "payment.authorized",
    "tenant_id": "tenant-001",
    "schema_version": "1.0",
    "occurred_at": "2026-10-18T10:00:00Z",
    "payload": {"transaction_id": "txn_001", "amount": 150.5, "currency": "USD"},
}
ALERT = {
    "event_id": "event_002",
    "event_type": "monitoring.alert",
    "tenant_id": "tenant-002",
    "schema_version": "1.0",
    "occurred_at": "2026-10-18T10:00:00Z",
    "payload": {"service": "auth", "severity": "critical", "message": "error rate high"},
}


def changed(base, **updates):
    event = copy.deepcopy(base)
    event.update(updates)
    return event


@pytest.mark.parametrize("event", [PAYMENT, ALERT])
def test_valid_events(event):
    result = validate_event(event)
    assert result.is_valid
    assert result.errors == []


def test_non_object_is_invalid():
    result = validate_event([1, 2])
    assert not result.is_valid


@pytest.mark.parametrize(
    "field", ["event_id", "event_type", "tenant_id", "schema_version", "occurred_at", "payload"]
)
def test_missing_envelope_field(field):
    event = copy.deepcopy(PAYMENT)
    del event[field]
    result = validate_event(event)
    assert not result.is_valid
    assert any(field in e for e in result.errors)


@pytest.mark.parametrize(
    "updates",
    [
        {"schema_version": "v1"},
        {"occurred_at": "yesterday"},
        {"tenant_id": ""},
        {"event_id": 123},
        {"payload": "text"},
    ]
)
def test_invalid_envelope_values(updates):
    result = validate_event(changed(PAYMENT, **updates))
    assert not result.is_valid
    assert len(result.errors) == 1


def test_envelope_reports_all_errors():
    result = validate_event(changed(PAYMENT, tenant_id="", schema_version="v1"))
    assert len(result.errors) == 2


def test_unknown_event_type():
    result = validate_event(changed(PAYMENT, event_type="order.created"))
    assert not result.is_valid
    assert "order.created/1.0" in result.errors[0]


def test_unknown_schema_version():
    result = validate_event(changed(PAYMENT, schema_version="9.9"))
    assert not result.is_valid
    assert "payment.authorized/9.9" in result.errors[0]


@pytest.mark.parametrize(
    "base, payload_update",
    [
        (PAYMENT, {"amount": -5}),
        (PAYMENT, {"amount": 0}),
        (PAYMENT, {"amount": "10"}),
        (PAYMENT, {"currency": "usd"}),
        (PAYMENT, {"transaction_id": ""}),
        (ALERT, {"severity": "fatal"}),
        (ALERT, {"message": ""}),
    ],
)
def test_invalid_payload_values(base, payload_update):
    event = copy.deepcopy(base)
    event["payload"].update(payload_update)
    result = validate_event(event)
    assert not result.is_valid
    assert all(e.startswith("payload: ") for e in result.errors)


@pytest.mark.parametrize("base, field", [(PAYMENT, "currency"), (ALERT, "service")])
def test_missing_payload_field(base, field):
    event = copy.deepcopy(base)
    del event["payload"][field]
    result = validate_event(event)
    assert not result.is_valid
    assert any(field in e for e in result.errors)


def test_extra_payload_fields_are_accepted():
    event = copy.deepcopy(PAYMENT)
    event["payload"]["note"] = "extra"
    assert validate_event(event).is_valid
