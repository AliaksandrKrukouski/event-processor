"""Business logic for a single event: validate, route and persist.

Knows nothing about Kafka: the consumer hands over the parsed event (or the raw bytes
of an unparseable one) with its position, and logs message details itself. Contract with the caller:
  - returning normally means the event reached a final disposition and may be acknowledged;
  - raising means it did not, and the event must be delivered again.
Bad events are an expected outcome, not an error: they are recorded, never raised.
"""
import json
import logging
from typing import Any, Optional

from app.store import EventStore, Source
from app.validator import EventValidator

log = logging.getLogger("processor")

# Routing table: tenant_id -> delivery target. Local and explicit; the processor only records
# the decision, a separate service does the delivery.
TENANT_ROUTES = {
    "tenant-001": "destination-A",
    "tenant-002": "destination-B",
    "tenant-003": "destination-C",
}


class EventProcessor:
    def __init__(self, validator: EventValidator, store: EventStore):
        self.validator = validator
        self.store = store

    def process(self, event: Any, source: Source) -> None:
        """Validate and route a parsed event, then persist it as READY or INVALID."""
        result = self.validator.validate(event)
        errors = list(result.errors)
        targets = None
        if result.is_valid:
            target = TENANT_ROUTES.get(event["tenant_id"])
            if target is None:  # a valid event for a tenant we have no route for is not deliverable
                errors.append("routing: no route for tenant_id %r" % event["tenant_id"])
            else:
                targets = [target]
        ids = "event_id=%s event_type=%s" % (
            event.get("event_id") if isinstance(event, dict) else None,
            event.get("event_type") if isinstance(event, dict) else None,
        )
        if not errors:
            self.store.save_ready(event, source, targets)
            log.info("READY %s targets=%s", ids, targets)
        else:
            raw = json.dumps(event).encode()  # the original bytes are not passed down; keep the parsed form
            self.store.save_invalid(errors, source, raw, event)
            log.warning("INVALID %s errors=%s", ids, errors)

    def process_invalid_json(self, raw: Optional[bytes], source: Source) -> None:
        """Persist a message value that is not valid JSON; the raw bytes are all we have."""
        errors = ["value is not valid JSON"]
        self.store.save_invalid(errors, source, raw)
        log.warning("INVALID errors=%s raw=%r", errors, (raw or b"")[:200])
