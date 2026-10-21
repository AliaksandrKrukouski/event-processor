"""Handles one event: validate, route, store.

Returning means the event is stored and its offset can be committed. Raising means it
is not. Invalid events are stored as INVALID, they do not raise.
"""
import json
import logging
from typing import Any, Optional

from app.store import EventStore, Source
from app.validator import EventValidator

log = logging.getLogger("processor")

# tenant_id -> delivery target. We only record the choice; another service delivers.
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
        """Store the event as READY, or as INVALID if validation or routing fails."""
        result = self.validator.validate(event)
        errors = list(result.errors)
        targets = None
        if result.is_valid:
            target = TENANT_ROUTES.get(event["tenant_id"])
            if target is None:
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
            raw = json.dumps(event).encode()  # the original bytes are not available here
            self.store.save_invalid(errors, source, raw, event)
            log.warning("INVALID %s errors=%s", ids, errors)

    def process_invalid_json(self, raw: Optional[bytes], source: Source) -> None:
        """Store a message whose value is not valid JSON."""
        errors = ["value is not valid JSON"]
        self.store.save_invalid(errors, source, raw)
        log.warning("INVALID errors=%s raw=%r", errors, (raw or b"")[:200])
