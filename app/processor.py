"""Business logic for a single event: validate and persist (routing follows).

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


class EventProcessor:
    def __init__(self, validator: EventValidator, store: EventStore):
        self.validator = validator
        self.store = store

    def process(self, event: Any, source: Source) -> None:
        """Validate a parsed event and persist it as READY or INVALID."""
        result = self.validator.validate(event)
        ids = "event_id=%s event_type=%s" % (
            event.get("event_id") if isinstance(event, dict) else None,
            event.get("event_type") if isinstance(event, dict) else None,
        )
        if result.is_valid:
            self.store.save_ready(event, source)
            log.info("READY %s", ids)
        else:
            raw = json.dumps(event).encode()  # the original bytes are not passed down; keep the parsed form
            self.store.save_invalid(result.errors, source, raw, event)
            log.warning("INVALID %s errors=%s", ids, result.errors)

    def process_invalid_json(self, raw: Optional[bytes], source: Source) -> None:
        """Persist a message value that is not valid JSON; the raw bytes are all we have."""
        errors = ["value is not valid JSON"]
        self.store.save_invalid(errors, source, raw)
        log.warning("INVALID errors=%s raw=%r", errors, (raw or b"")[:200])
