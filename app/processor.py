"""Business logic for a single event: parse, validate (routing and persistence follow).

Knows nothing about Kafka: the consumer hands over the parsed event (or the raw bytes
of an unparseable one) and logs message details itself. Contract with the caller:
  - returning normally means the event reached a final disposition and may be acknowledged;
  - raising means it did not, and the event must be delivered again.
Bad events are an expected outcome, not an error: they are recorded, never raised.
"""
import logging
from typing import Any, Optional

from app.validator import EventValidator

log = logging.getLogger("processor")


class EventProcessor:
    def __init__(self, validator: EventValidator):
        self.validator = validator

    def process(self, event: Any) -> None:
        """Handle an event the consumer already parsed from a message's JSON."""
        result = self.validator.validate(event)
        ids = "event_id=%s event_type=%s" % (
            event.get("event_id") if isinstance(event, dict) else None,
            event.get("event_type") if isinstance(event, dict) else None,
        )
        if result.is_valid:
            log.info("READY %s", ids)
        else:
            log.warning("INVALID %s errors=%s", ids, result.errors)

    def process_invalid_json(self, raw: Optional[bytes]) -> None:
        """Handle a message value that is not valid JSON; the raw bytes are all we have."""
        log.warning("INVALID errors=%s raw=%r", ["value is not valid JSON"], (raw or b"")[:200])
