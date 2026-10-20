"""Business logic for a single event: parse, validate (routing and persistence follow).

Receives Kafka messages from the consumer. Contract with the caller:
  - returning normally means the event reached a final disposition and may be acknowledged;
  - raising means it did not, and the event must be delivered again.
Bad events are an expected outcome, not an error: they are recorded, never raised.
"""
import json
import logging

from confluent_kafka import Message

from app.validator import EventValidator

log = logging.getLogger("processor")


class EventProcessor:
    def __init__(self, validator: EventValidator):
        self.validator = validator

    def process(self, message: Message) -> None:
        source = "topic=%s partition=%d offset=%d key=%s" % (
            message.topic(), message.partition(), message.offset(),
            message.key().decode() if message.key() else None,
        )

        event = json.loads(message.value())  # assumes well-formed JSON for now

        result = self.validator.validate(event)
        ids = "event_id=%s event_type=%s" % (
            event.get("event_id") if isinstance(event, dict) else None,
            event.get("event_type") if isinstance(event, dict) else None,
        )
        if result.is_valid:
            log.info("READY %s %s", source, ids)
        else:
            log.warning("INVALID %s %s errors=%s", source, ids, result.errors)
