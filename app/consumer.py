"""Entry point. Owns Kafka communication and message delivery guarantees.

Delivery is at-least-once: an offset is committed only after the processor has
handled the message, so a crash re-delivers it instead of losing it. Business
logic (validation, routing, persistence) lives in app.processor.
"""
import json
import logging
import os
import signal
import sys

from confluent_kafka import Consumer

from app.processor import EventProcessor
from app.store import EventStore, Source
from app.validator import EventValidator

log = logging.getLogger("consumer")


class EventConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str, processor: EventProcessor):
        self.topic = topic
        self.processor = processor
        self.running = True
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit manually, after the message is processed
        })

    def stop(self) -> None:
        """Ask run() to finish the message in flight and return. Safe to call from a signal handler."""
        self.running = False

    def run(self) -> int:
        """Consume until stop() is called, or SIGTERM/SIGINT arrives. Returns a process exit code.

        Must be called from the main thread, the only one that can install signal handlers.
        """
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT, lambda *_: self.stop())
        self._consumer.subscribe(
            [self.topic],
            on_assign=lambda _, parts: log.info("assigned partitions: %s", [p.partition for p in parts]),
        )
        log.info("consuming topic=%s", self.topic)

        exit_code = 0
        try:
            while self.running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("kafka error: %s", msg.error())
                    if msg.error().fatal():
                        exit_code = 1
                        break
                    continue

                log.info(
                    "received topic=%s partition=%d offset=%d key=%s",
                    msg.topic(), msg.partition(), msg.offset(),
                    msg.key().decode(errors="replace") if msg.key() else None,
                )
                source = Source(msg.topic(), msg.partition(), msg.offset())
                try:
                    event = json.loads(msg.value())
                except (ValueError, TypeError):  # bad JSON, bad UTF-8, or an empty (None) value
                    self.processor.process_invalid_json(msg.value(), source)
                else:
                    self.processor.process(event, source)
                # Reached only after the processor's database transaction committed;
                # if it raised, nothing is committed and the message is redelivered.
                self._consumer.commit(message=msg, asynchronous=False)
        finally:
            log.info("shutting down")
            self._consumer.close()  # leaves the group so partitions are reassigned promptly
        return exit_code


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    validator = EventValidator()  # loads and checks all schemas; fails fast if any is bad

    store = EventStore(os.getenv("POSTGRES_URL", "postgresql://events:events@postgres:5432/events"))
    try:
        consumer = EventConsumer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            group_id=os.getenv("KAFKA_GROUP_ID", "event-processor"),
            topic=os.getenv("KAFKA_TOPIC", "events"),
            processor=EventProcessor(validator, store),
        )
        return consumer.run()
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
