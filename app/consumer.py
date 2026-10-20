"""Entry point. Owns Kafka communication and message delivery guarantees.

Delivery is at-least-once: an offset is committed only after the processor has
handled the message, so a crash re-delivers it instead of losing it. Business
logic (validation, routing, persistence) lives in app.processor.
"""
import logging
import os
import signal
import sys
import threading

from confluent_kafka import Consumer

from app.processor import EventProcessor
from app.validator import EventValidator

log = logging.getLogger("consumer")


class EventConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str, processor: EventProcessor):
        self.topic = topic
        self.processor = processor
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit manually, after the message is processed
        })

    def run(self, stop: threading.Event) -> int:
        """Consume until `stop` is set. Returns a process exit code."""
        self._consumer.subscribe(
            [self.topic],
            on_assign=lambda _, parts: log.info("assigned partitions: %s", [p.partition for p in parts]),
        )
        log.info("consuming topic=%s", self.topic)

        exit_code = 0
        try:
            while not stop.is_set():
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("kafka error: %s", msg.error())
                    if msg.error().fatal():
                        exit_code = 1
                        break
                    continue
                    
                self.processor.process(msg)
                # Reached only if process() returned; if it raised, nothing is committed.
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

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    consumer = EventConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "event-processor"),
        topic=os.getenv("KAFKA_TOPIC", "events"),
        processor=EventProcessor(validator),
    )
    return consumer.run(stop)


if __name__ == "__main__":
    sys.exit(main())
