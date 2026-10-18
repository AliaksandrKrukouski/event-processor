"""Kafka consumer: reads events from the `events` topic and logs their metadata.

Offsets are committed only after a message has been handled, so the service
can be stopped and restarted without losing its position.
"""
import json
import logging
import os
import signal
import sys
import threading

from confluent_kafka import Consumer, KafkaError

log = logging.getLogger("event_processor")


def handle(msg) -> None:
    """Deserialize one message and log its metadata."""
    try:
        event = json.loads(msg.value())
    except ValueError:  # covers bad JSON and bad UTF-8
        log.warning(
            "malformed message topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )
        return

    if not isinstance(event, dict):
        event = {}
    log.info(
        "received topic=%s partition=%d offset=%d key=%s event_id=%s event_type=%s",
        msg.topic(), msg.partition(), msg.offset(), msg.key().decode() if msg.key() else None,
        event.get("event_id"), event.get("event_type"),
    )


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    topic = os.getenv("KAFKA_TOPIC", "events")
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "event-processor"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit manually, after the message is handled
    })

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    consumer.subscribe(
        [topic],
        on_assign=lambda _, parts: log.info("assigned partitions: %s", [p.partition for p in parts]),
    )
    log.info("consuming topic=%s", topic)

    exit_code = 0
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                if msg.error().fatal():
                    exit_code = 1
                    break
                continue

            handle(msg)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        log.info("shutting down")
        consumer.close()  # leaves the group so partitions are reassigned promptly
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
