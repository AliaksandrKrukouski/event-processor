"""Kafka entry point: reads messages, passes them to the processor, commits offsets afterwards.

Messages the processor cannot handle go to the dead-letter topic before their offset is
committed. If the database is down the consumer stops instead, and Kafka redelivers.
"""
import json
import logging
import os
import signal
import sys

from confluent_kafka import Consumer, Message, Producer

from app.processor import EventProcessor
from app.store import EventStore, Source, StoreUnavailable
from app.validator import EventValidator

log = logging.getLogger("consumer")


class EventConsumer:
    def __init__(
        self, bootstrap_servers: str, group_id: str, topic: str, processor: EventProcessor,
        dlq_topic: str = "events.dlq",
    ):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.processor = processor
        self.running = True
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit after processing
        })
        self._producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})

    def stop(self) -> None:
        """Finish the current message and leave run(). Safe to call from a signal handler."""
        self.running = False

    def run(self) -> int:
        """Consume until stop() or SIGTERM/SIGINT. Returns the exit code. Main thread only."""
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
                    try:
                        event = json.loads(msg.value())
                    except (ValueError, TypeError):  # bad JSON, bad UTF-8 or no value at all
                        self.processor.process_invalid_json(msg.value(), source)
                    else:
                        self.processor.process(event, source)
                except StoreUnavailable:
                    raise  # the database is the problem, not the message: leave it uncommitted
                except Exception as error:  # poison message
                    log.exception("processing failed, sending to %s", self.dlq_topic)
                    self._dead_letter(msg, error)
                # only reached once the message is stored or dead-lettered
                self._consumer.commit(message=msg, asynchronous=False)
        finally:
            log.info("shutting down")
            self._consumer.close()  # leave the group so partitions move to another consumer
        return exit_code

    def _dead_letter(self, msg: Message, error: Exception) -> None:
        """Copy the message to the DLQ topic and wait for the broker to confirm it."""
        results = []
        self._producer.produce(
            self.dlq_topic,
            value=msg.value(),
            key=msg.key(),
            headers=[
                ("error", ("%s: %s" % (type(error).__name__, error)).encode()),
                ("source-topic", msg.topic().encode()),
                ("source-partition", str(msg.partition()).encode()),
                ("source-offset", str(msg.offset()).encode()),
            ],
            on_delivery=lambda err, _msg: results.append(err),
        )
        unsent = self._producer.flush(10)
        if unsent or results != [None]:
            raise RuntimeError("could not publish to %s: %s" % (self.dlq_topic, results or "timeout"))
        log.warning("dead-lettered topic=%s partition=%d offset=%d", msg.topic(), msg.partition(), msg.offset())


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    validator = EventValidator()  # loads and checks all schemas

    store = EventStore(os.getenv("POSTGRES_URL", "postgresql://events:events@postgres:5432/events"))
    try:
        consumer = EventConsumer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            group_id=os.getenv("KAFKA_GROUP_ID", "event-processor"),
            topic=os.getenv("KAFKA_TOPIC", "events"),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "events.dlq"),
            processor=EventProcessor(validator, store),
        )
        return consumer.run()
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
