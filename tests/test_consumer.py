import threading

import pytest

from app.consumer import EventConsumer


class FakeKafkaMessage:
    def error(self): return None
    def topic(self): return "events"
    def partition(self): return 0
    def offset(self): return 5
    def key(self): return b"tenant-001"
    def value(self): return b"{}"


class FakeKafka:
    """Stands in for confluent_kafka.Consumer: delivers one message, then asks to stop."""

    def __init__(self, stop):
        self.stop, self.committed, self.closed = stop, [], False

    def subscribe(self, *args, **kwargs): pass

    def poll(self, timeout):
        self.stop.set()
        return FakeKafkaMessage()

    def commit(self, message, asynchronous):
        self.committed.append(message.offset())

    def close(self): self.closed = True


class RecordingProcessor:
    def __init__(self): self.seen = []
    def process(self, message): self.seen.append(message)


class FailingProcessor:
    def process(self, message): raise RuntimeError("db down")


def build(processor):
    stop = threading.Event()
    consumer = EventConsumer("unused:9092", "g", "events", processor)
    consumer._consumer = FakeKafka(stop)  # no broker needed
    return consumer, stop


def test_offset_is_committed_after_processing():
    processor = RecordingProcessor()
    consumer, stop = build(processor)
    assert consumer.run(stop) == 0
    assert len(processor.seen) == 1  # the Kafka message itself is handed to the processor
    assert consumer._consumer.committed == [5]
    assert consumer._consumer.closed


def test_offset_is_not_committed_when_processing_fails():
    consumer, stop = build(FailingProcessor())
    with pytest.raises(RuntimeError):
        consumer.run(stop)
    assert consumer._consumer.committed == []
    assert consumer._consumer.closed  # still leaves the group cleanly
