import logging
import pytest

from app.consumer import EventConsumer
from app.store import Source


class FakeKafkaMessage:
    def __init__(self, value=b"{}"): self._value = value
    def error(self): return None
    def topic(self): return "events"
    def partition(self): return 0
    def offset(self): return 5
    def key(self): return b"tenant-001"
    def value(self): return self._value


class FakeKafka:
    """Stands in for confluent_kafka.Consumer: delivers one message, then asks the consumer to stop."""

    def __init__(self, stop, value=b"{}"):
        self.value = value
        self.stop, self.committed, self.closed = stop, [], False

    def subscribe(self, *args, **kwargs): pass

    def poll(self, timeout):
        self.stop()
        return FakeKafkaMessage(self.value)

    def commit(self, message, asynchronous):
        self.committed.append(message.offset())

    def close(self): self.closed = True


class RecordingProcessor:
    def __init__(self): self.events, self.invalid = [], []
    def process(self, event, source): self.events.append((event, source))
    def process_invalid_json(self, raw, source): self.invalid.append((raw, source))


class FailingProcessor:
    def process(self, event, source): raise RuntimeError("db down")
    def process_invalid_json(self, raw, source): raise RuntimeError("db down")


def build(processor, value=b"{}"):
    consumer = EventConsumer("unused:9092", "g", "events", processor)
    consumer._consumer = FakeKafka(consumer.stop, value)  # delivers one message, then stops
    return consumer


def test_offset_is_committed_after_processing():
    processor = RecordingProcessor()
    consumer = build(processor)
    assert consumer.run() == 0
    assert processor.events == [({}, Source('events', 0, 5))]  # the parsed JSON is handed to the processor
    assert processor.invalid == []
    assert consumer._consumer.committed == [5]
    assert consumer._consumer.closed


def test_offset_is_not_committed_when_processing_fails():
    consumer = build(FailingProcessor())
    with pytest.raises(RuntimeError):
        consumer.run()
    assert consumer._consumer.committed == []
    assert consumer._consumer.closed  # still leaves the group cleanly


@pytest.mark.parametrize("value", [b"{not json", b"", b"\xff\xfe", None])
def test_invalid_json_goes_to_process_invalid_json_and_is_committed(value):
    processor = RecordingProcessor()
    consumer = build(processor, value)
    assert consumer.run() == 0
    assert processor.events == []
    assert processor.invalid == [(value, Source('events', 0, 5))]
    assert consumer._consumer.committed == [5]


def test_kafka_message_details_are_logged_by_the_consumer(caplog):
    caplog.set_level(logging.INFO)
    consumer = build(RecordingProcessor())
    consumer.run()
    assert "received topic=events partition=0 offset=5 key=tenant-001" in caplog.text


def test_sigterm_stops_the_consumer():
    import os
    import signal

    class SignalingKafka(FakeKafka):
        def poll(self, timeout):
            os.kill(os.getpid(), signal.SIGTERM)  # what `docker stop` does
            return None

    consumer = build(RecordingProcessor())
    consumer._consumer = SignalingKafka(consumer.stop)
    assert consumer.run() == 0
    assert consumer._consumer.closed
