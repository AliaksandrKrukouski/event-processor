"""PostgreSQL persistence for processed events.

Every write is a single INSERT in its own transaction, with ON CONFLICT (event_id) DO
NOTHING, so replaying a message (after a crash, rebalance or producer retry) is harmless.
A method returns only after the transaction has committed; it raises if the write failed.
"""
import logging
from typing import Any, List, NamedTuple, Optional

import psycopg
from psycopg.types.json import Jsonb

log = logging.getLogger("store")

INSERT_SQL = """
INSERT INTO events (
    event_id, tenant_id, event_type, schema_version, occurred_at,
    payload, raw_message, status, validation_errors,
    source_topic, source_partition, source_offset
) VALUES (
    %(event_id)s, %(tenant_id)s, %(event_type)s, %(schema_version)s, %(occurred_at)s,
    %(payload)s, %(raw_message)s, %(status)s, %(validation_errors)s,
    %(source_topic)s, %(source_partition)s, %(source_offset)s
)
ON CONFLICT (event_id) DO NOTHING
"""


class Source(NamedTuple):
    """Where a message came from in Kafka."""
    topic: str
    partition: int
    offset: int


def _text(value: Any) -> Optional[str]:
    """A non-empty string, else None: keeps wrongly typed values out of TEXT columns."""
    return value if isinstance(value, str) and value else None


class EventStore:
    def __init__(self, url: str):
        # autocommit: each `with conn.transaction()` below is then a real BEGIN/COMMIT
        self._conn = psycopg.connect(url, autocommit=True)

    def save_ready(self, event: dict, source: Source) -> bool:
        """Store a valid event. Returns False if an event with this event_id already exists."""
        return self._insert(
            status="READY",
            event_id=event["event_id"],
            tenant_id=event["tenant_id"],
            event_type=event["event_type"],
            schema_version=event["schema_version"],
            occurred_at=event["occurred_at"],  # validated as RFC 3339; Postgres parses it
            payload=Jsonb(event["payload"]),
            source=source,
        )

    def save_invalid(self, errors: List[str], source: Source, raw: Optional[bytes], event: Any = None) -> bool:
        """Store an event that failed validation (`event` is None if it was not parseable JSON).

        Envelope columns are filled best-effort; the original bytes are always kept.
        Without a usable event_id the key falls back to the Kafka position.
        """
        fields = event if isinstance(event, dict) else {}
        return self._insert(
            status="INVALID",
            event_id=_text(fields.get("event_id")) or "invalid:%s:%d:%d" % source,
            tenant_id=_text(fields.get("tenant_id")),
            event_type=_text(fields.get("event_type")),
            schema_version=_text(fields.get("schema_version")),
            raw_message=raw if raw is not None else b"",
            validation_errors=Jsonb(errors),
            source=source,
        )

    def _insert(self, source: Source, **columns: Any) -> bool:
        row = {
            "tenant_id": None, "event_type": None, "schema_version": None, "occurred_at": None,
            "payload": None, "raw_message": None, "validation_errors": None,
            "source_topic": source.topic, "source_partition": source.partition,
            "source_offset": source.offset,
            **columns,
        }
        with self._conn.transaction():
            inserted = self._conn.execute(INSERT_SQL, row).rowcount == 1
        if not inserted:
            log.info("duplicate event_id=%s ignored", row["event_id"])
        return inserted

    def close(self) -> None:
        self._conn.close()
