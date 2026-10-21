"""Writes events to PostgreSQL.

Each save is one INSERT ... ON CONFLICT (event_id) DO NOTHING in its own transaction, so
saving the same event twice is harmless. Connection-level errors are retried with
exponential backoff; if they persist, StoreUnavailable is raised. Other errors (bad data
and so on) are raised straight away.
"""
import logging
import time
from typing import Any, List, NamedTuple, Optional

import psycopg
from psycopg.types.json import Jsonb

log = logging.getLogger("store")

# connection failures, server shutdown, deadlocks; InterfaceError means the connection is closed
TRANSIENT_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

INSERT_SQL = """
INSERT INTO events (
    event_id, tenant_id, event_type, schema_version, occurred_at,
    payload, raw_message, status, validation_errors, routing_targets,
    source_topic, source_partition, source_offset
) VALUES (
    %(event_id)s, %(tenant_id)s, %(event_type)s, %(schema_version)s, %(occurred_at)s,
    %(payload)s, %(raw_message)s, %(status)s, %(validation_errors)s, %(routing_targets)s,
    %(source_topic)s, %(source_partition)s, %(source_offset)s
)
ON CONFLICT (event_id) DO NOTHING
"""


class StoreUnavailable(Exception):
    """The database is still failing after all retries. The message itself may be fine."""


class Source(NamedTuple):
    """Where a message came from in Kafka."""
    topic: str
    partition: int
    offset: int


def _text(value: Any) -> Optional[str]:
    """The value if it is a non-empty string, otherwise None."""
    return value if isinstance(value, str) and value else None


class EventStore:
    def __init__(
        self,
        url: str,
        max_attempts: int = 6,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
    ):
        # Defaults wait 0.5, 1, 2, 4 and 8 seconds (about 15s), less than the 30s stop grace period.
        self._url = url
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._conn: Optional[psycopg.Connection] = None  # opened on first use

    def save_ready(self, event: dict, source: Source, routing_targets: List[str]) -> bool:
        """Store a valid event. Returns False if the event_id is already stored."""
        return self._insert(
            status="READY",
            event_id=event["event_id"],
            tenant_id=event["tenant_id"],
            event_type=event["event_type"],
            schema_version=event["schema_version"],
            occurred_at=event["occurred_at"],  # already checked as a date-time
            payload=Jsonb(event["payload"]),
            routing_targets=Jsonb(routing_targets),
            source=source,
        )

    def save_invalid(self, errors: List[str], source: Source, raw: Optional[bytes], event: Any = None) -> bool:
        """Store an event that failed validation; `event` is None if it was not JSON.

        Envelope columns are filled in where the values look usable. Without a usable
        event_id the key is built from the Kafka position.
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
            "payload": None, "raw_message": None, "validation_errors": None, "routing_targets": None,
            "source_topic": source.topic, "source_partition": source.partition,
            "source_offset": source.offset,
            **columns,
        }
        for attempt in range(1, self._max_attempts + 1):
            try:
                if self._conn is None:
                    self._conn = psycopg.connect(self._url, autocommit=True, connect_timeout=10)
                with self._conn.transaction():
                    inserted = self._conn.execute(INSERT_SQL, row).rowcount == 1
                break
            except TRANSIENT_ERRORS as e:
                self._drop_connection()  # reconnect on the next attempt
                if attempt == self._max_attempts:
                    log.error("insert failed after %d attempts: %s", attempt, e)
                    raise StoreUnavailable(str(e)) from e
                delay = min(self._max_delay, self._base_delay * 2 ** (attempt - 1))
                log.warning("insert failed (attempt %d/%d), retrying in %.1fs: %s",
                            attempt, self._max_attempts, delay, e)
                time.sleep(delay)
        if not inserted:
            log.info("duplicate event_id=%s ignored", row["event_id"])
        return inserted

    def _drop_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except psycopg.Error:
                pass

    def close(self) -> None:
        self._drop_connection()
