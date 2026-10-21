"""PostgreSQL persistence for processed events.

Every write is a single INSERT in its own transaction, with ON CONFLICT (event_id) DO
NOTHING, so replaying a message (after a crash, rebalance or producer retry) is harmless.
A method returns only after the transaction has committed; it raises if the write failed.

Transient database errors (connection lost, server restarting, deadlock, ...) are retried
with exponential backoff on a fresh connection. If the connection drops after a commit but
before the reply arrives, the retry hits the unique constraint and is reported as a
duplicate, which is harmless. Errors that retrying cannot fix (bad data, constraint or
syntax errors) are raised immediately. When retries run out, the error is raised, nothing
is acknowledged to Kafka, and the message is redelivered.
"""
import logging
import time
from typing import Any, List, NamedTuple, Optional

import psycopg
from psycopg.types.json import Jsonb

log = logging.getLogger("store")

# OperationalError covers connection failures, admin shutdown, serialization failures and
# deadlocks; InterfaceError is raised when using a connection that has already been closed.
TRANSIENT_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

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
    def __init__(
        self,
        url: str,
        max_attempts: int = 6,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
    ):
        """With the defaults the waits between attempts are 0.5, 1, 2, 4 and 8 seconds.

        That total (about 15s) stays below the 30s container stop grace period, so a SIGTERM
        received while retrying still ends in a clean exit.
        """
        self._url = url
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._conn: Optional[psycopg.Connection] = None  # opened on first use, reopened after errors

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
        for attempt in range(1, self._max_attempts + 1):
            try:
                if self._conn is None:
                    # autocommit: each `with transaction()` below is then a real BEGIN/COMMIT
                    self._conn = psycopg.connect(self._url, autocommit=True, connect_timeout=10)
                with self._conn.transaction():
                    inserted = self._conn.execute(INSERT_SQL, row).rowcount == 1
                break
            except TRANSIENT_ERRORS as e:
                self._drop_connection()  # the next attempt starts from a fresh connection
                if attempt == self._max_attempts:
                    log.error("insert failed after %d attempts: %s", attempt, e)
                    raise
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
