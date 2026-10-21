-- Events produced by the Event Processor, ready for the Sender service.

CREATE TABLE IF NOT EXISTS events (
    id                BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- identity (envelope)
    event_id          TEXT        NOT NULL UNIQUE, -- the UNIQUE constraint makes writes idempotent
    tenant_id         TEXT,
    event_type        TEXT,
    schema_version    TEXT,
    occurred_at       TIMESTAMPTZ,

    -- content
    payload           JSONB,          -- NULL when the message could not be parsed
    raw_message       BYTEA,          -- original bytes, kept for unparseable/invalid messages

    -- processing result
    status            TEXT        NOT NULL CHECK (status IN ('READY', 'INVALID', 'DELIVERED')),
    validation_errors JSONB,          -- list of errors when status = 'INVALID'
    routing_targets   JSONB,          -- routing decision, e.g. ["webhook:client-a"]

    -- timestamps
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- where it came from, for troubleshooting
    source_topic      TEXT        NOT NULL,
    source_partition  INTEGER     NOT NULL,
    source_offset     BIGINT      NOT NULL
);

-- Sender hot path: next batch of ready events
CREATE INDEX idx_events_ready
    ON events (received_at, id)
    WHERE status = 'READY';

-- Per-tenant history
CREATE INDEX IF NOT EXISTS idx_events_tenant_received
    ON events (tenant_id, received_at);
