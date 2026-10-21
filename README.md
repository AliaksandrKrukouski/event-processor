# Event Processor

A Kafka consumer that validates events, routes them by tenant, and stores every outcome in
PostgreSQL. Invalid events are stored, not dropped; duplicates are ignored; Kafka offsets are
committed only after the database write succeeds; messages that cannot be processed go to a
dead-letter topic.

See [DESIGN.md](DESIGN.md) for the architecture, failure handling and trade-offs.

## How it works

```
Kafka (events) ─► consumer.py ─► processor.py ─┬─► validator.py  (JSON Schema)
                       │                       └─► store.py ─► PostgreSQL
                       └─ poison message ─► Kafka (events.dlq)
```

For each message the consumer parses the JSON, hands it to the processor, and commits the
Kafka offset only after the processor returns.

| Message | Result |
| --- | --- |
| Valid event for a known tenant | `READY` row with `routing_targets`, e.g. `["destination-A"]` |
| Fails envelope or payload schema, unknown type/version, unknown tenant, not an object | `INVALID` row with the error list |
| Not valid JSON | `INVALID` row with the raw bytes |
| Same `event_id` seen again | Ignored (`INSERT ... ON CONFLICT (event_id) DO NOTHING`) |
| Cannot be processed, whatever the retries (poison message) | Copied to `events.dlq`, then committed |
| Database unreachable after retries | Nothing committed; the process exits and the message is redelivered |

Delivery is at-least-once; the unique `event_id` makes it effectively once in the database.

## Run it

Requires Docker with Compose.

```bash
docker compose up -d --build
docker compose ps                      # kafka and postgres should be healthy
docker logs -f ep-processor            # follow the processor
```

This starts Kafka (KRaft, single node), creates the `events` (3 partitions) and `events.dlq`
topics, starts PostgreSQL with the schema from `db/init.sql`, and runs the processor.
Data lives in the `kafkadata` and `pgdata` volumes. `docker compose down -v` deletes them.

`db/init.sql` runs only when the Postgres volume is empty. After changing it, recreate the
volume.

### Send events

The generator runs on the host and publishes to `localhost:9092`:

```bash
pip install -r requirements.txt        # once, in a virtualenv
python tools/event_generator.py --scenario mixed --count 20
```

| Scenario | Sends |
| --- | --- |
| `valid` | Valid events |
| `invalid-payload` | Payload breaks its schema |
| `unknown-type` / `unknown-version` | No schema for the type or version |
| `missing-fields` | Envelope field missing |
| `malformed-json` | Not parseable JSON |
| `duplicates` | Each event sent twice |
| `poison` | Valid events that cannot be stored (NUL character in a string); end up in the DLQ |
| `mixed` | Cycles through the scenarios above except `duplicates` and `poison` |

Options: `--count N`, `--start N` (first event number, `event_001`, ...), `--seed N`,
`--dry-run` (print instead of sending), `--topic`, `--bootstrap`.

### Check the results

```bash
# Stored events
docker exec ep-postgres psql -U events -d events -c \
  "select event_id, tenant_id, status, routing_targets, validation_errors from events order by id desc limit 20"

# Counts per status
docker exec ep-postgres psql -U events -d events -c \
  "select status, count(*) from events group by status"

# Dead-lettered messages, with the error and source position in the headers
docker exec ep-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
  --topic events.dlq --from-beginning --timeout-ms 5000 --property print.headers=true
```

Try it: run `--scenario duplicates`, `--scenario poison`, then `docker compose stop
event-processor` and `docker compose start event-processor` to see that processing resumes
without reprocessing committed messages.

## Test

```bash
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

No Kafka is needed: the consumer tests use fakes. The store tests in `tests/test_store.py`
run against the PostgreSQL from `docker compose up -d postgres` (`localhost:5432`) and are
skipped if it is not reachable. Use `TEST_POSTGRES_URL` to point them elsewhere.

## Configuration

Environment variables of the processor (set in `docker-compose.yml`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092` | Kafka brokers |
| `KAFKA_TOPIC` | `events` | Input topic |
| `KAFKA_DLQ_TOPIC` | `events.dlq` | Dead-letter topic |
| `KAFKA_GROUP_ID` | `event-processor` | Consumer group |
| `POSTGRES_URL` | `postgresql://events:events@postgres:5432/events` | Database |
| `LOG_LEVEL` | `INFO` | Log level |

## Event format

Every event has a common envelope plus a payload that depends on `event_type`:

```json
{
  "event_id": "event_001",
  "event_type": "payment.authorized",
  "tenant_id": "tenant-001",
  "schema_version": "1.0",
  "occurred_at": "2026-10-18T10:00:00Z",
  "payload": { "transaction_id": "txn_001", "amount": 150.5, "currency": "USD" }
}
```

Schemas are in `schemas/`: `envelope.json` and one file per payload,
`schemas/<event_type>/<schema_version>.json`. They are loaded and checked at startup, so a
broken schema stops the service immediately. To add an event type, add its schema file; no
code change is needed for validation.

Supported types: `payment.authorized` and `monitoring.alert`.

Routing is the `TENANT_ROUTES` dictionary in `app/processor.py` (`tenant_id` → target).

## Layout

```
app/consumer.py     entry point: Kafka, offsets, graceful shutdown, DLQ
app/processor.py    per-event logic: validate, route, persist
app/validator.py    JSON Schema validation (envelope, then payload)
app/store.py        PostgreSQL writes: idempotent insert, retry with backoff
schemas/            JSON Schemas
db/init.sql         events table and indexes
tools/              event generator
tests/              unit and integration tests
```

## Shutdown and failures

- **SIGTERM / SIGINT** (`docker stop`): the consumer finishes the message in progress, closes
  its Kafka session so partitions are reassigned quickly, and exits with code 0.
- **Database errors:** connection problems and similar errors are retried with exponential
  backoff (0.5 s up to 8 s, about 15 s in total). If they persist, the process exits without
  committing and Docker restarts it; the message is read again.
- **Crash between the database write and the offset commit:** the message is redelivered and
  ignored as a duplicate.

## Known limitations

- Single consumer processing one message at a time; offsets are committed per message.
- `DELIVERED` exists in the schema, but nothing sets it: the delivery service is out of scope.
- Routing is by tenant only, from an in-code dictionary. There is no tenant authentication.
- A re-sent event with an already stored `event_id` is ignored, even if the first copy was
  `INVALID`.
- No tool yet for replaying messages from the DLQ.
