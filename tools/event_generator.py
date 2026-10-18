"""Sample event producer: publishes valid and invalid events to Kafka.

Usage:
    python tools/event_generator.py --scenario mixed --count 20
    python tools/event_generator.py --scenario valid --count 5 --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone

SCENARIOS = [
    "valid",
    "invalid-payload",
    "unknown-type",
    "unknown-version",
    "missing-fields",
    "malformed-json",
    "duplicates",
    "mixed",
]
TENANTS = ["tenant-001", "tenant-002", "tenant-003"]
# Scenarios cycled through by "mixed" (duplicates are covered by their own scenario).
MIXED = [s for s in SCENARIOS if s not in ("mixed", "duplicates")]


def payment_payload(rng: random.Random, n: int) -> dict:
    return {
        "transaction_id": f"txn_{n:03d}",
        "amount": round(rng.uniform(1, 500), 2),
        "currency": rng.choice(["USD", "EUR", "BYN"]),
    }


def alert_payload(rng: random.Random, n: int) -> dict:
    return {
        "service": rng.choice(["auth", "payments", "gateway"]),
        "severity": rng.choice(["info", "warning", "critical"]),
        "message": f"alert {n}",
    }


def valid_event(rng: random.Random, n: int) -> dict:
    event_type = rng.choice(["payment.authorized", "monitoring.alert"])
    payload = payment_payload(rng, n) if event_type == "payment.authorized" else alert_payload(rng, n)
    return {
        "event_id": f"event_{n:03d}",
        "event_type": event_type,
        "tenant_id": rng.choice(TENANTS),
        "schema_version": "1.0",
        "occurred_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "payload": payload,
    }


def build(kind: str, rng: random.Random, n: int):
    """Return (tenant_key, raw_bytes) for one message of the given kind."""
    event = valid_event(rng, n)

    if kind == "invalid-payload":
        if event["event_type"] == "payment.authorized":
            event["payload"]["amount"] = -5
        else:
            event["payload"]["severity"] = "fatal"
    elif kind == "unknown-type":
        event["event_type"] = "order.created"
    elif kind == "unknown-version":
        event["schema_version"] = "9.9"
    elif kind == "missing-fields":
        del event["tenant_id"]
    elif kind == "malformed-json":
        return None, b'{"event_id": "event_%03d", "event_type": ' % n

    return event.get("tenant_id"), json.dumps(event).encode()


def messages(scenario: str, count: int, start: int, rng: random.Random):
    """Yield (key, value) pairs for the scenario."""
    for i in range(count):
        n = start + i
        if scenario == "mixed":
            kind = MIXED[i % len(MIXED)]
        elif scenario == "duplicates":
            kind = "valid"
        else:
            kind = scenario

        key, value = build(kind, rng, n)
        yield key, value
        if scenario == "duplicates":
            yield key, value  # same event_id sent twice


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish sample events to Kafka.")
    parser.add_argument("--scenario", choices=SCENARIOS, default="mixed")
    parser.add_argument("--count", type=int, default=10, help="number of events (duplicates sends each twice)")
    parser.add_argument("--start", type=int, default=1, help="first event number: event_001, event_002, ...")
    parser.add_argument("--seed", type=int, default=None, help="seed for reproducible data")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "events"))
    parser.add_argument("--dry-run", action="store_true", help="print messages instead of sending")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    msgs = list(messages(args.scenario, args.count, args.start, rng))

    if args.dry_run:
        for key, value in msgs:
            print(f"key={key} value={value.decode()}")
        return 0

    from confluent_kafka import Producer  # imported here so --dry-run needs no Kafka client

    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(err)

    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})
    for key, value in msgs:
        producer.produce(args.topic, value=value, key=key, on_delivery=on_delivery)
        producer.poll(0)
    producer.flush(30)

    sent = len(msgs) - len(failures)
    print(f"sent {sent}/{len(msgs)} messages to {args.topic} ({args.scenario})")
    if failures:
        print(f"first error: {failures[0]}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
