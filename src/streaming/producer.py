"""Publish inventory-affecting sale events to Redpanda."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def build_producer(bootstrap_servers: str | None = None) -> Producer:
    """Create a Kafka-compatible producer configured for Redpanda."""
    servers = bootstrap_servers or os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
    return Producer({
        "bootstrap.servers": servers,
        "client.id": os.getenv("REDPANDA_CLIENT_ID", "smart-inventory-producer"),
        "acks": "all",
        "enable.idempotence": True,
    })


def publish_event(
    producer: Producer,
    event: dict[str, Any],
    *,
    topic: str | None = None,
    timeout_seconds: float = 10,
) -> None:
    """Publish one JSON event and wait for broker acknowledgement."""
    topic_name = topic or os.getenv("REDPANDA_TOPIC", "sales.events")
    payload = json.dumps(event, separators=(",", ":"), default=str).encode("utf-8")

    delivery_errors: list[str] = []

    def on_delivery(error: Any, message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    producer.produce(topic_name, key=str(event.get("event_id", "")), value=payload, callback=on_delivery)
    remaining = producer.flush(timeout_seconds)
    if remaining:
        raise TimeoutError(f"{remaining} event(s) were not delivered within {timeout_seconds}s")
    if delivery_errors:
        raise RuntimeError(f"Redpanda delivery failed: {delivery_errors[0]}")


def publish_events(events: Iterable[dict[str, Any]]) -> int:
    """Publish events sequentially; return the number acknowledged by the broker."""
    producer = build_producer()
    count = 0
    for event in events:
        publish_event(producer, event)
        count += 1
        logger.info("Published event_id=%s", event.get("event_id", "<missing>"))
    return count


def demo_events(product_id: int = 101) -> list[dict[str, Any]]:
    """Build a deterministic accepted/duplicate/invalid demonstration sequence."""
    event = {
        "event_id": "demo-sale-event-001",
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "order_id": "demo-order-001",
        "product_id": product_id,
        "quantity": 1,
    }
    duplicate = dict(event)
    invalid = {
        "event_id": "demo-sale-event-invalid-001",
        "event_time": event["event_time"],
        "order_id": "demo-order-invalid",
        "product_id": product_id,
        "quantity": 0,
    }
    return [event, duplicate, invalid]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo", action="store_true", help="publish accepted, duplicate, and invalid examples")
    parser.add_argument("--product-id", type=int, default=101, help="product used by --demo (default: 101)")
    parser.add_argument("--event-file", help="JSON file containing one event or a list of events")
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    if args.demo:
        events = demo_events(args.product_id)
    elif args.event_file:
        with open(args.event_file, "r", encoding="utf-8") as event_file:
            payload = json.load(event_file)
        events = payload if isinstance(payload, list) else [payload]
    else:
        parser.error("provide --demo or --event-file")

    print(f"Published {publish_events(events)} event(s) to {os.getenv('REDPANDA_TOPIC', 'sales.events')}")


if __name__ == "__main__":
    main()
