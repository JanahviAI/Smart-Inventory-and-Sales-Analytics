"""Consume Redpanda sale events and apply the agreed inventory rule."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.common.db import DatabaseManager

logger = logging.getLogger(__name__)


def _parse_event_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_time is required")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("event_time must include a timezone")
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("event_time must be UTC")
    return parsed.astimezone(timezone.utc)


def _validate_event(event: Any) -> tuple[dict[str, Any], datetime]:
    if not isinstance(event, dict):
        raise ValueError("event payload must be a JSON object")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip() or len(event_id) > 100:
        raise ValueError("event_id must be a non-empty string of at most 100 characters")
    order_id = event.get("order_id")
    if not isinstance(order_id, str) or not order_id.strip() or len(order_id) > 50:
        raise ValueError("order_id must be a non-empty string of at most 50 characters")
    product_id = event.get("product_id")
    if isinstance(product_id, bool) or not isinstance(product_id, int) or product_id <= 0:
        raise ValueError("product_id must be a positive integer")
    quantity = event.get("quantity")
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise ValueError("quantity must be a positive integer")
    event_time = _parse_event_time(event.get("event_time"))
    return {
        "event_id": event_id.strip(),
        "order_id": order_id.strip(),
        "product_id": product_id,
        "quantity": quantity,
    }, event_time


def _record_rejection(event: Any, reason: str, *, run_id: str | None = None) -> dict[str, Any]:
    """Persist malformed/duplicate events in the shared quality audit table."""
    payload = json.dumps(event, default=str, separators=(",", ":"))
    event_id = event.get("event_id") if isinstance(event, dict) else None
    safe_id = str(event_id)[:100] if event_id is not None else None
    audit_run_id = (run_id or (f"stream-{safe_id}" if safe_id else f"stream-{uuid.uuid4().hex}"))[:100]
    params = {
        "run_id": audit_run_id,
        "record_identifier": safe_id,
        "payload": payload,
        "reason": reason[:255],
    }
    with DatabaseManager.get_engine().begin() as connection:
        if connection.dialect.name == "postgresql":
            connection.execute(text(
                "INSERT INTO rejected_records "
                "(run_id, source_table, record_identifier, raw_payload, reason) "
                "VALUES (:run_id, 'inventory_events', :record_identifier, CAST(:payload AS JSONB), :reason)"
            ), params)
        else:
            connection.execute(text(
                "INSERT INTO rejected_records "
                "(run_id, source_table, record_identifier, raw_payload, reason) "
                "VALUES (:run_id, 'inventory_events', :record_identifier, :payload, :reason)"
            ), params)
    return {
        "status": "rejected",
        "event_id": safe_id,
        "reason": reason,
    }


def process_inventory_event(event: dict[str, Any], published_at: datetime | None = None) -> dict[str, Any]:
    """Validate, deduplicate, persist, and atomically decrement inventory once.

    ``published_at`` is the Kafka record timestamp, allowing the persisted row to
    include measured publish-to-database latency. Invalid payloads and duplicate
    IDs are written to ``rejected_records`` so attempts remain inspectable.
    """
    started = time.perf_counter()
    try:
        cleaned, event_time = _validate_event(event)
    except (TypeError, ValueError, OverflowError) as exc:
        return _record_rejection(event, f"INVALID_EVENT: {exc}")

    latency_ms = None
    if published_at is not None:
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        latency_ms = max(0.0, (datetime.now(timezone.utc) - published_at.astimezone(timezone.utc)).total_seconds() * 1000)

    engine = DatabaseManager.get_engine()
    lock_suffix = " FOR UPDATE" if engine.dialect.name == "postgresql" else ""
    try:
        with engine.begin() as connection:
            inventory = connection.execute(text(
                "SELECT current_stock FROM inventory WHERE product_id = :product_id" + lock_suffix
            ), {"product_id": cleaned["product_id"]}).mappings().first()

            product = connection.execute(text(
                "SELECT product_id FROM products WHERE product_id = :product_id"
            ), {"product_id": cleaned["product_id"]}).first()
            if not product:
                raise LookupError("UNKNOWN_PRODUCT: product_id does not exist")

            rejection_reason = None
            status = "ACCEPTED"
            if not inventory:
                status, rejection_reason = "REJECTED", "MISSING_INVENTORY: no inventory row for product"
            elif inventory["current_stock"] < cleaned["quantity"]:
                status, rejection_reason = "REJECTED", "INSUFFICIENT_STOCK: sale would make stock negative"

            connection.execute(text(
                "INSERT INTO inventory_events "
                "(event_id, event_time, order_id, product_id, quantity, status, rejection_reason, latency_ms) "
                "VALUES (:event_id, :event_time, :order_id, :product_id, :quantity, :status, :reason, :latency_ms)"
            ), {
                **cleaned,
                "event_time": event_time,
                "status": status,
                "reason": rejection_reason,
                "latency_ms": latency_ms,
            })

            if status == "ACCEPTED":
                connection.execute(text(
                    "UPDATE inventory SET current_stock = current_stock - :quantity, "
                    "last_updated = CURRENT_TIMESTAMP WHERE product_id = :product_id"
                ), {"quantity": cleaned["quantity"], "product_id": cleaned["product_id"]})
    except LookupError as exc:
        return _record_rejection(event, str(exc))
    except IntegrityError:
        # The inventory row lock serializes normal same-product events; the unique
        # event_id constraint is the final protection for concurrent duplicate IDs.
        existing = DatabaseManager.fetch_one(
            "SELECT event_id FROM inventory_events WHERE event_id = :event_id",
            {"event_id": cleaned["event_id"]},
        )
        if existing:
            return _record_rejection(event, "DUPLICATE_EVENT_ID: event_id was already processed")
        raise

    duration_ms = (time.perf_counter() - started) * 1000
    if status == "REJECTED":
        return {
            "status": "rejected",
            "event_id": cleaned["event_id"],
            "reason": rejection_reason,
            "latency_ms": latency_ms,
            "processing_duration_ms": duration_ms,
        }
    return {
        "status": "accepted",
        "event_id": cleaned["event_id"],
        "product_id": cleaned["product_id"],
        "quantity": cleaned["quantity"],
        "latency_ms": latency_ms,
        "processing_duration_ms": duration_ms,
    }


def build_consumer() -> Consumer:
    """Create a consumer configured for the configured Redpanda topic."""
    servers = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("REDPANDA_CONSUMER_GROUP", "smart-inventory-consumer")
    consumer = Consumer({
        "bootstrap.servers": servers,
        "group.id": group_id,
        "auto.offset.reset": os.getenv("REDPANDA_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.getenv("REDPANDA_TOPIC", "sales.events")])
    return consumer


def _record_timestamp(message: Any) -> datetime | None:
    timestamp_type, timestamp_ms = message.timestamp()
    if timestamp_type < 0 or timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def consume_events(
    max_events: int | None = None,
    idle_timeout_seconds: float | None = 5.0,
) -> dict[str, Any]:
    """
    Consume events from the configured Redpanda topic.

    Args:
        max_events: If specified, stop after consuming this many events.
        idle_timeout_seconds: If specified and >0, stop if no new messages arrive within this window after receiving at least one event.
    """
    consumer = build_consumer()
    topic = os.getenv("REDPANDA_TOPIC", "sales.events")
    logger.info("Listening on topic %s (max_events=%s, timeout=%s)", topic, max_events, idle_timeout_seconds)

    consumed = 0
    accepted = 0
    rejected = 0
    duplicate = 0
    last_msg_time = time.time()
    started = time.time()

    try:
        while True:
            message = consumer.poll(1.0)
            now = time.time()
            if message is None:
                if consumed > 0 and idle_timeout_seconds is not None and (now - last_msg_time) >= idle_timeout_seconds:
                    logger.info("Idle timeout of %.1fs reached without new messages; stopping.", idle_timeout_seconds)
                    break
                continue

            last_msg_time = now

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error: %s", message.error())
                continue

            consumed += 1
            try:
                event = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                event = {"raw": message.value().decode("utf-8", errors="replace")}
                outcome = _record_rejection(event, f"INVALID_JSON: {exc}")
                rejected += 1
            else:
                outcome = process_inventory_event(event, published_at=_record_timestamp(message))
                status = outcome.get("status")
                if status == "accepted":
                    accepted += 1
                elif status == "rejected":
                    if "DUPLICATE_EVENT_ID" in outcome.get("reason", ""):
                        duplicate += 1
                    else:
                        rejected += 1

            # Persist before acknowledging the offset
            consumer.commit(message=message, asynchronous=False)

            if consumed % 200 == 0:
                logger.info("Consumed %d events: %d accepted, %d rejected, %d duplicate", consumed, accepted, rejected, duplicate)

            if max_events is not None and consumed >= max_events:
                logger.info("Reached target limit of %d events; stopping.", max_events)
                break
    finally:
        consumer.close()

    duration = round(time.time() - started, 3)
    summary = {
        "consumed": consumed,
        "accepted": accepted,
        "rejected": rejected,
        "duplicate": duplicate,
        "duration_seconds": duration,
    }
    logger.info("Consumer execution finished: %s", summary)
    return summary


def consume_forever() -> None:
    consume_events(max_events=None, idle_timeout_seconds=None)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-events", type=int, default=None, help="Stop after consuming N events")
    parser.add_argument("--timeout", type=float, default=5.0, help="Idle timeout in seconds (default: 5.0)")
    parser.add_argument("--report", action="store_true", help="Generate results/streaming_results.json after consuming")
    args = parser.parse_args()

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    if args.max_events is not None:
        stats = consume_events(max_events=args.max_events, idle_timeout_seconds=args.timeout)
        print(f"Streaming Consumer Finished:")
        print(f"  - Total Consumed: {stats['consumed']}")
        print(f"  - Accepted:       {stats['accepted']}")
        print(f"  - Rejected:       {stats['rejected']}")
        print(f"  - Duplicate:      {stats['duplicate']}")
        print(f"  - Duration:       {stats['duration_seconds']}s")
    else:
        consume_forever()

    if args.report:
        from scripts.report_streaming_results import main as report_main
        report_main()


if __name__ == "__main__":
    main()
