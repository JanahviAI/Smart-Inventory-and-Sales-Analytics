"""
Streaming Event Generator.
Generates reproducible synthetic streaming sales events for Redpanda and the consumer.

Includes:
- Valid sales events for known catalog products
- Intentional duplicate events to test deduplication and exactly-once processing
- Specific invalid/malformed events to test rejected-event logging
- Deterministic output with configurable seed
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def generate_streaming_events(
    total_count: int = 1050,
    seed: int = 42,
    duplicate_count: int = 50,
    invalid_count: int = 50,
    product_id_range: tuple[int, int] = (101, 300),
    output_file: Path | None = None,
) -> List[Dict[str, Any]]:
    """
    Deterministically generates synthetic streaming events.

    Args:
        total_count: Total events in stream (default: 1,050).
        seed: Random seed for 100% reproducibility.
        duplicate_count: Number of duplicate events to inject (default: 50).
        invalid_count: Number of malformed/invalid events to inject (default: 50).
        product_id_range: Range of known product IDs with ample stock.
        output_file: Optional path to persist the generated JSON event list.

    Returns:
        List of generated event dictionaries ready to publish.
    """
    rng = random.Random(seed)
    valid_count = max(0, total_count - duplicate_count - invalid_count)

    base_time = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    events: List[Dict[str, Any]] = []

    # 1. Generate Valid Events
    valid_events: List[Dict[str, Any]] = []
    min_pid, max_pid = product_id_range

    for i in range(1, valid_count + 1):
        evt_time = base_time + timedelta(seconds=i * 2)
        pid = rng.randint(min_pid, max_pid)
        qty = rng.choice([1, 1, 1, 2])  # mostly 1, occasionally 2
        evt = {
            "event_id": f"evt-stream-{i:06d}",
            "event_time": evt_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "order_id": f"ord-stream-{i:06d}",
            "product_id": pid,
            "quantity": qty,
        }
        valid_events.append(evt)

    # 2. Generate Duplicate Events (exact clones of earlier valid events)
    duplicate_events: List[Dict[str, Any]] = []
    # Pick randomly from the first half of valid events
    sampled_for_dup = rng.sample(valid_events[:max(1, len(valid_events) // 2)], min(duplicate_count, len(valid_events)))
    for orig in sampled_for_dup:
        duplicate_events.append(dict(orig))

    # 3. Generate Invalid / Malformed Events
    invalid_events: List[Dict[str, Any]] = []
    invalid_types = [
        "negative_quantity",
        "zero_quantity",
        "missing_event_id",
        "unknown_product",
        "bad_timestamp",
        "excessive_quantity",
    ]

    for j in range(1, invalid_count + 1):
        itype = rng.choice(invalid_types)
        evt_time = (base_time + timedelta(seconds=j * 5)).strftime("%Y-%m-%dT%H:%M:%SZ")

        if itype == "negative_quantity":
            inv_evt = {
                "event_id": f"evt-invalid-negqty-{j:04d}",
                "event_time": evt_time,
                "order_id": f"ord-invalid-{j:04d}",
                "product_id": rng.randint(min_pid, max_pid),
                "quantity": -rng.randint(1, 5),
            }
        elif itype == "zero_quantity":
            inv_evt = {
                "event_id": f"evt-invalid-zeroqty-{j:04d}",
                "event_time": evt_time,
                "order_id": f"ord-invalid-{j:04d}",
                "product_id": rng.randint(min_pid, max_pid),
                "quantity": 0,
            }
        elif itype == "missing_event_id":
            inv_evt = {
                "event_id": "",
                "event_time": evt_time,
                "order_id": f"ord-invalid-{j:04d}",
                "product_id": rng.randint(min_pid, max_pid),
                "quantity": 1,
            }
        elif itype == "unknown_product":
            inv_evt = {
                "event_id": f"evt-invalid-unkprod-{j:04d}",
                "event_time": evt_time,
                "order_id": f"ord-invalid-{j:04d}",
                "product_id": 999999,
                "quantity": 1,
            }
        elif itype == "bad_timestamp":
            inv_evt = {
                "event_id": f"evt-invalid-badtime-{j:04d}",
                "event_time": "2026-03-01 10:00:00",  # missing UTC timezone indicator
                "order_id": f"ord-invalid-{j:04d}",
                "product_id": rng.randint(min_pid, max_pid),
                "quantity": 1,
            }
        else:  # excessive_quantity
            inv_evt = {
                "event_id": f"evt-invalid-excess-{j:04d}",
                "event_time": evt_time,
                "order_id": f"ord-invalid-{j:04d}",
                "product_id": rng.randint(min_pid, max_pid),
                "quantity": 99999,
            }
        invalid_events.append(inv_evt)

    # 4. Assemble the stream sequence:
    # We place valid events first, then intersperse duplicates and invalids
    # so that the valid original is processed before its duplicate arrives.
    events = []
    # Split valid into chunks and intersperse
    events.extend(valid_events)
    events.extend(duplicate_events)
    events.extend(invalid_events)

    # Deterministically shuffle the second half so duplicates and invalids arrive naturally
    # but ensure originals remain before duplicates:
    seen_ids = set()
    ordered_stream = []
    pending_dups = []

    for evt in events:
        eid = evt.get("event_id")
        if eid in seen_ids:
            pending_dups.append(evt)
        else:
            seen_ids.add(eid)
            ordered_stream.append(evt)

    # Append duplicate attempts after their originals
    ordered_stream.extend(pending_dups)

    if output_file:
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(ordered_stream, f, indent=2)
        logger.info("Saved %d streaming events to %s", len(ordered_stream), output_file)

    return ordered_stream

