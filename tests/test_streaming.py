"""
Streaming Event Consumer Tests (Person 4 / Acceptance AT-05, AT-06, AT-07).
Verifies:
- AT-05: Valid event decrements inventory stock exactly once and persists to inventory_events.
- AT-06: Duplicate event ID does not decrement stock a second time.
- AT-07: Malformed/invalid events are rejected and recorded in rejected_records.
- Insufficient stock rejects the event without modifying inventory.
"""

from datetime import datetime, timezone
import pytest
from sqlalchemy import text

from src.common.db import DatabaseManager, init_db
from src.streaming.consumer import process_inventory_event


@pytest.fixture(autouse=True)
def setup_streaming_db():
    """Initializes schema and seeds baseline products and inventory."""
    init_db()
    with DatabaseManager.get_connection() as conn:
        # Seed known test product and stock
        conn.execute(
            text(
                "INSERT INTO products (product_id, product_name, category, supplier_id) "
                "VALUES (101, 'Test Mouse', 'Electronics', 'SUPP-001') "
                "ON CONFLICT (product_id) DO NOTHING"
            )
        )
        conn.execute(
            text(
                "INSERT INTO inventory (product_id, current_stock, reorder_level) "
                "VALUES (101, 50, 10) "
                "ON CONFLICT (product_id) DO UPDATE SET current_stock = 50"
            )
        )


def test_at_05_valid_event_decrements_stock_once():
    """AT-05: Valid event decrements stock by quantity and persists event."""
    event = {
        "event_id": "evt-test-valid-001",
        "event_time": "2026-01-16T10:00:00Z",
        "order_id": "ord-test-001",
        "product_id": 101,
        "quantity": 5,
    }

    result = process_inventory_event(event)
    assert result["status"] == "accepted"
    assert result["quantity"] == 5

    with DatabaseManager.get_connection() as conn:
        stock = conn.execute(text("SELECT current_stock FROM inventory WHERE product_id = 101")).scalar()
        event_row = conn.execute(
            text("SELECT status, quantity FROM inventory_events WHERE event_id = 'evt-test-valid-001'")
        ).fetchone()

    assert stock == 45
    assert event_row[0] == "ACCEPTED"
    assert event_row[1] == 5


def test_at_06_duplicate_event_prevents_second_stock_decrement():
    """AT-06: Duplicate event ID has no second effect on inventory stock."""
    event = {
        "event_id": "evt-test-dup-001",
        "event_time": "2026-01-16T10:00:00Z",
        "order_id": "ord-test-002",
        "product_id": 101,
        "quantity": 10,
    }

    # First attempt: accepted, stock drops 50 -> 40
    res1 = process_inventory_event(event)
    assert res1["status"] == "accepted"

    with DatabaseManager.get_connection() as conn:
        stock_after_first = conn.execute(text("SELECT current_stock FROM inventory WHERE product_id = 101")).scalar()
    assert stock_after_first == 40

    # Second attempt (same event_id): duplicate rejection, stock stays 40
    res2 = process_inventory_event(event)
    assert res2["status"] == "rejected"
    assert "DUPLICATE_EVENT" in res2["reason"]

    with DatabaseManager.get_connection() as conn:
        stock = conn.execute(text("SELECT current_stock FROM inventory WHERE product_id = 101")).scalar()
        rejections = conn.execute(
            text("SELECT count(*) FROM rejected_records WHERE record_identifier = 'evt-test-dup-001'")
        ).scalar()

    assert stock == 40  # Stock was NOT decremented a second time
    assert rejections >= 1  # Duplicate attempt was audited


def test_at_07_invalid_event_logged_to_rejected_records():
    """AT-07: Malformed event is rejected and visible in rejected_records."""
    bad_event = {
        "event_id": "evt-bad-001",
        "event_time": "2026-01-16T10:00:00Z",
        "order_id": "ord-bad",
        "product_id": 101,
        "quantity": -5,  # Invalid negative quantity
    }

    res = process_inventory_event(bad_event)
    assert res["status"] == "rejected"

    with DatabaseManager.get_connection() as conn:
        # Stock untouched
        stock = conn.execute(text("SELECT current_stock FROM inventory WHERE product_id = 101")).scalar()
        reject_entry = conn.execute(
            text("SELECT reason FROM rejected_records WHERE record_identifier = 'evt-bad-001'")
        ).fetchone()

    assert stock == 50
    assert reject_entry is not None
    assert "INVALID_EVENT" in reject_entry[0]


def test_insufficient_stock_rejection():
    """Event requesting more quantity than current stock is rejected."""
    excess_event = {
        "event_id": "evt-excess-001",
        "event_time": "2026-01-16T10:00:00Z",
        "order_id": "ord-excess",
        "product_id": 101,
        "quantity": 999,  # Exceeds current stock 50
    }

    res = process_inventory_event(excess_event)
    assert res["status"] == "rejected"
    assert "INSUFFICIENT_STOCK" in res["reason"]

    with DatabaseManager.get_connection() as conn:
        stock = conn.execute(text("SELECT current_stock FROM inventory WHERE product_id = 101")).scalar()

    assert stock == 50

