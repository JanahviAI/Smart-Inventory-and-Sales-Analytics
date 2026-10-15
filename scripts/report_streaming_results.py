"""Write a streaming results report from persisted PostgreSQL/SQLite rows."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.common.config import Config
from src.common.db import DatabaseManager


def main() -> None:
    event_counts = DatabaseManager.fetch_all(
        "SELECT status, COUNT(*) AS count FROM inventory_events GROUP BY status ORDER BY status"
    )
    latency = DatabaseManager.fetch_one(
        "SELECT COUNT(latency_ms) AS measured_events, "
        "ROUND(AVG(latency_ms), 2) AS average_ms, "
        "ROUND(MIN(latency_ms), 2) AS min_ms, "
        "ROUND(MAX(latency_ms), 2) AS max_ms "
        "FROM inventory_events WHERE latency_ms IS NOT NULL"
    )
    duplicate_rows = DatabaseManager.fetch_one(
        "SELECT COUNT(*) AS count FROM rejected_records "
        "WHERE source_table = 'inventory_events' AND reason LIKE 'DUPLICATE_EVENT_ID:%'"
    )
    invalid_rows = DatabaseManager.fetch_one(
        "SELECT COUNT(*) AS count FROM rejected_records "
        "WHERE source_table = 'inventory_events' AND reason LIKE 'INVALID_%'"
    )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_backend": DatabaseManager.get_engine().dialect.name,
        "event_counts_by_status": {row["status"]: row["count"] for row in event_counts},
        "duplicate_attempts_recorded": duplicate_rows["count"] if duplicate_rows else 0,
        "invalid_attempts_recorded": invalid_rows["count"] if invalid_rows else 0,
        "publish_to_persistence_latency_ms": dict(latency) if latency else {},
        "source": "database inventory_events and rejected_records",
    }
    Config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output = Config.RESULTS_DIR / "streaming_results.json"
    output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"Wrote measured streaming results to {output}")


if __name__ == "__main__":
    main()
