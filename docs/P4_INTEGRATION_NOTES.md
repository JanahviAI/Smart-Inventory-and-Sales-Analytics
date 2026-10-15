# Person 4 Streaming and Dashboard Notes

## Contracts used

- Redpanda topic: `sales.events`.
- Event fields: `event_id`, timezone-aware ISO-8601 `event_time`, `order_id`, integer `product_id`, and positive integer `quantity`.
- Accepted events decrement `inventory.current_stock` once. Insufficient stock is rejected without an inventory change.
- `inventory_events.event_id` is the database deduplication key. Invalid payloads and duplicate event IDs are written to `rejected_records` with source `inventory_events`.
- Kafka record timestamps are used for measured publish-to-persistence latency and saved to `inventory_events.latency_ms`.

## Local run commands

Install project dependencies, configure `.env`, start the team Redpanda/PostgreSQL services, and initialize the shared schema before running these commands. The referenced product must already exist in both `products` and `inventory`.

```powershell
python -m src.streaming.consumer
```

In a second terminal, publish a valid event, its duplicate, and an invalid quantity event for product 101:

```powershell
python -m src.streaming.producer --demo --product-id 101
```

Run the dashboard:

```powershell
streamlit run dashboard/app.py
```

After consuming the events, generate the report from persisted database rows:

```powershell
python -m scripts.report_streaming_results
```

Environment settings:

```dotenv
REDPANDA_BOOTSTRAP_SERVERS=localhost:9092
REDPANDA_TOPIC=sales.events
REDPANDA_CONSUMER_GROUP=smart-inventory-consumer
REDPANDA_AUTO_OFFSET_RESET=earliest
```

## Integration dependencies / current gaps

- A Compose file and Redpanda service are not present in the current repository checkout; coordinate ports, health checks, and shared environment values with the teammate integrating Compose.
- Airflow, NiFi, the analytics/recommendation modules, and FastAPI service are not present in this checkout yet. Their dashboard views show empty states or service/data errors until those outputs exist.
- Schema creation is intentionally not triggered by the dashboard or consumer. The schema contains destructive `DROP TABLE` statements, so initialize it only through the team's agreed setup procedure.
- `results/streaming_results.json` and dashboard screenshots should be generated only after a real end-to-end run. No execution results are asserted by this implementation note.

## Operational behavior

The consumer commits a Kafka offset only after database processing completes. Replayed messages are safe because accepted event IDs have a primary key and the inventory decrement and event insert share one database transaction. Malformed JSON, invalid event data, unknown products, and duplicate IDs are captured in `rejected_records`; valid-shaped events rejected for insufficient stock or missing inventory are captured in `inventory_events` with their status and reason.
