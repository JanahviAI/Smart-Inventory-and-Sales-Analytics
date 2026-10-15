# Smart Inventory and Sales Analytics Platform

An end-to-end Data Engineering project demonstrating batch ingestion, data quality, orchestration, analytical processing, event streaming, inventory recommendations, reverse ETL, and dashboard-based monitoring.

> **Status:** Initial planning / implementation. Add results, benchmarks, screenshots, and completion claims only after the corresponding workflow has actually run.

## 1. Goals

The platform demonstrates that the team can:
- Ingest sales, product, and inventory data repeatably.
- Detect invalid and duplicate records, explain rejections, and protect trusted tables.
- Orchestrate transformations with Airflow.
- Analyze data with SQL/DuckDB and export cleaned data to Parquet.
- Process events through Redpanda and persist them safely.
- Generate inventory reorder recommendations and send them to a mock downstream API.
- Visualize actual results and failures in a dashboard.

## 2. Architecture

### Batch path

```text
Source CSV files
    -> Apache NiFi
    -> shared data/landing/ directory
    -> Apache Airflow DAG
    -> PostgreSQL staging
    -> validation / deduplication / rejected-record logging
    -> trusted PostgreSQL tables
    -> Parquet exports
    -> DuckDB analytics
    -> reorder recommendations
    -> mock FastAPI inventory API
    -> reverse_etl_audit table
```

### Streaming path

```text
Python event producer
    -> Redpanda topic sales.events
    -> Python consumer
    -> validate and deduplicate by event_id
    -> PostgreSQL inventory_events
    -> inventory update using the agreed business rule
    -> Streamlit dashboard reads real stored results
```

| Technology | Responsibility |
|---|---|
| NiFi | Routes source files to the shared landing directory and exposes ingestion/queue status |
| Airflow | Schedules and orchestrates batch tasks, dependencies, retries, and task status |
| PostgreSQL | Operational/trusted tables, staging, rejects, event records, run metadata, audit |
| Parquet | Portable columnar exports of cleaned data |
| DuckDB | Analytical SQL over Parquet and/or documented extracts |
| Redpanda | Broker for `sales.events` |
| Python consumer | Validates, deduplicates, persists events, applies inventory rule |
| FastAPI | Mock downstream inventory/recommendation service |
| Streamlit | Visualizes actual pipeline, quality, streaming, analytics, and audit results |
| Polars | Optional fair benchmark against DuckDB for the same task |

**Storage decision:** PostgreSQL is the operational source of truth; plain Parquet is the analytics export format. Delta Lake is out of the initial scope unless the core workflow is complete and there is time to demonstrate a real versioning/time-travel use case. Do not claim Delta features for ordinary Parquet.

## 3. Repository layout

```text
.
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── README.md
├── data/
│   ├── source/                 # small deterministic demo CSVs
│   ├── landing/                # NiFi output
│   └── processed/              # generated Parquet/CSV outputs
├── sql/schema.sql
├── src/
│   ├── common/{config.py,db.py}
│   ├── etl/{staging.py,validation.py,export.py}
│   ├── analytics/{queries.py,recommendations.py,benchmark.py}
│   ├── streaming/{producer.py,consumer.py}
│   └── api/main.py
├── dags/retail_batch_pipeline.py
├── dashboard/app.py
├── tests/
├── results/                    # generated evidence; never invent values
└── docs/
    ├── ARCHITECTURE.md
    ├── DATA_DICTIONARY.md
    ├── INTERFACES.md
    └── ACCEPTANCE_TESTS.md
```

## 4. Source data contracts

Keep deterministic demo files in `data/source/`. A generator may create larger repeatable data for benchmarking.

**`sales.csv`**
- `order_id`
- `order_date` (use one documented ISO date/time format)
- `product_id`
- `quantity`
- `unit_price`
- Add `order_line_id` if repeated lines for the same product in an order are allowed.

**`products.csv`**
- `product_id`, `product_name`, `category`, `supplier_id`

**`inventory.csv`**
- `product_id`, `current_stock`, `reorder_level`

Before coding, agree on the sales business key. Use `(order_id, product_id)` only if the business rule guarantees one line per product per order; otherwise add a stable `order_line_id`.

### Minimum quality rules

1. Required columns must exist.
2. IDs and numeric fields must parse into agreed types.
3. Sales must reference known products.
4. `quantity` must be a positive integer.
5. `unit_price` must be numeric and nonnegative.
6. `current_stock` and `reorder_level` must be nonnegative integers.
7. Duplicate handling must follow the agreed business key.
8. Invalid records must be written to `rejected_records` with source, reason, run ID, and raw payload/row where available.
9. Re-running the same batch must not duplicate trusted sales.
10. Do not silently coerce invalid values into valid-looking values.

## 5. Streaming event contract

Topic: `sales.events`

```json
{
  "event_id": "evt-000001",
  "event_time": "2026-01-01T10:00:00Z",
  "order_id": "ord-000001",
  "product_id": 101,
  "quantity": 1
}
```

- `event_id` is required and is the idempotency key.
- `event_time` must be an ISO-8601 timestamp with timezone.
- `product_id` must exist; `quantity` must be a positive integer.
- Persist accepted events in `inventory_events`.
- A duplicate `event_id` must never decrement stock twice.
- Invalid events must be observable in rejected-event records.
- For the demo, accepted sale events decrement stock by `quantity`. If stock would become negative, reject the event and record the reason.
- Measure publish-to-database latency locally. Do not claim an unmeasured performance target.

## 6. Database contract

The canonical schema belongs in `sql/schema.sql`. Required tables:

- `stg_sales`, `stg_products`, `stg_inventory`
- `sales`, `products`, `inventory`
- `rejected_records`
- `pipeline_runs`
- `inventory_events`
- `reorder_recommendations`
- `reverse_etl_audit`

Use primary keys, foreign keys, unique constraints, suitable numeric/date types, and indexes. Every module must use these agreed table/column names. Do not create incompatible schema variants in separate modules.

## 7. Airflow batch DAG

DAG ID: `retail_batch_pipeline`

Required tasks and order:

1. `check_input_files`
2. `load_staging`
3. `validate_and_clean`
4. `export_clean_data`
5. `calculate_analytics`
6. `generate_reorder_recommendations`
7. `sync_recommendations`
8. `reconcile_pipeline`

The DAG must call reusable functions from `src/`; business logic should not be copied into the DAG. Use explicit retries where safe, useful logs, a run ID, and failure behavior. A failed input check or required quality gate must block dependent downstream tasks.

**NiFi/Airflow handoff:** NiFi writes the required files into `data/landing/`, shared through a Docker volume. Airflow detects those files using a scheduled check/sensor. Do not claim direct event-driven triggering unless implemented.

## 8. Analytics and recommendations

Minimum analytics:
- Valid sales rows and units sold.
- Revenue, calculated as `quantity * unit_price`.
- Daily and category sales rollups.
- Top products by units and revenue.
- Products below their reorder level.

Initial transparent reorder rule:
- If `current_stock <= reorder_level`, recommend replenishment.
- `recommended_quantity = max(0, target_stock - current_stock)`.
- Define `target_stock` explicitly, e.g. `2 * reorder_level`.
- Handle a zero reorder level explicitly.
- Explain that this is a deterministic demo rule, not a trained demand forecast.

## 9. Reverse ETL API

Use a local FastAPI mock service with these endpoints:

- `GET /health`
- `GET /inventory`
- `PUT /recommendations/{recommendation_key}` — idempotently create/update a recommendation
- `GET /recommendations`

Validate incoming payloads. Use a stable recommendation key so retries do not create duplicates. Write every attempt and outcome to `reverse_etl_audit`, including response status and timestamp. Test a valid payload, repeated request, invalid payload, and API failure/retry. This is a mock downstream system, not a real ERP/supplier integration.

## 10. DuckDB vs Polars benchmark

Optional until core functionality works. If implemented:
- Use the same input data and exact same computation.
- Record row count, environment, warm-up policy, repetitions, elapsed times, and output-equivalence check.
- Generate `results/benchmark_results.csv` from code.
- Do not hardcode timings or compare different workloads.

## 11. Streaming and dashboard

A producer publishes valid, invalid, and duplicate events. The consumer validates, deduplicates, persists, and updates stock. Publishing is not success until the consumer has persisted the event.

Suggested Streamlit tabs:
1. Overview — latest pipeline state, sales/revenue, low-stock products.
2. Pipeline — run history and outcomes.
3. Data Quality — accepted/rejected/duplicate counts and reasons.
4. Analytics — daily/category sales and top products.
5. Streaming — event counts and measured latency.
6. Reverse ETL — recommendations and API audit.
7. Benchmark — actual benchmark data, if available.

Dashboard values must come from PostgreSQL, API responses, or generated result files. Include empty/error states when services are unavailable. Never hardcode success metrics.

## 12. Local setup

The exact commands and ports must be committed only after the actual Docker Compose file and image versions are finalized.

1. Install Docker Desktop and enable its Linux/WSL2 backend on Windows if applicable.
2. Clone this repository.
3. Copy `.env.example` to `.env`; fill local settings. Never commit `.env` or credentials.
4. Start services using the final documented Compose command.
5. Initialize the schema using the final documented command.
6. Run unit/integration tests.
7. Open the documented NiFi, Airflow, API, and Streamlit interfaces.
8. Run the batch DAG and streaming producer/consumer.
9. Verify output with SQL and the dashboard.

**Do not advertise unverified ports or commands.** Complete this section when the real Compose configuration has been committed.

## 13. Tests and acceptance checklist

- [ ] Fresh setup can initialize the schema and start documented services.
- [ ] A clean batch run creates trusted data, Parquet, analytics, recommendations, and audit rows.
- [ ] Invalid rows are rejected with specific reasons.
- [ ] Re-running identical input does not duplicate trusted sales.
- [ ] Missing inputs or a failed required quality gate block dependent tasks.
- [ ] NiFi routes the files and Airflow shows real task states/logs.
- [ ] A valid Redpanda event reaches PostgreSQL and changes stock once.
- [ ] A duplicate event does not change stock a second time.
- [ ] An invalid event is visible as rejected.
- [ ] The mock API accepts a recommendation and the audit table records the outcome.
- [ ] Dashboard counts match SQL/API results.
- [ ] Benchmark outputs, if included, are generated by code and include conditions.
- [ ] Tests run from documented commands.
- [ ] Screenshots and metrics are from actual executions.

Put detailed cases and expected outcomes in `docs/ACCEPTANCE_TESTS.md`. Decide expected behavior before asking AI to write the tests; review tests independently of the implementation.

## 14. Team workflow and scope

Freeze table names, function signatures, event schema, API payload, recommendation formula, paths, ports, and versions before parallel coding. Give each member ownership of one module and its tests. Use feature branches and pull requests; never commit secrets, virtual environments, database volumes, or huge generated datasets.

**Must-have scope:** NiFi-to-landing, Airflow DAG, PostgreSQL staging/trusted tables, quality/rejections, Parquet export, DuckDB analytics, Redpanda consumer, mock reverse-ETL API with audit, Streamlit dashboard, tests.

**Optional only after core works:** Delta Lake versioning, MongoDB, Elasticsearch, advanced ML/forecasting, cloud deployment, and large-scale performance claims.

## 15. Evidence and definition of done

Generate actual artifacts where applicable:
- `results/pipeline_run_summary.json`
- `results/data_quality_report.json`
- `results/analytics_results.json`
- `results/streaming_results.json`
- `results/benchmark_results.csv`
- `results/reorder_recommendations.csv`

Capture actual NiFi, Airflow, dashboard, API, and SQL evidence. For every reported metric, state the data, script/command, and conditions. If not run, mark it pending.

The project is ready to present when a teammate can follow the README from a fresh clone, run the end-to-end demo, verify PostgreSQL data, see results in the dashboard, trigger at least one failure case, and explain every major handoff.

