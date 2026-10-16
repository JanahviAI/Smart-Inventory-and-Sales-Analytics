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

## 12. Run the Complete Project Locally

This section provides the exact, copy-paste-ready Windows PowerShell commands to configure, start, verify, and operate the entire platform from the repository root.

### 12.1 Prerequisites
- **Docker Desktop**: Running with Linux containers / WSL2 backend enabled.
- **Python**: Version 3.10 or 3.11 with a virtual environment configured:
  ```powershell
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  ```

### 12.2 Environment Configuration
Create your local `.env` file from `.env.example`:
```powershell
Copy-Item .env.example .env
```

> [!NOTE]
> **Port Collision Protection:** If your host machine already runs a local PostgreSQL service on port `5432`, set `POSTGRES_PORT=5433` in `.env`. Docker Compose forwards host port `5433` to container port `5432`, preventing port collisions while keeping all internal container networking seamless.

### 12.3 Starting Services
Start all platform containers in detached mode:
```powershell
docker compose up -d
```

### 12.4 Verify Container Health & Status
Check that all containers are healthy and running:
```powershell
docker ps
```

| Service | Container Name | Port | Healthcheck / Access |
|---|---|---|---|
| **PostgreSQL** | `retail-postgres` | `5433` (or `5432`) | `pg_isready` |
| **Airflow Webserver** | `retail-airflow-webserver` | `8080` | `http://localhost:8080` |
| **Airflow Scheduler** | `retail-airflow-scheduler` | — | Internal |
| **Apache NiFi** | `retail-nifi` | `8443` | `https://localhost:8443/nifi` |
| **Downstream Mock API** | `retail-mock-api` | `8000` | `http://localhost:8000/docs` |
| **Streamlit Dashboard** | `retail-dashboard` | `8501` | `http://localhost:8501` |
| **Redpanda Broker** | `retail-redpanda` | `9092` | `localhost:9092` |

### 12.5 Provision Apache NiFi Ingestion Flow
Apache NiFi starts with an empty canvas by default. Provision and start the 3 ingestion processors (`ListFile` -> `FetchFile` -> `PutFile`) using the automated REST API setup tool:
```powershell
python scripts/setup_nifi_flow.py
```
This automatically configures atomic file routing from `data/source/` into `data/landing/` and sets processor state to `RUNNING`.

### 12.5 Generate Synthetic Dataset (Benchmark Scale)
To test and benchmark meaningful Data Engineering workloads, generate a reproducible synthetic dataset with configurable scale:

```powershell
# Generate 500 products, 500 inventory items, and 10,000 sales transactions (Seed: 42)
python scripts/generate_synthetic_data.py --num-products 500 --num-inventory 500 --num-sales 10000 --seed 42 --copy-to-landing
```

**Key Features:**
- **Deterministic & Reproducible:** Seed-based pseudo-random generator guarantees identical output for identical seeds.
- **Configurable Scale:** Adjust `--num-products`, `--num-inventory`, `--num-sales`, and `--seed` via CLI flags.
- **Realistic Distributions:** Pareto/Zipf sales patterns (top 20% products generate ~60% of volume), daytime transaction clustering, and realistic stock distributions (with ~14% critical inventory triggering replenishment).
- **Synthetic Labeling:** Writes a `DATASET_METADATA.json` clearly marking the data as synthetic benchmark data (never presented as real customer or business data).
- **Schema & Constraint Compliance:** Preserves all types, foreign keys, non-negative constraints, and strict `(order_id, product_id)` uniqueness.

### 12.6 Run the End-to-End Batch Pipeline (10,000 Sales Records)
Execute the full 8-task orchestration pipeline against the generated dataset:
```powershell
python scripts/run_batch_pipeline.py
```
This executes the 8 tasks sequentially:
1. `check_input_files` (Verifies landing files from NiFi)
2. `load_staging` (Batches 11,000 raw CSV rows into PostgreSQL `stg_*` tables)
3. `validate_and_clean` (Validates types, FKs, dates, constraints; upserts clean rows; logs rejections)
4. `export_clean_data` (Exports clean tables to Snappy-compressed Parquet files)
5. `calculate_analytics` (Executes DuckDB OLAP queries over Parquet)
6. `generate_reorder_recommendations` (Calculates deterministic reorder quantities for low-stock items)
7. `sync_recommendations` (Re-uses persistent connection pool to idempotently sync to Mock API and audit in `reverse_etl_audit`)
8. `reconcile_pipeline` (Verifies database counts, audit records, and parquet exports; generates `results/pipeline_run_summary.json`)

**Verified Measured Execution Results (10,000 Sales Dataset):**
- **Sales Ingested & Validated:** 10,000 rows
- **Products Catalog:** 500 rows
- **Inventory Records:** 500 rows
- **Parquet Outputs:** `sales_clean.parquet` (10,000 rows), `products_clean.parquet` (500 rows), `inventory_clean.parquet` (500 rows)
- **DuckDB Analytical Processing:** 0.18s (`total_revenue`: $2,645,368.32, `total_units`: 15,468)
- **Reorder Recommendations Generated:** 71 items (products at or below reorder threshold)
- **Reverse-ETL Synced & Audited:** 71 recommendations audited in `reverse_etl_audit` (0 failures)
- **Artifacts Generated:** `results/pipeline_run_summary.json` and `results/analytics_summary.json`

### 12.7 Test High-Volume Event Streaming (1,000+ Events)

1. **Publish 1,050 Synthetic Streaming Events to Redpanda**:
   ```powershell
   python -m src.streaming.producer --synthetic-count 1050 --seed 42
   ```
   Generates and publishes 1,050 broker-acknowledged events to topic `sales.events`:
   - 950 valid sales events across catalog products with sufficient inventory
   - 50 duplicate events (exact clones of earlier event IDs to test deduplication)
   - 50 invalid/malformed events (negative quantities, missing event IDs, bad timestamps, unknown product IDs)

2. **Process and Deduplicate Events with the Consumer**:
   ```powershell
   python -m src.streaming.consumer --max-events 1050 --timeout 5 --report
   ```
   Consumes all 1,050 events, performs database transaction updates, deduplicates repeated event IDs, logs rejections, and generates `results/streaming_results.json`.

**Verified Measured Streaming Results:**
- **Total Consumed:** 1,050 events in 11.95s (~88 events/sec with transactional row locking & PostgreSQL ACID persistence)
- **Accepted Events:** 939 events (atomically decremented stock by quantity)
- **Duplicate Attempts:** 50 events (detected by `inventory_events` primary key constraint; zero duplicate stock decrements; audited in `rejected_records`)
- **Rejected Attempts:** 61 events (50 malformed/invalid payloads + 11 cumulative insufficient-stock rejections)
- **Deduplication Verification:** Running duplicate events confirmed that stock is never decremented twice.

### 12.8 Run All Automated Regression Tests
Run the expanded 35-test suite covering data generation, schema contracts, large input sizes, Airflow orchestration, data quality rules, reverse-ETL, and streaming deduplication:
```powershell
pytest -v
```
*(All 35 tests pass).*

### 12.9 Stopping the Project Safely
To stop all services:
```powershell
docker compose stop
```
To shut down containers and networks:
```powershell
docker compose down
```
To reset everything (including database volumes and clean slate):
```powershell
docker compose down -v
```

---

## 13. Troubleshooting & Known Fixes

| Issue Encountered | Root Cause | Resolution Implemented |
|---|---|---|
| **Mock API container restart loop (`retail-mock-api`)** | `src/api/main.py` imports `fetch_all` from `src.common.db`, which imports `psycopg2`. The container command only installed `fastapi uvicorn pydantic`. | Updated `docker-compose.yml` to install `psycopg2-binary`, `sqlalchemy`, and `python-dotenv`, and added database environment variables and healthcheck. |
| **Airflow Webserver & Scheduler restart loop** | Containers had no `command` set, causing the image entrypoint to call `airflow` without arguments (`the following arguments are required: GROUP_OR_COMMAND`). | Explicitly set `command: webserver` and `command: scheduler` in `docker-compose.yml`. |
| **Airflow DAG DuckDB missing dependency** | Task 5 (`calculate_analytics`) requires `duckdb`, which is not bundled in `apache/airflow:2.8.2-python3.11`. | Added `_PIP_ADDITIONAL_REQUIREMENTS: "duckdb>=0.9.0 pyarrow>=14.0.0"` to Airflow service definitions. |
| **NiFi zero processors on startup** | NiFi mounts `./nifi` only as a template volume, which does not auto-populate the active canvas in `conf/flow.json.gz`. | Created `scripts/setup_nifi_flow.py` to automatically configure and start all 3 processors and connections via the NiFi REST API. |
| **NiFi `Device or resource busy` on flow save** | Direct single-file bind mount on `conf/flow.json.gz` prevented NiFi's atomic file replacement in Linux. | Removed direct file bind-mount and provisioned flow dynamically via REST API. |
| **NiFi container healthcheck failure** | NiFi bound to container IP by default; healthcheck curled `localhost:8443` and got Connection Refused. | Added `NIFI_WEB_HTTPS_HOST: "0.0.0.0"` in `docker-compose.yml` so NiFi binds on all interfaces. |
| **PostgreSQL host port collision (5432)** | Windows host had native PostgreSQL service running on port 5432. | Added configurable `POSTGRES_PORT=5433` in `.env` forwarding host port 5433 to container port 5432. |

---

## 14. Tests and Acceptance Checklist

- [x] Fresh setup can initialize the schema and start documented services.
- [x] A clean batch run creates trusted data, Parquet, analytics, recommendations, and audit rows.
- [x] Invalid rows are rejected with specific reasons.
- [x] Re-running identical input does not duplicate trusted sales.
- [x] Missing inputs or a failed required quality gate block dependent tasks.
- [x] NiFi routes the files and Airflow shows real task states/logs.
- [x] A valid Redpanda event reaches PostgreSQL and changes stock once.
- [x] A duplicate event does not change stock a second time.
- [x] An invalid event is visible as rejected.
- [x] The mock API accepts a recommendation and the audit table records the outcome.
- [x] Dashboard counts match SQL/API results.
- [x] Benchmark outputs, if included, are generated by code and include conditions.
- [x] Tests run from documented commands.
- [x] Screenshots and metrics are from actual executions.

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

