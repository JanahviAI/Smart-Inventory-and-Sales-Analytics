# Person 1 — NiFi Ingestion & Airflow Orchestration Guide

This guide provides step-by-step commands, architectural explanations, and verification procedures so you can run, test, and inspect every component of your assignment (**Person 1**) on your own machine.

---

## 1. Quick Start Commands

Run these commands in PowerShell or terminal from the project root:

```powershell
# 1. Ingest/Route source CSVs to shared landing directory atomically
python scripts/simulate_nifi_ingest.py

# 2. Run the complete 8-task retail batch orchestration pipeline
python scripts/run_batch_pipeline.py

# 3. Run Person 1's automated orchestration test suite (DAG integrity, dependencies, AT-01, AT-02)
pytest -v tests/test_airflow_dag.py
```

---

## 2. Interactive CLI Commands to Inspect Your Components

You can run individual one-liners or targeted scripts to inspect and verify each component:

### A. Verify Landing Files Readiness
Verify that `sales.csv`, `products.csv`, and `inventory.csv` are landed, non-empty, and ready for batch processing:
```powershell
python scripts/simulate_nifi_ingest.py --verify
```

### B. Clean Landing Directory (Test AT-02 Failure Case)
Remove landing files to verify fail-fast prerequisite blocking:
```powershell
python scripts/simulate_nifi_ingest.py --clean
python scripts/run_batch_pipeline.py --clean-landing --no-auto-ingest
```

### C. Run the Batch Pipeline with a Custom Run ID
```powershell
python scripts/run_batch_pipeline.py --run-id "prod-batch-20261016-01"
```

### D. Inspect Pipeline Run Record in the Database
Inspect the recorded run status, start/end timestamps, and completed task milestones:
```powershell
python -c "from src.common.db import fetch_all; import json; print(json.dumps(fetch_all('SELECT run_id, status, started_at, ended_at, tasks_completed FROM pipeline_runs ORDER BY started_at DESC LIMIT 1'), indent=2))"
```

### E. Inspect Generated Run Summary Artifact
View the canonical execution summary saved to disk:
```powershell
Get-Content results\pipeline_run_summary.json
```

### F. Verify Airflow DAG Integrity & Task Graph
Test that the Airflow DAG loads cleanly and verifies all 8 tasks in sequence:
```powershell
pytest -v tests/test_airflow_dag.py -k "TestDagIntegrity"
```

---

## 3. What Each of Your Components Does

| File | Purpose | Key Contract / Responsibility |
|---|---|---|
| `docker-compose.yml` | Shared container orchestration across all 4 roles | Postgres (5432), NiFi (8443), Airflow (8080), Redpanda (9092), Mock API (8000), Streamlit (8501) |
| `dags/retail_batch_pipeline.py` | Canonical Airflow DAG with 8 tasks | Linear dependency: `check_input_files >> load_staging >> validate_and_clean >> export_clean_data >> calculate_analytics >> generate_reorder_recommendations >> sync_recommendations >> reconcile_pipeline` |
| `src/common/pipeline_tracker.py` | Run lifecycle tracking in database and JSON artifact generation | Updates `pipeline_runs` table (`RUNNING`, `SUCCESS`, `FAILED`) and outputs `results/pipeline_run_summary.json` |
| `nifi/flow_retail_ingest.json` | NiFi Process Group flow specification | `ListFile` -> `FetchFile` -> `PutFile` with atomic `.tmp` staging |
| `docs/NIFI_SETUP.md` | NiFi setup, processor configuration, and handoff manual | Explains port 8443 UI, processor parameters, and landing handoff |
| `scripts/simulate_nifi_ingest.py` | Local & CI NiFi routing simulator | Atomically routes CSVs from `data/source/` to `data/landing/` |
| `scripts/run_batch_pipeline.py` | Standalone local orchestrator runner | Executes the 8-task batch pipeline end-to-end and outputs formatted status |
| `tests/test_airflow_dag.py` | Automated orchestration test suite (8 tests) | Tests DAG structure, linear dependencies, retry policy, AT-01 (clean batch), and AT-02 (missing input) |
| `results/pipeline_run_summary.json` | Tangible run execution artifact | Detailed audit of task milestones, durations, row counts, and reconciliation metrics |

---

## 4. The 8 Airflow Tasks in Order

1. **`check_input_files`**: Validates presence, non-empty size, and readiness of `products.csv`, `inventory.csv`, and `sales.csv` in `data/landing/`. Raises `AirflowFailException` if any file is missing, safely blocking downstream tasks.
2. **`load_staging`**: Calls `src.etl.staging.load_staging(run_id)` to ingest raw values into `stg_products`, `stg_inventory`, and `stg_sales`.
3. **`validate_and_clean`**: Calls `src.etl.validation.validate_and_clean(run_id)` to validate constraints, types, foreign keys, deduplicate, upsert trusted tables, and log rejections to `rejected_records`.
4. **`export_clean_data`**: Calls `src.etl.export.export_clean_data(run_id)` to write trusted data to snappy-compressed Parquet files in `data/processed/`.
5. **`calculate_analytics`**: Calls `src.analytics.queries.calculate_analytics(run_id)` to execute DuckDB analytical queries over Parquet exports.
6. **`generate_reorder_recommendations`**: Calls `src.analytics.recommendations.generate_reorder_recommendations(run_id)` to generate replenishment suggestions for low-stock products.
7. **`sync_recommendations`**: Calls `src.analytics.recommendations.sync_recommendations(run_id)` to sync recommendations to downstream mock API and audit results in `reverse_etl_audit`.
8. **`reconcile_pipeline`**: Reconciles counts across PostgreSQL and Parquet, audits pipeline milestones, updates `pipeline_runs` to `SUCCESS`, and writes `results/pipeline_run_summary.json`.

---

## 5. How Your Components Handoff to Teammates

- **To Person 2 (ETL & Quality):**
  Your DAG orchestrates Person 2's `load_staging`, `validate_and_clean`, and `export_clean_data` in Tasks 2, 3, and 4. You provide the clean landing directory from NiFi ingestion and manage the `run_id`.
- **To Person 3 (Analytics & Reverse-ETL):**
  Your DAG orchestrates Person 3's `calculate_analytics`, `generate_reorder_recommendations`, and `sync_recommendations` in Tasks 5, 6, and 7. Your `docker-compose.yml` provides the container for their FastAPI mock service.
- **To Person 4 (Streaming & Dashboard):**
  Your `docker-compose.yml` coordinates the Redpanda streaming broker and Streamlit dashboard services. Your `pipeline_runs` table and `results/pipeline_run_summary.json` provide the run history and status displayed in the dashboard's **Overview** and **Pipeline** tabs.

---

## 6. Docker Compose Multi-Service Setup

When running in full containerized mode:
```bash
# 1. Start all platform services
docker compose up -d

# 2. Check container status
docker compose ps

# 3. Access interfaces:
# - Airflow Webserver: http://localhost:8080 (admin / admin)
# - NiFi Web UI:       https://localhost:8443/nifi (admin / Antigravity2026Admin!)
# - Streamlit:         http://localhost:8501
# - Mock API:          http://localhost:8000/docs
# - Redpanda Console:  http://localhost:9644
```

---

## 7. Acceptance Test Verification (AT-01 & AT-02)

| Test ID | Scenario | Command | Expected Outcome |
|---|---|---|---|
| **AT-01** | Clean batch execution | `pytest tests/test_airflow_dag.py -k "test_at_01"` | All 8 tasks succeed, Parquet exports exist, summary saved with `SUCCESS` |
| **AT-02** | Missing landing input | `pytest tests/test_airflow_dag.py -k "test_at_02"` | `check_input_files` fails immediately, downstream tasks blocked, run marked `FAILED` |
