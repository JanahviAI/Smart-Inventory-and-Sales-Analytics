# Person 2 — ETL, PostgreSQL & Data Quality Guide

This guide provides step-by-step commands and explanations so you can run, test, and inspect every component of your assignment (Person 2) on your own machine.

---

## 1. Quick Start Commands

Run these commands in PowerShell from the project root:

```powershell
# 1. Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# 2. Run the complete ETL pipeline (Staging -> Validation -> Trusted Upsert -> Parquet -> Reconciliation)
python scripts/run_etl_pipeline.py

# 3. Run the automated data quality test suite (12 tests)
pytest -v tests/test_etl_quality.py
```

---

## 2. Interactive CLI Commands to Understand Your Components

You can run individual python one-liners or short scripts to inspect each stage of the pipeline:

### A. Inspect Canonical Schema & Tables
Verify that all canonical tables exist:
```powershell
python -c "from src.common.db import DatabaseManager, init_db; init_db(); print('Database tables ready.')"
```

### B. Run Staging Ingestion Only (`load_staging`)
Loads `products.csv`, `inventory.csv`, and `sales.csv` as raw text into staging:
```powershell
python -c "from src.etl.staging import load_staging; res = load_staging('manual-run-001'); print(res)"
```

### C. Run Data Quality Validation (`validate_and_clean`)
Validates types, ISO dates, foreign keys, non-negative constraints, deduplicates, and logs rejections:
```powershell
python -c "from src.etl.validation import validate_and_clean; res = validate_and_clean('manual-run-001'); print(res)"
```

### D. View Logged Rejections and Reasons
Inspect which rows were rejected and why:
```powershell
python -c "from src.common.db import fetch_all; import json; print(json.dumps(fetch_all('SELECT id, source_table, record_identifier, reason, raw_payload FROM rejected_records'), indent=2))"
```

### E. Run Parquet Export (`export_clean_data`)
Exports trusted PostgreSQL tables into Snappy-compressed Parquet files under `data/processed/`:
```powershell
python -c "from src.etl.export import export_clean_data; res = export_clean_data('manual-run-001'); print(res)"
```

### F. Read and Inspect Exported Parquet Data
Inspect the generated Parquet files using Pandas/PyArrow:
```powershell
python -c "import pandas as pd; df = pd.read_parquet('data/processed/sales_clean.parquet'); print(df.head(10)); print('\nTotal sales count:', len(df))"
```

### G. Reconcile SQL Database Totals vs Parquet
Verify that database totals match Parquet export totals:
```powershell
python -c "from src.common.db import fetch_one; import pandas as pd; db = fetch_one('SELECT count(*) as cnt, sum(quantity) as qty, round(sum(total_price),2) as rev FROM sales'); df = pd.read_parquet('data/processed/sales_clean.parquet'); print('DB:     ', db); print('Parquet:', {'cnt': len(df), 'qty': int(df['quantity'].sum()), 'rev': round(float(df['total_price'].sum()), 2)})"
```

### H. Inspect Generated Data Quality Report
View the JSON quality report generated on disk:
```powershell
Get-Content results\data_quality_report.json
```

---

## 3. What Each of Your Components Does

| File | Purpose | Key Contract |
|---|---|---|
| `sql/schema.sql` | Canonical database DDL for all 11 tables | PKs, FKs, Unique constraints, Check constraints, and Indexes |
| `docs/DATA_DICTIONARY.md` | Single source of truth for all entities and validation rules | Full documentation of columns, formats, and rejection taxonomy |
| `src/common/config.py` | Environment variable management & path resolution | Loads `.env`, manages project directories |
| `src/common/db.py` | Connection pooling & query execution | Connects to PostgreSQL, automatic fallback to SQLite for offline dev |
| `src/etl/staging.py` | Ingests raw CSV landing files into staging tables | `load_staging(run_id) -> dict` |
| `src/etl/validation.py` | Deterministic validation, trusted upsert, rejection logging | `validate_and_clean(run_id) -> dict` |
| `src/etl/export.py` | Columnar Parquet exports for analytical processing | `export_clean_data(run_id) -> dict` |
| `tests/test_etl_quality.py` | 12 automated unit and integration tests | Nulls, types, FKs, duplicates, idempotency, dates, reconciliation |
| `results/data_quality_report.json` | Real executed evidence artifact | Summary pass rates, breakdown per table, rejections by reason |

---

## 4. How Your Components Handoff to Teammates

- **Person 1 (Airflow / NiFi):**
  Person 1's Airflow DAG (`dags/retail_batch_pipeline.py`) imports and calls your reusable functions directly:
  - Task 2: `load_staging(run_id)`
  - Task 3: `validate_and_clean(run_id)`
  - Task 4: `export_clean_data(run_id)`
- **Person 3 (DuckDB Analytics & Reverse ETL):**
  Person 3 runs DuckDB queries directly on your Parquet files (`data/processed/sales_clean.parquet`, etc.) and reads trusted `inventory` and `products` from the schema you built.
- **Person 4 (Streaming & Dashboard):**
  Person 4 inserts streaming events into `inventory_events` (created in your `sql/schema.sql`) and displays your `data_quality_report.json` and rejection statistics in the Streamlit Data Quality tab.

---

## 5. PostgreSQL vs Local Fallback Notes

- The system is built for **PostgreSQL** as the operational source of truth.
- When PostgreSQL credentials are provided in `.env` (e.g., when Docker Compose runs `postgres` or local Postgres is configured), the pipeline connects directly to PostgreSQL.
- If PostgreSQL is unreachable or password authentication is pending, `src/common/db.py` automatically falls back to SQLite (`data/local_inventory.db`), allowing your ETL code, validations, exports, and test suites to run completely offline without crashing.

