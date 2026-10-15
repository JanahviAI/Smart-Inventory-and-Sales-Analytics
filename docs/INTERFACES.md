# Shared Interfaces and Team Architecture Contract

**Team Contract Version:** 1.0.0  
**Status:** Frozen Decisions  
**Repository:** https://github.com/JanahviAI/Smart-Inventory-and-Sales-Analytics  

---

## 1. Frozen Architecture Decisions

1. **Source Inputs:** The canonical raw inputs are `data/source/sales.csv`, `data/source/products.csv`, and `data/source/inventory.csv`.
2. **Sales Business Key:** `order_line_id` serves as the primary key (`{order_id}-{product_id}`). Composite unique constraint `(order_id, product_id)` ensures at most one line item per product per order.
3. **Date/Time Standard:** ISO-8601 UTC format: `YYYY-MM-DDTHH:MM:SSZ` (e.g., `2026-01-15T10:30:00Z`). All timestamps in PostgreSQL are stored as `TIMESTAMP WITH TIME ZONE`.
4. **Invalid Data Handling:** Invalid or unparseable records are never silently coerced or dropped; they are logged to `rejected_records` with exact payload, source, reason code, and `run_id`.
5. **Storage of Truth:** PostgreSQL is the operational system of record. Cleaned Parquet files in `data/processed/` are the analytical export format.
6. **Streaming Stock Rule:** Accepted sale events decrement inventory by `quantity`. Deduplication uses unique `event_id`. If `current_stock < quantity`, the event is rejected.
7. **Reorder Recommendation Rule:** If `current_stock <= reorder_level`, recommended quantity is `max(0, target_stock - current_stock)` where `target_stock = 2 * reorder_level` (minimum target 10 if reorder_level is 0).
8. **Reverse ETL Contract:** Mock FastAPI endpoint `PUT /recommendations/{recommendation_key}` with idempotent upsert. Every request/response is audited in `reverse_etl_audit`.
9. **NiFi/Airflow Handoff:** NiFi writes CSVs to shared Docker volume `data/landing/`. Airflow task `check_input_files` validates file presence and readiness before triggering `load_staging`.
10. **Environment & Ports:**
    - PostgreSQL: `5432` (or host forwarded)
    - Airflow Webserver: `8080`
    - NiFi: `8443`
    - Mock API (FastAPI): `8000`
    - Streamlit Dashboard: `8501`
    - Redpanda: `9092`

---

## 2. Python Module Function Contracts

All ETL and analytics modules adhere strictly to standard signatures and return dictionary summaries:

```python
# src/etl/staging.py
def load_staging(run_id: str) -> dict:
    """Load required landing files into staging tables."""

# src/etl/validation.py
def validate_and_clean(run_id: str) -> dict:
    """Validate, deduplicate, upsert trusted data, and log rejects."""

# src/etl/export.py
def export_clean_data(run_id: str) -> dict:
    """Export trusted datasets to Parquet and return paths/counts."""

# src/analytics/queries.py
def calculate_analytics(run_id: str) -> dict:
    """Calculate agreed sales and inventory metrics."""

# src/analytics/recommendations.py
def generate_reorder_recommendations(run_id: str) -> dict:
    """Store deterministic recommendations and return counts."""

def sync_recommendations(run_id: str) -> dict:
    """Call mock API and record outcomes in reverse_etl_audit."""

# src/streaming/consumer.py
def process_inventory_event(event: dict) -> dict:
    """Validate/deduplicate/persist event and apply inventory rule."""
```

### Return Format Convention
Every contract function returns a dictionary matching:
```json
{
  "status": "success",        // "success" | "partial" | "failed"
  "run_id": "run-20261015-01",
  "processed": 100,
  "failed": 0,
  "rejected": 2,
  "message": "Completed successfully",
  "duration_seconds": 0.42,
  "details": { ... }
}
```

---

## 3. Person 2 ETL Component Specifications

### 3.1 `load_staging(run_id: str) -> dict`
- Reads landing files:
  - `data/landing/products.csv` -> `stg_products`
  - `data/landing/inventory.csv` -> `stg_inventory`
  - `data/landing/sales.csv` -> `stg_sales`
- Loads raw strings into staging tables tagged with `run_id`.
- Returns row counts loaded for each table.

### 3.2 `validate_and_clean(run_id: str) -> dict`
- Processes staging rows for `run_id`.
- Execution order:
  1. `stg_products` -> validate -> upsert `products` -> log invalid to `rejected_records`.
  2. `stg_inventory` -> validate (FK to `products`, stock `>= 0`) -> upsert `inventory` -> log invalid.
  3. `stg_sales` -> validate (FK to `products`, quantity `> 0`, price `>= 0`, valid ISO date, duplicate check) -> upsert `sales` -> log invalid.
- Generates data quality metrics report.

### 3.3 `export_clean_data(run_id: str) -> dict`
- Exports trusted tables from database to Parquet files:
  - `data/processed/products_clean.parquet`
  - `data/processed/inventory_clean.parquet`
  - `data/processed/sales_clean.parquet`
- Returns generated file paths, file sizes, and record counts.

