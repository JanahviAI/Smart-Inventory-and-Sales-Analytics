# Data Dictionary — Smart Inventory and Sales Analytics Platform

**Owner:** Person 2 (PostgreSQL, ETL, and Data Quality)  
**Status:** Canonical Reference  
**Last Updated:** 2026-10-15  

This document serves as the single source of truth for all data entities, attributes, constraints, validation rules, and rejection taxonomies across the platform.

---

## 1. Source Data Contracts (`data/source/` & `data/landing/`)

Deterministic source CSV files reside in `data/source/`. NiFi routes verbatim copies to `data/landing/` for Airflow batch consumption.

### 1.1 `products.csv`
Master catalogue of items available in the retail platform.

| Column | Type | Nullable | Example | Description |
|---|---|---|---|---|
| `product_id` | Integer | No | `101` | Unique surrogate identifier for the product |
| `product_name` | String(255) | No | `Wireless Ergonomic Mouse` | Commercial name of the product |
| `category` | String(100) | No | `Electronics` | Merchandising hierarchy category |
| `supplier_id` | String(50) | No | `SUPP-001` | Identifier for the primary supplier |

### 1.2 `inventory.csv`
Current warehouse inventory levels and reorder parameters.

| Column | Type | Nullable | Example | Description |
|---|---|---|---|---|
| `product_id` | Integer | No | `101` | Primary key referencing `products.csv` |
| `current_stock` | Integer | No | `45` | Current on-hand quantity in the distribution centre (`>= 0`) |
| `reorder_level` | Integer | No | `20` | Threshold stock quantity triggering replenishment recommendation (`>= 0`) |

### 1.3 `sales.csv`
Transactional sales orders from point-of-sale / online store.

| Column | Type | Nullable | Example | Description |
|---|---|---|---|---|
| `order_line_id` | String(100) | No | `ord-001-101` | Stable business key identifying this specific line item (`order_id + '-' + product_id`) |
| `order_id` | String(50) | No | `ord-001` | Transactional order number |
| `order_date` | ISO-8601 Timestamp | No | `2026-01-10T14:30:00Z` | Order placement timestamp (UTC) |
| `product_id` | Integer | No | `101` | Foreign key referencing `products.product_id` |
| `quantity` | Integer | No | `2` | Number of units purchased (`> 0`) |
| `unit_price` | Decimal(10,2) | No | `29.99` | Price per unit in USD (`>= 0.00`) |

---

## 2. PostgreSQL Staging Tables

Staging tables receive unvalidated raw data directly from files during `load_staging(run_id)`. All data fields are ingested as text (`VARCHAR`) so parsing errors and malformed values can be captured and routed to `rejected_records` without crashing the ingest step.

### 2.1 `stg_products`
- `raw_product_id` (VARCHAR 100)
- `raw_product_name` (VARCHAR 255)
- `raw_category` (VARCHAR 100)
- `raw_supplier_id` (VARCHAR 100)
- `run_id` (VARCHAR 100, NOT NULL)
- `loaded_at` (TIMESTAMP WITH TIME ZONE, DEFAULT CURRENT_TIMESTAMP)

### 2.2 `stg_inventory`
- `raw_product_id` (VARCHAR 100)
- `raw_current_stock` (VARCHAR 100)
- `raw_reorder_level` (VARCHAR 100)
- `run_id` (VARCHAR 100, NOT NULL)
- `loaded_at` (TIMESTAMP WITH TIME ZONE, DEFAULT CURRENT_TIMESTAMP)

### 2.3 `stg_sales`
- `raw_order_line_id` (VARCHAR 100)
- `raw_order_id` (VARCHAR 100)
- `raw_order_date` (VARCHAR 100)
- `raw_product_id` (VARCHAR 100)
- `raw_quantity` (VARCHAR 100)
- `raw_unit_price` (VARCHAR 100)
- `run_id` (VARCHAR 100, NOT NULL)
- `loaded_at` (TIMESTAMP WITH TIME ZONE, DEFAULT CURRENT_TIMESTAMP)

---

## 3. Trusted Operational Tables

Trusted tables store clean, validated, strictly typed data. Writes are idempotent (upserts via `ON CONFLICT`).

### 3.1 `products`
- **Primary Key:** `product_id`
- **Columns:**
  - `product_id`: `INT PRIMARY KEY`
  - `product_name`: `VARCHAR(255) NOT NULL`
  - `category`: `VARCHAR(100) NOT NULL`
  - `supplier_id`: `VARCHAR(50) NOT NULL`
  - `created_at`: `TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP`
  - `updated_at`: `TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP`

### 3.2 `inventory`
- **Primary Key:** `product_id`
- **Foreign Key:** `product_id REFERENCES products(product_id) ON DELETE RESTRICT`
- **Columns:**
  - `product_id`: `INT PRIMARY KEY`
  - `current_stock`: `INT NOT NULL CHECK (current_stock >= 0)`
  - `reorder_level`: `INT NOT NULL CHECK (reorder_level >= 0)`
  - `last_updated`: `TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP`

### 3.3 `sales`
- **Primary Key:** `order_line_id`
- **Unique Constraint:** `(order_id, product_id)`
- **Foreign Key:** `product_id REFERENCES products(product_id) ON DELETE RESTRICT`
- **Columns:**
  - `order_line_id`: `VARCHAR(100) PRIMARY KEY`
  - `order_id`: `VARCHAR(50) NOT NULL`
  - `order_date`: `TIMESTAMP WITH TIME ZONE NOT NULL`
  - `product_id`: `INT NOT NULL`
  - `quantity`: `INT NOT NULL CHECK (quantity > 0)`
  - `unit_price`: `NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0)`
  - `total_price`: `NUMERIC(12, 2) NOT NULL CHECK (total_price >= 0)`
  - `run_id`: `VARCHAR(100) NOT NULL`
  - `created_at`: `TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP`
  - `updated_at`: `TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP`

---

## 4. Quality & Audit Tables

### 4.1 `rejected_records`
Every row failing quality validation is recorded with exact context and reason.

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL PRIMARY KEY | Auto-incrementing identifier |
| `run_id` | VARCHAR(100) NOT NULL | Associated pipeline batch run ID |
| `source_table` | VARCHAR(50) NOT NULL | Source name, e.g. `stg_sales`, `stg_products`, `stg_inventory` |
| `record_identifier` | VARCHAR(100) | Identifier extracted from payload if available (e.g. order_line_id) |
| `raw_payload` | JSONB NOT NULL | Complete raw record payload serialized as JSON |
| `reason` | VARCHAR(255) NOT NULL | Standardized reason code and explanation |
| `rejected_at` | TIMESTAMP WITH TIME ZONE | Timestamp when the rejection occurred |

### 4.2 `pipeline_runs`
Tracks the execution lifecycle of Airflow batches.

| Column | Type | Description |
|---|---|---|
| `run_id` | VARCHAR(100) PRIMARY KEY | Unique run identifier |
| `status` | VARCHAR(50) NOT NULL | `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL` |
| `started_at` | TIMESTAMP WITH TIME ZONE | Run start time |
| `ended_at` | TIMESTAMP WITH TIME ZONE | Run completion time |
| `tasks_completed` | JSONB | Array of task names completed |
| `summary` | JSONB | Metrics dictionary (counts, durations, paths) |
| `created_at` | TIMESTAMP WITH TIME ZONE | Record creation timestamp |

---

## 5. Streaming & Downstream Entities

### 5.1 `inventory_events` (Owned by Person 4)
- `event_id`: VARCHAR(100) PRIMARY KEY (Deduplication key)
- `event_time`: TIMESTAMP WITH TIME ZONE NOT NULL
- `order_id`: VARCHAR(50) NOT NULL
- `product_id`: INT NOT NULL REFERENCES products(product_id)
- `quantity`: INT NOT NULL CHECK (quantity > 0)
- `status`: VARCHAR(50) NOT NULL (`ACCEPTED`, `REJECTED`, `DUPLICATE`)
- `rejection_reason`: VARCHAR(255)
- `processed_at`: TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
- `latency_ms`: NUMERIC(10, 2)

### 5.2 `reorder_recommendations` (Owned by Person 3)
- `recommendation_key`: VARCHAR(100) PRIMARY KEY (`rec-{run_id}-{product_id}`)
- `run_id`: VARCHAR(100) NOT NULL
- `product_id`: INT NOT NULL REFERENCES products(product_id)
- `current_stock`: INT NOT NULL
- `reorder_level`: INT NOT NULL
- `target_stock`: INT NOT NULL
- `recommended_quantity`: INT NOT NULL
- `reason`: VARCHAR(255) NOT NULL
- `created_at`: TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP

### 5.3 `reverse_etl_audit` (Owned by Person 3)
- `id`: SERIAL PRIMARY KEY
- `run_id`: VARCHAR(100) NOT NULL
- `recommendation_key`: VARCHAR(100) NOT NULL
- `endpoint`: VARCHAR(255) NOT NULL
- `http_status`: INT
- `payload`: JSONB NOT NULL
- `response_body`: TEXT
- `status`: VARCHAR(50) NOT NULL (`SUCCESS`, `FAILED`, `RETRY`)
- `synced_at`: TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP

---

## 6. Data Quality Validation Taxonomy

When records are evaluated in `validate_and_clean(run_id)`, the following rejection reasons are assigned:

| Rule Code | Reason Category | Trigger Condition |
|---|---|---|
| `NULL_REQUIRED_FIELD` | Schema Completeness | Any required field is null, empty string, or whitespace |
| `INVALID_DATA_TYPE` | Type Conformance | Value cannot be parsed to expected type (e.g. non-numeric ID) |
| `INVALID_DATE_FORMAT` | Format Conformance | Date fails ISO-8601 parsing (`YYYY-MM-DDTHH:MM:SSZ`) |
| `UNKNOWN_FOREIGN_KEY` | Referential Integrity | `product_id` does not exist in master `products` table |
| `NON_POSITIVE_QUANTITY` | Domain Constraint | Sales `quantity <= 0` |
| `NEGATIVE_UNIT_PRICE` | Domain Constraint | Sales `unit_price < 0` |
| `NEGATIVE_STOCK_VALUE` | Domain Constraint | Inventory `current_stock < 0` or `reorder_level < 0` |
| `DUPLICATE_BUSINESS_KEY` | Uniqueness | Same `(order_id, product_id)` or `order_line_id` occurs twice within batch |

---

## 7. Processed Parquet Export Specification (`data/processed/`)

Exported by `export_clean_data(run_id)`:
1. `data/processed/sales_clean.parquet` — All trusted sales records (Snappy compressed, columnar).
2. `data/processed/products_clean.parquet` — Master product catalog.
3. `data/processed/inventory_clean.parquet` — Current inventory status.

