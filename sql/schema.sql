-- ============================================================================
-- Smart Inventory and Sales Analytics Platform - Canonical Schema
-- Owner: Person 2 (PostgreSQL, ETL, and Data Quality)
-- Shared across: Person 1 (Airflow/NiFi), Person 3 (Analytics/API), Person 4 (Streaming/Dashboard)
-- ============================================================================

-- Drop existing tables in reverse dependency order if recreating
DROP TABLE IF EXISTS reverse_etl_audit CASCADE;
DROP TABLE IF EXISTS reorder_recommendations CASCADE;
DROP TABLE IF EXISTS inventory_events CASCADE;
DROP TABLE IF EXISTS rejected_records CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;
DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS stg_sales CASCADE;
DROP TABLE IF EXISTS stg_inventory CASCADE;
DROP TABLE IF EXISTS stg_products CASCADE;

-- ----------------------------------------------------------------------------
-- 1. STAGING TABLES (Unvalidated raw loads with run metadata)
-- ----------------------------------------------------------------------------

CREATE TABLE stg_products (
    raw_product_id VARCHAR(100),
    raw_product_name VARCHAR(255),
    raw_category VARCHAR(100),
    raw_supplier_id VARCHAR(100),
    run_id VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_inventory (
    raw_product_id VARCHAR(100),
    raw_current_stock VARCHAR(100),
    raw_reorder_level VARCHAR(100),
    run_id VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_sales (
    raw_order_line_id VARCHAR(100),
    raw_order_id VARCHAR(100),
    raw_order_date VARCHAR(100),
    raw_product_id VARCHAR(100),
    raw_quantity VARCHAR(100),
    raw_unit_price VARCHAR(100),
    run_id VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stg_products_run ON stg_products(run_id);
CREATE INDEX idx_stg_inventory_run ON stg_inventory(run_id);
CREATE INDEX idx_stg_sales_run ON stg_sales(run_id);

-- ----------------------------------------------------------------------------
-- 2. TRUSTED OPERATIONAL TABLES (Validated, typed, constrained)
-- ----------------------------------------------------------------------------

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    supplier_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventory (
    product_id INT PRIMARY KEY REFERENCES products(product_id) ON DELETE RESTRICT,
    current_stock INT NOT NULL CHECK (current_stock >= 0),
    reorder_level INT NOT NULL CHECK (reorder_level >= 0),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales (
    order_line_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP WITH TIME ZONE NOT NULL,
    product_id INT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_price NUMERIC(12, 2) NOT NULL CHECK (total_price >= 0),
    run_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_sales_order_product UNIQUE (order_id, product_id)
);

CREATE INDEX idx_sales_order_date ON sales(order_date);
CREATE INDEX idx_sales_product_id ON sales(product_id);
CREATE INDEX idx_sales_order_id ON sales(order_id);
CREATE INDEX idx_sales_run_id ON sales(run_id);
CREATE INDEX idx_inventory_stock ON inventory(current_stock, reorder_level);

-- ----------------------------------------------------------------------------
-- 3. AUDIT & DATA QUALITY TABLES
-- ----------------------------------------------------------------------------

CREATE TABLE rejected_records (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    source_table VARCHAR(50) NOT NULL,
    record_identifier VARCHAR(100),
    raw_payload JSONB NOT NULL,
    reason VARCHAR(255) NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_rejected_run_id ON rejected_records(run_id);
CREATE INDEX idx_rejected_source_table ON rejected_records(source_table);

CREATE TABLE pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(50) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP WITH TIME ZONE,
    tasks_completed JSONB DEFAULT '[]'::jsonb,
    summary JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 4. DOWNSTREAM / STREAMING / REVERSE-ETL TABLES
-- ----------------------------------------------------------------------------

-- Streaming Inventory Events (Owned by Person 4)
CREATE TABLE inventory_events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    status VARCHAR(50) NOT NULL, -- 'ACCEPTED', 'REJECTED', 'DUPLICATE'
    rejection_reason VARCHAR(255),
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    latency_ms NUMERIC(10, 2)
);

CREATE INDEX idx_events_product_id ON inventory_events(product_id);
CREATE INDEX idx_events_status ON inventory_events(status);

-- Analytical Reorder Recommendations (Owned by Person 3)
CREATE TABLE reorder_recommendations (
    recommendation_key VARCHAR(100) PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    product_id INT NOT NULL REFERENCES products(product_id),
    current_stock INT NOT NULL,
    reorder_level INT NOT NULL,
    target_stock INT NOT NULL,
    recommended_quantity INT NOT NULL,
    reason VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_reorder_run_id ON reorder_recommendations(run_id);
CREATE INDEX idx_reorder_product_id ON reorder_recommendations(product_id);

-- Reverse ETL Audit Log (Owned by Person 3)
CREATE TABLE reverse_etl_audit (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    recommendation_key VARCHAR(100) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    http_status INT,
    payload JSONB NOT NULL,
    response_body TEXT,
    status VARCHAR(50) NOT NULL, -- 'SUCCESS', 'FAILED', 'RETRY'
    synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_run_id ON reverse_etl_audit(run_id);
CREATE INDEX idx_audit_key ON reverse_etl_audit(recommendation_key);

