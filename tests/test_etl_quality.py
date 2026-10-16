"""
Comprehensive Data Quality and ETL Acceptance Tests for Person 2.
Tests cover:
- Schema initialization and constraint validation
- Staging ingestion
- Null value rejections (NULL_REQUIRED_FIELD)
- Invalid type rejections (INVALID_DATA_TYPE)
- Malformed date rejections (INVALID_DATE_FORMAT)
- Negative constraint rejections (NON_POSITIVE_QUANTITY, NEGATIVE_UNIT_PRICE, NEGATIVE_STOCK_VALUE)
- Foreign key integrity rejections (UNKNOWN_FOREIGN_KEY)
- Uniqueness / Duplicate business key handling (DUPLICATE_BUSINESS_KEY)
- Idempotency across repeated batch executions
- Parquet export and independent SQL reconciliation
"""

import json
import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.common.config import Config
from src.common.db import DatabaseManager
from src.etl.export import export_clean_data
from src.etl.staging import load_staging
from src.etl.validation import validate_and_clean


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Ensure test database and schema are initialized."""
    # Force SQLite fallback for fast isolated test runner
    Config.DB_TYPE = "sqlite"
    DatabaseManager.reset_engine()
    DatabaseManager.init_schema()
    yield
    DatabaseManager.reset_engine()


@pytest.fixture(autouse=True)
def clean_tables():
    """Clean all tables before each test for test isolation."""
    with DatabaseManager.get_connection() as conn:
        for tbl in [
            "reverse_etl_audit",
            "reorder_recommendations",
            "inventory_events",
            "rejected_records",
            "pipeline_runs",
            "sales",
            "inventory",
            "products",
            "stg_sales",
            "stg_inventory",
            "stg_products",
        ]:
            conn.execute(text(f"DELETE FROM {tbl}"))


def test_01_canonical_schema_initialization():
    """Verify all 11 canonical tables exist in the database."""
    expected_tables = {
        "stg_products",
        "stg_inventory",
        "stg_sales",
        "products",
        "inventory",
        "sales",
        "rejected_records",
        "pipeline_runs",
        "inventory_events",
        "reorder_recommendations",
        "reverse_etl_audit",
    }
    with DatabaseManager.get_connection() as conn:
        if DatabaseManager.is_sqlite():
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            existing_tables = {r[0] for r in result}
        else:
            result = conn.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            ).fetchall()
            existing_tables = {r[0] for r in result}

    assert expected_tables.issubset(existing_tables), f"Missing tables: {expected_tables - existing_tables}"


def _count_csv_rows(file_path: Path) -> int:
    with open(file_path, mode="r", encoding="utf-8-sig") as f:
        return sum(1 for line in f if line.strip()) - 1


def test_02_staging_load_clean_demo_data():
    """Verify deterministic CSVs load successfully into staging tables."""
    run_id = "test-run-staging-001"
    summary = load_staging(run_id=run_id, landing_dir=Config.DATA_SOURCE_DIR)

    expected_prod = _count_csv_rows(Config.DATA_SOURCE_DIR / "products.csv")
    expected_inv = _count_csv_rows(Config.DATA_SOURCE_DIR / "inventory.csv")
    expected_sales = _count_csv_rows(Config.DATA_SOURCE_DIR / "sales.csv")

    assert summary["status"] == "success"
    assert summary["run_id"] == run_id
    assert summary["processed"] == expected_prod + expected_inv + expected_sales
    assert summary["details"]["stg_products"] == expected_prod
    assert summary["details"]["stg_inventory"] == expected_inv
    assert summary["details"]["stg_sales"] == expected_sales


def test_03_clean_validation_and_trusted_upsert():
    """Verify clean staging data passes validation and populates trusted tables."""
    run_id = "test-run-clean-002"
    load_staging(run_id=run_id, landing_dir=Config.DATA_SOURCE_DIR)
    val_summary = validate_and_clean(run_id=run_id)

    expected_prod = _count_csv_rows(Config.DATA_SOURCE_DIR / "products.csv")
    expected_inv = _count_csv_rows(Config.DATA_SOURCE_DIR / "inventory.csv")
    expected_sales = _count_csv_rows(Config.DATA_SOURCE_DIR / "sales.csv")
    total_expected = expected_prod + expected_inv + expected_sales

    assert val_summary["status"] == "success"
    assert val_summary["rejected"] == 0
    assert val_summary["processed"] == total_expected

    with DatabaseManager.get_connection() as conn:
        prod_count = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        inv_count = conn.execute(text("SELECT COUNT(*) FROM inventory")).scalar()
        sales_count = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
        reject_count = conn.execute(text("SELECT COUNT(*) FROM rejected_records")).scalar()

    assert prod_count == expected_prod
    assert inv_count == expected_inv
    assert sales_count == expected_sales
    assert reject_count == 0


def test_04_validation_null_required_fields():
    """Verify records missing required columns are rejected with NULL_REQUIRED_FIELD."""
    run_id = "test-run-nulls-003"
    with DatabaseManager.get_connection() as conn:
        # Missing category and product_name
        conn.execute(
            text("INSERT INTO stg_products (raw_product_id, raw_product_name, raw_category, raw_supplier_id, run_id) VALUES ('201', '', 'Electronics', 'SUPP-1', :r)"),
            {"r": run_id},
        )
        # Missing order_date in sales
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-99-201', 'ord-99', '', '201', '1', '10.0', :r)"),
            {"r": run_id},
        )

    val_summary = validate_and_clean(run_id=run_id)
    assert val_summary["rejected"] >= 2
    assert "NULL_REQUIRED_FIELD" in val_summary["details"]["rejections_by_reason"]


def test_05_validation_invalid_data_types():
    """Verify non-numeric values in numeric columns are rejected with INVALID_DATA_TYPE."""
    run_id = "test-run-types-004"
    with DatabaseManager.get_connection() as conn:
        # Non-integer product_id
        conn.execute(
            text("INSERT INTO stg_products (raw_product_id, raw_product_name, raw_category, raw_supplier_id, run_id) VALUES ('NOT_AN_INT', 'Bad Product', 'Electronics', 'SUPP-1', :r)"),
            {"r": run_id},
        )
        # String quantity and bad price
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-bad-qty', 'ord-bad', '2026-01-15T10:00:00Z', '101', 'TWO_PIECES', '29.99', :r)"),
            {"r": run_id},
        )

    val_summary = validate_and_clean(run_id=run_id)
    assert val_summary["rejected"] >= 2
    assert "INVALID_DATA_TYPE" in val_summary["details"]["rejections_by_reason"]


def test_06_validation_malformed_dates():
    """Verify malformed timestamps are rejected with INVALID_DATE_FORMAT."""
    run_id = "test-run-dates-005"
    with DatabaseManager.get_connection() as conn:
        # Setup valid product 101 first
        conn.execute(
            text("INSERT INTO products (product_id, product_name, category, supplier_id) VALUES (101, 'Mouse', 'Electronics', 'SUPP-1')")
        )
        # Malformed order date
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-bad-date', 'ord-999', '2026/99/99 Invalid-Time', '101', '1', '25.0', :r)"),
            {"r": run_id},
        )

    val_summary = validate_and_clean(run_id=run_id)
    assert "INVALID_DATE_FORMAT" in val_summary["details"]["rejections_by_reason"]


def test_07_validation_negative_and_zero_constraints():
    """Verify quantity <= 0, unit_price < 0, and stock < 0 are rejected."""
    run_id = "test-run-constraints-006"
    with DatabaseManager.get_connection() as conn:
        conn.execute(
            text("INSERT INTO products (product_id, product_name, category, supplier_id) VALUES (101, 'Mouse', 'Electronics', 'SUPP-1')")
        )
        # Negative quantity
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-neg-qty', 'ord-1', '2026-01-15T10:00:00Z', '101', '-5', '20.0', :r)"),
            {"r": run_id},
        )
        # Negative unit price
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-neg-price', 'ord-2', '2026-01-15T10:00:00Z', '101', '2', '-15.50', :r)"),
            {"r": run_id},
        )
        # Negative inventory stock
        conn.execute(
            text("INSERT INTO stg_inventory (raw_product_id, raw_current_stock, raw_reorder_level, run_id) VALUES ('101', '-10', '20', :r)"),
            {"r": run_id},
        )

    val_summary = validate_and_clean(run_id=run_id)
    reasons = val_summary["details"]["rejections_by_reason"]
    assert "NON_POSITIVE_QUANTITY" in reasons
    assert "NEGATIVE_UNIT_PRICE" in reasons
    assert "NEGATIVE_STOCK_VALUE" in reasons


def test_08_validation_unknown_foreign_key():
    """Verify sales or inventory for non-existent products are rejected with UNKNOWN_FOREIGN_KEY."""
    run_id = "test-run-fk-007"
    with DatabaseManager.get_connection() as conn:
        # Product 9999 does not exist in products table
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-fk-fail', 'ord-1', '2026-01-15T10:00:00Z', '9999', '1', '100.0', :r)"),
            {"r": run_id},
        )

    val_summary = validate_and_clean(run_id=run_id)
    assert "UNKNOWN_FOREIGN_KEY" in val_summary["details"]["rejections_by_reason"]


def test_09_validation_duplicate_business_keys():
    """Verify duplicate business keys within the same batch are rejected with DUPLICATE_BUSINESS_KEY."""
    run_id = "test-run-dups-008"
    with DatabaseManager.get_connection() as conn:
        conn.execute(
            text("INSERT INTO products (product_id, product_name, category, supplier_id) VALUES (101, 'Mouse', 'Electronics', 'SUPP-1')")
        )
        # First valid line
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-dup-1', 'ord-DUP', '2026-01-15T10:00:00Z', '101', '1', '25.0', :r)"),
            {"r": run_id},
        )
        # Duplicate line in same batch with same (order_id, product_id)
        conn.execute(
            text("INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id) VALUES ('ord-dup-1', 'ord-DUP', '2026-01-15T10:00:00Z', '101', '2', '25.0', :r)"),
            {"r": run_id},
        )

    val_summary = validate_and_clean(run_id=run_id)
    assert "DUPLICATE_BUSINESS_KEY" in val_summary["details"]["rejections_by_reason"]


def test_10_idempotency_repeated_batch_runs():
    """Verify running the exact same batch multiple times does not duplicate trusted records."""
    run_id_1 = "batch-run-idempotent-001"
    run_id_2 = "batch-run-idempotent-002"

    # Run 1
    load_staging(run_id=run_id_1, landing_dir=Config.DATA_SOURCE_DIR)
    validate_and_clean(run_id=run_id_1)

    with DatabaseManager.get_connection() as conn:
        initial_sales = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
        initial_products = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        initial_inventory = conn.execute(text("SELECT COUNT(*) FROM inventory")).scalar()

    # Run 2 (Rerun identical data under new run_id)
    load_staging(run_id=run_id_2, landing_dir=Config.DATA_SOURCE_DIR)
    validate_and_clean(run_id=run_id_2)

    with DatabaseManager.get_connection() as conn:
        final_sales = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
        final_products = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        final_inventory = conn.execute(text("SELECT COUNT(*) FROM inventory")).scalar()

    expected_sales = _count_csv_rows(Config.DATA_SOURCE_DIR / "sales.csv")
    expected_products = _count_csv_rows(Config.DATA_SOURCE_DIR / "products.csv")
    expected_inventory = _count_csv_rows(Config.DATA_SOURCE_DIR / "inventory.csv")

    # Counts must be identical (no duplicate rows created)
    assert final_sales == initial_sales == expected_sales
    assert final_products == initial_products == expected_products
    assert final_inventory == initial_inventory == expected_inventory


def test_11_parquet_export_and_schema_validation(tmp_path):
    """Verify clean data exports to Parquet files and can be read back with PyArrow."""
    run_id = "test-run-export-010"
    load_staging(run_id=run_id, landing_dir=Config.DATA_SOURCE_DIR)
    validate_and_clean(run_id=run_id)

    expected_sales = _count_csv_rows(Config.DATA_SOURCE_DIR / "sales.csv")
    expected_products = _count_csv_rows(Config.DATA_SOURCE_DIR / "products.csv")
    expected_inventory = _count_csv_rows(Config.DATA_SOURCE_DIR / "inventory.csv")
    expected_total = expected_sales + expected_products + expected_inventory

    export_summary = export_clean_data(run_id=run_id, output_dir=tmp_path)
    assert export_summary["status"] == "success"
    assert export_summary["processed"] == expected_total

    # Verify Parquet files exist and can be loaded
    products_pq = tmp_path / "products_clean.parquet"
    inventory_pq = tmp_path / "inventory_clean.parquet"
    sales_pq = tmp_path / "sales_clean.parquet"

    assert products_pq.exists() and products_pq.stat().st_size > 0
    assert inventory_pq.exists() and inventory_pq.stat().st_size > 0
    assert sales_pq.exists() and sales_pq.stat().st_size > 0

    df_sales = pd.read_parquet(sales_pq)
    assert len(df_sales) == expected_sales
    assert "order_line_id" in df_sales.columns
    assert "total_price" in df_sales.columns


def test_12_reconciliation_sql_vs_parquet(tmp_path):
    """Reconcile aggregate totals between SQL queries and Parquet output."""
    run_id = "test-run-reconcile-011"
    load_staging(run_id=run_id, landing_dir=Config.DATA_SOURCE_DIR)
    validate_and_clean(run_id=run_id)
    export_clean_data(run_id=run_id, output_dir=tmp_path)

    # SQL aggregates
    with DatabaseManager.get_connection() as conn:
        sql_totals = conn.execute(
            text("SELECT COUNT(*), SUM(quantity), ROUND(SUM(total_price), 2) FROM sales")
        ).fetchone()

    sql_count, sql_units, sql_revenue = sql_totals[0], int(sql_totals[1]), float(sql_totals[2])

    # Parquet aggregates
    df_sales = pd.read_parquet(tmp_path / "sales_clean.parquet")
    pq_count = len(df_sales)
    pq_units = int(df_sales["quantity"].sum())
    pq_revenue = round(float(df_sales["total_price"].sum()), 2)

    # Independent reconciliation assert
    assert sql_count == pq_count, f"Count mismatch: SQL={sql_count}, Parquet={pq_count}"
    assert sql_units == pq_units, f"Units mismatch: SQL={sql_units}, Parquet={pq_units}"
    assert abs(sql_revenue - pq_revenue) < 0.01, f"Revenue mismatch: SQL={sql_revenue}, Parquet={pq_revenue}"

