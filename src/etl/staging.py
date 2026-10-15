"""
Staging ETL Module.
Loads landing CSV files into raw staging tables in PostgreSQL/Database.
Does not perform business validation or type conversion; all values are ingested as text
so that malformed values can be captured and routed to rejected_records downstream.
"""

import csv
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager

logger = logging.getLogger(__name__)


def _read_csv_raw(file_path: Path) -> List[Dict[str, str]]:
    """Reads a CSV file returning all column values as strings."""
    if not file_path.exists():
        raise FileNotFoundError(f"Landing file not found: {file_path}")

    rows = []
    with open(file_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Strip whitespace from keys and values
            cleaned_row = {
                (k.strip() if k else ""): (v.strip() if v is not None else "")
                for k, v in row.items()
            }
            rows.append(cleaned_row)
    return rows


def load_staging(run_id: str, landing_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load required landing files into staging tables.

    Args:
        run_id: Unique pipeline batch run identifier.
        landing_dir: Optional override directory for landing files.

    Returns:
        Summary dictionary with status, processed count, and details.
    """
    start_time = time.time()
    source_dir = landing_dir or Config.DATA_LANDING_DIR

    logger.info("Starting load_staging for run_id=%s from %s", run_id, source_dir)

    # Required files map: file_name -> (staging_table, column_mapping)
    files_to_load = {
        "products.csv": "stg_products",
        "inventory.csv": "stg_inventory",
        "sales.csv": "stg_sales",
    }

    # Verify presence of landing files. If missing in landing but present in source (local dev convenience), copy.
    missing_files = []
    for file_name in files_to_load:
        target_path = source_dir / file_name
        if not target_path.exists():
            fallback_source = Config.DATA_SOURCE_DIR / file_name
            if fallback_source.exists():
                logger.info("Landing file %s missing, copying from source %s", target_path, fallback_source)
                source_dir.mkdir(parents=True, exist_ok=True)
                target_path.write_text(fallback_source.read_text(encoding="utf-8"), encoding="utf-8")
            else:
                missing_files.append(file_name)

    if missing_files:
        error_msg = f"Missing required landing files: {', '.join(missing_files)}"
        logger.error(error_msg)
        return {
            "status": "failed",
            "run_id": run_id,
            "processed": 0,
            "failed": len(missing_files),
            "rejected": 0,
            "message": error_msg,
            "duration_seconds": round(time.time() - start_time, 4),
            "details": {"missing_files": missing_files},
        }

    total_rows_loaded = 0
    table_counts = {}

    try:
        with DatabaseManager.get_connection() as conn:
            # 1. Clean previous staging records for this run_id to ensure idempotency
            for table_name in files_to_load.values():
                conn.execute(
                    text(f"DELETE FROM {table_name} WHERE run_id = :run_id"),
                    {"run_id": run_id},
                )

            # 2. Ingest products
            products_file = source_dir / "products.csv"
            product_rows = _read_csv_raw(products_file)
            for r in product_rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO stg_products (raw_product_id, raw_product_name, raw_category, raw_supplier_id, run_id)
                        VALUES (:pid, :pname, :cat, :supp, :run_id)
                        """
                    ),
                    {
                        "pid": r.get("product_id", ""),
                        "pname": r.get("product_name", ""),
                        "cat": r.get("category", ""),
                        "supp": r.get("supplier_id", ""),
                        "run_id": run_id,
                    },
                )
            table_counts["stg_products"] = len(product_rows)
            total_rows_loaded += len(product_rows)

            # 3. Ingest inventory
            inventory_file = source_dir / "inventory.csv"
            inventory_rows = _read_csv_raw(inventory_file)
            for r in inventory_rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO stg_inventory (raw_product_id, raw_current_stock, raw_reorder_level, run_id)
                        VALUES (:pid, :stock, :reorder, :run_id)
                        """
                    ),
                    {
                        "pid": r.get("product_id", ""),
                        "stock": r.get("current_stock", ""),
                        "reorder": r.get("reorder_level", ""),
                        "run_id": run_id,
                    },
                )
            table_counts["stg_inventory"] = len(inventory_rows)
            total_rows_loaded += len(inventory_rows)

            # 4. Ingest sales
            sales_file = source_dir / "sales.csv"
            sales_rows = _read_csv_raw(sales_file)
            for r in sales_rows:
                order_id = r.get("order_id", "")
                product_id = r.get("product_id", "")
                order_line_id = r.get("order_line_id") or (
                    f"{order_id}-{product_id}" if order_id and product_id else ""
                )

                conn.execute(
                    text(
                        """
                        INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id)
                        VALUES (:line_id, :oid, :odate, :pid, :qty, :price, :run_id)
                        """
                    ),
                    {
                        "line_id": order_line_id,
                        "oid": order_id,
                        "odate": r.get("order_date", ""),
                        "pid": product_id,
                        "qty": r.get("quantity", ""),
                        "price": r.get("unit_price", ""),
                        "run_id": run_id,
                    },
                )
            table_counts["stg_sales"] = len(sales_rows)
            total_rows_loaded += len(sales_rows)

        duration = round(time.time() - start_time, 4)
        logger.info(
            "load_staging completed successfully: loaded %d rows across %d tables in %.2fs",
            total_rows_loaded, len(table_counts), duration
        )

        return {
            "status": "success",
            "run_id": run_id,
            "processed": total_rows_loaded,
            "failed": 0,
            "rejected": 0,
            "message": f"Successfully loaded {total_rows_loaded} rows into staging",
            "duration_seconds": duration,
            "details": table_counts,
        }

    except Exception as exc:
        duration = round(time.time() - start_time, 4)
        logger.exception("Failed during load_staging: %s", exc)
        return {
            "status": "failed",
            "run_id": run_id,
            "processed": total_rows_loaded,
            "failed": 1,
            "rejected": 0,
            "message": f"Staging load failed: {str(exc)}",
            "duration_seconds": duration,
            "details": {"error": str(exc)},
        }

