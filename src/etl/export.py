"""
Parquet Export ETL Module.
Exports trusted PostgreSQL operational datasets into clean, columnar Parquet files
under data/processed/ for high-performance downstream DuckDB analytical querying.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager

logger = logging.getLogger(__name__)


def export_clean_data(run_id: str, output_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Export trusted datasets to Parquet and return paths and counts.

    Args:
        run_id: Unique pipeline batch run identifier.
        output_dir: Optional override for export destination directory.

    Returns:
        Summary dictionary conforming to the team function contract.
    """
    start_time = time.time()
    dest_dir = output_dir or Config.DATA_PROCESSED_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting export_clean_data for run_id=%s into %s", run_id, dest_dir)

    # Tables to export into corresponding parquet files
    exports_config = [
        {
            "name": "products_clean",
            "table": "products",
            "query": "SELECT product_id, product_name, category, supplier_id, created_at, updated_at FROM products ORDER BY product_id",
            "file": dest_dir / "products_clean.parquet",
        },
        {
            "name": "inventory_clean",
            "table": "inventory",
            "query": "SELECT product_id, current_stock, reorder_level, last_updated FROM inventory ORDER BY product_id",
            "file": dest_dir / "inventory_clean.parquet",
        },
        {
            "name": "sales_clean",
            "table": "sales",
            "query": "SELECT order_line_id, order_id, order_date, product_id, quantity, unit_price, total_price, run_id, created_at FROM sales ORDER BY order_date",
            "file": dest_dir / "sales_clean.parquet",
        },
    ]

    total_records_exported = 0
    export_details: Dict[str, Dict[str, Any]] = {}

    try:
        with DatabaseManager.get_connection() as conn:
            for item in exports_config:
                query_str = item["query"]
                out_file = item["file"]

                # Read directly from DB into DataFrame
                df = pd.read_sql(text(query_str), conn)

                # Format datetime columns to ISO standard for Arrow compatibility
                for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
                    df[col] = df[col].astype(str)

                # Write to Parquet using PyArrow with snappy compression
                df.to_parquet(out_file, engine="pyarrow", compression="snappy", index=False)

                row_count = len(df)
                file_size_bytes = out_file.stat().st_size
                total_records_exported += row_count

                export_details[item["name"]] = {
                    "path": str(out_file),
                    "row_count": row_count,
                    "file_size_bytes": file_size_bytes,
                }
                logger.info(
                    "Exported %s: %d rows (%d bytes) to %s",
                    item["name"], row_count, file_size_bytes, out_file
                )

        duration = round(time.time() - start_time, 4)
        logger.info(
            "export_clean_data completed in %.2fs: %d total rows exported",
            duration, total_records_exported
        )

        return {
            "status": "success",
            "run_id": run_id,
            "processed": total_records_exported,
            "failed": 0,
            "rejected": 0,
            "message": f"Successfully exported {total_records_exported} clean records across {len(exports_config)} Parquet files",
            "duration_seconds": duration,
            "details": export_details,
        }

    except Exception as exc:
        duration = round(time.time() - start_time, 4)
        logger.exception("Failed during export_clean_data: %s", exc)
        return {
            "status": "failed",
            "run_id": run_id,
            "processed": total_records_exported,
            "failed": 1,
            "rejected": 0,
            "message": f"Parquet export failed: {str(exc)}",
            "duration_seconds": duration,
            "details": {"error": str(exc)},
        }

