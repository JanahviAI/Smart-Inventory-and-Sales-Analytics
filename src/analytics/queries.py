"""
Analytical processing module using DuckDB and SQL.
Calculates agreed sales and inventory metrics from clean Parquet exports and trusted DB tables.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd
from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager

logger = logging.getLogger(__name__)


def calculate_analytics(run_id: str) -> Dict[str, Any]:
    """
    Calculate agreed sales and inventory metrics.
    Uses DuckDB over Parquet files, falling back to database tables if necessary.

    Args:
        run_id: Pipeline batch run identifier.

    Returns:
        Standard summary dict according to TEAM_IMPLEMENTATION_PLAN contract.
    """
    start_time = time.time()
    logger.info("Starting calculate_analytics for run_id: %s", run_id)

    sales_pq = Config.DATA_PROCESSED_DIR / "sales_clean.parquet"
    products_pq = Config.DATA_PROCESSED_DIR / "products_clean.parquet"
    inventory_pq = Config.DATA_PROCESSED_DIR / "inventory_clean.parquet"

    metrics: Dict[str, Any] = {}
    record_count = 0

    try:
        if sales_pq.exists() and products_pq.exists() and inventory_pq.exists():
            logger.info("Executing DuckDB analytics over Parquet files")
            con = duckdb.connect(database=":memory:")

            con.execute(f"CREATE VIEW v_sales AS SELECT * FROM read_parquet('{sales_pq.as_posix()}')")
            con.execute(f"CREATE VIEW v_products AS SELECT * FROM read_parquet('{products_pq.as_posix()}')")
            con.execute(f"CREATE VIEW v_inventory AS SELECT * FROM read_parquet('{inventory_pq.as_posix()}')")

            # 1. Total sales metrics
            total_stats = con.execute("""
                SELECT 
                    COUNT(*) as total_orders,
                    COALESCE(SUM(quantity), 0) as total_units,
                    COALESCE(ROUND(SUM(total_price), 2), 0.0) as total_revenue
                FROM v_sales
            """).fetchone()

            metrics["total_orders"] = int(total_stats[0])
            metrics["total_units"] = int(total_stats[1])
            metrics["total_revenue"] = float(total_stats[2])
            record_count = metrics["total_orders"]

            # 2. Category sales breakdown
            cat_df = con.execute("""
                SELECT 
                    p.category,
                    COALESCE(SUM(s.quantity), 0) as units_sold,
                    COALESCE(ROUND(SUM(s.total_price), 2), 0.0) as revenue
                FROM v_sales s
                JOIN v_products p ON s.product_id = p.product_id
                GROUP BY p.category
                ORDER BY revenue DESC
            """).df()
            metrics["category_breakdown"] = cat_df.to_dict(orient="records")

            # 3. Top selling products
            top_df = con.execute("""
                SELECT 
                    p.product_id,
                    p.product_name,
                    p.category,
                    COALESCE(SUM(s.quantity), 0) as units_sold,
                    COALESCE(ROUND(SUM(s.total_price), 2), 0.0) as total_revenue
                FROM v_sales s
                JOIN v_products p ON s.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.category
                ORDER BY total_revenue DESC
                LIMIT 5
            """).df()
            metrics["top_products"] = top_df.to_dict(orient="records")

            # 4. Low stock inventory items
            low_stock_df = con.execute("""
                SELECT 
                    i.product_id,
                    p.product_name,
                    i.current_stock,
                    i.reorder_level
                FROM v_inventory i
                JOIN v_products p ON i.product_id = p.product_id
                WHERE i.current_stock <= i.reorder_level
                ORDER BY i.current_stock ASC
            """).df()
            metrics["low_stock_items"] = low_stock_df.to_dict(orient="records")
            metrics["low_stock_count"] = len(low_stock_df)

            con.close()
        else:
            logger.info("Parquet files not yet found; computing analytics from DB tables")
            with DatabaseManager.get_connection() as conn:
                stats = conn.execute(text("""
                    SELECT 
                        COUNT(*), 
                        COALESCE(SUM(quantity), 0), 
                        COALESCE(ROUND(SUM(total_price), 2), 0.0)
                    FROM sales
                """)).fetchone()
                metrics["total_orders"] = int(stats[0] if stats else 0)
                metrics["total_units"] = int(stats[1] if stats else 0)
                metrics["total_revenue"] = float(stats[2] if stats else 0.0)
                record_count = metrics["total_orders"]

                low_stock = conn.execute(text("""
                    SELECT COUNT(*) FROM inventory WHERE current_stock <= reorder_level
                """)).scalar()
                metrics["low_stock_count"] = int(low_stock or 0)
                metrics["low_stock_items"] = []
                metrics["category_breakdown"] = []
                metrics["top_products"] = []

        # Save analytics output to results/
        analytics_file = Config.RESULTS_DIR / "analytics_summary.json"
        with open(analytics_file, "w", encoding="utf-8") as f:
            json.dump({
                "run_id": run_id,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "metrics": metrics
            }, f, indent=2)

        duration = round(time.time() - start_time, 3)
        return {
            "status": "success",
            "run_id": run_id,
            "processed": record_count,
            "failed": 0,
            "rejected": 0,
            "message": f"Successfully calculated analytics for {record_count} sales records",
            "duration_seconds": duration,
            "details": metrics,
        }

    except Exception as e:
        logger.error("Error in calculate_analytics: %s", str(e), exc_info=True)
        duration = round(time.time() - start_time, 3)
        return {
            "status": "failed",
            "run_id": run_id,
            "processed": record_count,
            "failed": 1,
            "rejected": 0,
            "message": f"Analytics calculation failed: {str(e)}",
            "duration_seconds": duration,
            "details": {"error": str(e)},
        }
