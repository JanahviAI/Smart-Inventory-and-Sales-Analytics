"""
Data Quality and Validation ETL Module.
Validates raw staging records deterministically, writes clean rows idempotently
to trusted operational tables, records invalid rows in rejected_records,
and produces a structured data quality summary report.
"""

import json
import logging
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dateutil import parser as date_parser
from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager

logger = logging.getLogger(__name__)


def _is_empty(val: Optional[str]) -> bool:
    """Checks if a string value is None, empty, or whitespace-only."""
    return val is None or str(val).strip() == ""


def _parse_iso_date(val: str) -> Optional[datetime]:
    """Parses an ISO-8601 date/time string. Returns None on failure."""
    if _is_empty(val):
        return None
    try:
        # Standardize 'Z' to UTC offset
        clean_val = val.strip()
        dt = date_parser.isoparse(clean_val)
        return dt
    except Exception:
        return None


def _log_rejection(
    conn,
    run_id: str,
    source_table: str,
    record_identifier: Optional[str],
    raw_payload: Dict[str, Any],
    reason: str,
) -> None:
    """Inserts a rejected row into rejected_records."""
    payload_json = json.dumps(raw_payload)
    conn.execute(
        text(
            """
            INSERT INTO rejected_records (run_id, source_table, record_identifier, raw_payload, reason)
            VALUES (:run_id, :source, :id, :payload, :reason)
            """
        ),
        {
            "run_id": run_id,
            "source": source_table,
            "id": record_identifier or "UNKNOWN",
            "payload": payload_json,
            "reason": reason,
        },
    )


def validate_and_clean(run_id: str) -> Dict[str, Any]:
    """
    Validate, deduplicate, upsert trusted data, and log rejects.

    Args:
        run_id: Unique pipeline batch run identifier.

    Returns:
        Summary dictionary conforming to the team function contract.
    """
    start_time = time.time()
    logger.info("Starting validate_and_clean for run_id=%s", run_id)

    total_staged = 0
    total_accepted = 0
    total_rejected = 0
    rejection_reasons_breakdown: Dict[str, int] = {}
    table_stats: Dict[str, Dict[str, int]] = {}

    def track_rejection(reason_code: str):
        nonlocal total_rejected
        total_rejected += 1
        rejection_reasons_breakdown[reason_code] = rejection_reasons_breakdown.get(reason_code, 0) + 1

    try:
        with DatabaseManager.get_connection() as conn:
            # -----------------------------------------------------------------
            # 1. PROCESS PRODUCTS (Master catalog foundation)
            # -----------------------------------------------------------------
            stg_products = conn.execute(
                text("SELECT raw_product_id, raw_product_name, raw_category, raw_supplier_id FROM stg_products WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).fetchall()

            prod_staged = len(stg_products)
            prod_accepted = 0
            prod_rejected = 0
            total_staged += prod_staged

            seen_batch_product_ids: Set[int] = set()

            for row in stg_products:
                raw_dict = {
                    "product_id": row[0],
                    "product_name": row[1],
                    "category": row[2],
                    "supplier_id": row[3],
                }

                # Validation 1: Required fields
                if _is_empty(row[0]) or _is_empty(row[1]) or _is_empty(row[2]) or _is_empty(row[3]):
                    reason = "NULL_REQUIRED_FIELD: Missing required product fields"
                    _log_rejection(conn, run_id, "stg_products", str(row[0]), raw_dict, reason)
                    track_rejection("NULL_REQUIRED_FIELD")
                    prod_rejected += 1
                    continue

                # Validation 2: Product ID integer parsing
                try:
                    product_id = int(str(row[0]).strip())
                except ValueError:
                    reason = f"INVALID_DATA_TYPE: product_id '{row[0]}' must be an integer"
                    _log_rejection(conn, run_id, "stg_products", str(row[0]), raw_dict, reason)
                    track_rejection("INVALID_DATA_TYPE")
                    prod_rejected += 1
                    continue

                # Validation 3: Duplicate within batch
                if product_id in seen_batch_product_ids:
                    reason = f"DUPLICATE_BUSINESS_KEY: product_id {product_id} appears multiple times in batch"
                    _log_rejection(conn, run_id, "stg_products", str(product_id), raw_dict, reason)
                    track_rejection("DUPLICATE_BUSINESS_KEY")
                    prod_rejected += 1
                    continue
                seen_batch_product_ids.add(product_id)

                # Upsert into trusted products table
                conn.execute(
                    text(
                        """
                        INSERT INTO products (product_id, product_name, category, supplier_id, updated_at)
                        VALUES (:pid, :pname, :cat, :supp, CURRENT_TIMESTAMP)
                        ON CONFLICT (product_id) DO UPDATE SET
                            product_name = EXCLUDED.product_name,
                            category = EXCLUDED.category,
                            supplier_id = EXCLUDED.supplier_id,
                            updated_at = CURRENT_TIMESTAMP
                        """
                    ),
                    {
                        "pid": product_id,
                        "pname": str(row[1]).strip(),
                        "cat": str(row[2]).strip(),
                        "supp": str(row[3]).strip(),
                    },
                )
                prod_accepted += 1

            table_stats["products"] = {
                "staged": prod_staged,
                "accepted": prod_accepted,
                "rejected": prod_rejected,
            }
            total_accepted += prod_accepted

            # Query all valid known product IDs currently in the trusted database
            valid_db_products: Set[int] = {
                r[0] for r in conn.execute(text("SELECT product_id FROM products")).fetchall()
            }

            # -----------------------------------------------------------------
            # 2. PROCESS INVENTORY
            # -----------------------------------------------------------------
            stg_inventory = conn.execute(
                text("SELECT raw_product_id, raw_current_stock, raw_reorder_level FROM stg_inventory WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).fetchall()

            inv_staged = len(stg_inventory)
            inv_accepted = 0
            inv_rejected = 0
            total_staged += inv_staged

            seen_batch_inv_pids: Set[int] = set()

            for row in stg_inventory:
                raw_dict = {
                    "product_id": row[0],
                    "current_stock": row[1],
                    "reorder_level": row[2],
                }

                # Validation 1: Required fields
                if _is_empty(row[0]) or _is_empty(row[1]) or _is_empty(row[2]):
                    reason = "NULL_REQUIRED_FIELD: Missing required inventory fields"
                    _log_rejection(conn, run_id, "stg_inventory", str(row[0]), raw_dict, reason)
                    track_rejection("NULL_REQUIRED_FIELD")
                    inv_rejected += 1
                    continue

                # Validation 2: Types
                try:
                    product_id = int(str(row[0]).strip())
                    current_stock = int(str(row[1]).strip())
                    reorder_level = int(str(row[2]).strip())
                except ValueError:
                    reason = "INVALID_DATA_TYPE: product_id, current_stock, and reorder_level must be integers"
                    _log_rejection(conn, run_id, "stg_inventory", str(row[0]), raw_dict, reason)
                    track_rejection("INVALID_DATA_TYPE")
                    inv_rejected += 1
                    continue

                # Validation 3: Non-negative stock constraints
                if current_stock < 0 or reorder_level < 0:
                    reason = f"NEGATIVE_STOCK_VALUE: stock ({current_stock}) and reorder ({reorder_level}) must be >= 0"
                    _log_rejection(conn, run_id, "stg_inventory", str(product_id), raw_dict, reason)
                    track_rejection("NEGATIVE_STOCK_VALUE")
                    inv_rejected += 1
                    continue

                # Validation 4: Referential integrity (Foreign key to products)
                if product_id not in valid_db_products:
                    reason = f"UNKNOWN_FOREIGN_KEY: product_id {product_id} does not exist in master products"
                    _log_rejection(conn, run_id, "stg_inventory", str(product_id), raw_dict, reason)
                    track_rejection("UNKNOWN_FOREIGN_KEY")
                    inv_rejected += 1
                    continue

                # Validation 5: Duplicate in batch
                if product_id in seen_batch_inv_pids:
                    reason = f"DUPLICATE_BUSINESS_KEY: product_id {product_id} appears multiple times in inventory batch"
                    _log_rejection(conn, run_id, "stg_inventory", str(product_id), raw_dict, reason)
                    track_rejection("DUPLICATE_BUSINESS_KEY")
                    inv_rejected += 1
                    continue
                seen_batch_inv_pids.add(product_id)

                # Upsert into trusted inventory table
                conn.execute(
                    text(
                        """
                        INSERT INTO inventory (product_id, current_stock, reorder_level, last_updated)
                        VALUES (:pid, :stock, :reorder, CURRENT_TIMESTAMP)
                        ON CONFLICT (product_id) DO UPDATE SET
                            current_stock = EXCLUDED.current_stock,
                            reorder_level = EXCLUDED.reorder_level,
                            last_updated = CURRENT_TIMESTAMP
                        """
                    ),
                    {"pid": product_id, "stock": current_stock, "reorder": reorder_level},
                )
                inv_accepted += 1

            table_stats["inventory"] = {
                "staged": inv_staged,
                "accepted": inv_accepted,
                "rejected": inv_rejected,
            }
            total_accepted += inv_accepted

            # -----------------------------------------------------------------
            # 3. PROCESS SALES TRANSACTIONS
            # -----------------------------------------------------------------
            stg_sales = conn.execute(
                text(
                    """
                    SELECT raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price
                    FROM stg_sales WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            ).fetchall()

            sales_staged = len(stg_sales)
            sales_accepted = 0
            sales_rejected = 0
            total_staged += sales_staged

            seen_batch_line_ids: Set[str] = set()
            seen_batch_order_products: Set[Tuple[str, int]] = set()

            for row in stg_sales:
                raw_dict = {
                    "order_line_id": row[0],
                    "order_id": row[1],
                    "order_date": row[2],
                    "product_id": row[3],
                    "quantity": row[4],
                    "unit_price": row[5],
                }

                # Validation 1: Required fields
                if (
                    _is_empty(row[1])
                    or _is_empty(row[2])
                    or _is_empty(row[3])
                    or _is_empty(row[4])
                    or _is_empty(row[5])
                ):
                    reason = "NULL_REQUIRED_FIELD: Missing required sales transaction fields"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("NULL_REQUIRED_FIELD")
                    sales_rejected += 1
                    continue

                order_id = str(row[1]).strip()

                # Validation 2: Types (integer product_id, integer quantity, numeric unit_price)
                try:
                    product_id = int(str(row[3]).strip())
                except ValueError:
                    reason = f"INVALID_DATA_TYPE: product_id '{row[3]}' must be an integer"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("INVALID_DATA_TYPE")
                    sales_rejected += 1
                    continue

                try:
                    quantity = int(str(row[4]).strip())
                except ValueError:
                    reason = f"INVALID_DATA_TYPE: quantity '{row[4]}' must be an integer"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("INVALID_DATA_TYPE")
                    sales_rejected += 1
                    continue

                try:
                    unit_price = float(Decimal(str(row[5]).strip()))
                except (InvalidOperation, ValueError):
                    reason = f"INVALID_DATA_TYPE: unit_price '{row[5]}' must be a valid decimal number"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("INVALID_DATA_TYPE")
                    sales_rejected += 1
                    continue

                # Validation 3: Date format (ISO-8601 UTC)
                parsed_date = _parse_iso_date(str(row[2]))
                if parsed_date is None:
                    reason = f"INVALID_DATE_FORMAT: '{row[2]}' does not conform to ISO-8601 format"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("INVALID_DATE_FORMAT")
                    sales_rejected += 1
                    continue

                # Validation 4: Positive quantity constraint
                if quantity <= 0:
                    reason = f"NON_POSITIVE_QUANTITY: quantity {quantity} must be strictly greater than 0"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("NON_POSITIVE_QUANTITY")
                    sales_rejected += 1
                    continue

                # Validation 5: Non-negative price constraint
                if unit_price < 0.0:
                    reason = f"NEGATIVE_UNIT_PRICE: unit_price {unit_price} must be >= 0"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("NEGATIVE_UNIT_PRICE")
                    sales_rejected += 1
                    continue

                # Validation 6: Referential integrity (FK to products)
                if product_id not in valid_db_products:
                    reason = f"UNKNOWN_FOREIGN_KEY: product_id {product_id} does not exist in master products"
                    _log_rejection(conn, run_id, "stg_sales", str(row[0]), raw_dict, reason)
                    track_rejection("UNKNOWN_FOREIGN_KEY")
                    sales_rejected += 1
                    continue

                # Stable order_line_id
                order_line_id = (
                    str(row[0]).strip()
                    if not _is_empty(row[0])
                    else f"{order_id}-{product_id}"
                )

                # Validation 7: Uniqueness within batch
                composite_key = (order_id, product_id)
                if order_line_id in seen_batch_line_ids or composite_key in seen_batch_order_products:
                    reason = f"DUPLICATE_BUSINESS_KEY: duplicate order_line_id or (order_id, product_id) in batch: {order_line_id}"
                    _log_rejection(conn, run_id, "stg_sales", order_line_id, raw_dict, reason)
                    track_rejection("DUPLICATE_BUSINESS_KEY")
                    sales_rejected += 1
                    continue

                seen_batch_line_ids.add(order_line_id)
                seen_batch_order_products.add(composite_key)

                total_price = round(quantity * unit_price, 2)

                # Upsert into trusted sales table (Idempotent: reruns will not duplicate rows)
                conn.execute(
                    text(
                        """
                        INSERT INTO sales (order_line_id, order_id, order_date, product_id, quantity, unit_price, total_price, run_id, updated_at)
                        VALUES (:lid, :oid, :odate, :pid, :qty, :price, :total, :run_id, CURRENT_TIMESTAMP)
                        ON CONFLICT (order_line_id) DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            unit_price = EXCLUDED.unit_price,
                            total_price = EXCLUDED.total_price,
                            run_id = EXCLUDED.run_id,
                            updated_at = CURRENT_TIMESTAMP
                        """
                    ),
                    {
                        "lid": order_line_id,
                        "oid": order_id,
                        "odate": parsed_date,
                        "pid": product_id,
                        "qty": quantity,
                        "price": unit_price,
                        "total": total_price,
                        "run_id": run_id,
                    },
                )
                sales_accepted += 1

            table_stats["sales"] = {
                "staged": sales_staged,
                "accepted": sales_accepted,
                "rejected": sales_rejected,
            }
            total_accepted += sales_accepted

        duration = round(time.time() - start_time, 4)

        # -----------------------------------------------------------------
        # 4. GENERATE DATA QUALITY REPORT
        # -----------------------------------------------------------------
        quality_report = {
            "run_id": run_id,
            "status": "success" if total_rejected == 0 else "partial",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "duration_seconds": duration,
            "summary": {
                "total_staged": total_staged,
                "total_accepted": total_accepted,
                "total_rejected": total_rejected,
                "pass_rate_percentage": round(
                    (total_accepted / total_staged * 100) if total_staged > 0 else 0.0, 2
                ),
            },
            "table_breakdown": table_stats,
            "rejections_by_reason": rejection_reasons_breakdown,
        }

        # Persist report to results directory
        report_path = Config.RESULTS_DIR / "data_quality_report.json"
        Config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as rf:
            json.dump(quality_report, rf, indent=2)

        logger.info(
            "validate_and_clean finished in %.2fs: %d accepted, %d rejected. Report saved to %s",
            duration, total_accepted, total_rejected, report_path
        )

        return {
            "status": "success" if total_rejected == 0 else "partial",
            "run_id": run_id,
            "processed": total_accepted,
            "failed": 0,
            "rejected": total_rejected,
            "message": f"Processed {total_staged} staged rows: {total_accepted} accepted, {total_rejected} rejected",
            "duration_seconds": duration,
            "details": quality_report,
        }

    except Exception as exc:
        duration = round(time.time() - start_time, 4)
        logger.exception("Failed during validate_and_clean: %s", exc)
        return {
            "status": "failed",
            "run_id": run_id,
            "processed": total_accepted,
            "failed": 1,
            "rejected": total_rejected,
            "message": f"Validation pipeline failure: {str(exc)}",
            "duration_seconds": duration,
            "details": {"error": str(exc)},
        }

