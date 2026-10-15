"""
Command-line driver to execute and demonstrate Person 2's components:
1. Canonical schema initialization
2. Landing data staging (load_staging)
3. Quality validation, trusted upsert & rejection tracking (validate_and_clean)
4. Clean Parquet export (export_clean_data)
5. SQL reconciliation against Parquet exports
6. Output summary of results/data_quality_report.json
"""

import json
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set console encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import pandas as pd
from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager, init_db
from src.etl.export import export_clean_data
from src.etl.staging import load_staging
from src.etl.validation import validate_and_clean

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Person2Runner")


def run_pipeline(run_id: str = "batch-demo-run-001", include_dirty_demo: bool = True):
    print("=" * 80)
    print(f">> EXECUTING PERSON 2 ETL & DATA QUALITY PIPELINE (run_id: {run_id})")
    print("=" * 80)

    # 1. Initialize schema
    print("\n[Step 1] Initializing Canonical Schema...")
    init_db()
    print("[OK] Canonical schema ready.")

    # 2. Load Landing Data into Staging
    print("\n[Step 2] Executing load_staging()...")
    staging_res = load_staging(run_id=run_id, landing_dir=Config.DATA_SOURCE_DIR)
    print(f"[OK] Staging Load Status: {staging_res['status']}")
    print(f"     Processed: {staging_res['processed']} rows across tables: {staging_res['details']}")

    # Optionally inject realistic test rejection cases to demonstrate rejection tracking
    if include_dirty_demo:
        print("\n[Step 2b] Injecting controlled dirty staging records for validation audit demonstration...")
        with DatabaseManager.get_connection() as conn:
            # 1. Null required field
            conn.execute(
                text(
                    """
                    INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id)
                    VALUES ('ord-err-01', 'ord-err', '', '101', '2', '29.99', :r)
                    """
                ),
                {"r": run_id},
            )
            # 2. Non-positive quantity
            conn.execute(
                text(
                    """
                    INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id)
                    VALUES ('ord-err-02', 'ord-err', '2026-01-16T12:00:00Z', '101', '-1', '29.99', :r)
                    """
                ),
                {"r": run_id},
            )
            # 3. Unknown foreign key product
            conn.execute(
                text(
                    """
                    INSERT INTO stg_sales (raw_order_line_id, raw_order_id, raw_order_date, raw_product_id, raw_quantity, raw_unit_price, run_id)
                    VALUES ('ord-err-03', 'ord-err', '2026-01-16T12:00:00Z', '9999', '1', '49.99', :r)
                    """
                ),
                {"r": run_id},
            )
        print("   Injected 3 test dirty records into stg_sales.")

    # 3. Validate and Clean
    print("\n[Step 3] Executing validate_and_clean()...")
    val_res = validate_and_clean(run_id=run_id)
    print(f"[OK] Validation Status: {val_res['status']}")
    print(f"     Trusted Rows Accepted: {val_res['processed']}")
    print(f"     Records Rejected:      {val_res['rejected']}")
    print(f"     Rejection Breakdown:   {val_res['details']['rejections_by_reason']}")

    # 4. Export Clean Data to Parquet
    print("\n[Step 4] Executing export_clean_data()...")
    export_res = export_clean_data(run_id=run_id)
    print(f"[OK] Parquet Export Status: {export_res['status']}")
    for tbl, info in export_res["details"].items():
        print(f"     - {tbl}: {info['row_count']} rows -> {info['path']} ({info['file_size_bytes']} bytes)")

    # 5. Independent SQL vs Parquet Reconciliation
    print("\n[Step 5] Reconciling Database State vs Parquet...")
    with DatabaseManager.get_connection() as conn:
        db_sales_cnt = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
        db_sales_rev = float(conn.execute(text("SELECT ROUND(SUM(total_price), 2) FROM sales")).scalar() or 0.0)
        db_sales_units = int(conn.execute(text("SELECT SUM(quantity) FROM sales")).scalar() or 0)
        reject_count = conn.execute(text("SELECT COUNT(*) FROM rejected_records WHERE run_id = :r"), {"r": run_id}).scalar()

    sales_pq_path = Config.DATA_PROCESSED_DIR / "sales_clean.parquet"
    df_pq = pd.read_parquet(sales_pq_path)
    pq_sales_cnt = len(df_pq)
    pq_sales_rev = round(float(df_pq["total_price"].sum()), 2)
    pq_sales_units = int(df_pq["quantity"].sum())

    print(f"     Metric               SQL Database       Parquet Export      Match?")
    print(f"     -------------------- ------------------ ------------------  ------")
    print(f"     Sales Record Count   {db_sales_cnt:<18} {pq_sales_cnt:<18}  {db_sales_cnt == pq_sales_cnt}")
    print(f"     Total Units Sold     {db_sales_units:<18} {pq_sales_units:<18}  {db_sales_units == pq_sales_units}")
    print(f"     Total Gross Revenue  ${db_sales_rev:<17} ${pq_sales_rev:<17} {abs(db_sales_rev - pq_sales_rev) < 0.01}")
    print(f"     Rejections Logged    {reject_count:<18} (Audit Table)")

    print("\n[Step 6] Verifying Data Quality Report on Disk...")
    report_file = Config.RESULTS_DIR / "data_quality_report.json"
    if report_file.exists():
        with open(report_file, "r") as rf:
            report_data = json.load(rf)
        print(f"[OK] Quality Report generated at {report_file}")
        print(f"     Pass Rate: {report_data['summary']['pass_rate_percentage']}%")
    else:
        print(f"[ERROR] Quality Report missing at {report_file}")

    print("\n" + "=" * 80)
    print(">> PERSON 2 ETL & DATA QUALITY EXECUTION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    run_pipeline()
