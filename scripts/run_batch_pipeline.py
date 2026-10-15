"""
Batch Pipeline Orchestration Runner.
Executes the retail_batch_pipeline 8-task sequence locally and generates execution evidence.
Can be executed directly via CLI or integrated into automated testing.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Project root setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Console encoding setup
if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from src.common.config import Config
from src.common.db import DatabaseManager, init_db
from src.common.pipeline_tracker import PipelineTracker
from dags.retail_batch_pipeline import (
    task_check_input_files,
    task_load_staging,
    task_validate_and_clean,
    task_export_clean_data,
    task_calculate_analytics,
    task_generate_reorder_recommendations,
    task_sync_recommendations,
    task_reconcile_pipeline,
    AirflowFailException,
)
from scripts.simulate_nifi_ingest import route_files_atomically, clean_landing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Orchestrator] %(message)s")
logger = logging.getLogger("Orchestrator")


def run_batch_pipeline(run_id: str = "batch-orch-run-001", auto_ingest: bool = True) -> dict:
    """
    Executes the 8-task retail batch orchestration pipeline in sequence.
    """
    print("=" * 80)
    print(f">> EXECUTING RETAIL BATCH ORCHESTRATION PIPELINE (run_id: {run_id})")
    print("=" * 80)

    # Auto-ingest files to landing if landing is missing required files
    if auto_ingest:
        landing_sales = Config.DATA_LANDING_DIR / "sales.csv"
        landing_products = Config.DATA_LANDING_DIR / "products.csv"
        landing_inv = Config.DATA_LANDING_DIR / "inventory.csv"
        if not (landing_sales.exists() and landing_products.exists() and landing_inv.exists()):
            print("\n[Prep] NiFi Ingestion: Routing source CSVs to shared landing directory...")
            route_files_atomically(Config.DATA_SOURCE_DIR, Config.DATA_LANDING_DIR)
            print("[OK] NiFi ingestion simulation complete.")

    context = {"run_id": run_id}

    # 1. check_input_files
    print("\n[Task 1/8] check_input_files...")
    t1_res = task_check_input_files(**context)
    print(f"  [OK] Input files verified: {list(t1_res['files'].keys())}")

    # 2. load_staging
    print("\n[Task 2/8] load_staging...")
    t2_res = task_load_staging(**context)
    print(f"  [OK] Staged rows: {t2_res['processed']} across {t2_res['details']}")

    # 3. validate_and_clean
    print("\n[Task 3/8] validate_and_clean...")
    t3_res = task_validate_and_clean(**context)
    print(f"  [OK] Accepted: {t3_res['processed']}, Rejected: {t3_res['rejected']}")

    # 4. export_clean_data
    print("\n[Task 4/8] export_clean_data...")
    t4_res = task_export_clean_data(**context)
    print(f"  [OK] Exported Parquet files: {list(t4_res['details'].keys())}")

    # 5. calculate_analytics
    print("\n[Task 5/8] calculate_analytics...")
    t5_res = task_calculate_analytics(**context)
    print(f"  [OK] Analytics: {t5_res['details'].get('total_orders', 0)} orders, ${t5_res['details'].get('total_revenue', 0.0)} revenue")

    # 6. generate_reorder_recommendations
    print("\n[Task 6/8] generate_reorder_recommendations...")
    t6_res = task_generate_reorder_recommendations(**context)
    print(f"  [OK] Recommendations generated: {t6_res['processed']}")

    # 7. sync_recommendations
    print("\n[Task 7/8] sync_recommendations...")
    t7_res = task_sync_recommendations(**context)
    print(f"  [OK] Recommendations synced: {t7_res['processed']}, failed: {t7_res['failed']}")

    # 8. reconcile_pipeline
    print("\n[Task 8/8] reconcile_pipeline...")
    t8_res = task_reconcile_pipeline(**context)
    print(f"  [OK] Reconciliation complete. Pipeline status: {t8_res['status']}")

    summary_file = Config.RESULTS_DIR / "pipeline_run_summary.json"
    print("\n" + "=" * 80)
    print(f">> PIPELINE RUN COMPLETE. SUMMARY SAVED TO: {summary_file}")
    print("=" * 80)
    if summary_file.exists():
        with open(summary_file, "r") as f:
            print(json.dumps(json.load(f), indent=2))

    return t8_res


def main():
    parser = argparse.ArgumentParser(description="Retail Batch Pipeline Orchestrator")
    parser.add_argument("--run-id", type=str, default="batch-orch-run-001", help="Custom run_id")
    parser.add_argument("--clean-landing", action="store_true", help="Clean landing dir first to test missing input failure (AT-02)")
    parser.add_argument("--no-auto-ingest", action="store_true", help="Do not automatically route files from source if landing is empty")

    args = parser.parse_args()

    if args.clean_landing:
        print("[Setup] Cleaning landing directory...")
        clean_landing(Config.DATA_LANDING_DIR)

    try:
        run_batch_pipeline(run_id=args.run_id, auto_ingest=not args.no_auto_ingest)
    except AirflowFailException as afe:
        print(f"\n[EXPECTED PIPELINE FAILURE] Task execution halted: {afe}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[UNEXPECTED PIPELINE ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
