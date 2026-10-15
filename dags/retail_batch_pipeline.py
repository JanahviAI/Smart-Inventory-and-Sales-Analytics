"""
Retail Batch Pipeline Airflow DAG.
DAG ID: retail_batch_pipeline
Owner: Person 1 (NiFi Ingestion & Airflow Orchestration)

Orchestrates the complete retail batch lifecycle:
1. check_input_files
2. load_staging
3. validate_and_clean
4. export_clean_data
5. calculate_analytics
6. generate_reorder_recommendations
7. sync_recommendations
8. reconcile_pipeline
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure project root is available on sys.path
DAG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Internal platform imports
from src.common.config import Config
from src.common.db import DatabaseManager, init_db
from src.common.pipeline_tracker import PipelineTracker
from src.etl.staging import load_staging
from src.etl.validation import validate_and_clean
from src.etl.export import export_clean_data
from src.analytics.queries import calculate_analytics
from src.analytics.recommendations import generate_reorder_recommendations, sync_recommendations

logger = logging.getLogger("airflow.task")

# -----------------------------------------------------------------------------
# Airflow compatibility shim:
# In Docker/Airflow, real Airflow classes are imported.
# In local Python environments without Apache Airflow, a compatible shim is used.
# -----------------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.exceptions import AirflowFailException
    AIRFLOW_IMPORTED = True
except ImportError:
    AIRFLOW_IMPORTED = False

    class AirflowFailException(Exception):
        """Custom failure exception mimicking AirflowFailException."""
        pass

    class DummyDAG:
        """Lightweight DAG stand-in for testing outside Airflow."""
        _current_dag = None

        def __init__(self, dag_id: str, default_args=None, schedule_interval=None, start_date=None, catchup=False, on_failure_callback=None, **kwargs):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule_interval = schedule_interval
            self.start_date = start_date
            self.catchup = catchup
            self.on_failure_callback = on_failure_callback
            self.tasks = []

        def __enter__(self):
            DummyDAG._current_dag = self
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            DummyDAG._current_dag = None

        def get_task(self, task_id: str):
            for t in self.tasks:
                if t.task_id == task_id:
                    return t
            raise KeyError(f"Task {task_id} not found in DAG {self.dag_id}")

    class DummyOperator:
        """Lightweight operator stand-in for testing outside Airflow."""
        def __init__(self, task_id: str, python_callable, op_kwargs=None, retries=0, retry_delay=None, execution_timeout=None, dag=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}
            self.retries = retries
            self.retry_delay = retry_delay
            self.execution_timeout = execution_timeout
            self.dag = dag or DummyDAG._current_dag
            self.upstream_list = []
            self.downstream_list = []
            if self.dag is not None and hasattr(self.dag, "tasks"):
                self.dag.tasks.append(self)

        def set_downstream(self, other):
            if other not in self.downstream_list:
                self.downstream_list.append(other)
            if self not in other.upstream_list:
                other.upstream_list.append(self)

        def __rshift__(self, other):
            self.set_downstream(other)
            return other

        def execute(self, context=None):
            return self.python_callable(**(self.op_kwargs or {}))

    PythonOperator = DummyOperator
    DAG = DummyDAG

# -----------------------------------------------------------------------------
# Reusable Task Execution Functions
# -----------------------------------------------------------------------------

REQUIRED_LANDING_FILES = ["products.csv", "inventory.csv", "sales.csv"]


def get_current_run_id(**kwargs) -> str:
    """Extracts or generates a stable run_id from execution context."""
    if "dag_run" in kwargs and kwargs["dag_run"] and hasattr(kwargs["dag_run"], "run_id"):
        return f"airflow-{kwargs['dag_run'].run_id}"
    elif "run_id" in kwargs and kwargs["run_id"]:
        return str(kwargs["run_id"])
    now_ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"batch-run-{now_ts}"


def task_check_input_files(**kwargs) -> Dict[str, Any]:
    """
    Task 1: check_input_files
    Validates presence, non-empty content, and readiness of all landing CSV files.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    init_db()
    PipelineTracker.start_run(run_id)

    landing_dir = Config.DATA_LANDING_DIR
    logger.info("[%s] Checking input files in %s", run_id, landing_dir)

    missing_files = []
    file_details = {}

    for fname in REQUIRED_LANDING_FILES:
        fpath = landing_dir / fname
        if not fpath.exists():
            missing_files.append(f"{fname} (file not found)")
        elif fpath.stat().st_size == 0:
            missing_files.append(f"{fname} (0 bytes - empty file)")
        else:
            file_details[fname] = {
                "size_bytes": fpath.stat().st_size,
                "modified": fpath.stat().st_mtime,
            }

    duration = time.time() - start_time

    if missing_files:
        error_msg = f"Landing directory missing required files: {', '.join(missing_files)}"
        logger.error("[%s] %s", run_id, error_msg)
        PipelineTracker.record_task(run_id, "check_input_files", "FAILED", duration, {"missing": missing_files})
        PipelineTracker.complete_run(run_id, status="FAILED", summary_details={"error": error_msg})
        raise AirflowFailException(error_msg)

    logger.info("[%s] All required landing files verified successfully: %s", run_id, list(file_details.keys()))
    PipelineTracker.record_task(run_id, "check_input_files", "SUCCESS", duration, file_details)
    return {"status": "success", "run_id": run_id, "files": file_details}


def task_load_staging(**kwargs) -> Dict[str, Any]:
    """
    Task 2: load_staging
    Ingests unvalidated landing CSVs into staging tables.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting load_staging", run_id)

    res = load_staging(run_id=run_id, landing_dir=Config.DATA_LANDING_DIR)
    duration = time.time() - start_time

    if res.get("status") == "failed":
        PipelineTracker.record_task(run_id, "load_staging", "FAILED", duration, res)
        raise AirflowFailException(f"load_staging failed: {res.get('message')}")

    PipelineTracker.record_task(run_id, "load_staging", "SUCCESS", duration, res)
    return res


def task_validate_and_clean(**kwargs) -> Dict[str, Any]:
    """
    Task 3: validate_and_clean
    Validates types, foreign keys, constraints, deduplicates, and logs rejected rows.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting validate_and_clean", run_id)

    res = validate_and_clean(run_id=run_id)
    duration = time.time() - start_time

    if res.get("status") == "failed":
        PipelineTracker.record_task(run_id, "validate_and_clean", "FAILED", duration, res)
        raise AirflowFailException(f"validate_and_clean failed: {res.get('message')}")

    PipelineTracker.record_task(run_id, "validate_and_clean", "SUCCESS", duration, res)
    return res


def task_export_clean_data(**kwargs) -> Dict[str, Any]:
    """
    Task 4: export_clean_data
    Exports trusted database tables into snappy-compressed Parquet files in data/processed/.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting export_clean_data", run_id)

    res = export_clean_data(run_id=run_id)
    duration = time.time() - start_time

    if res.get("status") == "failed":
        PipelineTracker.record_task(run_id, "export_clean_data", "FAILED", duration, res)
        raise AirflowFailException(f"export_clean_data failed: {res.get('message')}")

    PipelineTracker.record_task(run_id, "export_clean_data", "SUCCESS", duration, res)
    return res


def task_calculate_analytics(**kwargs) -> Dict[str, Any]:
    """
    Task 5: calculate_analytics
    Calculates revenue, units, category breakdowns, and low stock items using DuckDB/SQL.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting calculate_analytics", run_id)

    res = calculate_analytics(run_id=run_id)
    duration = time.time() - start_time

    if res.get("status") == "failed":
        PipelineTracker.record_task(run_id, "calculate_analytics", "FAILED", duration, res)
        raise AirflowFailException(f"calculate_analytics failed: {res.get('message')}")

    PipelineTracker.record_task(run_id, "calculate_analytics", "SUCCESS", duration, res)
    return res


def task_generate_reorder_recommendations(**kwargs) -> Dict[str, Any]:
    """
    Task 6: generate_reorder_recommendations
    Applies deterministic formula to suggest stock replenishment.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting generate_reorder_recommendations", run_id)

    res = generate_reorder_recommendations(run_id=run_id)
    duration = time.time() - start_time

    if res.get("status") == "failed":
        PipelineTracker.record_task(run_id, "generate_reorder_recommendations", "FAILED", duration, res)
        raise AirflowFailException(f"generate_reorder_recommendations failed: {res.get('message')}")

    PipelineTracker.record_task(run_id, "generate_reorder_recommendations", "SUCCESS", duration, res)
    return res


def task_sync_recommendations(**kwargs) -> Dict[str, Any]:
    """
    Task 7: sync_recommendations
    Calls mock downstream API and logs all outcomes in reverse_etl_audit.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting sync_recommendations", run_id)

    res = sync_recommendations(run_id=run_id)
    duration = time.time() - start_time

    if res.get("status") == "failed":
        PipelineTracker.record_task(run_id, "sync_recommendations", "FAILED", duration, res)
        raise AirflowFailException(f"sync_recommendations failed: {res.get('message')}")

    PipelineTracker.record_task(run_id, "sync_recommendations", "SUCCESS", duration, res)
    return res


def task_reconcile_pipeline(**kwargs) -> Dict[str, Any]:
    """
    Task 8: reconcile_pipeline
    Reconciles database records, verifies Parquet artifacts, finalizes pipeline_runs,
    and writes results/pipeline_run_summary.json.
    """
    start_time = time.time()
    run_id = get_current_run_id(**kwargs)
    logger.info("[%s] Starting reconcile_pipeline", run_id)

    reconciliation_details: Dict[str, Any] = {}

    with DatabaseManager.get_connection() as conn:
        from sqlalchemy import text
        sales_cnt = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar() or 0
        products_cnt = conn.execute(text("SELECT COUNT(*) FROM products")).scalar() or 0
        inv_cnt = conn.execute(text("SELECT COUNT(*) FROM inventory")).scalar() or 0
        rejects_cnt = conn.execute(text("SELECT COUNT(*) FROM rejected_records WHERE run_id = :r"), {"r": run_id}).scalar() or 0
        recs_cnt = conn.execute(text("SELECT COUNT(*) FROM reorder_recommendations WHERE run_id = :r"), {"r": run_id}).scalar() or 0
        audit_cnt = conn.execute(text("SELECT COUNT(*) FROM reverse_etl_audit WHERE run_id = :r"), {"r": run_id}).scalar() or 0

    reconciliation_details = {
        "sales_count": sales_cnt,
        "products_count": products_cnt,
        "inventory_count": inv_cnt,
        "rejections_logged": rejects_cnt,
        "recommendations_generated": recs_cnt,
        "reverse_etl_audits": audit_cnt,
        "parquet_exports": {
            "sales_clean.parquet": (Config.DATA_PROCESSED_DIR / "sales_clean.parquet").exists(),
            "products_clean.parquet": (Config.DATA_PROCESSED_DIR / "products_clean.parquet").exists(),
            "inventory_clean.parquet": (Config.DATA_PROCESSED_DIR / "inventory_clean.parquet").exists(),
        }
    }

    duration = time.time() - start_time
    PipelineTracker.record_task(run_id, "reconcile_pipeline", "SUCCESS", duration, reconciliation_details)

    # Finalize pipeline run status to SUCCESS and generate pipeline_run_summary.json
    summary = PipelineTracker.complete_run(run_id, status="SUCCESS", summary_details=reconciliation_details)
    logger.info("[%s] Pipeline run successfully reconciled and finalized", run_id)
    return summary


def dag_failure_callback(context):
    """Callback triggered if any Airflow task fails."""
    run_id = get_current_run_id(**context)
    task_id = context.get("task_instance").task_id if "task_instance" in context else "unknown"
    error = str(context.get("exception", "Unknown exception"))
    logger.error("DAG failure callback: task %s failed in run %s with %s", task_id, run_id, error)
    PipelineTracker.complete_run(run_id, status="FAILED", summary_details={"failed_task": task_id, "error": error})


# -----------------------------------------------------------------------------
# DAG Definition & Dependency Graph
# -----------------------------------------------------------------------------

default_args = {
    "owner": "person1_orchestration",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="retail_batch_pipeline",
    default_args=default_args,
    description="End-to-end retail batch ingestion, quality validation, analytics, recommendations and reconciliation",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_failure_callback=dag_failure_callback,
    tags=["retail", "inventory", "sales", "batch", "person1"],
) as dag:

    # 1. Check landing files
    t1_check_input_files = PythonOperator(
        task_id="check_input_files",
        python_callable=task_check_input_files,
        provide_context=True,
    )

    # 2. Load unvalidated data to staging
    t2_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=task_load_staging,
        provide_context=True,
    )

    # 3. Validate, deduplicate, log rejects, and upsert trusted tables
    t3_validate_and_clean = PythonOperator(
        task_id="validate_and_clean",
        python_callable=task_validate_and_clean,
        provide_context=True,
    )

    # 4. Export trusted tables to Parquet
    t4_export_clean_data = PythonOperator(
        task_id="export_clean_data",
        python_callable=task_export_clean_data,
        provide_context=True,
    )

    # 5. Calculate analytics via DuckDB/SQL
    t5_calculate_analytics = PythonOperator(
        task_id="calculate_analytics",
        python_callable=task_calculate_analytics,
        provide_context=True,
    )

    # 6. Generate deterministic reorder recommendations
    t6_generate_reorder_recommendations = PythonOperator(
        task_id="generate_reorder_recommendations",
        python_callable=task_generate_reorder_recommendations,
        provide_context=True,
    )

    # 7. Reverse-ETL sync to downstream API with audit logging
    t7_sync_recommendations = PythonOperator(
        task_id="sync_recommendations",
        python_callable=task_sync_recommendations,
        provide_context=True,
    )

    # 8. Reconcile database state, verify artifacts, finalize summary
    t8_reconcile_pipeline = PythonOperator(
        task_id="reconcile_pipeline",
        python_callable=task_reconcile_pipeline,
        provide_context=True,
    )

    # Enforce strict linear dependency chain
    (
        t1_check_input_files
        >> t2_load_staging
        >> t3_validate_and_clean
        >> t4_export_clean_data
        >> t5_calculate_analytics
        >> t6_generate_reorder_recommendations
        >> t7_sync_recommendations
        >> t8_reconcile_pipeline
    )
