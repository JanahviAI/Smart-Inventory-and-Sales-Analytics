"""
Automated Orchestration Test Suite (Person 1).
Tests Airflow DAG integrity, linear dependencies, retry/timeout policies,
AT-01 (Clean Batch Execution), AT-02 (Missing Input Blocking), and Pipeline Tracker.
"""

import json
import shutil
import pytest
from pathlib import Path
from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager, init_db
from src.common.pipeline_tracker import PipelineTracker
from dags.retail_batch_pipeline import (
    dag,
    task_check_input_files,
    task_load_staging,
    task_validate_and_clean,
    task_export_clean_data,
    task_calculate_analytics,
    task_generate_reorder_recommendations,
    task_sync_recommendations,
    task_reconcile_pipeline,
    AirflowFailException,
    REQUIRED_LANDING_FILES,
)
from scripts.simulate_nifi_ingest import route_files_atomically, clean_landing, verify_landing_readiness


@pytest.fixture(autouse=True)
def setup_database():
    """Ensure database schema is initialized before every test."""
    init_db()


class TestDagIntegrity:
    """Verifies Airflow DAG structure, task graph, dependencies and configuration."""

    def test_dag_exists_and_id_matches(self):
        """Verify DAG is loaded and has canonical ID 'retail_batch_pipeline'."""
        assert dag is not None
        assert dag.dag_id == "retail_batch_pipeline"

    def test_all_eight_tasks_exist(self):
        """Verify that all 8 required tasks exist in the DAG."""
        expected_task_ids = [
            "check_input_files",
            "load_staging",
            "validate_and_clean",
            "export_clean_data",
            "calculate_analytics",
            "generate_reorder_recommendations",
            "sync_recommendations",
            "reconcile_pipeline",
        ]
        dag_task_ids = [t.task_id for t in dag.tasks]

        for expected_id in expected_task_ids:
            assert expected_id in dag_task_ids, f"Task {expected_id} missing from DAG"
        assert len(dag.tasks) == 8, f"Expected exactly 8 tasks, found {len(dag.tasks)}"

    def test_linear_task_dependencies(self):
        """Verify the strict linear upstream/downstream dependency chain."""
        expected_sequence = [
            "check_input_files",
            "load_staging",
            "validate_and_clean",
            "export_clean_data",
            "calculate_analytics",
            "generate_reorder_recommendations",
            "sync_recommendations",
            "reconcile_pipeline",
        ]

        for idx in range(len(expected_sequence) - 1):
            curr_id = expected_sequence[idx]
            next_id = expected_sequence[idx + 1]

            curr_task = dag.get_task(curr_id)
            next_task = dag.get_task(next_id)

            downstream_ids = [t.task_id for t in curr_task.downstream_list]
            upstream_ids = [t.task_id for t in next_task.upstream_list]

            assert next_id in downstream_ids, f"{next_id} should be downstream of {curr_id}"
            assert curr_id in upstream_ids, f"{curr_id} should be upstream of {next_id}"

    def test_retry_and_timeout_policies(self):
        """Verify DAG default_args specify safe retries, retry delay and timeouts."""
        assert dag.default_args.get("retries", 0) >= 1
        assert dag.default_args.get("retry_delay") is not None
        assert dag.default_args.get("execution_timeout") is not None


class TestNiFiHandoff:
    """Tests NiFi landing simulation and atomic routing."""

    def test_atomic_routing_and_landing(self):
        """Test routing files from source to landing directory atomically."""
        clean_landing(Config.DATA_LANDING_DIR)
        landed = route_files_atomically(Config.DATA_SOURCE_DIR, Config.DATA_LANDING_DIR)

        assert len(landed) == 3
        assert verify_landing_readiness(Config.DATA_LANDING_DIR) is True

        for fname in REQUIRED_LANDING_FILES:
            target = Config.DATA_LANDING_DIR / fname
            assert target.exists()
            assert target.stat().st_size > 0


class TestAcceptanceScenarios:
    """Implements Acceptance Tests AT-01 and AT-02."""

    def test_at_01_clean_batch_execution(self):
        """
        AT-01: Clean batch execution.
        When landing files exist, pipeline executes all 8 tasks, updates pipeline_runs,
        exports Parquet, calculates analytics, generates recommendations, audits reverse-ETL,
        and saves results/pipeline_run_summary.json.
        """
        run_id = "test-at01-clean"

        # 1. Ensure landing files are in place
        route_files_atomically(Config.DATA_SOURCE_DIR, Config.DATA_LANDING_DIR)

        # 2. Execute all 8 tasks sequentially
        context = {"run_id": run_id}

        t1 = task_check_input_files(**context)
        assert t1["status"] == "success"

        t2 = task_load_staging(**context)
        assert t2["status"] == "success"
        assert t2["processed"] > 0

        t3 = task_validate_and_clean(**context)
        assert t3["status"] == "success"
        assert t3["processed"] > 0

        t4 = task_export_clean_data(**context)
        assert t4["status"] == "success"
        assert len(t4["details"]) == 3

        t5 = task_calculate_analytics(**context)
        assert t5["status"] == "success"

        t6 = task_generate_reorder_recommendations(**context)
        assert t6["status"] == "success"

        t7 = task_sync_recommendations(**context)
        assert t7["status"] in ("success", "partial")

        t8 = task_reconcile_pipeline(**context)
        assert t8["status"] == "SUCCESS"

        # 3. Verify pipeline_runs table
        run_record = PipelineTracker.get_run(run_id)
        assert run_record is not None
        assert run_record["status"] == "SUCCESS"
        assert len(run_record["tasks_completed"]) == 8

        # 4. Verify results/pipeline_run_summary.json
        summary_path = Config.RESULTS_DIR / "pipeline_run_summary.json"
        assert summary_path.exists()
        with open(summary_path, "r") as f:
            summary = json.load(f)
        assert summary["run_id"] == run_id
        assert summary["status"] == "SUCCESS"

    def test_at_02_missing_input_blocks_pipeline(self):
        """
        AT-02: Missing input files.
        When landing directory is missing files, check_input_files raises AirflowFailException,
        records status 'FAILED' in pipeline_runs, and blocks subsequent tasks.
        """
        run_id = "test-at02-missing"

        # Clean landing directory
        clean_landing(Config.DATA_LANDING_DIR)

        # check_input_files must raise AirflowFailException
        with pytest.raises(AirflowFailException) as exc_info:
            task_check_input_files(run_id=run_id)

        assert "missing required files" in str(exc_info.value).lower()

        # Verify pipeline_runs record is marked FAILED
        run_record = PipelineTracker.get_run(run_id)
        assert run_record is not None
        assert run_record["status"] == "FAILED"


class TestPipelineTracker:
    """Verifies pipeline lifecycle tracking and state persistence."""

    def test_tracker_lifecycle(self):
        run_id = "test-tracker-lifecycle"
        PipelineTracker.start_run(run_id)

        rec = PipelineTracker.get_run(run_id)
        assert rec["status"] == "RUNNING"
        assert len(rec["tasks_completed"]) == 0

        # Record milestones
        PipelineTracker.record_task(run_id, "mock_task_1", "SUCCESS", 0.05, {"rows": 42})
        PipelineTracker.record_task(run_id, "mock_task_2", "SUCCESS", 0.10, {"status": "ok"})

        rec2 = PipelineTracker.get_run(run_id)
        assert len(rec2["tasks_completed"]) == 2
        assert rec2["tasks_completed"][0]["task_name"] == "mock_task_1"

        # Complete run
        final = PipelineTracker.complete_run(run_id, status="SUCCESS", summary_details={"total": 42})
        assert final["status"] == "SUCCESS"

        rec3 = PipelineTracker.get_run(run_id)
        assert rec3["status"] == "SUCCESS"
