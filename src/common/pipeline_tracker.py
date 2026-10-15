"""
Pipeline Run Tracker Module.
Coordinates updates to the canonical `pipeline_runs` table in PostgreSQL / Database,
records task milestones, and generates the required `results/pipeline_run_summary.json` artifact.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from src.common.config import Config
from src.common.db import DatabaseManager

logger = logging.getLogger(__name__)


class PipelineTracker:
    """Manages lifecycle states and audit trails for retail batch pipeline executions."""

    @staticmethod
    def now_iso() -> str:
        """Returns current UTC timestamp in ISO 8601 format."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def start_run(cls, run_id: str) -> Dict[str, Any]:
        """
        Registers a new pipeline run in the pipeline_runs table with status 'RUNNING'.
        """
        started_at = cls.now_iso()
        logger.info("Initializing pipeline run [%s] at %s", run_id, started_at)

        with DatabaseManager.get_connection() as conn:
            # Check if exists
            existing = conn.execute(
                text("SELECT run_id FROM pipeline_runs WHERE run_id = :r"),
                {"r": run_id}
            ).fetchone()

            if existing:
                conn.execute(
                    text("""
                        UPDATE pipeline_runs
                        SET status = 'RUNNING',
                            started_at = CURRENT_TIMESTAMP,
                            ended_at = NULL,
                            tasks_completed = :tasks,
                            summary = :summary
                        WHERE run_id = :r
                    """),
                    {
                        "r": run_id,
                        "tasks": json.dumps([]),
                        "summary": json.dumps({"started_at": started_at}),
                    }
                )
            else:
                conn.execute(
                    text("""
                        INSERT INTO pipeline_runs (run_id, status, tasks_completed, summary)
                        VALUES (:r, 'RUNNING', :tasks, :summary)
                    """),
                    {
                        "r": run_id,
                        "tasks": json.dumps([]),
                        "summary": json.dumps({"started_at": started_at}),
                    }
                )

        return {"run_id": run_id, "status": "RUNNING", "started_at": started_at}

    @classmethod
    def record_task(
        cls,
        run_id: str,
        task_name: str,
        status: str,
        duration_seconds: float,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Appends a completed task milestone to the run's tasks_completed array.
        """
        task_entry = {
            "task_name": task_name,
            "status": status,
            "duration_seconds": round(duration_seconds, 3),
            "completed_at": cls.now_iso(),
            "details": details or {},
        }

        with DatabaseManager.get_connection() as conn:
            row = conn.execute(
                text("SELECT tasks_completed FROM pipeline_runs WHERE run_id = :r"),
                {"r": run_id}
            ).fetchone()

            current_tasks: List[Dict[str, Any]] = []
            if row and row[0]:
                raw_tasks = row[0]
                if isinstance(raw_tasks, str):
                    try:
                        current_tasks = json.loads(raw_tasks)
                    except Exception:
                        current_tasks = []
                elif isinstance(raw_tasks, list):
                    current_tasks = list(raw_tasks)

            # Update or append task
            updated = False
            for idx, t in enumerate(current_tasks):
                if t.get("task_name") == task_name:
                    current_tasks[idx] = task_entry
                    updated = True
                    break
            if not updated:
                current_tasks.append(task_entry)

            conn.execute(
                text("""
                    UPDATE pipeline_runs
                    SET tasks_completed = :tasks
                    WHERE run_id = :r
                """),
                {"r": run_id, "tasks": json.dumps(current_tasks)}
            )

        logger.info("Recorded task [%s] for run [%s]: status=%s", task_name, run_id, status)
        return task_entry

    @classmethod
    def complete_run(
        cls,
        run_id: str,
        status: str = "SUCCESS",
        summary_details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Finalizes the pipeline run status ('SUCCESS', 'FAILED', or 'PARTIAL'),
        updates ended_at, and writes the canonical results/pipeline_run_summary.json artifact.
        """
        ended_at = cls.now_iso()
        summary_payload = summary_details or {}
        summary_payload["ended_at"] = ended_at
        summary_payload["final_status"] = status

        with DatabaseManager.get_connection() as conn:
            # Retrieve completed tasks
            row = conn.execute(
                text("SELECT started_at, tasks_completed FROM pipeline_runs WHERE run_id = :r"),
                {"r": run_id}
            ).fetchone()

            tasks_list = []
            started_at_str = None
            if row:
                started_at_str = str(row[0]) if row[0] else ended_at
                raw_tasks = row[1]
                if isinstance(raw_tasks, str):
                    try:
                        tasks_list = json.loads(raw_tasks)
                    except Exception:
                        tasks_list = []
                elif isinstance(raw_tasks, list):
                    tasks_list = list(raw_tasks)

            conn.execute(
                text("""
                    UPDATE pipeline_runs
                    SET status = :status,
                        ended_at = CURRENT_TIMESTAMP,
                        summary = :summary
                    WHERE run_id = :r
                """),
                {
                    "r": run_id,
                    "status": status,
                    "summary": json.dumps(summary_payload),
                }
            )

        # Build full summary artifact
        full_summary = {
            "run_id": run_id,
            "status": status,
            "started_at": started_at_str,
            "ended_at": ended_at,
            "tasks_count": len(tasks_list),
            "tasks_completed": tasks_list,
            "summary": summary_payload,
        }

        # Save to results/pipeline_run_summary.json
        summary_file = Config.RESULTS_DIR / "pipeline_run_summary.json"
        Config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(full_summary, f, indent=2)

        logger.info("Saved pipeline run summary to %s", summary_file)
        return full_summary

    @classmethod
    def get_run(cls, run_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a pipeline run by run_id."""
        with DatabaseManager.get_connection() as conn:
            row = conn.execute(
                text("""
                    SELECT run_id, status, started_at, ended_at, tasks_completed, summary
                    FROM pipeline_runs
                    WHERE run_id = :r
                """),
                {"r": run_id}
            ).fetchone()

            if not row:
                return None

            tasks = row[4]
            if isinstance(tasks, str):
                try:
                    tasks = json.loads(tasks)
                except Exception:
                    tasks = []

            summary = row[5]
            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    summary = {}

            return {
                "run_id": row[0],
                "status": row[1],
                "started_at": str(row[2]) if row[2] else None,
                "ended_at": str(row[3]) if row[3] else None,
                "tasks_completed": tasks or [],
                "summary": summary or {},
            }
