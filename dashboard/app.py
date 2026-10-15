"""Operational dashboard for batch, quality, analytics, streaming, and sync data."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import streamlit as st

from src.common.config import Config
from src.common.db import DatabaseManager


st.set_page_config(page_title="Smart Inventory Analytics", page_icon="📦", layout="wide")


@st.cache_data(ttl=15, show_spinner=False)
def query(sql: str) -> list[dict[str, Any]]:
    return DatabaseManager.fetch_all(sql)


def frame(rows: list[dict[str, Any]], empty_message: str) -> None:
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info(empty_message)


def safe_query(sql: str, label: str) -> list[dict[str, Any]]:
    try:
        return query(sql)
    except Exception as exc:
        st.warning(f"{label} is unavailable: {exc}")
        return []


def show_parquet_analytics() -> None:
    import duckdb

    sales_path = Config.DATA_PROCESSED_DIR / "sales_clean.parquet"
    products_path = Config.DATA_PROCESSED_DIR / "products_clean.parquet"
    if not sales_path.exists() or not products_path.exists():
        st.info("Analytics will appear after the ETL export creates the clean sales and products Parquet files.")
        return

    # Use DuckDB's parameter binding for file paths; Parquet content is never hardcoded.
    con = duckdb.connect(database=":memory:")
    try:
        sales = str(sales_path.resolve())
        products = str(products_path.resolve())
        daily = con.execute(
            "SELECT CAST(order_date AS DATE) AS order_date, SUM(quantity) AS units, "
            "ROUND(SUM(total_price), 2) AS revenue "
            "FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
            [sales],
        ).fetchdf()
        category = con.execute(
            "SELECT p.category, SUM(s.quantity) AS units, ROUND(SUM(s.total_price), 2) AS revenue "
            "FROM read_parquet(?) s JOIN read_parquet(?) p USING (product_id) "
            "GROUP BY p.category ORDER BY revenue DESC",
            [sales, products],
        ).fetchdf()
        st.subheader("Daily sales")
        if not daily.empty:
            st.line_chart(daily.set_index("order_date")["revenue"])
        frame(daily.to_dict("records"), "No daily sales rows are available.")
        st.subheader("Category sales")
        frame(category.to_dict("records"), "No category sales rows are available.")
    finally:
        con.close()


def show_results_json(name: str) -> None:
    path = Config.RESULTS_DIR / name
    if not path.exists():
        st.info(f"`results/{name}` will appear after its pipeline step runs.")
        return
    try:
        st.json(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        st.warning(f"Could not read `results/{name}`: {exc}")


st.title("Smart Inventory & Sales Analytics")
st.caption("Live views of stored pipeline, quality, inventory, streaming, and reverse-ETL data.")

try:
    backend = DatabaseManager.get_engine().dialect.name
    st.caption(f"Database backend: **{backend}** · auto-refresh on page reload")
except Exception as exc:
    st.error(f"Database connection is unavailable: {exc}")
    st.stop()

overview, pipeline, quality, analytics, streaming, reverse_etl, benchmark = st.tabs([
    "Overview", "Pipeline", "Data Quality", "Analytics", "Streaming", "Reverse ETL", "Benchmark"
])

with overview:
    runs = safe_query("SELECT run_id, status, started_at, ended_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 1", "Pipeline status")
    sales = safe_query("SELECT COUNT(*) AS line_items, COALESCE(SUM(quantity), 0) AS units, "
                       "COALESCE(ROUND(SUM(total_price), 2), 0) AS revenue FROM sales", "Sales totals")
    low_stock = safe_query("SELECT i.product_id, p.product_name, i.current_stock, i.reorder_level "
                           "FROM inventory i JOIN products p USING (product_id) "
                           "WHERE i.current_stock <= i.reorder_level ORDER BY i.current_stock", "Low-stock products")
    c1, c2, c3 = st.columns(3)
    if runs:
        c1.metric("Latest pipeline run", runs[0]["status"], help=str(runs[0]["run_id"]))
    else:
        c1.metric("Latest pipeline run", "No runs yet")
    if sales:
        c2.metric("Sales units", sales[0]["units"])
        c3.metric("Revenue (USD)", f"${sales[0]['revenue']}")
    else:
        c2.info("Sales data is not available yet.")
    st.subheader("Products at or below reorder level")
    frame(low_stock, "No low-stock products are currently recorded.")

with pipeline:
    frame(safe_query("SELECT run_id, status, started_at, ended_at, tasks_completed, summary "
                     "FROM pipeline_runs ORDER BY started_at DESC LIMIT 100", "Pipeline runs"),
          "No pipeline runs have been recorded yet.")
    st.subheader("Latest pipeline output")
    show_results_json("pipeline_run_summary.json")

with quality:
    q1, q2 = st.columns(2)
    with q1:
        st.subheader("Rejected records")
        frame(safe_query("SELECT run_id, source_table, record_identifier, reason, rejected_at "
                         "FROM rejected_records ORDER BY rejected_at DESC LIMIT 100", "Rejected records"),
              "No rejected records are available.")
    with q2:
        st.subheader("Streaming event outcomes")
        frame(safe_query("SELECT status, COUNT(*) AS events FROM inventory_events GROUP BY status ORDER BY status", "Event quality"),
              "No streaming events are available.")
    st.subheader("Batch quality report")
    show_results_json("data_quality_report.json")

with analytics:
    st.caption("Computed from the clean Parquet exports using DuckDB.")
    try:
        show_parquet_analytics()
    except Exception as exc:
        st.error(f"Analytics data is unavailable: {exc}")

with streaming:
    st.subheader("Recent inventory events")
    frame(safe_query("SELECT event_id, event_time, order_id, product_id, quantity, status, "
                     "rejection_reason, processed_at, latency_ms "
                     "FROM inventory_events ORDER BY processed_at DESC LIMIT 200", "Streaming events"),
          "No streaming events have been persisted yet.")
    st.subheader("Publish-to-persistence latency")
    latency = safe_query("SELECT COUNT(latency_ms) AS measured_events, "
                         "ROUND(AVG(latency_ms), 2) AS average_ms, "
                         "ROUND(MAX(latency_ms), 2) AS max_ms FROM inventory_events WHERE latency_ms IS NOT NULL",
                         "Streaming latency")
    if latency and latency[0]["measured_events"]:
        cols = st.columns(3)
        cols[0].metric("Measured events", latency[0]["measured_events"])
        cols[1].metric("Average latency", f"{latency[0]['average_ms']} ms")
        cols[2].metric("Maximum latency", f"{latency[0]['max_ms']} ms")
    else:
        st.info("Latency appears after consumed messages with broker timestamps are persisted.")
    st.subheader("Streaming run evidence")
    show_results_json("streaming_results.json")

with reverse_etl:
    st.subheader("Reorder recommendations")
    frame(safe_query("SELECT recommendation_key, run_id, product_id, current_stock, reorder_level, "
                     "recommended_quantity, reason, created_at "
                     "FROM reorder_recommendations ORDER BY created_at DESC LIMIT 100", "Recommendations"),
          "No recommendations are available yet.")
    st.subheader("Reverse-ETL audit")
    frame(safe_query("SELECT run_id, recommendation_key, endpoint, http_status, status, synced_at "
                     "FROM reverse_etl_audit ORDER BY synced_at DESC LIMIT 100", "Reverse-ETL audit"),
          "No reverse-ETL attempts are available yet.")

with benchmark:
    st.caption("Benchmark figures are shown only when generated by a real benchmark run.")
    show_results_json("benchmark_results.json")
    csv_path = Config.RESULTS_DIR / "benchmark_results.csv"
    if csv_path.exists():
        try:
            st.dataframe(pd.read_csv(csv_path), use_container_width=True, hide_index=True)
        except Exception as exc:
            st.warning(f"Could not read benchmark CSV: {exc}")
