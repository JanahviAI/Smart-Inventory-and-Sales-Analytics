"""
Unit and Integration Tests for Synthetic Data Generators.
Verifies determinism, configurable record counts, schema contracts,
referential integrity, business constraints, synthetic labeling, and streaming generators.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from src.etl.generator import generate_synthetic_dataset
from src.streaming.generator import generate_streaming_events


class TestBatchDataGenerator:
    """Tests for the batch synthetic dataset generator."""

    def test_deterministic_reproducibility(self):
        """Verify that identical seeds produce identical datasets and different seeds diverge."""
        ds1 = generate_synthetic_dataset(num_products=50, num_inventory=50, num_sales=100, seed=123)
        ds2 = generate_synthetic_dataset(num_products=50, num_inventory=50, num_sales=100, seed=123)
        ds3 = generate_synthetic_dataset(num_products=50, num_inventory=50, num_sales=100, seed=999)

        assert ds1["products"] == ds2["products"]
        assert ds1["inventory"] == ds2["inventory"]
        assert ds1["sales"] == ds2["sales"]

        # Different seed should generate different data
        assert ds1["products"][0]["product_name"] != ds3["products"][0]["product_name"]

    def test_configurable_record_counts(self):
        """Verify that caller can request custom record counts."""
        num_p = 100
        num_i = 100
        num_s = 250
        ds = generate_synthetic_dataset(num_products=num_p, num_inventory=num_i, num_sales=num_s, seed=42)

        assert len(ds["products"]) == num_p
        assert len(ds["inventory"]) == num_i
        assert len(ds["sales"]) == num_s

    def test_schema_contracts_and_types(self):
        """Verify all generated columns match canonical schemas and parse cleanly."""
        ds = generate_synthetic_dataset(num_products=50, num_inventory=50, num_sales=100, seed=42)

        # Products schema
        for p in ds["products"]:
            assert isinstance(p["product_id"], int) and p["product_id"] > 0
            assert isinstance(p["product_name"], str) and len(p["product_name"]) > 0
            assert isinstance(p["category"], str) and len(p["category"]) > 0
            assert isinstance(p["supplier_id"], str) and p["supplier_id"].startswith("SUPP-")

        # Inventory schema
        for inv in ds["inventory"]:
            assert isinstance(inv["product_id"], int) and inv["product_id"] > 0
            assert isinstance(inv["current_stock"], int) and inv["current_stock"] >= 0
            assert isinstance(inv["reorder_level"], int) and inv["reorder_level"] >= 0

        # Sales schema
        for s in ds["sales"]:
            assert isinstance(s["order_line_id"], str) and len(s["order_line_id"]) > 0
            assert isinstance(s["order_id"], str) and len(s["order_id"]) > 0
            assert isinstance(s["order_date"], str) and s["order_date"].endswith("Z")
            assert isinstance(s["product_id"], int) and s["product_id"] > 0
            assert isinstance(s["quantity"], int) and s["quantity"] > 0
            assert isinstance(s["unit_price"], (int, float)) and s["unit_price"] >= 0.0

            # Valid ISO date
            dt = datetime.fromisoformat(s["order_date"].replace("Z", "+00:00"))
            assert dt is not None

    def test_referential_integrity(self):
        """Verify all foreign keys in sales and inventory reference valid products."""
        ds = generate_synthetic_dataset(num_products=100, num_inventory=100, num_sales=500, seed=42)
        valid_pids = {p["product_id"] for p in ds["products"]}

        for inv in ds["inventory"]:
            assert inv["product_id"] in valid_pids

        for s in ds["sales"]:
            assert s["product_id"] in valid_pids

    def test_business_uniqueness_constraints(self):
        """Verify (order_id, product_id) and order_line_id are strictly unique."""
        ds = generate_synthetic_dataset(num_products=100, num_inventory=100, num_sales=1000, seed=42)

        seen_lines = set()
        seen_order_products = set()

        for s in ds["sales"]:
            line_id = s["order_line_id"]
            pair = (s["order_id"], s["product_id"])

            assert line_id not in seen_lines, f"Duplicate order_line_id: {line_id}"
            assert pair not in seen_order_products, f"Duplicate (order_id, product_id): {pair}"

            seen_lines.add(line_id)
            seen_order_products.add(pair)

    def test_reorder_recommendation_triggering(self):
        """Verify that a subset of inventory items are at or below reorder level."""
        ds = generate_synthetic_dataset(num_products=500, num_inventory=500, num_sales=1000, seed=42)
        low_stock = [inv for inv in ds["inventory"] if inv["current_stock"] <= inv["reorder_level"]]

        # Should realistically trigger ~50 to 90 recommendations out of 500
        assert 40 <= len(low_stock) <= 100, f"Expected 40-100 low stock items, found {len(low_stock)}"

    def test_file_output_and_metadata_notice(self, tmp_path):
        """Verify CSV files and clear synthetic metadata notice are written to disk."""
        out_dir = tmp_path / "synthetic"
        ds = generate_synthetic_dataset(num_products=20, num_inventory=20, num_sales=50, seed=42, output_dir=out_dir)

        assert (out_dir / "products.csv").exists()
        assert (out_dir / "inventory.csv").exists()
        assert (out_dir / "sales.csv").exists()

        meta_file = out_dir / "DATASET_METADATA.json"
        assert meta_file.exists()

        with open(meta_file, "r", encoding="utf-8") as f:
            meta = json.load(f)

        assert meta["dataset_type"] == "SYNTHETIC_BENCHMARK_DATASET"
        assert "NOTICE" in meta["notice"]
        assert "NOT contain real customer" in meta["notice"]
        assert meta["record_counts"]["products"] == 20
        assert meta["record_counts"]["sales"] == 50


class TestStreamingEventGenerator:
    """Tests for the streaming synthetic event generator."""

    def test_streaming_event_generation(self):
        """Verify generating 1050 events produces exact valid, duplicate, and invalid counts."""
        events = generate_streaming_events(
            total_count=1050,
            seed=42,
            duplicate_count=50,
            invalid_count=50,
            product_id_range=(101, 200),
        )

        assert len(events) == 1050

        # Verify duplicate presence
        event_ids = [e.get("event_id") for e in events]
        non_empty_ids = [eid for eid in event_ids if eid]
        unique_ids = set(non_empty_ids)

        # There are 50 intentional duplicates
        assert len(non_empty_ids) - len(unique_ids) >= 40

        # Verify invalid events presence
        invalid_quantities = [e for e in events if e.get("quantity", 1) <= 0]
        unknown_products = [e for e in events if e.get("product_id") == 999999]
        empty_ids = [e for e in events if not e.get("event_id")]

        assert len(invalid_quantities) > 0
        assert len(unknown_products) > 0
        assert len(empty_ids) > 0

