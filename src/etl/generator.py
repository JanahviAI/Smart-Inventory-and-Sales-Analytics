"""
Synthetic Retail Dataset Generator.
Generates reproducible, realistic product, inventory, and sales datasets
for data engineering benchmarking and pipeline validation.

Features:
- Fully deterministic with configurable random seed
- Clear synthetic labelling (never presented as real customer/business data)
- Preserves all canonical schema requirements and business rules
- Realistic category structures, price distributions, and sales patterns
- Configurable record counts (default: 500 products, 500 inventory, 10,000 sales)
"""

from __future__ import annotations

import csv
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

# Realistic product naming components by category
CATEGORY_TEMPLATES = {
    "Electronics": {
        "adjectives": ["Ultra", "Pro", "Precision", "Wireless", "Smart", "Compact", "4K UHD", "High-Speed"],
        "nouns": ["Monitor", "Docking Station", "Webcam", "USB-C Hub", "Presenter Remote", "Capture Card", "Power Bank", "Graphics Tablet"],
        "price_range": (29.99, 499.99),
    },
    "Computer Peripherals": {
        "adjectives": ["Mechanical", "Silent", "Ergonomic", "RGB Backlit", "Low-Profile", "Bluetooth", "Multi-Device"],
        "nouns": ["Keyboard", "Mouse", "Trackball", "Number Pad", "Desk Mat", "Wrist Rest", "Switch Set"],
        "price_range": (19.99, 189.99),
    },
    "Furniture": {
        "adjectives": ["Ergonomic", "Executive", "Adjustable", "Pneumatic", "Mesh", "Solid Wood", "Dual-Motor"],
        "nouns": ["Standing Desk", "Task Chair", "Monitor Arm", "Footrest", "Filing Cabinet", "Cable Management Tray", "Desk Converter"],
        "price_range": (49.99, 799.00),
    },
    "Audio": {
        "adjectives": ["Noise-Cancelling", "Studio Reference", "Wireless", "Hi-Fi", "Omnidirectional", "Cardioid"],
        "nouns": ["Headphones", "Earbuds", "Desk Microphone", "Soundbar", "Audio Interface", "Boom Arm", "Pop Filter"],
        "price_range": (24.99, 349.99),
    },
    "Smart Lighting": {
        "adjectives": ["Dimmable", "Adaptive", "Color-Changing", "Eye-Care", "Smart Sensor", "Architectural"],
        "nouns": ["Desk Lamp", "Monitor Light Bar", "LED Strip Light", "Under-Cabinet Bar", "Floor Lamp", "Ring Light"],
        "price_range": (14.99, 129.99),
    },
    "Networking": {
        "adjectives": ["Gigabit", "Wi-Fi 6E", "Managed", "Mesh", "PoE+", "High-Gain", "Dual-Band"],
        "nouns": ["Router", "Network Switch", "Ethernet Adapter", "Range Extender", "Access Point", "Patch Cable Reel"],
        "price_range": (18.99, 299.99),
    },
    "Storage & Accessories": {
        "adjectives": ["High-Speed", "Rugged", "Portable", "Aluminum", "Braided", "Modular"],
        "nouns": ["NVMe SSD Enclosure", "External Hard Drive", "Cable Organizer", "Laptop Stand", "Screen Protector", "Travel Case"],
        "price_range": (9.99, 149.99),
    },
    "Office Supplies": {
        "adjectives": ["Heavy-Duty", "Magnetic", "Dry-Erase", "Recycled", "Desktop", "Precision"],
        "nouns": ["Whiteboard", "Paper Shredder", "Document Scanner", "Label Maker", "Stapler", "Desk Organizer"],
        "price_range": (12.50, 199.50),
    },
}


def generate_synthetic_dataset(
    num_products: int = 500,
    num_inventory: int = 500,
    num_sales: int = 10000,
    seed: int = 42,
    output_dir: Path | None = None,
) -> Dict[str, Any]:
    """
    Deterministically generates synthetic products, inventory, and sales datasets.

    Args:
        num_products: Number of products to generate (default: 500).
        num_inventory: Number of inventory rows to generate (default: 500).
        num_sales: Approximate number of sales rows to generate (default: 10,000).
        seed: Random seed for 100% deterministic reproducibility.
        output_dir: Destination directory for CSV files.

    Returns:
        Dictionary containing generated data structures and metadata.
    """
    rng = random.Random(seed)
    num_inventory = min(num_inventory, num_products)

    categories = list(CATEGORY_TEMPLATES.keys())
    suppliers = [f"SUPP-{i:03d}" for i in range(1, 31)]

    # -------------------------------------------------------------------------
    # 1. PRODUCTS GENERATION
    # -------------------------------------------------------------------------
    products: List[Dict[str, Any]] = []
    product_price_map: Dict[int, float] = {}

    start_pid = 101
    used_names = set()

    for i in range(num_products):
        pid = start_pid + i
        cat = rng.choice(categories)
        tmpl = CATEGORY_TEMPLATES[cat]

        # Generate unique product name
        adj = rng.choice(tmpl["adjectives"])
        noun = rng.choice(tmpl["nouns"])
        code = f"{pid}"
        pname = f"{adj} {noun} {code}"
        while pname in used_names:
            code = f"{pid}-{rng.randint(10, 99)}"
            pname = f"{adj} {noun} {code}"
        used_names.add(pname)

        supp = rng.choice(suppliers)
        min_p, max_p = tmpl["price_range"]
        base_price = round(rng.uniform(min_p, max_p), 2)

        products.append({
            "product_id": pid,
            "product_name": pname,
            "category": cat,
            "supplier_id": supp,
        })
        product_price_map[pid] = base_price

    # -------------------------------------------------------------------------
    # 2. INVENTORY GENERATION
    # -------------------------------------------------------------------------
    # Rule: ~12-15% of products have current_stock <= reorder_level
    # to realistically trigger reorder recommendations downstream.
    inventory: List[Dict[str, Any]] = []
    reorder_levels_pool = [10, 15, 20, 25, 30, 40, 50]

    for i in range(num_inventory):
        pid = products[i]["product_id"]
        reorder_lvl = rng.choice(reorder_levels_pool)

        # 14% chance of being at or below reorder level (critical stock)
        is_low_stock = rng.random() < 0.14
        if is_low_stock:
            current_stock = rng.randint(0, reorder_lvl)
        else:
            current_stock = reorder_lvl + rng.randint(5, 180)

        inventory.append({
            "product_id": pid,
            "current_stock": current_stock,
            "reorder_level": reorder_lvl,
        })

    # -------------------------------------------------------------------------
    # 3. SALES GENERATION
    # -------------------------------------------------------------------------
    # Power-law / Pareto product popularity: top 20% products get ~60% sales
    product_ids = [p["product_id"] for p in products]
    weights = [1.0 / ((idx + 1) ** 0.65) for idx in range(num_products)]
    total_w = sum(weights)
    norm_weights = [w / total_w for w in weights]

    sales: List[Dict[str, Any]] = []

    # Timeline: 90 days between 2026-01-01 and 2026-03-31
    base_start_date = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    total_seconds = 90 * 86400

    order_counter = 1000
    quantity_choices = [1, 2, 3, 4, 5]
    quantity_weights = [0.65, 0.22, 0.08, 0.03, 0.02]

    while len(sales) < num_sales:
        order_counter += 1
        order_id = f"ord-synth-{order_counter}"

        # Realistic timestamp: daytime biased (8am to 8pm UTC)
        sec_offset = rng.randint(0, total_seconds)
        raw_dt = base_start_date + timedelta(seconds=sec_offset)
        # Shift hour to business/shopping hours (08:00 - 21:59)
        order_dt = raw_dt.replace(hour=rng.randint(8, 21), minute=rng.randint(0, 59), second=rng.randint(0, 59))
        iso_date = order_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # 1 to 4 items per order
        items_in_order = rng.choices([1, 2, 3, 4], weights=[0.55, 0.30, 0.10, 0.05], k=1)[0]
        # Ensure remaining capacity is respected
        items_in_order = min(items_in_order, num_sales - len(sales))

        # Pick unique products for this order to satisfy (order_id, product_id) uniqueness constraint
        chosen_pids = set()
        attempts = 0
        while len(chosen_pids) < items_in_order and attempts < 20:
            pid = rng.choices(product_ids, weights=norm_weights, k=1)[0]
            chosen_pids.add(pid)
            attempts += 1

        for pid in chosen_pids:
            qty = rng.choices(quantity_choices, weights=quantity_weights, k=1)[0]
            price = product_price_map[pid]
            order_line_id = f"{order_id}-{pid}"

            sales.append({
                "order_line_id": order_line_id,
                "order_id": order_id,
                "order_date": iso_date,
                "product_id": pid,
                "quantity": qty,
                "unit_price": price,
            })

    # Sort sales chronologically
    sales.sort(key=lambda s: s["order_date"])

    # -------------------------------------------------------------------------
    # 4. METADATA & PERSISTENCE
    # -------------------------------------------------------------------------
    metadata = {
        "dataset_type": "SYNTHETIC_BENCHMARK_DATASET",
        "notice": (
            "NOTICE: This dataset was programmatically generated using a deterministic pseudo-random "
            "algorithm for testing, data engineering benchmarks, and pipeline load validation. "
            "It does NOT contain real customer, transaction, or proprietary business data."
        ),
        "seed": seed,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "record_counts": {
            "products": len(products),
            "inventory": len(inventory),
            "sales": len(sales),
        },
        "catalog_summary": {
            "categories": len(categories),
            "suppliers": len(suppliers),
            "low_stock_products_count": sum(1 for inv in inventory if inv["current_stock"] <= inv["reorder_level"]),
        },
    }

    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write products.csv
        with open(output_dir / "products.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["product_id", "product_name", "category", "supplier_id"])
            writer.writeheader()
            writer.writerows(products)

        # Write inventory.csv
        with open(output_dir / "inventory.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["product_id", "current_stock", "reorder_level"])
            writer.writeheader()
            writer.writerows(inventory)

        # Write sales.csv
        with open(output_dir / "sales.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["order_line_id", "order_id", "order_date", "product_id", "quantity", "unit_price"])
            writer.writeheader()
            writer.writerows(sales)

        # Write metadata.json
        with open(output_dir / "DATASET_METADATA.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        logger.info(
            "Saved synthetic dataset to %s (%d products, %d inventory, %d sales)",
            output_dir, len(products), len(inventory), len(sales)
        )

    return {
        "products": products,
        "inventory": inventory,
        "sales": sales,
        "metadata": metadata,
    }

