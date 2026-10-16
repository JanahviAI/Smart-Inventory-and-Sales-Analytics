"""
Command-Line Interface for Synthetic Retail Dataset Generation.
Generates reproducible synthetic data for products, inventory, and sales.

Usage:
    python scripts/generate_synthetic_data.py --num-products 500 --num-inventory 500 --num-sales 10000 --seed 42
"""

import argparse
import logging
import sys
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
from src.etl.generator import generate_synthetic_dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [DataGen] %(message)s")
logger = logging.getLogger("DataGen")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic retail dataset for testing and benchmarking.")
    parser.add_argument("--num-products", type=int, default=500, help="Number of products to generate (default: 500)")
    parser.add_argument("--num-inventory", type=int, default=500, help="Number of inventory rows to generate (default: 500)")
    parser.add_argument("--num-sales", type=int, default=10000, help="Number of sales rows to generate (default: 10000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for deterministic output (default: 42)")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(Config.DATA_SOURCE_DIR),
        help=f"Target directory for generated CSV files (default: {Config.DATA_SOURCE_DIR})",
    )
    parser.add_argument(
        "--copy-to-landing",
        action="store_true",
        help="Also write or copy the generated files directly to data/landing for immediate batch ingestion.",
    )

    args = parser.parse_args()
    output_path = Path(args.output_dir)

    print("=" * 80)
    print(f">> GENERATING SYNTHETIC BENCHMARK DATASET (Seed: {args.seed})")
    print(f">> Products: {args.num_products} | Inventory: {args.num_inventory} | Sales: {args.num_sales}")
    print(f">> Destination: {output_path}")
    print("=" * 80)

    result = generate_synthetic_dataset(
        num_products=args.num_products,
        num_inventory=args.num_inventory,
        num_sales=args.num_sales,
        seed=args.seed,
        output_dir=output_path,
    )

    metadata = result["metadata"]
    print("\n[Summary] Generation completed successfully:")
    print(f"  - Products CSV:  {output_path / 'products.csv'} ({len(result['products'])} rows)")
    print(f"  - Inventory CSV: {output_path / 'inventory.csv'} ({len(result['inventory'])} rows)")
    print(f"  - Sales CSV:     {output_path / 'sales.csv'} ({len(result['sales'])} rows)")
    print(f"  - Metadata:      {output_path / 'DATASET_METADATA.json'}")
    print(f"  - Low Stock:     {metadata['catalog_summary']['low_stock_products_count']} items <= reorder level")

    if args.copy_to_landing:
        from scripts.simulate_nifi_ingest import route_files_atomically
        landing_dir = Config.DATA_LANDING_DIR
        print(f"\n[Landing] Routing generated files to shared landing directory: {landing_dir}")
        route_files_atomically(output_path, landing_dir)
        print("  - Routing to landing completed.")


if __name__ == "__main__":
    main()

