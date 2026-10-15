"""
NiFi Ingestion Simulator.
Simulates Apache NiFi routing from data/source to shared data/landing.
Implements observable, atomic file landing using a temporary staging pattern
to guarantee Airflow never consumes partial or in-flight files.
"""

import argparse
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import List, Tuple

# Project root setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [NiFi-Ingest] %(message)s")
logger = logging.getLogger("NiFiIngest")

REQUIRED_FILES = ["products.csv", "inventory.csv", "sales.csv"]


def clean_landing(landing_dir: Path) -> None:
    """Removes existing landing CSV files."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    for fname in REQUIRED_FILES:
        target = landing_dir / fname
        if target.exists():
            target.unlink()
            logger.info("Cleaned landing file: %s", target)
    # Clean tmp dir
    tmp_dir = landing_dir / ".tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
        logger.info("Cleaned temporary staging dir: %s", tmp_dir)


def route_files_atomically(source_dir: Path, landing_dir: Path) -> List[Tuple[str, int]]:
    """
    Simulates NiFi PutFile processor with Temporary File Directory:
    1. Writes file to landing/.tmp/filename.tmp
    2. Atomically moves file to landing/filename
    Returns list of (filename, byte_count).
    """
    source_dir.mkdir(parents=True, exist_ok=True)
    landing_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = landing_dir / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    landed_files = []

    for fname in REQUIRED_FILES:
        src_path = source_dir / fname
        if not src_path.exists():
            logger.error("Source file not found: %s", src_path)
            raise FileNotFoundError(f"Source file {src_path} is missing from source directory.")

        tmp_path = tmp_dir / f"{fname}.tmp"
        dest_path = landing_dir / fname

        # Copy to staging tmp
        shutil.copyfile(src_path, tmp_path)
        size_bytes = tmp_path.stat().st_size

        # Atomic rename/move to destination landing path
        if dest_path.exists():
            dest_path.unlink()
        tmp_path.rename(dest_path)

        logger.info("Atomically landed [%s] (%d bytes) to %s", fname, size_bytes, dest_path)
        landed_files.append((fname, size_bytes))

    # Remove temporary directory
    if tmp_dir.exists():
        try:
            tmp_dir.rmdir()
        except Exception:
            pass

    return landed_files


def verify_landing_readiness(landing_dir: Path) -> bool:
    """
    Checks that all required files exist in data/landing/ and have non-zero size.
    """
    all_ready = True
    for fname in REQUIRED_FILES:
        fpath = landing_dir / fname
        if not fpath.exists():
            logger.warning("Missing required file in landing: %s", fpath)
            all_ready = False
        elif fpath.stat().st_size == 0:
            logger.warning("File in landing is empty (0 bytes): %s", fpath)
            all_ready = False
        else:
            logger.info("Verified file ready in landing: %s (%d bytes)", fname, fpath.stat().st_size)

    return all_ready


def main():
    parser = argparse.ArgumentParser(description="NiFi Landing Ingestion Simulator")
    parser.add_argument("--clean", action="store_true", help="Remove files from landing directory (for failure testing)")
    parser.add_argument("--verify", action="store_true", help="Only verify current landing directory readiness")
    parser.add_argument("--source", type=str, default=str(Config.DATA_SOURCE_DIR), help="Path to source directory")
    parser.add_argument("--landing", type=str, default=str(Config.DATA_LANDING_DIR), help="Path to landing directory")

    args = parser.parse_args()
    source_dir = Path(args.source)
    landing_dir = Path(args.landing)

    if args.clean:
        clean_landing(landing_dir)
        print("Landing directory cleaned. Ready for missing-input test (AT-02).")
        return

    if args.verify:
        ready = verify_landing_readiness(landing_dir)
        if ready:
            print("[SUCCESS] All required landing files are present and ready.")
            sys.exit(0)
        else:
            print("[FAILED] Landing files are missing or incomplete.")
            sys.exit(1)

    print("=" * 80)
    print(">> EXECUTING NIFI INGESTION ROUTING (Source -> Landing)")
    print("=" * 80)
    landed = route_files_atomically(source_dir, landing_dir)
    ready = verify_landing_readiness(landing_dir)

    if ready:
        print(f"\n[OK] NiFi Ingestion Complete. {len(landed)} files landed in {landing_dir}")
    else:
        print("\n[ERROR] NiFi Ingestion failed readiness check.")
        sys.exit(1)


if __name__ == "__main__":
    main()
