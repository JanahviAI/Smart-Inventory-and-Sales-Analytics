"""
Configuration management for the Smart Inventory and Sales Analytics Platform.
Loads settings from environment variables or .env file with sensible defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load .env file if it exists
dotenv_path = PROJECT_ROOT / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)


class Config:
    """System configuration parameters."""

    # Project paths
    BASE_DIR = PROJECT_ROOT
    DATA_SOURCE_DIR = PROJECT_ROOT / os.getenv("DATA_SOURCE_DIR", "data/source")
    DATA_LANDING_DIR = PROJECT_ROOT / os.getenv("DATA_LANDING_DIR", "data/landing")
    DATA_PROCESSED_DIR = PROJECT_ROOT / os.getenv("DATA_PROCESSED_DIR", "data/processed")
    RESULTS_DIR = PROJECT_ROOT / os.getenv("REPORTS_DIR", "results")
    SCHEMA_FILE = PROJECT_ROOT / "sql" / "schema.sql"

    # Database configuration
    DB_TYPE = os.getenv("DB_TYPE", "postgresql").lower()
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "inventory_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    SQLITE_DB_PATH = PROJECT_ROOT / os.getenv("SQLITE_DB_PATH", "data/local_inventory.db")

    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they do not exist."""
        cls.DATA_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
        cls.DATA_LANDING_DIR.mkdir(parents=True, exist_ok=True)
        cls.DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        cls.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        cls.SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_postgres_uri(cls) -> str:
        """Returns standard PostgreSQL connection URI for SQLAlchemy."""
        return (
            f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@"
            f"{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
        )


# Ensure directories on module import
Config.ensure_directories()

