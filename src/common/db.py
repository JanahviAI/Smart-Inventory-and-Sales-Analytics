"""
Database abstraction layer for the Smart Inventory and Sales Analytics Platform.
Provides connection management, schema initialization, and query execution.
Supports PostgreSQL (canonical system of record) and SQLite (offline test fallback).
"""

import logging
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.common.config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections, schemas, and queries."""

    _engine: Optional[Engine] = None

    @classmethod
    def get_engine(cls) -> Engine:
        """Returns or initializes a SQLAlchemy engine."""
        if cls._engine is not None:
            return cls._engine

        if Config.DB_TYPE == "sqlite":
            db_path = str(Config.SQLITE_DB_PATH.resolve())
            cls._engine = create_engine(f"sqlite:///{db_path}")
            logger.info("Initialized SQLite engine at %s", db_path)
        else:
            try:
                uri = Config.get_postgres_uri()
                engine = create_engine(uri, pool_pre_ping=True)
                # Test connectivity
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                cls._engine = engine
                logger.info("Connected to PostgreSQL at %s:%s/%s",
                            Config.POSTGRES_HOST, Config.POSTGRES_PORT, Config.POSTGRES_DB)
            except Exception as e:
                logger.warning(
                    "PostgreSQL connection failed (%s). Falling back to SQLite for local operations.", e
                )
                db_path = str(Config.SQLITE_DB_PATH.resolve())
                cls._engine = create_engine(f"sqlite:///{db_path}")

        return cls._engine

    @classmethod
    def is_sqlite(cls) -> bool:
        """Returns True if the current engine is SQLite."""
        engine = cls.get_engine()
        return "sqlite" in engine.dialect.name.lower()

    @classmethod
    @contextmanager
    def get_connection(cls):
        """Context manager yielding a database connection with auto-commit/rollback."""
        engine = cls.get_engine()
        connection = engine.connect()
        trans = connection.begin()
        try:
            yield connection
            trans.commit()
        except Exception:
            trans.rollback()
            raise
        finally:
            connection.close()

    @classmethod
    def execute(cls, query: str, params: Optional[Union[Dict, List, Tuple]] = None):
        """Executes a SQL statement within a transactional connection."""
        with cls.get_connection() as conn:
            return conn.execute(text(query), params or {})

    @classmethod
    def fetch_all(cls, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> List[Dict[str, Any]]:
        """Executes a query and returns all rows as a list of dictionaries."""
        with cls.get_connection() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]

    @classmethod
    def fetch_one(cls, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> Optional[Dict[str, Any]]:
        """Executes a query and returns one row as a dictionary or None."""
        with cls.get_connection() as conn:
            result = conn.execute(text(query), params or {})
            row = result.fetchone()
            return dict(row._mapping) if row else None

    @classmethod
    def init_schema(cls, schema_path: Optional[str] = None) -> None:
        """
        Initializes canonical tables.
        Applies sql/schema.sql for PostgreSQL, or translates for SQLite if in fallback mode.
        """
        target_schema = schema_path or str(Config.SCHEMA_FILE)
        with open(target_schema, "r", encoding="utf-8") as f:
            sql_content = f.read()

        if cls.is_sqlite():
            # Translate Postgres-specific syntax for SQLite compatibility
            cls._init_sqlite_schema()
        else:
            with cls.get_connection() as conn:
                # Execute statements block by block
                statements = [s.strip() for s in sql_content.split(";") if s.strip()]
                for stmt in statements:
                    conn.execute(text(stmt))
            logger.info("PostgreSQL canonical schema successfully initialized from %s", target_schema)

    @classmethod
    def _init_sqlite_schema(cls) -> None:
        """Initializes compatible schema for SQLite fallback mode."""
        sqlite_ddl = """
        DROP TABLE IF EXISTS reverse_etl_audit;
        DROP TABLE IF EXISTS reorder_recommendations;
        DROP TABLE IF EXISTS inventory_events;
        DROP TABLE IF EXISTS rejected_records;
        DROP TABLE IF EXISTS pipeline_runs;
        DROP TABLE IF EXISTS sales;
        DROP TABLE IF EXISTS inventory;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS stg_sales;
        DROP TABLE IF EXISTS stg_inventory;
        DROP TABLE IF EXISTS stg_products;

        CREATE TABLE stg_products (
            raw_product_id TEXT,
            raw_product_name TEXT,
            raw_category TEXT,
            raw_supplier_id TEXT,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE stg_inventory (
            raw_product_id TEXT,
            raw_current_stock TEXT,
            raw_reorder_level TEXT,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE stg_sales (
            raw_order_line_id TEXT,
            raw_order_id TEXT,
            raw_order_date TEXT,
            raw_product_id TEXT,
            raw_quantity TEXT,
            raw_unit_price TEXT,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE inventory (
            product_id INTEGER PRIMARY KEY REFERENCES products(product_id) ON DELETE RESTRICT,
            current_stock INTEGER NOT NULL CHECK (current_stock >= 0),
            reorder_level INTEGER NOT NULL CHECK (reorder_level >= 0),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE sales (
            order_line_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            order_date TIMESTAMP NOT NULL,
            product_id INTEGER NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price REAL NOT NULL CHECK (unit_price >= 0),
            total_price REAL NOT NULL CHECK (total_price >= 0),
            run_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_sales_order_product UNIQUE (order_id, product_id)
        );

        CREATE TABLE rejected_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source_table TEXT NOT NULL,
            record_identifier TEXT,
            raw_payload TEXT NOT NULL,
            reason TEXT NOT NULL,
            rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE pipeline_runs (
            run_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ended_at TIMESTAMP,
            tasks_completed TEXT DEFAULT '[]',
            summary TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE inventory_events (
            event_id TEXT PRIMARY KEY,
            event_time TIMESTAMP NOT NULL,
            order_id TEXT NOT NULL,
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            status TEXT NOT NULL,
            rejection_reason TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            latency_ms REAL
        );

        CREATE TABLE reorder_recommendations (
            recommendation_key TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            current_stock INTEGER NOT NULL,
            reorder_level INTEGER NOT NULL,
            target_stock INTEGER NOT NULL,
            recommended_quantity INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE reverse_etl_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            recommendation_key TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            http_status INTEGER,
            payload TEXT NOT NULL,
            response_body TEXT,
            status TEXT NOT NULL,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with cls.get_connection() as conn:
            for stmt in sqlite_ddl.strip().split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
        logger.info("SQLite fallback schema initialized successfully.")

    @classmethod
    def reset_engine(cls):
        """Disposes and resets the active engine (useful in test suites)."""
        if cls._engine is not None:
            cls._engine.dispose()
            cls._engine = None


# Module-level convenience functions
def get_db_connection():
    return DatabaseManager.get_connection()


def execute_query(query: str, params: Optional[Union[Dict, List, Tuple]] = None):
    return DatabaseManager.execute(query, params)


def fetch_all(query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> List[Dict[str, Any]]:
    return DatabaseManager.fetch_all(query, params)


def fetch_one(query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> Optional[Dict[str, Any]]:
    return DatabaseManager.fetch_one(query, params)


def init_db(schema_path: Optional[str] = None) -> None:
    DatabaseManager.init_schema(schema_path)

