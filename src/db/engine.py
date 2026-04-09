"""Database engine creation helpers for PostgreSQL connections.

This module creates a reusable SQLAlchemy engine for the macro lab project.
The engine is used by ingestion, processing, and reporting code to read from
and write to PostgreSQL.
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config.settings import get_database_settings


def get_engine(echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for the PostgreSQL database.

    Args:
        echo: Whether SQLAlchemy should echo SQL statements for debugging.

    Returns:
        A SQLAlchemy Engine connected to the configured PostgreSQL database.
    """
    db_settings = get_database_settings()
    return create_engine(db_settings.sqlalchemy_url, echo=echo)