"""Simple database connection test for the macro lab project.

This script checks whether environment variables are loaded correctly,
a SQLAlchemy engine can be created, and a basic query can be executed
against the PostgreSQL database.
"""

from sqlalchemy import text

from src.config.logging_config import get_logger
from src.db.engine import get_engine


logger = get_logger(__name__)


def main() -> None:
    """Run a simple PostgreSQL connection test."""
    engine = get_engine()

    with engine.connect() as connection:
        result = connection.execute(text("SELECT current_database();"))
        database_name = result.scalar_one()

    logger.info("Database connection successful.")
    logger.info("Connected to database: %s", database_name)


if __name__ == "__main__":
    main()