"""Project settings and environment variable helpers.

This module centralizes project paths and environment-based configuration
used across the macro lab project. It loads variables from a local `.env`
file when available and exposes simple helpers for database connectivity
and path management.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


# Load environment variables from a local .env file if it exists.
load_dotenv()


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
FINAL_DATA_DIR = DATA_DIR / "final"
BLD_DIR = PROJECT_ROOT / "bld"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
DOCS_DIR = PROJECT_ROOT / "docs"


@dataclass(frozen=True)
class DatabaseSettings:
    """Container for PostgreSQL connection settings.

    Attributes:
        host: Database host name.
        port: Database port.
        database: Database name.
        user: Database user name.
        password: Database password. This may be an empty string for
            local development setups that do not require a password.
    """

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build a SQLAlchemy-compatible PostgreSQL connection URL.

        Returns:
            A PostgreSQL connection string for SQLAlchemy using psycopg.
        """
        if self.password:
            return (
                f"postgresql+psycopg://{self.user}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )

        return (
            f"postgresql+psycopg://{self.user}"
            f"@{self.host}:{self.port}/{self.database}"
        )


def get_database_settings() -> DatabaseSettings:
    """Read PostgreSQL settings from environment variables.

    Returns:
        A populated DatabaseSettings object.

    Raises:
        ValueError: If one or more required database environment variables
            are missing.
    """
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD", "")

    required_values = {
        "POSTGRES_HOST": host,
        "POSTGRES_PORT": port,
        "POSTGRES_DB": database,
        "POSTGRES_USER": user,
    }

    missing_keys = [key for key, value in required_values.items() if not value]
    if missing_keys:
        missing = ", ".join(missing_keys)
        raise ValueError(
            f"Missing required database environment variables: {missing}"
        )

    return DatabaseSettings(
        host=host,
        port=int(port),
        database=database,
        user=user,
        password=password,
    )