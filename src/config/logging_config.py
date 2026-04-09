"""Logging configuration utilities for the macro lab project.

This module provides a simple project-wide logger setup so that ingestion,
processing, modeling, and reporting modules can use consistent logging.
"""

from __future__ import annotations

import logging
import os


def get_logger(name: str) -> logging.Logger:
    """Create or retrieve a configured logger.

    The log level is read from the `LOG_LEVEL` environment variable and
    defaults to `INFO` when not set.

    Args:
        name: Logger name, usually `__name__`.

    Returns:
        A configured logger instance.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    return logging.getLogger(name)