"""Normalization utilities for Eurostat processed datasets.

This module converts processed Eurostat parquet files into a common
project-wide normalized structure that can later be loaded into
PostgreSQL fact tables.

Current normalized output columns:
    - source_code
    - country_code
    - indicator_code
    - frequency_code
    - observation_date
    - observation_value
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR, PROCESSED_DATA_DIR


logger = get_logger(__name__)


def _load_processed_parquet(file_path: Path) -> pd.DataFrame:
    """Load a processed parquet file into a pandas DataFrame.

    Args:
        file_path: Path to the processed parquet file.

    Returns:
        A pandas DataFrame loaded from parquet.
    """
    return pd.read_parquet(file_path)


def normalize_hicp_inflation() -> pd.DataFrame:
    """Normalize processed Eurostat HICP inflation data.

    Source file:
        data/processed/eurostat/prc_hicp_manr.parquet

    Returns:
        A normalized DataFrame using the common project column structure.
    """
    input_path = PROCESSED_DATA_DIR / "eurostat" / "prc_hicp_manr.parquet"
    df = _load_processed_parquet(input_path).copy()

    normalized = pd.DataFrame(
        {
            "source_code": "EUROSTAT",
            "country_code": df["country_code"],
            "indicator_code": "inflation_yoy",
            "frequency_code": df["frequency_code"],
            "observation_date": pd.to_datetime(df["time_period"]).dt.to_period("M").dt.to_timestamp(),
            "observation_value": df["value"].astype(float),
        }
    )

    return normalized.sort_values(
        by=["country_code", "observation_date"]
    ).reset_index(drop=True)


def normalize_unemployment() -> pd.DataFrame:
    """Normalize processed Eurostat unemployment data.

    Source file:
        data/processed/eurostat/une_rt_m.parquet

    Returns:
        A normalized DataFrame using the common project column structure.
    """
    input_path = PROCESSED_DATA_DIR / "eurostat" / "une_rt_m.parquet"
    df = _load_processed_parquet(input_path).copy()

    normalized = pd.DataFrame(
        {
            "source_code": "EUROSTAT",
            "country_code": df["country_code"],
            "indicator_code": "unemployment_rate",
            "frequency_code": "M",
            "observation_date": pd.to_datetime(df["time_period"]).dt.to_period("M").dt.to_timestamp(),
            "observation_value": df["value"].astype(float),
        }
    )

    return normalized.sort_values(
        by=["country_code", "observation_date"]
    ).reset_index(drop=True)


def combine_eurostat_normalized() -> pd.DataFrame:
    """Combine currently supported normalized Eurostat datasets.

    Returns:
        A single normalized DataFrame containing all currently supported
        Eurostat indicators.
    """
    frames = [
        normalize_hicp_inflation(),
        normalize_unemployment(),
    ]

    combined = pd.concat(frames, ignore_index=True)

    return combined.sort_values(
        by=["indicator_code", "country_code", "observation_date"]
    ).reset_index(drop=True)


def save_eurostat_normalized(df: pd.DataFrame) -> Path:
    """Save combined normalized Eurostat data to the final data layer.

    Args:
        df: Combined normalized Eurostat DataFrame.

    Returns:
        Path to the saved parquet file.

    Output location:
        data/final/eurostat/eurostat_normalized.parquet
    """
    output_path = FINAL_DATA_DIR / "eurostat" / "eurostat_normalized.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved normalized Eurostat parquet to %s", output_path)
    return output_path