"""Normalization utilities for ECB processed datasets.

This module converts processed ECB parquet files into a common
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


def normalize_deposit_facility_rate() -> pd.DataFrame:
    """Normalize processed ECB deposit facility rate data.

    Source file:
        data/processed/ecb/ecb_deposit_facility_rate.parquet

    Returns:
        A normalized DataFrame using the common project column structure.

    Notes:
        The ECB deposit facility rate is kept at daily frequency in this
        normalized layer. Monthly transformations will be handled later in
        the feature-engineering stage.
    """
    input_path = PROCESSED_DATA_DIR / "ecb" / "ecb_deposit_facility_rate.parquet"
    df = _load_processed_parquet(input_path).copy()

    normalized = pd.DataFrame(
        {
            "source_code": "ECB",
            "country_code": "EA",
            "indicator_code": "policy_rate",
            "frequency_code": "D",
            "observation_date": pd.to_datetime(df["time_period"]),
            "observation_value": df["value"].astype(float),
        }
    )

    return normalized.sort_values(by=["observation_date"]).reset_index(drop=True)


def save_ecb_normalized(df: pd.DataFrame) -> Path:
    """Save normalized ECB data to the final data layer.

    Args:
        df: Normalized ECB DataFrame.

    Returns:
        Path to the saved parquet file.

    Output location:
        data/final/ecb/ecb_normalized.parquet
    """
    output_path = FINAL_DATA_DIR / "ecb" / "ecb_normalized.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved normalized ECB parquet to %s", output_path)
    return output_path