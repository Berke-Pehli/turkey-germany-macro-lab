"""Normalization utilities for CBRT FX processed datasets.

This module converts processed CBRT FX parquet files into a common
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


def normalize_cbrt_fx_rates() -> pd.DataFrame:
    """Normalize processed CBRT FX rates into project-standard format.

    Source file:
        data/processed/cbrt_fx/cbrt_fx_rates.parquet

    Returns:
        A normalized DataFrame using the common project column structure.

    Notes:
        This first version maps:
            - EUR -> try_eur_eom
            - USD -> try_usd_eom

        and uses `forex_selling` as the selected official rate field.
    """
    input_path = PROCESSED_DATA_DIR / "cbrt_fx" / "cbrt_fx_rates.parquet"
    df = _load_processed_parquet(input_path).copy()

    currency_to_indicator = {
        "EUR": "try_eur_eom",
        "USD": "try_usd_eom",
    }

    df = df[df["currency_code"].isin(currency_to_indicator.keys())].copy()
    df["indicator_code"] = df["currency_code"].map(currency_to_indicator)

    normalized = pd.DataFrame(
        {
            "source_code": "CBRT",
            "country_code": "TR",
            "indicator_code": df["indicator_code"],
            "frequency_code": "D",
            "observation_date": pd.to_datetime(df["observation_date"]),
            "observation_value": df["forex_selling"].astype(float),
        }
    )

    return normalized.sort_values(
        by=["indicator_code", "observation_date"]
    ).reset_index(drop=True)


def save_cbrt_fx_normalized(df: pd.DataFrame) -> Path:
    """Save normalized CBRT FX data to the final data layer.

    Args:
        df: Normalized CBRT FX DataFrame.

    Returns:
        Path to the saved parquet file.

    Output location:
        data/final/cbrt_fx/cbrt_fx_normalized.parquet
    """
    output_path = FINAL_DATA_DIR / "cbrt_fx" / "cbrt_fx_normalized.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved normalized CBRT FX parquet to %s", output_path)
    return output_path