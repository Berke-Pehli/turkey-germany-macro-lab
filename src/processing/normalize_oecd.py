"""Normalization utilities for OECD processed datasets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR, PROCESSED_DATA_DIR


logger = get_logger(__name__)


def _load_processed_parquet(file_path: Path) -> pd.DataFrame:
    """Load a processed parquet file into a pandas DataFrame."""
    return pd.read_parquet(file_path)


def normalize_oecd_turkiye_cpi() -> pd.DataFrame:
    """Normalize processed OECD Türkiye CPI data into project-standard format."""
    input_path = PROCESSED_DATA_DIR / "oecd" / "oecd_turkiye_cpi.parquet"
    df = _load_processed_parquet(input_path).copy()

    df = df[df["REF_AREA"] == "TUR"].copy()
    df = df[df["FREQ"] == "M"].copy()
    df = df[df["MEASURE"] == "CPI"].copy()
    df = df[df["ADJUSTMENT"] == "N"].copy()
    df = df[df["TRANSFORMATION"] == "GY"].copy()

    normalized = pd.DataFrame(
        {
            "source_code": "OECD",
            "country_code": "TR",
            "indicator_code": "inflation_yoy",
            "frequency_code": "M",
            "observation_date": pd.to_datetime(df["TIME_PERIOD"]).dt.to_period("M").dt.to_timestamp(),
            "observation_value": df["value"].astype(float),
        }
    )

    return normalized.sort_values(by=["observation_date"]).reset_index(drop=True)


def save_oecd_normalized(df: pd.DataFrame, file_name: str) -> Path:
    """Save normalized OECD data to the final data layer."""
    output_path = FINAL_DATA_DIR / "oecd" / f"{file_name}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved normalized OECD parquet to %s", output_path)
    return output_path