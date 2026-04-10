"""Prepare euro area model data for the first inflation baseline.

This script loads the exported euro area model-input parquet file, keeps a
small and interpretable feature set, constructs the target variable, and
returns a clean modeling DataFrame for the first baseline workflow.

Current design:
    - Target: one-month-ahead inflation
    - Features: lag-1 values of core macro indicators available for the
      euro area
"""

from __future__ import annotations

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def load_model_input_ea() -> pd.DataFrame:
    """Load the exported euro area model-input dataset.

    Returns:
        A pandas DataFrame containing the euro area modeling view exported
        from PostgreSQL.
    """
    input_path = FINAL_DATA_DIR / "model_inputs" / "model_input_ea_v1.parquet"
    return pd.read_parquet(input_path)


def prepare_baseline_model_data_ea(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare a clean baseline modeling dataset for the euro area.

    Args:
        df: Raw euro area model-input DataFrame.

    Returns:
        A DataFrame containing the date column, selected lag features,
        and the one-month-ahead inflation target with rows containing
        missing values removed.
    """
    selected_columns = [
        "year_month",
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "target_inflation_yoy_t1",
    ]

    model_df = df[selected_columns].copy()
    model_df = model_df.dropna().reset_index(drop=True)

    return model_df


def main() -> None:
    """Load and prepare the first euro area baseline modeling dataset."""
    raw_df = load_model_input_ea()
    model_df = prepare_baseline_model_data_ea(raw_df)

    logger.info("Raw euro area model-input rows: %s", len(raw_df))
    logger.info("Prepared baseline rows: %s", len(model_df))
    logger.info("Prepared columns: %s", list(model_df.columns))
    logger.info(
        "Prepared date range: %s to %s",
        model_df["year_month"].min(),
        model_df["year_month"].max(),
    )
    logger.info("Prepared preview:\n%s", model_df.head())


if __name__ == "__main__":
    main()