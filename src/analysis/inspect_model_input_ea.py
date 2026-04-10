"""Inspect euro area model input parquet for first modeling setup.

This script loads the exported euro area model-input parquet file and checks
its structure, date coverage, and missingness before baseline modeling.

Current purpose:
    - verify that the euro area model-input export is ready for the first
      baseline forecasting workflow
"""

from __future__ import annotations

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def main() -> None:
    """Load and inspect the euro area model input dataset."""
    input_path = FINAL_DATA_DIR / "model_inputs" / "model_input_ea_v1.parquet"
    df = pd.read_parquet(input_path)

    logger.info("Loaded euro area model input rows: %s", len(df))
    logger.info("Loaded columns: %s", list(df.columns))
    logger.info("Date range: %s to %s", df["year_month"].min(), df["year_month"].max())
    logger.info("Preview:\n%s", df.head())

    missingness = df.isna().sum().sort_values(ascending=False)
    logger.info("Missing values by column:\n%s", missingness)

    logger.info(
        "Target preview:\n%s",
        df[
            [
                "year_month",
                "inflation_yoy",
                "target_inflation_yoy_t1",
                "target_inflation_yoy_t3",
            ]
        ].head(10),
    )


if __name__ == "__main__":
    main()