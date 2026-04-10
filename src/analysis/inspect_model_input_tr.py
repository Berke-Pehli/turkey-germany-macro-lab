"""Inspect Türkiye model input parquet for first modeling setup."""

from __future__ import annotations

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def main() -> None:
    """Load and inspect the Türkiye model input dataset."""
    input_path = FINAL_DATA_DIR / "model_inputs" / "model_input_tr_v1.parquet"
    df = pd.read_parquet(input_path)

    logger.info("Loaded Türkiye model input rows: %s", len(df))
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