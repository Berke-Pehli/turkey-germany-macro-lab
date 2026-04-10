"""Run a naive inflation baseline for Germany.

This script builds the simplest benchmark for one-month-ahead inflation.
The forecast is the previous month's inflation value, which provides a
useful reference point for later regression and machine-learning models.

Current setup:
    - Target: target_inflation_yoy_t1
    - Prediction: inflation_yoy_lag_1
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def load_prepared_model_data_de() -> pd.DataFrame:
    """Load the prepared Germany baseline modeling dataset.

    Returns:
        A pandas DataFrame containing the cleaned Germany baseline
        modeling data.
    """
    input_path = FINAL_DATA_DIR / "model_inputs" / "model_input_de_v1.parquet"
    df = pd.read_parquet(input_path)

    selected_columns = [
        "year_month",
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "consumer_confidence_index_lag_1",
        "target_inflation_yoy_t1",
    ]

    return df[selected_columns].dropna().reset_index(drop=True)


def mean_absolute_error(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Compute mean absolute error."""
    return float(np.mean(np.abs(y_true - y_pred)))


def root_mean_squared_error(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Compute root mean squared error."""
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def main() -> None:
    """Run the naive Germany inflation baseline and report metrics."""
    df = load_prepared_model_data_de().copy()

    df["naive_prediction"] = df["inflation_yoy_lag_1"]

    y_true = df["target_inflation_yoy_t1"]
    y_pred = df["naive_prediction"]

    mae = mean_absolute_error(y_true, y_pred)
    rmse = root_mean_squared_error(y_true, y_pred)

    logger.info("Naive baseline rows: %s", len(df))
    logger.info("Date range: %s to %s", df["year_month"].min(), df["year_month"].max())
    logger.info("Naive baseline MAE: %.4f", mae)
    logger.info("Naive baseline RMSE: %.4f", rmse)
    logger.info(
        "Prediction preview:\n%s",
        df[
            [
                "year_month",
                "inflation_yoy_lag_1",
                "target_inflation_yoy_t1",
                "naive_prediction",
            ]
        ].head(10),
    )


if __name__ == "__main__":
    main()