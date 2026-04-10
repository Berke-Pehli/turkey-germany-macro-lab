"""Run a first linear regression benchmark for Türkiye inflation.

This script estimates a simple linear regression model for one-month-ahead
inflation using lag-1 macro features. It provides the first benchmark to
compare against the naive baseline.

Current setup:
    - Target: target_inflation_yoy_t1
    - Features:
        - inflation_yoy_lag_1
        - unemployment_rate_lag_1
        - industrial_production_index_lag_1
        - sentiment_index_lag_1
        - consumer_confidence_index_lag_1
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def load_prepared_model_data_tr() -> pd.DataFrame:
    """Load the prepared Türkiye baseline modeling dataset.

    Returns:
        A pandas DataFrame containing the cleaned Türkiye baseline
        modeling data.
    """
    input_path = FINAL_DATA_DIR / "model_inputs" / "model_input_tr_v1.parquet"
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


def mean_absolute_error(y_true: pd.Series, y_pred: np.ndarray) -> float:
    """Compute mean absolute error."""
    return float(np.mean(np.abs(y_true - y_pred)))


def root_mean_squared_error(y_true: pd.Series, y_pred: np.ndarray) -> float:
    """Compute root mean squared error."""
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def main() -> None:
    """Fit and evaluate a simple linear regression benchmark."""
    df = load_prepared_model_data_tr().copy()

    feature_columns = [
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "consumer_confidence_index_lag_1",
    ]

    X = df[feature_columns]
    y = df["target_inflation_yoy_t1"]

    model = LinearRegression()
    model.fit(X, y)

    df["linear_regression_prediction"] = model.predict(X)

    mae = mean_absolute_error(y, df["linear_regression_prediction"])
    rmse = root_mean_squared_error(y, df["linear_regression_prediction"])

    coefficients = pd.Series(model.coef_, index=feature_columns)

    logger.info("Linear regression rows: %s", len(df))
    logger.info("Date range: %s to %s", df["year_month"].min(), df["year_month"].max())
    logger.info("Linear regression MAE: %.4f", mae)
    logger.info("Linear regression RMSE: %.4f", rmse)
    logger.info("Intercept: %.6f", model.intercept_)
    logger.info("Coefficients:\n%s", coefficients)
    logger.info(
        "Prediction preview:\n%s",
        df[
            [
                "year_month",
                "target_inflation_yoy_t1",
                "linear_regression_prediction",
            ]
        ].head(10),
    )


if __name__ == "__main__":
    main()