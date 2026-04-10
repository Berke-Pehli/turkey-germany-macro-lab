"""Evaluate Türkiye inflation benchmarks using a time-based train/test split.

This script compares a naive benchmark and a linear regression model for
one-month-ahead inflation using an out-of-sample test period. The goal is
to move from simple in-sample benchmarking to a more realistic forecasting
evaluation.

Current setup:
    - Target: target_inflation_yoy_t1
    - Features:
        - inflation_yoy_lag_1
        - unemployment_rate_lag_1
        - industrial_production_index_lag_1
        - sentiment_index_lag_1
        - consumer_confidence_index_lag_1
    - Train/test split:
        - Train: observations before 2019-01-01
        - Test: observations from 2019-01-01 onward
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
        modeling data with lag features and target values.
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

    df = df[selected_columns].dropna().copy()
    df["year_month"] = pd.to_datetime(df["year_month"])

    return df.sort_values("year_month").reset_index(drop=True)


def mean_absolute_error(y_true: pd.Series, y_pred: np.ndarray) -> float:
    """Compute mean absolute error."""
    return float(np.mean(np.abs(y_true - y_pred)))


def root_mean_squared_error(y_true: pd.Series, y_pred: np.ndarray) -> float:
    """Compute root mean squared error."""
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def main() -> None:
    """Run a time-based train/test evaluation for Türkiye."""
    df = load_prepared_model_data_tr().copy()

    split_date = pd.Timestamp("2019-01-01")

    train_df = df[df["year_month"] < split_date].copy()
    test_df = df[df["year_month"] >= split_date].copy()

    feature_columns = [
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "consumer_confidence_index_lag_1",
    ]

    X_train = train_df[feature_columns]
    y_train = train_df["target_inflation_yoy_t1"]

    X_test = test_df[feature_columns]
    y_test = test_df["target_inflation_yoy_t1"]

    test_df["naive_prediction"] = test_df["inflation_yoy_lag_1"]

    model = LinearRegression()
    model.fit(X_train, y_train)
    test_df["linear_regression_prediction"] = model.predict(X_test)

    naive_mae = mean_absolute_error(y_test, test_df["naive_prediction"])
    naive_rmse = root_mean_squared_error(y_test, test_df["naive_prediction"])

    lr_mae = mean_absolute_error(y_test, test_df["linear_regression_prediction"])
    lr_rmse = root_mean_squared_error(y_test, test_df["linear_regression_prediction"])

    logger.info("Train rows: %s", len(train_df))
    logger.info("Test rows: %s", len(test_df))
    logger.info(
        "Train period: %s to %s",
        train_df["year_month"].min(),
        train_df["year_month"].max(),
    )
    logger.info(
        "Test period: %s to %s",
        test_df["year_month"].min(),
        test_df["year_month"].max(),
    )

    logger.info("Naive test MAE: %.4f", naive_mae)
    logger.info("Naive test RMSE: %.4f", naive_rmse)

    logger.info("Linear regression test MAE: %.4f", lr_mae)
    logger.info("Linear regression test RMSE: %.4f", lr_rmse)

    logger.info("Linear regression intercept: %.6f", model.intercept_)
    logger.info(
        "Linear regression coefficients:\n%s",
        pd.Series(model.coef_, index=feature_columns),
    )

    logger.info(
        "Test prediction preview:\n%s",
        test_df[
            [
                "year_month",
                "target_inflation_yoy_t1",
                "naive_prediction",
                "linear_regression_prediction",
            ]
        ].head(10),
    )


if __name__ == "__main__":
    main()