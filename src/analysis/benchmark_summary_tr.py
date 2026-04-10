"""Summarize Türkiye baseline benchmark results.

This script compares simple benchmark models for one-month-ahead inflation
in Türkiye. It reports both in-sample and out-of-sample results for the
naive baseline and the linear regression model.

Current setup:
    - Target: target_inflation_yoy_t1
    - Features:
        - inflation_yoy_lag_1
        - unemployment_rate_lag_1
        - industrial_production_index_lag_1
        - sentiment_index_lag_1
        - consumer_confidence_index_lag_1
    - Time split:
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


def load_model_data_tr() -> pd.DataFrame:
    """Load and prepare the Türkiye modeling dataset.

    Returns:
        A cleaned pandas DataFrame containing the selected lag features,
        the target variable, and the monthly date column.
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


def mean_absolute_error(y_true: pd.Series, y_pred: np.ndarray | pd.Series) -> float:
    """Compute mean absolute error."""
    return float(np.mean(np.abs(y_true - y_pred)))


def root_mean_squared_error(y_true: pd.Series, y_pred: np.ndarray | pd.Series) -> float:
    """Compute root mean squared error."""
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def main() -> None:
    """Compute and report benchmark summary results for Türkiye."""
    df = load_model_data_tr().copy()

    feature_columns = [
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "consumer_confidence_index_lag_1",
    ]
    target_column = "target_inflation_yoy_t1"

    split_date = pd.Timestamp("2019-01-01")

    train_df = df[df["year_month"] < split_date].copy()
    test_df = df[df["year_month"] >= split_date].copy()

    X_full = df[feature_columns]
    y_full = df[target_column]

    X_train = train_df[feature_columns]
    y_train = train_df[target_column]

    X_test = test_df[feature_columns]
    y_test = test_df[target_column]

    df["naive_prediction"] = df["inflation_yoy_lag_1"]
    test_df["naive_prediction"] = test_df["inflation_yoy_lag_1"]

    full_model = LinearRegression()
    full_model.fit(X_full, y_full)
    df["linear_regression_prediction"] = full_model.predict(X_full)

    split_model = LinearRegression()
    split_model.fit(X_train, y_train)
    test_df["linear_regression_prediction"] = split_model.predict(X_test)

    results = pd.DataFrame(
        [
            {
                "evaluation": "in_sample",
                "model": "naive",
                "mae": mean_absolute_error(y_full, df["naive_prediction"]),
                "rmse": root_mean_squared_error(y_full, df["naive_prediction"]),
            },
            {
                "evaluation": "in_sample",
                "model": "linear_regression",
                "mae": mean_absolute_error(y_full, df["linear_regression_prediction"]),
                "rmse": root_mean_squared_error(y_full, df["linear_regression_prediction"]),
            },
            {
                "evaluation": "out_of_sample",
                "model": "naive",
                "mae": mean_absolute_error(y_test, test_df["naive_prediction"]),
                "rmse": root_mean_squared_error(y_test, test_df["naive_prediction"]),
            },
            {
                "evaluation": "out_of_sample",
                "model": "linear_regression",
                "mae": mean_absolute_error(y_test, test_df["linear_regression_prediction"]),
                "rmse": root_mean_squared_error(y_test, test_df["linear_regression_prediction"]),
            },
        ]
    )

    logger.info("Train rows: %s", len(train_df))
    logger.info("Test rows: %s", len(test_df))
    logger.info("Benchmark summary:\n%s", results)

    best_mae = results.loc[results["mae"].idxmin()]
    best_rmse = results.loc[results["rmse"].idxmin()]

    logger.info(
        "Best MAE result: %s / %s / %.4f",
        best_mae["evaluation"],
        best_mae["model"],
        best_mae["mae"],
    )
    logger.info(
        "Best RMSE result: %s / %s / %.4f",
        best_rmse["evaluation"],
        best_rmse["model"],
        best_rmse["rmse"],
    )


if __name__ == "__main__":
    main()