"""Run a rolling one-step-ahead evaluation for euro area inflation.

This script compares a naive benchmark and a linear regression model using
a rolling expanding-window evaluation. At each step, the model is trained
on all data available up to that point and then predicts the next month.

Current setup:
    - Target: target_inflation_yoy_t1
    - Features:
        - inflation_yoy_lag_1
        - unemployment_rate_lag_1
        - industrial_production_index_lag_1
        - sentiment_index_lag_1
    - Evaluation:
        - expanding training window
        - one-step-ahead prediction
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def load_prepared_model_data_ea() -> pd.DataFrame:
    """Load the prepared euro area baseline modeling dataset.

    Returns:
        A pandas DataFrame containing the cleaned euro area modeling data
        with lag features and target values.
    """
    input_path = FINAL_DATA_DIR / "model_inputs" / "model_input_ea_v1.parquet"
    df = pd.read_parquet(input_path)

    selected_columns = [
        "year_month",
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "target_inflation_yoy_t1",
    ]

    df = df[selected_columns].dropna().copy()
    df["year_month"] = pd.to_datetime(df["year_month"])

    return df.sort_values("year_month").reset_index(drop=True)


def mean_absolute_error(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Compute mean absolute error."""
    return float(np.mean(np.abs(y_true - y_pred)))


def root_mean_squared_error(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Compute root mean squared error."""
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def main() -> None:
    """Run an expanding-window one-step-ahead evaluation for the euro area."""
    df = load_prepared_model_data_ea().copy()

    feature_columns = [
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
    ]

    min_train_size = 120

    predictions: list[dict[str, float | str | pd.Timestamp]] = []

    for i in range(min_train_size, len(df)):
        train_df = df.iloc[:i].copy()
        test_row = df.iloc[i : i + 1].copy()

        X_train = train_df[feature_columns]
        y_train = train_df["target_inflation_yoy_t1"]

        X_test = test_row[feature_columns]

        model = LinearRegression()
        model.fit(X_train, y_train)

        naive_prediction = float(test_row["inflation_yoy_lag_1"].iloc[0])
        linear_prediction = float(model.predict(X_test)[0])
        actual = float(test_row["target_inflation_yoy_t1"].iloc[0])

        predictions.append(
            {
                "year_month": test_row["year_month"].iloc[0],
                "actual": actual,
                "naive_prediction": naive_prediction,
                "linear_regression_prediction": linear_prediction,
            }
        )

    results_df = pd.DataFrame(predictions)

    naive_mae = mean_absolute_error(results_df["actual"], results_df["naive_prediction"])
    naive_rmse = root_mean_squared_error(results_df["actual"], results_df["naive_prediction"])

    lr_mae = mean_absolute_error(
        results_df["actual"],
        results_df["linear_regression_prediction"],
    )
    lr_rmse = root_mean_squared_error(
        results_df["actual"],
        results_df["linear_regression_prediction"],
    )

    logger.info("Rolling evaluation rows: %s", len(results_df))
    logger.info(
        "Rolling evaluation period: %s to %s",
        results_df["year_month"].min(),
        results_df["year_month"].max(),
    )
    logger.info("Naive rolling MAE: %.4f", naive_mae)
    logger.info("Naive rolling RMSE: %.4f", naive_rmse)
    logger.info("Linear regression rolling MAE: %.4f", lr_mae)
    logger.info("Linear regression rolling RMSE: %.4f", lr_rmse)
    logger.info("Rolling prediction preview:\n%s", results_df.head(10))


if __name__ == "__main__":
    main()