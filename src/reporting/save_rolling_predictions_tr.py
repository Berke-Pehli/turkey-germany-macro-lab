"""Save rolling prediction results for Türkiye.

This script runs the same expanding-window one-step-ahead evaluation used
in the Türkiye rolling benchmark and saves the prediction results to a CSV
file for reporting and dashboard use.

Input:
    - data/final/model_inputs/model_input_tr_v1.parquet

Output:
    - outputs/tables/rolling_predictions_tr.csv
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.linear_model import LinearRegression

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR


logger = get_logger(__name__)


def get_project_root() -> Path:
    """Return the project root directory.

    Returns:
        The root directory of the project based on the location of this
        script file.
    """
    return Path(__file__).resolve().parents[2]


def load_prepared_model_data_tr() -> pd.DataFrame:
    """Load the prepared Türkiye modeling dataset.

    Returns:
        A pandas DataFrame containing the cleaned Türkiye modeling data
        with lag features and target values.
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


def main() -> None:
    """Create and save rolling prediction results for Türkiye."""
    df = load_prepared_model_data_tr().copy()

    feature_columns = [
        "inflation_yoy_lag_1",
        "unemployment_rate_lag_1",
        "industrial_production_index_lag_1",
        "sentiment_index_lag_1",
        "consumer_confidence_index_lag_1",
    ]

    min_train_size = 120
    predictions: list[dict[str, float | pd.Timestamp | str]] = []

    for i in range(min_train_size, len(df)):
        train_df = df.iloc[:i].copy()
        test_row = df.iloc[i : i + 1].copy()

        X_train = train_df[feature_columns]
        y_train = train_df["target_inflation_yoy_t1"]
        X_test = test_row[feature_columns]

        model = LinearRegression()
        model.fit(X_train, y_train)

        predictions.append(
            {
                "country": "TR",
                "year_month": test_row["year_month"].iloc[0],
                "actual": float(test_row["target_inflation_yoy_t1"].iloc[0]),
                "naive_prediction": float(test_row["inflation_yoy_lag_1"].iloc[0]),
                "linear_regression_prediction": float(model.predict(X_test)[0]),
            }
        )

    results_df = pd.DataFrame(predictions)

    output_dir = get_project_root() / "outputs" / "tables"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "rolling_predictions_tr.csv"
    results_df.to_csv(output_path, index=False)

    logger.info("Saved Türkiye rolling predictions: %s", output_path)
    logger.info("Rows saved: %s", len(results_df))
    logger.info("Preview:\n%s", results_df.head())


if __name__ == "__main__":
    main()