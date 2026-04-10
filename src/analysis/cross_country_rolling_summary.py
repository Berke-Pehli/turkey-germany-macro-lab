"""Summarize rolling benchmark results across Türkiye, Germany, and the euro area.

This script collects the rolling out-of-sample benchmark results from the
second evaluation phase and presents a clean cross-country comparison.

Current scope:
    - Türkiye
    - Germany
    - Euro area
    - Naive benchmark
    - Linear regression benchmark
"""

from __future__ import annotations

import pandas as pd

from src.config.logging_config import get_logger


logger = get_logger(__name__)


def main() -> None:
    """Create and report a cross-country summary of rolling results."""
    results = pd.DataFrame(
        [
            {
                "country": "TR",
                "model": "naive",
                "evaluation": "rolling",
                "mae": 3.8966,
                "rmse": 6.6393,
            },
            {
                "country": "TR",
                "model": "linear_regression",
                "evaluation": "rolling",
                "mae": 3.8648,
                "rmse": 6.7155,
            },
            {
                "country": "DE",
                "model": "naive",
                "evaluation": "rolling",
                "mae": 0.6346,
                "rmse": 0.9124,
            },
            {
                "country": "DE",
                "model": "linear_regression",
                "evaluation": "rolling",
                "mae": 0.5881,
                "rmse": 0.8308,
            },
            {
                "country": "EA",
                "model": "naive",
                "evaluation": "rolling",
                "mae": 0.4877,
                "rmse": 0.6895,
            },
            {
                "country": "EA",
                "model": "linear_regression",
                "evaluation": "rolling",
                "mae": 0.4513,
                "rmse": 0.6153,
            },
        ]
    )

    logger.info("Cross-country rolling summary:\n%s", results)

    best_by_country_mae = (
        results.loc[results.groupby("country")["mae"].idxmin()]
        .sort_values("country")
        .reset_index(drop=True)
    )

    best_by_country_rmse = (
        results.loc[results.groupby("country")["rmse"].idxmin()]
        .sort_values("country")
        .reset_index(drop=True)
    )

    logger.info("Best rolling model by country using MAE:\n%s", best_by_country_mae)
    logger.info("Best rolling model by country using RMSE:\n%s", best_by_country_rmse)


if __name__ == "__main__":
    main()