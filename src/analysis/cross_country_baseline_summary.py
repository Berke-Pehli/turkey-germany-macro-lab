"""Summarize first-round baseline results across Türkiye, Germany, and the euro area.

This script collects the benchmark results from the first baseline phase
and presents a clean cross-country comparison. It focuses on out-of-sample
performance because that is the most important criterion for model quality.

Current scope:
    - Türkiye
    - Germany
    - Euro area
    - Naive benchmark
    - Linear regression benchmark
    - Ridge benchmark
"""

from __future__ import annotations

import pandas as pd

from src.config.logging_config import get_logger


logger = get_logger(__name__)


def main() -> None:
    """Create and report a cross-country summary of baseline results."""
    results = pd.DataFrame(
        [
            {
                "country": "TR",
                "model": "naive",
                "evaluation": "out_of_sample",
                "mae": 5.150353,
                "rmse": 8.108454,
            },
            {
                "country": "TR",
                "model": "linear_regression",
                "evaluation": "out_of_sample",
                "mae": 5.398590,
                "rmse": 8.234951,
            },
            {
                "country": "TR",
                "model": "ridge",
                "evaluation": "out_of_sample",
                "mae": 5.403300,
                "rmse": 8.243200,
            },
            {
                "country": "DE",
                "model": "naive",
                "evaluation": "out_of_sample",
                "mae": 0.775904,
                "rmse": 1.077816,
            },
            {
                "country": "DE",
                "model": "linear_regression",
                "evaluation": "out_of_sample",
                "mae": 0.829860,
                "rmse": 1.225512,
            },
            {
                "country": "DE",
                "model": "ridge",
                "evaluation": "out_of_sample",
                "mae": 0.845900,
                "rmse": 1.244600,
            },
            {
                "country": "EA",
                "model": "naive",
                "evaluation": "out_of_sample",
                "mae": 0.595181,
                "rmse": 0.812256,
            },
            {
                "country": "EA",
                "model": "linear_regression",
                "evaluation": "out_of_sample",
                "mae": 0.556241,
                "rmse": 0.758246,
            },
            {
                "country": "EA",
                "model": "ridge",
                "evaluation": "out_of_sample",
                "mae": 0.557300,
                "rmse": 0.763100,
            },
        ]
    )

    logger.info("Cross-country out-of-sample summary:\n%s", results)

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

    logger.info("Best model by country using MAE:\n%s", best_by_country_mae)
    logger.info("Best model by country using RMSE:\n%s", best_by_country_rmse)


if __name__ == "__main__":
    main()