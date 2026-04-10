"""Combine rolling prediction files for dashboard and reporting use.

This script reads the country-level rolling prediction files created for
Türkiye, Germany, and the euro area, combines them into one dataset, and
saves a single CSV file for Power BI and other reporting uses.

Inputs:
    - outputs/tables/rolling_predictions_tr.csv
    - outputs/tables/rolling_predictions_de.csv
    - outputs/tables/rolling_predictions_ea.csv

Output:
    - outputs/tables/rolling_predictions_all_countries.csv
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.logging_config import get_logger


logger = get_logger(__name__)


def get_project_root() -> Path:
    """Return the project root directory.

    Returns:
        The root directory of the project based on the location of this
        script file.
    """
    return Path(__file__).resolve().parents[2]


def main() -> None:
    """Combine country-level rolling prediction files into one table."""
    tables_dir = get_project_root() / "outputs" / "tables"

    tr_path = tables_dir / "rolling_predictions_tr.csv"
    de_path = tables_dir / "rolling_predictions_de.csv"
    ea_path = tables_dir / "rolling_predictions_ea.csv"

    tr_df = pd.read_csv(tr_path, parse_dates=["year_month"])
    de_df = pd.read_csv(de_path, parse_dates=["year_month"])
    ea_df = pd.read_csv(ea_path, parse_dates=["year_month"])

    combined_df = pd.concat([tr_df, de_df, ea_df], ignore_index=True)
    combined_df = combined_df.sort_values(["country", "year_month"]).reset_index(drop=True)

    output_path = tables_dir / "rolling_predictions_all_countries.csv"
    combined_df.to_csv(output_path, index=False)

    logger.info("Saved combined rolling predictions: %s", output_path)
    logger.info("Rows saved: %s", len(combined_df))
    logger.info("Country counts:\n%s", combined_df["country"].value_counts())
    logger.info("Preview:\n%s", combined_df.head(10))


if __name__ == "__main__":
    main()