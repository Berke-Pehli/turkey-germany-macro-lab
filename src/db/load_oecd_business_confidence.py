"""Load normalized OECD Türkiye business confidence data into PostgreSQL fact tables."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR
from src.db.engine import get_engine
from src.db.io import write_dataframe_to_table


logger = get_logger(__name__)


def load_normalized_oecd_turkiye_business_confidence() -> pd.DataFrame:
    """Load normalized OECD Türkiye business confidence parquet data."""
    input_path = FINAL_DATA_DIR / "oecd" / "oecd_turkiye_business_confidence_normalized.parquet"
    return pd.read_parquet(input_path)


def fetch_dimension_lookup(query: str) -> pd.DataFrame:
    """Run a lookup query against PostgreSQL and return the result."""
    engine = get_engine()
    return pd.read_sql(query, engine)


def prepare_fact_macro_observation_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Map normalized OECD business confidence rows to fact table structure."""
    source_lookup = fetch_dimension_lookup(
        "SELECT source_id, source_code FROM macro_lab.dim_source"
    )
    country_lookup = fetch_dimension_lookup(
        "SELECT country_id, country_code FROM macro_lab.dim_country"
    )
    indicator_lookup = fetch_dimension_lookup(
        "SELECT indicator_id, indicator_code FROM macro_lab.dim_indicator"
    )

    prepared = (
        df.merge(source_lookup, on="source_code", how="left")
        .merge(country_lookup, on="country_code", how="left")
        .merge(indicator_lookup, on="indicator_code", how="left")
        .copy()
    )

    prepared["retrieved_at"] = datetime.now(UTC)
    prepared["value_status"] = None
    prepared["is_preliminary"] = False

    fact_df = prepared[
        [
            "country_id",
            "indicator_id",
            "source_id",
            "frequency_code",
            "observation_date",
            "observation_value",
            "value_status",
            "is_preliminary",
            "retrieved_at",
        ]
    ].copy()

    return fact_df


def validate_prepared_fact_rows(df: pd.DataFrame) -> None:
    """Validate fact rows before inserting them into PostgreSQL."""
    required_id_columns = ["country_id", "indicator_id", "source_id"]
    missing_summary = {
        column: int(df[column].isna().sum()) for column in required_id_columns
    }

    invalid_columns = {key: value for key, value in missing_summary.items() if value > 0}
    if invalid_columns:
        raise ValueError(f"Missing dimension mappings detected: {invalid_columns}")


def load_fact_macro_observation(df: pd.DataFrame) -> None:
    """Insert prepared fact rows into PostgreSQL."""
    engine = get_engine()
    write_dataframe_to_table(
        df=df,
        table_name="fact_macro_observation",
        engine=engine,
        schema="macro_lab",
        if_exists="append",
    )
    logger.info("Inserted %s rows into macro_lab.fact_macro_observation", len(df))


def main() -> None:
    """Run the normalized OECD Türkiye business confidence load process."""
    normalized_df = load_normalized_oecd_turkiye_business_confidence()
    prepared_df = prepare_fact_macro_observation_rows(normalized_df)
    validate_prepared_fact_rows(prepared_df)
    load_fact_macro_observation(prepared_df)

    logger.info("OECD Türkiye business confidence load complete.")
    logger.info("Prepared rows: %s", len(prepared_df))
    logger.info("Preview:\n%s", prepared_df.head())


if __name__ == "__main__":
    main()