"""Simple Eurostat ingestion test for the macro lab project."""

from src.config.logging_config import get_logger
from src.ingestion.eurostat import (
    fetch_hicp_inflation_raw,
    fetch_unemployment_raw,
    parse_hicp_inflation_json,
    parse_unemployment_json,
    save_hicp_inflation_processed,
    save_hicp_inflation_raw,
    save_unemployment_processed,
    save_unemployment_raw,
)


logger = get_logger(__name__)


def main() -> None:
    """Fetch, parse, and save selected Eurostat datasets for testing."""
    hicp_raw = fetch_hicp_inflation_raw()
    hicp_raw_path = save_hicp_inflation_raw(hicp_raw)
    hicp_df = parse_hicp_inflation_json(hicp_raw)
    hicp_processed_path = save_hicp_inflation_processed(hicp_df)

    unemployment_raw = fetch_unemployment_raw()
    unemployment_raw_path = save_unemployment_raw(unemployment_raw)
    unemployment_df = parse_unemployment_json(unemployment_raw)
    unemployment_processed_path = save_unemployment_processed(unemployment_df)

    logger.info("HICP rows: %s", len(hicp_df))
    logger.info("HICP raw file: %s", hicp_raw_path)
    logger.info("HICP processed file: %s", hicp_processed_path)
    logger.info("HICP preview:\n%s", hicp_df.head())

    logger.info("Unemployment rows: %s", len(unemployment_df))
    logger.info("Unemployment raw file: %s", unemployment_raw_path)
    logger.info("Unemployment processed file: %s", unemployment_processed_path)
    logger.info("Unemployment preview:\n%s", unemployment_df.head())


if __name__ == "__main__":
    main()