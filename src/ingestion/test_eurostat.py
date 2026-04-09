"""Simple Eurostat ingestion test for the macro lab project."""

from src.config.logging_config import get_logger
from src.ingestion.eurostat import (
    fetch_hicp_inflation_raw,
    parse_hicp_inflation_json,
    save_hicp_inflation_processed,
    save_hicp_inflation_raw,
)


logger = get_logger(__name__)


def main() -> None:
    """Fetch, parse, and save HICP inflation data for testing."""
    raw_data = fetch_hicp_inflation_raw()
    raw_output_path = save_hicp_inflation_raw(raw_data)

    df = parse_hicp_inflation_json(raw_data)
    processed_output_path = save_hicp_inflation_processed(df)

    logger.info("Eurostat raw fetch successful.")
    logger.info("Saved raw file: %s", raw_output_path)
    logger.info("Processed rows: %s", len(df))
    logger.info("Saved processed file: %s", processed_output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()