"""Simple ECB ingestion test for the macro lab project."""

from src.config.logging_config import get_logger
from src.ingestion.ecb import (
    fetch_deposit_facility_rate_raw,
    parse_deposit_facility_rate_csv,
    save_deposit_facility_rate_processed,
    save_deposit_facility_rate_raw,
)


logger = get_logger(__name__)


def main() -> None:
    """Fetch, parse, and save ECB deposit facility rate data for testing."""
    raw_csv = fetch_deposit_facility_rate_raw()
    raw_output_path = save_deposit_facility_rate_raw(raw_csv)

    df = parse_deposit_facility_rate_csv(raw_csv)
    processed_output_path = save_deposit_facility_rate_processed(df)

    logger.info("ECB raw fetch successful.")
    logger.info("Saved raw file: %s", raw_output_path)
    logger.info("Processed rows: %s", len(df))
    logger.info("Saved processed file: %s", processed_output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()