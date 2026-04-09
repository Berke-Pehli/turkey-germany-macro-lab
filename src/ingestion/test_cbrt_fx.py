"""Simple CBRT FX ingestion test for the macro lab project."""

from src.config.logging_config import get_logger
from src.ingestion.cbrt_fx import (
    fetch_cbrt_fx_xml,
    parse_cbrt_fx_xml,
    save_cbrt_fx_processed,
    save_raw_cbrt_fx_xml,
)


logger = get_logger(__name__)


def main() -> None:
    """Fetch, parse, and save CBRT FX data for testing."""
    raw_xml = fetch_cbrt_fx_xml()
    raw_output_path = save_raw_cbrt_fx_xml(raw_xml)

    df = parse_cbrt_fx_xml(raw_xml)
    processed_output_path = save_cbrt_fx_processed(df)

    logger.info("CBRT FX fetch successful.")
    logger.info("Saved raw file: %s", raw_output_path)
    logger.info("Processed rows: %s", len(df))
    logger.info("Saved processed file: %s", processed_output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()