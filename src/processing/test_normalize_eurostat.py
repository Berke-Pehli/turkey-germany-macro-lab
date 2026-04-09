"""Simple normalization test for Eurostat processed datasets."""

from src.config.logging_config import get_logger
from src.processing.normalize_eurostat import (
    combine_eurostat_normalized,
    save_eurostat_normalized,
)


logger = get_logger(__name__)


def main() -> None:
    """Run a simple Eurostat normalization test."""
    df = combine_eurostat_normalized()
    output_path = save_eurostat_normalized(df)

    logger.info("Normalized Eurostat rows: %s", len(df))
    logger.info("Saved normalized file: %s", output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()