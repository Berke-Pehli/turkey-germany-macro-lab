"""Simple normalization test for ECB processed datasets."""

from src.config.logging_config import get_logger
from src.processing.normalize_ecb import (
    normalize_deposit_facility_rate,
    save_ecb_normalized,
)


logger = get_logger(__name__)


def main() -> None:
    """Run a simple ECB normalization test."""
    df = normalize_deposit_facility_rate()
    output_path = save_ecb_normalized(df)

    logger.info("Normalized ECB rows: %s", len(df))
    logger.info("Saved normalized file: %s", output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()