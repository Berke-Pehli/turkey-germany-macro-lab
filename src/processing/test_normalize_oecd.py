"""Simple normalization test for OECD processed datasets."""

from src.config.logging_config import get_logger
from src.processing.normalize_oecd import (
    normalize_oecd_turkiye_cpi,
    save_oecd_normalized,
)


logger = get_logger(__name__)


def main() -> None:
    """Run a simple OECD normalization test."""
    df = normalize_oecd_turkiye_cpi()
    output_path = save_oecd_normalized(df, file_name="oecd_turkiye_cpi_normalized")

    logger.info("Normalized OECD Türkiye CPI rows: %s", len(df))
    logger.info("Saved normalized file: %s", output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()