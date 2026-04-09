"""Simple normalization test for CBRT FX processed datasets."""

from src.config.logging_config import get_logger
from src.processing.normalize_cbrt_fx import (
    normalize_cbrt_fx_rates,
    save_cbrt_fx_normalized,
)


logger = get_logger(__name__)


def main() -> None:
    """Run a simple CBRT FX normalization test."""
    df = normalize_cbrt_fx_rates()
    output_path = save_cbrt_fx_normalized(df)

    logger.info("Normalized CBRT FX rows: %s", len(df))
    logger.info("Saved normalized file: %s", output_path)
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()