"""Simple normalization test for OECD processed datasets."""

from src.config.logging_config import get_logger
from src.processing.normalize_oecd import (
    normalize_oecd_turkiye_cpi,
    normalize_oecd_turkiye_unemployment,
    normalize_oecd_turkiye_industrial_production,
    save_oecd_normalized,
)


logger = get_logger(__name__)


def main() -> None:
    """Run simple OECD normalization tests."""
    cpi_df = normalize_oecd_turkiye_cpi()
    cpi_output_path = save_oecd_normalized(
        cpi_df,
        file_name="oecd_turkiye_cpi_normalized",
    )

    unemployment_df = normalize_oecd_turkiye_unemployment()
    unemployment_output_path = save_oecd_normalized(
        unemployment_df,
        file_name="oecd_turkiye_unemployment_normalized",
    )

    industrial_df = normalize_oecd_turkiye_industrial_production()
    industrial_output_path = save_oecd_normalized(
        industrial_df,
        file_name="oecd_turkiye_industrial_production_normalized",
    )

    logger.info("Normalized OECD Türkiye CPI rows: %s", len(cpi_df))
    logger.info("Saved CPI normalized file: %s", cpi_output_path)
    logger.info("CPI preview:\n%s", cpi_df.head())

    logger.info("Normalized OECD Türkiye unemployment rows: %s", len(unemployment_df))
    logger.info("Saved unemployment normalized file: %s", unemployment_output_path)
    logger.info("Unemployment preview:\n%s", unemployment_df.head())

    logger.info("Normalized OECD Türkiye industrial production rows: %s", len(industrial_df))
    logger.info("Saved industrial production normalized file: %s", industrial_output_path)
    logger.info("Industrial production preview:\n%s", industrial_df.head())


if __name__ == "__main__":
    main()