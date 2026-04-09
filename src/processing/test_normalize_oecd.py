"""Simple normalization test for OECD processed datasets."""

from src.config.logging_config import get_logger
from src.processing.normalize_oecd import (
    normalize_oecd_euro_area_business_confidence,
    normalize_oecd_germany_business_confidence,
    normalize_oecd_germany_consumer_confidence,
    normalize_oecd_turkiye_business_confidence,
    normalize_oecd_turkiye_consumer_confidence,
    normalize_oecd_turkiye_cpi,
    normalize_oecd_turkiye_industrial_production,
    normalize_oecd_turkiye_unemployment,
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

    confidence_df = normalize_oecd_turkiye_business_confidence()
    confidence_output_path = save_oecd_normalized(
        confidence_df,
        file_name="oecd_turkiye_business_confidence_normalized",
    )

    germany_confidence_df = normalize_oecd_germany_business_confidence()
    germany_confidence_output_path = save_oecd_normalized(
        germany_confidence_df,
        file_name="oecd_germany_business_confidence_normalized",
    )

    euro_area_confidence_df = normalize_oecd_euro_area_business_confidence()
    euro_area_confidence_output_path = save_oecd_normalized(
        euro_area_confidence_df,
        file_name="oecd_euro_area_business_confidence_normalized",
    )

    consumer_confidence_df = normalize_oecd_turkiye_consumer_confidence()
    consumer_confidence_output_path = save_oecd_normalized(
        consumer_confidence_df,
        file_name="oecd_turkiye_consumer_confidence_normalized",
    )

    germany_consumer_confidence_df = normalize_oecd_germany_consumer_confidence()
    germany_consumer_confidence_output_path = save_oecd_normalized(
        germany_consumer_confidence_df,
        file_name="oecd_germany_consumer_confidence_normalized",
    )

    logger.info("Normalized OECD Türkiye CPI rows: %s", len(cpi_df))
    logger.info("Saved CPI normalized file: %s", cpi_output_path)
    logger.info("CPI preview:\n%s", cpi_df.head())

    logger.info("Normalized OECD Türkiye unemployment rows: %s", len(unemployment_df))
    logger.info("Saved unemployment normalized file: %s", unemployment_output_path)
    logger.info("Unemployment preview:\n%s", unemployment_df.head())

    logger.info(
        "Normalized OECD Türkiye industrial production rows: %s",
        len(industrial_df),
    )
    logger.info(
        "Saved industrial production normalized file: %s",
        industrial_output_path,
    )
    logger.info("Industrial production preview:\n%s", industrial_df.head())

    logger.info(
        "Normalized OECD Türkiye business confidence rows: %s",
        len(confidence_df),
    )
    logger.info(
        "Saved business confidence normalized file: %s",
        confidence_output_path,
    )
    logger.info("Business confidence preview:\n%s", confidence_df.head())

    logger.info(
        "Normalized OECD Germany business confidence rows: %s",
        len(germany_confidence_df),
    )
    logger.info(
        "Saved Germany business confidence normalized file: %s",
        germany_confidence_output_path,
    )
    logger.info("Germany business confidence preview:\n%s", germany_confidence_df.head())

    logger.info(
        "Normalized OECD euro area business confidence rows: %s",
        len(euro_area_confidence_df),
    )
    logger.info(
        "Saved euro area business confidence normalized file: %s",
        euro_area_confidence_output_path,
    )
    logger.info("Euro area business confidence preview:\n%s", euro_area_confidence_df.head())

    logger.info(
        "Normalized OECD Türkiye consumer confidence rows: %s",
        len(consumer_confidence_df),
    )
    logger.info(
        "Saved Türkiye consumer confidence normalized file: %s",
        consumer_confidence_output_path,
    )
    logger.info("Türkiye consumer confidence preview:\n%s", consumer_confidence_df.head())

    logger.info(
        "Normalized OECD Germany consumer confidence rows: %s",
        len(germany_consumer_confidence_df),
    )
    logger.info(
        "Saved Germany consumer confidence normalized file: %s",
        germany_consumer_confidence_output_path,
    )
    logger.info("Germany consumer confidence preview:\n%s", germany_consumer_confidence_df.head())


if __name__ == "__main__":
    main()