"""Simple OECD API ingestion test for the macro lab project."""

from src.config.logging_config import get_logger
from src.config.settings import RAW_DATA_DIR
from src.ingestion.oecd import (
    fetch_oecd_json_from_url,
    parse_oecd_sdmx_json,
    save_oecd_processed,
    save_raw_oecd_json,
)

logger = get_logger(__name__)


def main() -> None:
    """Fetch, parse, and save OECD consumer confidence data for the euro area."""
    query_url = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.STES,DSD_STES@DF_CLI,/EA20.M.LI...AA...H"
        "?dimensionAtObservation=AllDimensions"
        "&format=jsondata"
    )
    file_stub = "oecd_euro_area_consumer_confidence"

    raw_data = fetch_oecd_json_from_url(query_url)

    raw_output_path = RAW_DATA_DIR / "oecd" / f"{file_stub}_raw.json"
    save_raw_oecd_json(raw_data, raw_output_path)

    df = parse_oecd_sdmx_json(raw_data, dataset_code=file_stub)
    processed_output_path = save_oecd_processed(df, file_name=file_stub)

    logger.info("OECD fetch successful.")
    logger.info("Saved raw file: %s", raw_output_path)
    logger.info("Processed rows: %s", len(df))
    logger.info("Saved processed file: %s", processed_output_path)
    logger.info("Columns: %s", list(df.columns))
    logger.info("Preview:\n%s", df.head())


if __name__ == "__main__":
    main()