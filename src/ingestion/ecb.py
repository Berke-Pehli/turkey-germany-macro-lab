"""ECB ingestion utilities for the macro lab project.

This module contains helper functions to download selected ECB datasets,
save raw API responses to the local project data directory, and transform
filtered ECB CSV responses into tidy pandas DataFrames for later processing
and database loading.

Current scope:
    - Fetch the ECB deposit facility rate series
    - Save the raw CSV response to `data/raw/ecb/`
    - Parse the filtered CSV response into a tidy pandas DataFrame
    - Save the processed DataFrame to `data/processed/ecb/`

Project output paths used by this module:
    - Raw CSV:
        data/raw/ecb/ecb_deposit_facility_rate_raw.csv
    - Processed parquet:
        data/processed/ecb/ecb_deposit_facility_rate.parquet
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from src.config.logging_config import get_logger
from src.config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR


logger = get_logger(__name__)


ECB_DATA_BASE_URL = "https://data-api.ecb.europa.eu/service/data"


def build_ecb_series_url(
    flow_ref: str,
    series_key: str,
    params: dict[str, str] | None = None,
) -> str:
    """Build an ECB data API URL for a single series request.

    Args:
        flow_ref: ECB flow reference, such as `FM`.
        series_key: ECB series key.
        params: Optional query parameters.

    Returns:
        A complete ECB API request URL.
    """
    base_url = f"{ECB_DATA_BASE_URL}/{flow_ref}/{series_key}"

    if not params:
        return base_url

    query_string = "&".join(f"{key}={value}" for key, value in params.items())
    return f"{base_url}?{query_string}"


def fetch_ecb_csv(
    flow_ref: str,
    series_key: str,
    params: dict[str, str] | None = None,
) -> str:
    """Fetch ECB series content as raw CSV text.

    Args:
        flow_ref: ECB flow reference.
        series_key: ECB series key.
        params: Optional query parameters.

    Returns:
        Raw CSV response text.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    url = build_ecb_series_url(flow_ref=flow_ref, series_key=series_key, params=params)
    logger.info("Requesting ECB series: %s", series_key)
    logger.info("ECB URL: %s", url)

    response = requests.get(
        url,
        headers={"Accept": "text/csv"},
        timeout=30,
    )
    response.raise_for_status()

    return response.text


def save_raw_ecb_csv(csv_text: str, output_path: Path) -> None:
    """Save raw ECB CSV output to disk.

    Args:
        csv_text: Raw CSV response text.
        output_path: Destination file path.

    Notes:
        Parent directories are created automatically if they do not exist.

    Example output:
        data/raw/ecb/ecb_deposit_facility_rate_raw.csv
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(csv_text, encoding="utf-8")
    logger.info("Saved raw ECB CSV to %s", output_path)


def fetch_deposit_facility_rate_raw() -> str:
    """Fetch the ECB deposit facility rate as raw CSV.

    Returns:
        Raw CSV response text.

    Notes:
        This first version fetches one ECB policy-rate series only.
    """
    flow_ref = "FM"
    series_key = "B.U2.EUR.4F.KR.DFR.LEV"

    params = {
        "format": "csvdata",
    }

    return fetch_ecb_csv(
        flow_ref=flow_ref,
        series_key=series_key,
        params=params,
    )


def save_deposit_facility_rate_raw(csv_text: str) -> Path:
    """Save the raw ECB deposit facility rate CSV to the project raw data folder.

    Args:
        csv_text: Raw ECB CSV response text.

    Returns:
        Path to the saved raw CSV file.

    Output location:
        data/raw/ecb/ecb_deposit_facility_rate_raw.csv
    """
    output_path = RAW_DATA_DIR / "ecb" / "ecb_deposit_facility_rate_raw.csv"
    save_raw_ecb_csv(csv_text, output_path)
    return output_path


def parse_deposit_facility_rate_csv(csv_text: str) -> pd.DataFrame:
    """Parse the ECB deposit facility rate CSV into a tidy DataFrame.

    Args:
        csv_text: Raw ECB CSV response text.

    Returns:
        A tidy pandas DataFrame.

    Output columns:
        - dataset_code
        - series_code
        - time_period
        - value

    Notes:
        This parser assumes the ECB response contains `TIME_PERIOD` and
        `OBS_VALUE` columns.
    """
    df = pd.read_csv(StringIO(csv_text))

    rename_map = {
        "TIME_PERIOD": "time_period",
        "OBS_VALUE": "value",
    }
    df = df.rename(columns=rename_map)

    df["dataset_code"] = "ecb_deposit_facility_rate"
    df["series_code"] = "B.U2.EUR.4F.KR.DFR.LEV"

    column_order = [
        "dataset_code",
        "series_code",
        "time_period",
        "value",
    ]
    existing_columns = [col for col in column_order if col in df.columns]

    return df[existing_columns].sort_values(by=["time_period"]).reset_index(drop=True)


def save_deposit_facility_rate_processed(df: pd.DataFrame) -> Path:
    """Save processed ECB deposit facility rate data to the project processed layer.

    Args:
        df: Processed ECB deposit facility rate DataFrame.

    Returns:
        Path to the saved parquet file.

    Output location:
        data/processed/ecb/ecb_deposit_facility_rate.parquet
    """
    output_path = PROCESSED_DATA_DIR / "ecb" / "ecb_deposit_facility_rate.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved processed ECB parquet to %s", output_path)
    return output_path