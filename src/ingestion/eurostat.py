"""Eurostat ingestion utilities for the macro lab project.

This module contains helper functions to download selected Eurostat datasets,
save raw API responses to the local project data directory, and transform
filtered Eurostat JSON responses into tidy pandas DataFrames for later
processing and database loading.

Current scope:
    - Fetch raw HICP inflation data from Eurostat for Germany and the euro area
    - Save the raw JSON response to `data/raw/eurostat/`
    - Parse the filtered JSON response into a tidy DataFrame
    - Save the processed DataFrame to `data/processed/eurostat/`

Project output paths used by this module:
    - Raw JSON:
        data/raw/eurostat/prc_hicp_manr_raw.json
    - Processed parquet:
        data/processed/eurostat/prc_hicp_manr.parquet
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
import requests

from src.config.logging_config import get_logger
from src.config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR


logger = get_logger(__name__)


EUROSTAT_BASE_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
)


def build_eurostat_url(
    dataset_code: str,
    params: list[tuple[str, str]] | None = None,
) -> str:
    """Build a Eurostat API URL for a dataset request.

    Args:
        dataset_code: Eurostat dataset code, such as `prc_hicp_manr`.
        params: Ordered query parameters for the Eurostat API. A list of
            key-value tuples is used so repeated parameters like `geo=DE`
            and `geo=EA` can both be included.

    Returns:
        A complete Eurostat API request URL.

    Example:
        A request for filtered HICP inflation data may produce a URL like:
        `.../prc_hicp_manr?freq=M&unit=RCH_A&coicop=CP00&geo=DE&geo=EA`
    """
    if not params:
        return f"{EUROSTAT_BASE_URL}/{dataset_code}"

    query_string = urlencode(params, doseq=True)
    return f"{EUROSTAT_BASE_URL}/{dataset_code}?{query_string}"


def fetch_eurostat_json(
    dataset_code: str,
    params: list[tuple[str, str]] | None = None,
) -> dict:
    """Fetch Eurostat dataset content as JSON.

    Args:
        dataset_code: Eurostat dataset code.
        params: Ordered query parameters for the Eurostat API.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        requests.HTTPError: If the API request fails.

    Notes:
        This function only fetches data from Eurostat. It does not save
        anything to disk. Saving is handled by separate helper functions.
    """
    url = build_eurostat_url(dataset_code, params=params)
    logger.info("Requesting Eurostat dataset: %s", dataset_code)
    logger.info("Eurostat URL: %s", url)

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    return response.json()


def save_raw_eurostat_json(data: dict, output_path: Path) -> None:
    """Save raw Eurostat JSON output to disk.

    Args:
        data: Parsed JSON response from Eurostat.
        output_path: Destination file path for the raw JSON file.

    Notes:
        Parent directories are created automatically if they do not exist.
        This function is used for traceability so that the original API
        response can be inspected later.

    Example output:
        data/raw/eurostat/prc_hicp_manr_raw.json
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    logger.info("Saved raw Eurostat JSON to %s", output_path)


def fetch_hicp_inflation_raw() -> dict:
    """Fetch raw HICP inflation data for Germany and the euro area.

    This request is intentionally filtered to keep the response small and
    relevant for the project.

    Dataset:
        prc_hicp_manr

    Filters:
        - freq=M: monthly data
        - unit=RCH_A: annual rate of change
        - coicop=CP00: all-items HICP
        - geo=DE and geo=EA: Germany and euro area

    Returns:
        Parsed Eurostat JSON response.

    Notes:
        This function only returns the raw JSON response. To save the raw
        output locally, use `save_hicp_inflation_raw`.
    """
    params = [
        ("freq", "M"),
        ("unit", "RCH_A"),
        ("coicop", "CP00"),
        ("geo", "DE"),
        ("geo", "EA"),
    ]

    return fetch_eurostat_json(
        dataset_code="prc_hicp_manr",
        params=params,
    )


def save_hicp_inflation_raw(data: dict) -> Path:
    """Save raw HICP inflation data to the project raw data folder.

    Args:
        data: Parsed Eurostat JSON response for the filtered HICP request.

    Returns:
        Path to the saved raw JSON file.

    Output location:
        data/raw/eurostat/prc_hicp_manr_raw.json
    """
    output_path = RAW_DATA_DIR / "eurostat" / "prc_hicp_manr_raw.json"
    save_raw_eurostat_json(data, output_path)
    return output_path


def parse_hicp_inflation_json(data: dict) -> pd.DataFrame:
    """Parse raw Eurostat HICP inflation JSON into a tidy DataFrame.

    This parser is tailored to the filtered `prc_hicp_manr` request used in
    this project. It extracts Germany and euro area inflation values by time
    from the Eurostat JSON structure and returns a row-based table.

    Args:
        data: Raw Eurostat JSON response.

    Returns:
        A tidy pandas DataFrame with one row per country and month.

    Raises:
        KeyError: If expected Eurostat JSON keys are missing.

    Output columns:
        - dataset_code
        - country_code
        - time_period
        - frequency_code
        - unit
        - coicop
        - value

    Notes:
        This function does not write anything to disk. To save the processed
        result, use `save_hicp_inflation_processed`.
    """
    dataset_code = "prc_hicp_manr"

    id_order = data["id"]
    size = data["size"]
    values = data["value"]
    dimensions = data["dimension"]

    position_maps: dict[str, dict[int, str]] = {}
    for dim_name in id_order:
        category_index = dimensions[dim_name]["category"]["index"]
        reverse_map = {position: label for label, position in category_index.items()}
        position_maps[dim_name] = reverse_map

    rows: list[dict[str, object]] = []

    for flat_index_str, value in values.items():
        flat_index = int(flat_index_str)
        remainder = flat_index
        positions: list[int] = []

        for dim_size in reversed(size):
            positions.append(remainder % dim_size)
            remainder //= dim_size

        positions.reverse()

        row = {
            "dataset_code": dataset_code,
            "value": value,
        }

        for dim_name, pos in zip(id_order, positions):
            row[dim_name] = position_maps[dim_name][pos]

        rows.append(row)

    df = pd.DataFrame(rows)

    rename_map = {
        "geo": "country_code",
        "time": "time_period",
        "freq": "frequency_code",
    }
    df = df.rename(columns=rename_map)

    column_order = [
        "dataset_code",
        "country_code",
        "time_period",
        "frequency_code",
        "unit",
        "coicop",
        "value",
    ]

    existing_columns = [col for col in column_order if col in df.columns]

    return (
        df[existing_columns]
        .sort_values(by=["country_code", "time_period"])
        .reset_index(drop=True)
    )


def save_hicp_inflation_processed(df: pd.DataFrame) -> Path:
    """Save processed HICP inflation data to the project processed data folder.

    Args:
        df: Processed tidy HICP inflation DataFrame.

    Returns:
        Path to the saved parquet file.

    Output location:
        data/processed/eurostat/prc_hicp_manr.parquet

    Notes:
        Parent directories are created automatically if they do not exist.
        This processed file is intended to support later normalization,
        analysis, and database loading steps.
    """
    output_path = PROCESSED_DATA_DIR / "eurostat" / "prc_hicp_manr.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved processed Eurostat parquet to %s", output_path)
    return output_path