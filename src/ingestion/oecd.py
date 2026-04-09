"""OECD API ingestion utilities for the macro lab project.

This module contains helper functions to download selected OECD datasets,
save raw SDMX-JSON responses to the local project data directory, and
transform the responses into tidy pandas DataFrames for later processing
and database loading.

Current scope:
    - Fetch OECD SDMX-JSON data via the public OECD API
    - Save the raw JSON response to `data/raw/oecd/`
    - Parse the response into a tidy pandas DataFrame
    - Save the processed DataFrame to `data/processed/oecd/`
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from src.config.logging_config import get_logger
from src.config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR


logger = get_logger(__name__)


def fetch_oecd_json_from_url(query_url: str) -> dict:
    """Fetch OECD dataset content as JSON from a full query URL.

    Args:
        query_url: Full OECD API query URL.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    logger.info("Requesting OECD URL: %s", query_url)

    response = requests.get(
        query_url,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def save_raw_oecd_json(data: dict, output_path: Path) -> None:
    """Save raw OECD JSON output to disk.

    Args:
        data: Parsed OECD JSON response.
        output_path: Destination file path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)

    logger.info("Saved raw OECD JSON to %s", output_path)


def parse_oecd_sdmx_json(data: dict, dataset_code: str) -> pd.DataFrame:
    """Parse OECD SDMX-JSON into a tidy DataFrame.

    This parser supports both:
        - series-based SDMX JSON responses
        - dataset-level observation responses such as those produced when
          `dimensionAtObservation=AllDimensions` is used

    Args:
        data: Raw OECD JSON response.
        dataset_code: Friendly dataset code label for the output.

    Returns:
        A tidy pandas DataFrame with one row per observation.
    """
    data_section = data["data"]
    structure = data_section["structures"][0]
    data_set = data_section["dataSets"][0]

    dimensions_block = structure["dimensions"]
    series_dimensions = dimensions_block.get("series", [])
    observation_dimensions = dimensions_block.get("observation", [])

    series_dim_names = [dim["id"] for dim in series_dimensions]
    observation_dim_names = [dim["id"] for dim in observation_dimensions]

    series_maps: list[dict[int, str]] = []
    for dim in series_dimensions:
        series_maps.append({idx: value["id"] for idx, value in enumerate(dim["values"])})

    observation_maps: list[dict[int, str]] = []
    for dim in observation_dimensions:
        observation_maps.append({idx: value["id"] for idx, value in enumerate(dim["values"])})

    rows: list[dict[str, object]] = []

    # Case 1: series-based response
    for series_key, series_content in data_set.get("series", {}).items():
        series_positions = [int(x) for x in series_key.split(":")] if series_key else []

        series_labels = {}
        for i, pos in enumerate(series_positions):
            if i < len(series_dim_names):
                series_labels[series_dim_names[i]] = series_maps[i][pos]

        for obs_key, obs_value in series_content.get("observations", {}).items():
            obs_positions = [int(x) for x in obs_key.split(":")] if obs_key else []

            observation_labels = {}
            for i, pos in enumerate(obs_positions):
                if i < len(observation_dim_names):
                    observation_labels[observation_dim_names[i]] = observation_maps[i][pos]

            value = obs_value[0] if isinstance(obs_value, list) else obs_value

            row = {
                "dataset_code": dataset_code,
                "value": pd.to_numeric(value, errors="coerce"),
            }
            row.update(series_labels)
            row.update(observation_labels)
            rows.append(row)

    # Case 2: dataset-level observations
    if not rows and "observations" in data_set:
        for obs_key, obs_value in data_set["observations"].items():
            obs_positions = [int(x) for x in obs_key.split(":")] if obs_key else []

            observation_labels = {}
            for i, pos in enumerate(obs_positions):
                if i < len(observation_dim_names):
                    observation_labels[observation_dim_names[i]] = observation_maps[i][pos]

            value = obs_value[0] if isinstance(obs_value, list) else obs_value

            row = {
                "dataset_code": dataset_code,
                "value": pd.to_numeric(value, errors="coerce"),
            }
            row.update(observation_labels)
            rows.append(row)

    df = pd.DataFrame(rows)
    return df.reset_index(drop=True)


def save_oecd_processed(df: pd.DataFrame, file_name: str) -> Path:
    """Save processed OECD data to the project processed layer.

    Args:
        df: Processed OECD DataFrame.
        file_name: File-friendly output name without extension.

    Returns:
        Path to the saved parquet file.
    """
    output_path = PROCESSED_DATA_DIR / "oecd" / f"{file_name}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved processed OECD parquet to %s", output_path)
    return output_path