"""CBRT exchange-rate ingestion utilities for the macro lab project.

This module downloads official CBRT indicative exchange-rate XML files,
parses selected exchange-rate series into tidy pandas DataFrames, and saves
raw and processed outputs for later normalization and database loading.

Current scope:
    - Fetch official CBRT daily exchange-rate XML
    - Save raw XML to `data/raw/cbrt_fx/`
    - Parse TRY/EUR and TRY/USD related values into tidy DataFrames
    - Save processed parquet to `data/processed/cbrt_fx/`

Project output paths used by this module:
    - Raw XML:
        data/raw/cbrt_fx/today.xml
    - Processed parquet:
        data/processed/cbrt_fx/cbrt_fx_rates.parquet
"""

from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from src.config.logging_config import get_logger
from src.config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR


logger = get_logger(__name__)


CBRT_TODAY_XML_URL = "https://www.tcmb.gov.tr/kurlar/today.xml"


def fetch_cbrt_fx_xml() -> str:
    """Fetch the current CBRT exchange-rate XML document.

    Returns:
        Raw XML response text.

    Raises:
        requests.HTTPError: If the HTTP request fails.
    """
    logger.info("Requesting CBRT FX XML: %s", CBRT_TODAY_XML_URL)
    response = requests.get(CBRT_TODAY_XML_URL, timeout=30)
    response.raise_for_status()
    return response.text


def save_raw_cbrt_fx_xml(xml_text: str) -> Path:
    """Save raw CBRT FX XML to the project raw data folder.

    Args:
        xml_text: Raw XML response text.

    Returns:
        Path to the saved XML file.

    Output location:
        data/raw/cbrt_fx/today.xml
    """
    output_path = RAW_DATA_DIR / "cbrt_fx" / "today.xml"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(xml_text, encoding="utf-8")
    logger.info("Saved raw CBRT FX XML to %s", output_path)
    return output_path


def parse_cbrt_fx_xml(xml_text: str) -> pd.DataFrame:
    """Parse the CBRT FX XML into a tidy DataFrame.

    This first version extracts EUR and USD entries and keeps selected fields
    that are useful for the project.

    Args:
        xml_text: Raw XML response text.

    Returns:
        A tidy pandas DataFrame with one row per currency.
    """
    root = ET.fromstring(xml_text)

    date_str = root.attrib.get("Date")
    rows: list[dict[str, object]] = []

    for currency in root.findall("Currency"):
        code = currency.attrib.get("CurrencyCode")

        if code not in {"EUR", "USD"}:
            continue

        def get_text(tag: str) -> str | None:
            element = currency.find(tag)
            if element is None or element.text is None:
                return None
            return element.text.strip()

        rows.append(
            {
                "dataset_code": "cbrt_fx_rates",
                "observation_date": pd.to_datetime(date_str),
                "currency_code": code,
                "forex_buying": pd.to_numeric(get_text("ForexBuying"), errors="coerce"),
                "forex_selling": pd.to_numeric(get_text("ForexSelling"), errors="coerce"),
                "banknote_buying": pd.to_numeric(get_text("BanknoteBuying"), errors="coerce"),
                "banknote_selling": pd.to_numeric(get_text("BanknoteSelling"), errors="coerce"),
            }
        )

    df = pd.DataFrame(rows)
    return df.sort_values(by=["currency_code"]).reset_index(drop=True)


def save_cbrt_fx_processed(df: pd.DataFrame) -> Path:
    """Save processed CBRT FX data to the project processed layer.

    Args:
        df: Processed CBRT FX DataFrame.

    Returns:
        Path to the saved parquet file.

    Output location:
        data/processed/cbrt_fx/cbrt_fx_rates.parquet
    """
    output_path = PROCESSED_DATA_DIR / "cbrt_fx" / "cbrt_fx_rates.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved processed CBRT FX parquet to %s", output_path)
    return output_path