"""Input and output helpers for PostgreSQL and file-based data operations.

This module contains small reusable utilities for reading SQL query results
into pandas DataFrames and writing DataFrames back to PostgreSQL tables or
local parquet/CSV files.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine


def read_sql_query(query: str, engine: Engine) -> pd.DataFrame:
    """Run a SQL query and return the result as a pandas DataFrame.

    Args:
        query: SQL query string to execute.
        engine: SQLAlchemy engine connected to PostgreSQL.

    Returns:
        A pandas DataFrame containing the query result.
    """
    return pd.read_sql(query, engine)


def write_dataframe_to_table(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "macro_lab",
    if_exists: str = "append",
) -> None:
    """Write a pandas DataFrame to a PostgreSQL table.

    Args:
        df: DataFrame to write.
        table_name: Target PostgreSQL table name.
        engine: SQLAlchemy engine connected to PostgreSQL.
        schema: Target PostgreSQL schema name.
        if_exists: Behavior when the table already exists. Common values are
            `append`, `replace`, or `fail`.
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )


def write_dataframe_to_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Write a DataFrame to a parquet file.

    Args:
        df: DataFrame to write.
        output_path: Destination parquet file path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def write_dataframe_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    """Write a DataFrame to a CSV file.

    Args:
        df: DataFrame to write.
        output_path: Destination CSV file path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)