"""Export model input views from PostgreSQL to parquet files."""

from __future__ import annotations

from pathlib import Path

from src.config.logging_config import get_logger
from src.config.settings import FINAL_DATA_DIR
from src.db.engine import get_engine
from src.db.io import read_sql_query, write_dataframe_to_parquet


logger = get_logger(__name__)


def export_view_to_parquet(view_name: str, output_file_name: str) -> Path:
    """Export a PostgreSQL view to a parquet file."""
    engine = get_engine()
    query = f"SELECT * FROM {view_name} ORDER BY year_month"
    df = read_sql_query(query, engine)

    output_path = FINAL_DATA_DIR / "model_inputs" / output_file_name
    write_dataframe_to_parquet(df, output_path)

    logger.info("Exported %s rows from %s", len(df), view_name)
    logger.info("Saved parquet file: %s", output_path)

    return output_path


def main() -> None:
    """Export project model input views to parquet files."""
    tr_path = export_view_to_parquet(
        "macro_lab.vw_macro_model_input_tr_v1",
        "model_input_tr_v1.parquet",
    )
    de_path = export_view_to_parquet(
        "macro_lab.vw_macro_model_input_de_v1",
        "model_input_de_v1.parquet",
    )
    ea_path = export_view_to_parquet(
        "macro_lab.vw_macro_model_input_ea_v1",
        "model_input_ea_v1.parquet",
    )

    logger.info("TR export path: %s", tr_path)
    logger.info("DE export path: %s", de_path)
    logger.info("EA export path: %s", ea_path)


if __name__ == "__main__":
    main()