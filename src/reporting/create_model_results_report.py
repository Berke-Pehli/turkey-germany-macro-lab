"""Create reporting tables and figures for first-round model results.

This script collects benchmark results from the first modeling phase and
creates clean summary tables and comparison charts for fixed-split and
rolling evaluation.

Inputs:
    - hard-coded benchmark results from the first-round modeling scripts
      for:
        - Türkiye
        - Germany
        - Euro area
        - naive benchmark
        - linear regression benchmark
        - ridge benchmark where available

Outputs:
    - outputs/tables/model_results_summary.csv
    - outputs/tables/best_models_summary.csv
    - outputs/tables/model_improvement_vs_naive.csv
    - outputs/tables/reporting_notes_summary.csv
    - outputs/figures/fixed_split_mae_comparison.png
    - outputs/figures/fixed_split_rmse_comparison.png
    - outputs/figures/rolling_mae_comparison.png
    - outputs/figures/rolling_rmse_comparison.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.config.logging_config import get_logger


logger = get_logger(__name__)


def get_project_root() -> Path:
    """Return the project root directory.

    Returns:
        The root directory of the project based on the location of this
        script file.
    """
    return Path(__file__).resolve().parents[2]


def get_outputs_dirs() -> tuple[Path, Path]:
    """Create and return reporting output directories.

    Returns:
        A tuple containing:
            - the tables output directory
            - the figures output directory
    """
    project_root = get_project_root()
    tables_dir = project_root / "outputs" / "tables"
    figures_dir = project_root / "outputs" / "figures"

    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    return tables_dir, figures_dir


def build_results_dataframe() -> pd.DataFrame:
    """Build the combined benchmark results table.

    Returns:
        A DataFrame containing fixed-split and rolling benchmark results
        across countries and models.
    """
    results = pd.DataFrame(
        [
            # Fixed split results
            {
                "country": "TR",
                "evaluation": "fixed_split",
                "model": "naive",
                "mae": 5.150353,
                "rmse": 8.108454,
            },
            {
                "country": "TR",
                "evaluation": "fixed_split",
                "model": "linear_regression",
                "mae": 5.398590,
                "rmse": 8.234951,
            },
            {
                "country": "TR",
                "evaluation": "fixed_split",
                "model": "ridge",
                "mae": 5.403300,
                "rmse": 8.243200,
            },
            {
                "country": "DE",
                "evaluation": "fixed_split",
                "model": "naive",
                "mae": 0.775904,
                "rmse": 1.077816,
            },
            {
                "country": "DE",
                "evaluation": "fixed_split",
                "model": "linear_regression",
                "mae": 0.829860,
                "rmse": 1.225512,
            },
            {
                "country": "DE",
                "evaluation": "fixed_split",
                "model": "ridge",
                "mae": 0.845900,
                "rmse": 1.244600,
            },
            {
                "country": "EA",
                "evaluation": "fixed_split",
                "model": "naive",
                "mae": 0.595181,
                "rmse": 0.812256,
            },
            {
                "country": "EA",
                "evaluation": "fixed_split",
                "model": "linear_regression",
                "mae": 0.556241,
                "rmse": 0.758246,
            },
            {
                "country": "EA",
                "evaluation": "fixed_split",
                "model": "ridge",
                "mae": 0.557300,
                "rmse": 0.763100,
            },
            # Rolling results
            {
                "country": "TR",
                "evaluation": "rolling",
                "model": "naive",
                "mae": 3.8966,
                "rmse": 6.6393,
            },
            {
                "country": "TR",
                "evaluation": "rolling",
                "model": "linear_regression",
                "mae": 3.8648,
                "rmse": 6.7155,
            },
            {
                "country": "DE",
                "evaluation": "rolling",
                "model": "naive",
                "mae": 0.6346,
                "rmse": 0.9124,
            },
            {
                "country": "DE",
                "evaluation": "rolling",
                "model": "linear_regression",
                "mae": 0.5881,
                "rmse": 0.8308,
            },
            {
                "country": "EA",
                "evaluation": "rolling",
                "model": "naive",
                "mae": 0.4877,
                "rmse": 0.6895,
            },
            {
                "country": "EA",
                "evaluation": "rolling",
                "model": "linear_regression",
                "mae": 0.4513,
                "rmse": 0.6153,
            },
        ]
    )

    country_order = ["TR", "DE", "EA"]
    model_order = ["naive", "linear_regression", "ridge"]
    evaluation_order = ["fixed_split", "rolling"]

    results["country"] = pd.Categorical(
        results["country"], categories=country_order, ordered=True
    )
    results["model"] = pd.Categorical(
        results["model"], categories=model_order, ordered=True
    )
    results["evaluation"] = pd.Categorical(
        results["evaluation"], categories=evaluation_order, ordered=True
    )

    return results.sort_values(["evaluation", "country", "model"]).reset_index(drop=True)


def build_best_models_summary(results: pd.DataFrame) -> pd.DataFrame:
    """Build a summary table of best models by country and metric.

    Args:
        results: Full benchmark results table.

    Returns:
        A DataFrame showing the best model for each country, evaluation
        type, and metric.
    """
    rows: list[dict[str, str | float]] = []

    for evaluation in results["evaluation"].cat.categories:
        subset = results[results["evaluation"] == evaluation].copy()
        if subset.empty:
            continue

        best_mae = subset.loc[subset.groupby("country", observed=False)["mae"].idxmin()]
        best_rmse = subset.loc[
            subset.groupby("country", observed=False)["rmse"].idxmin()
        ]

        for _, row in best_mae.iterrows():
            rows.append(
                {
                    "evaluation": evaluation,
                    "country": row["country"],
                    "metric": "mae",
                    "best_model": row["model"],
                    "metric_value": row["mae"],
                }
            )

        for _, row in best_rmse.iterrows():
            rows.append(
                {
                    "evaluation": evaluation,
                    "country": row["country"],
                    "metric": "rmse",
                    "best_model": row["model"],
                    "metric_value": row["rmse"],
                }
            )

    summary = pd.DataFrame(rows).sort_values(
        by=["evaluation", "country", "metric"]
    ).reset_index(drop=True)

    return summary


def build_improvement_vs_naive(results: pd.DataFrame) -> pd.DataFrame:
    """Build a table comparing each model against the naive benchmark.

    Negative differences mean the model improved relative to the naive
    benchmark because lower error values are better.

    Args:
        results: Full benchmark results table.

    Returns:
        A DataFrame summarizing MAE and RMSE differences versus the naive
        benchmark for each country, evaluation type, and non-naive model.
    """
    rows: list[dict[str, str | float]] = []

    for evaluation in results["evaluation"].cat.categories:
        for country in results["country"].cat.categories:
            subset = results[
                (results["evaluation"] == evaluation) & (results["country"] == country)
            ].copy()

            if subset.empty:
                continue

            naive_row = subset[subset["model"] == "naive"]
            if naive_row.empty:
                continue

            naive_mae = float(naive_row["mae"].iloc[0])
            naive_rmse = float(naive_row["rmse"].iloc[0])

            comparison_rows = subset[subset["model"] != "naive"]

            for _, row in comparison_rows.iterrows():
                rows.append(
                    {
                        "evaluation": evaluation,
                        "country": country,
                        "model": row["model"],
                        "naive_mae": naive_mae,
                        "model_mae": float(row["mae"]),
                        "mae_difference_vs_naive": float(row["mae"]) - naive_mae,
                        "naive_rmse": naive_rmse,
                        "model_rmse": float(row["rmse"]),
                        "rmse_difference_vs_naive": float(row["rmse"]) - naive_rmse,
                    }
                )

    improvement_df = pd.DataFrame(rows).sort_values(
        by=["evaluation", "country", "model"]
    ).reset_index(drop=True)

    return improvement_df


def build_reporting_notes_summary() -> pd.DataFrame:
    """Build a compact interpretation table for reporting and dashboard use.

    Returns:
        A DataFrame summarizing the main interpretation for each country
        and evaluation type.
    """
    notes = pd.DataFrame(
        [
            {
                "evaluation": "fixed_split",
                "country": "TR",
                "best_model_mae": "naive",
                "best_model_rmse": "naive",
                "main_interpretation": "Naive benchmark outperformed linear and ridge models.",
            },
            {
                "evaluation": "fixed_split",
                "country": "DE",
                "best_model_mae": "naive",
                "best_model_rmse": "naive",
                "main_interpretation": "Naive benchmark outperformed linear and ridge models.",
            },
            {
                "evaluation": "fixed_split",
                "country": "EA",
                "best_model_mae": "linear_regression",
                "best_model_rmse": "linear_regression",
                "main_interpretation": "Linear regression improved on the naive benchmark.",
            },
            {
                "evaluation": "rolling",
                "country": "TR",
                "best_model_mae": "linear_regression",
                "best_model_rmse": "naive",
                "main_interpretation": "Mixed result: linear regression improved MAE, but naive remained better on RMSE.",
            },
            {
                "evaluation": "rolling",
                "country": "DE",
                "best_model_mae": "linear_regression",
                "best_model_rmse": "linear_regression",
                "main_interpretation": "Linear regression clearly improved on the naive benchmark.",
            },
            {
                "evaluation": "rolling",
                "country": "EA",
                "best_model_mae": "linear_regression",
                "best_model_rmse": "linear_regression",
                "main_interpretation": "Linear regression clearly improved on the naive benchmark.",
            },
        ]
    )

    return notes.sort_values(["evaluation", "country"]).reset_index(drop=True)


def save_tables(
    results: pd.DataFrame,
    best_models: pd.DataFrame,
    improvement_vs_naive: pd.DataFrame,
    reporting_notes: pd.DataFrame,
    tables_dir: Path,
) -> None:
    """Save reporting tables to CSV files.

    Args:
        results: Full benchmark results table.
        best_models: Best-model summary table.
        improvement_vs_naive: Model-improvement summary table.
        reporting_notes: Compact interpretation summary table.
        tables_dir: Output directory for tables.
    """
    results_path = tables_dir / "model_results_summary.csv"
    best_models_path = tables_dir / "best_models_summary.csv"
    improvement_path = tables_dir / "model_improvement_vs_naive.csv"
    notes_path = tables_dir / "reporting_notes_summary.csv"

    results.to_csv(results_path, index=False)
    best_models.to_csv(best_models_path, index=False)
    improvement_vs_naive.to_csv(improvement_path, index=False)
    reporting_notes.to_csv(notes_path, index=False)

    logger.info("Saved results summary table: %s", results_path)
    logger.info("Saved best-model summary table: %s", best_models_path)
    logger.info("Saved improvement-vs-naive summary table: %s", improvement_path)
    logger.info("Saved reporting notes summary table: %s", notes_path)


def add_bar_value_labels(ax: plt.Axes) -> None:
    """Add numeric value labels above each bar.

    Args:
        ax: Matplotlib axes object containing bar patches.
    """
    for patch in ax.patches:
        height = patch.get_height()
        if pd.isna(height):
            continue

        ax.annotate(
            f"{height:.3f}",
            (patch.get_x() + patch.get_width() / 2, height),
            ha="center",
            va="bottom",
            xytext=(0, 3),
            textcoords="offset points",
            fontsize=9,
        )


def annotate_best_bars(ax: plt.Axes, pivot_df: pd.DataFrame) -> None:
    """Annotate the best-performing bar for each country.

    A small 'Best' label is placed above the lowest bar in each country
    because lower error values are better.

    Args:
        ax: Matplotlib axes object containing the plotted bars.
        pivot_df: Pivoted DataFrame used to create the grouped bar chart.
    """
    n_countries = len(pivot_df.index)
    n_models = len(pivot_df.columns)

    if n_countries == 0 or n_models == 0:
        return

    patches = ax.patches

    for country_position, (_, row) in enumerate(pivot_df.iterrows()):
        valid_row = row.dropna()
        if valid_row.empty:
            continue

        best_model = valid_row.idxmin()
        model_position = list(pivot_df.columns).index(best_model)
        patch_index = model_position * n_countries + country_position

        if patch_index >= len(patches):
            continue

        patch = patches[patch_index]
        height = patch.get_height()

        ax.annotate(
            "Best",
            (patch.get_x() + patch.get_width() / 2, height),
            ha="center",
            va="bottom",
            xytext=(0, 18),
            textcoords="offset points",
            fontsize=9,
            fontweight="bold",
        )


def get_display_model_names() -> dict[str, str]:
    """Return presentation-friendly model labels.

    Returns:
        A dictionary mapping internal model names to display labels.
    """
    return {
        "naive": "Naive",
        "linear_regression": "Linear Regression",
        "ridge": "Ridge",
    }


def plot_metric_comparison(
    results: pd.DataFrame,
    evaluation: str,
    metric: str,
    output_path: Path,
) -> None:
    """Create a clean grouped bar chart for one metric and evaluation type.

    The figure includes value labels, a 'Best' annotation for the
    lowest-error bar in each country, and a legend placed outside the
    plot area for cleaner presentation.

    Args:
        results: Full benchmark results table.
        evaluation: Evaluation type to plot, such as 'fixed_split' or
            'rolling'.
        metric: Metric to plot, such as 'mae' or 'rmse'.
        output_path: File path for the output figure.
    """
    subset = results[results["evaluation"] == evaluation].copy()
    pivot_df = subset.pivot(index="country", columns="model", values=metric)

    desired_country_order = ["TR", "DE", "EA"]
    pivot_df = pivot_df.reindex(desired_country_order)
    pivot_df = pivot_df.dropna(how="all")

    display_names = get_display_model_names()
    renamed_columns = [display_names.get(col, col) for col in pivot_df.columns]
    pivot_df.columns = renamed_columns

    ax = pivot_df.plot(kind="bar", figsize=(11.5, 6.5), width=0.8)

    title_metric = metric.upper()
    title_eval = evaluation.replace("_", " ").title()

    if evaluation == "fixed_split":
        subtitle = "Lower is better | Train: before 2019-01 | Test: from 2019-01"
    else:
        subtitle = "Lower is better | Expanding window | One-step-ahead forecast"

    ax.set_title(f"{title_metric} Comparison - {title_eval}\n{subtitle}")
    ax.set_xlabel("Country")
    ax.set_ylabel(title_metric)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.xticks(rotation=0)

    ax.legend(
        title="Model",
        loc="upper left",
        bbox_to_anchor=(1.01, 1.0),
        frameon=True,
    )

    add_bar_value_labels(ax)
    annotate_best_bars(ax, pivot_df)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("Saved figure: %s", output_path)


def main() -> None:
    """Create reporting tables and figures for the first model results."""
    tables_dir, figures_dir = get_outputs_dirs()

    results = build_results_dataframe()
    best_models = build_best_models_summary(results)
    improvement_vs_naive = build_improvement_vs_naive(results)
    reporting_notes = build_reporting_notes_summary()

    save_tables(
        results=results,
        best_models=best_models,
        improvement_vs_naive=improvement_vs_naive,
        reporting_notes=reporting_notes,
        tables_dir=tables_dir,
    )

    plot_metric_comparison(
        results=results,
        evaluation="fixed_split",
        metric="mae",
        output_path=figures_dir / "fixed_split_mae_comparison.png",
    )
    plot_metric_comparison(
        results=results,
        evaluation="fixed_split",
        metric="rmse",
        output_path=figures_dir / "fixed_split_rmse_comparison.png",
    )
    plot_metric_comparison(
        results=results,
        evaluation="rolling",
        metric="mae",
        output_path=figures_dir / "rolling_mae_comparison.png",
    )
    plot_metric_comparison(
        results=results,
        evaluation="rolling",
        metric="rmse",
        output_path=figures_dir / "rolling_rmse_comparison.png",
    )

    logger.info("Reporting layer completed.")
    logger.info("Results preview:\n%s", results)
    logger.info("Best models preview:\n%s", best_models)
    logger.info("Improvement-vs-naive preview:\n%s", improvement_vs_naive)
    logger.info("Reporting notes preview:\n%s", reporting_notes)


if __name__ == "__main__":
    main()