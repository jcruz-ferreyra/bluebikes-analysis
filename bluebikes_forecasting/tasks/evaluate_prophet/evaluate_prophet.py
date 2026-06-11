# tasks/evaluate_prophet/evaluate_prophet.py

from datetime import datetime, timedelta
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from prophet import Prophet

from .types import EvaluateProphetContext

logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _find_station_files(ctx: EvaluateProphetContext) -> list[Path]:
    """
    Find all station timeseries CSV files.

    Args:
        ctx: EvaluateProphetContext with configuration

    Returns:
        List of paths to station CSV files

    Raises:
        FileNotFoundError: If directory doesn't exist or no files found
    """
    if not ctx.station_timeseries_dir.exists():
        raise FileNotFoundError(
            f"Station timeseries directory not found: {ctx.station_timeseries_dir}\n"
            f"Please run generate_timeseries task first."
        )

    csv_files = list(ctx.station_timeseries_dir.glob("*_morning_demand.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No station timeseries files found in {ctx.station_timeseries_dir}"
        )

    logger.info(f"Found {len(csv_files)} station files")

    return csv_files


def _load_single_station(csv_file: Path) -> tuple[str, pd.DataFrame]:
    """
    Load a single station timeseries from CSV.

    Args:
        csv_file: Path to station CSV file

    Returns:
        Tuple of (station_id, DataFrame)
    """
    # Extract station ID from filename
    station_id = csv_file.stem.replace("_morning_demand", "")

    # Load and parse dates
    df = pd.read_csv(csv_file)
    df["date"] = pd.to_datetime(df["date"])

    return station_id, df


def _prepare_prophet_data(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    Prepare timeseries data for Prophet (rename columns to ds/y format).

    Args:
        df: Station timeseries DataFrame
        target_col: Column name to forecast ('morning_pickups' or 'morning_dropoffs')

    Returns:
        DataFrame with 'ds' (date) and 'y' (target) columns
    """
    prophet_df = df[["date", target_col]].copy()
    prophet_df.columns = ["ds", "y"]
    return prophet_df


def _train_prophet_model(train_data: pd.DataFrame) -> Prophet:
    """
    Train Prophet model on provided data.

    Args:
        train_data: DataFrame with 'ds' and 'y' columns

    Returns:
        Fitted Prophet model
    """
    model = Prophet(
        seasonality_mode="multiplicative",
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        interval_width=0.80,
    )
    model.add_country_holidays(country_name="US")

    import logging as base_logging

    base_logging.getLogger("prophet").setLevel(base_logging.WARNING)

    model.fit(train_data)

    return model


def _generate_forecast(model: Prophet, periods: int) -> pd.DataFrame:
    """
    Generate forecast for specified number of periods.

    Args:
        model: Fitted Prophet model
        periods: Number of days to forecast

    Returns:
        DataFrame with forecast (ds, yhat)
    """
    future = model.make_future_dataframe(periods=periods, freq="D")
    forecast = model.predict(future)

    # Keep only forecast period
    forecast = forecast.tail(periods)

    # Keep only prediction column
    forecast = forecast[["ds", "yhat"]].copy()

    return forecast


def _evaluate_split(
    station_id: str,
    df: pd.DataFrame,
    split_idx: int,
    split_start: pd.Timestamp,
    ctx: EvaluateProphetContext,
) -> dict:
    """
    Evaluate model on a single cross-validation split.

    Args:
        station_id: Station identifier
        df: Station timeseries DataFrame
        split_idx: Split index (for logging)
        split_start: Start date of test period
        ctx: EvaluateProphetContext with configuration

    Returns:
        Dictionary with evaluation results for this split
    """
    logger.info(f"  Split {split_idx + 1}: Test start = {split_start.date()}")

    # Define test period
    split_end = split_start + timedelta(days=ctx.test_split_days - 1)

    # Prepare data
    pickups_data = _prepare_prophet_data(df, "morning_pickups")
    dropoffs_data = _prepare_prophet_data(df, "morning_dropoffs")

    # Train data: everything before split_start
    train_pickups = pickups_data[pickups_data["ds"] < split_start]
    train_dropoffs = dropoffs_data[dropoffs_data["ds"] < split_start]

    # Test data: split_start to split_end
    test_pickups = pickups_data[
        (pickups_data["ds"] >= split_start) & (pickups_data["ds"] <= split_end)
    ]
    test_dropoffs = dropoffs_data[
        (dropoffs_data["ds"] >= split_start) & (dropoffs_data["ds"] <= split_end)
    ]

    logger.info(f"    Training on {len(train_pickups)} days, testing on {len(test_pickups)} days")

    # Case 1: No retraining - train once, forecast entire test period
    if ctx.retrain_every_days == 0:
        # Train models
        model_pickups = _train_prophet_model(train_pickups)
        model_dropoffs = _train_prophet_model(train_dropoffs)

        # Generate forecasts
        forecast_pickups = _generate_forecast(model_pickups, ctx.test_split_days)
        forecast_dropoffs = _generate_forecast(model_dropoffs, ctx.test_split_days)

        # Extract predictions
        predictions_pickups = forecast_pickups["yhat"].values[: len(test_pickups)]
        predictions_dropoffs = forecast_dropoffs["yhat"].values[: len(test_dropoffs)]

    # Case 2: Rolling retraining
    else:
        predictions_pickups = []
        predictions_dropoffs = []

        current_date = split_start
        day_counter = 0

        while current_date <= split_end:
            # Retrain every N days
            if day_counter % ctx.retrain_every_days == 0:
                # Update training data up to current_date
                train_pickups_current = pickups_data[pickups_data["ds"] < current_date]
                train_dropoffs_current = dropoffs_data[dropoffs_data["ds"] < current_date]

                # Train models
                model_pickups = _train_prophet_model(train_pickups_current)
                model_dropoffs = _train_prophet_model(train_dropoffs_current)

                # Forecast next retrain_every_days (or remaining days)
                periods = min(ctx.retrain_every_days, (split_end - current_date).days + 1)
                forecast_pickups_batch = _generate_forecast(model_pickups, periods)
                forecast_dropoffs_batch = _generate_forecast(model_dropoffs, periods)

                # Store forecasts for this batch
                batch_predictions_pickups = forecast_pickups_batch["yhat"].values
                batch_predictions_dropoffs = forecast_dropoffs_batch["yhat"].values

                batch_idx = 0

            # Use prediction from current batch
            predictions_pickups.append(batch_predictions_pickups[batch_idx])
            predictions_dropoffs.append(batch_predictions_dropoffs[batch_idx])

            current_date += timedelta(days=1)
            day_counter += 1
            batch_idx += 1

        predictions_pickups = np.array(predictions_pickups)
        predictions_dropoffs = np.array(predictions_dropoffs)

    # Extract actuals
    actuals_pickups = test_pickups["y"].values
    actuals_dropoffs = test_dropoffs["y"].values

    # Calculate errors
    errors_pickups = actuals_pickups - predictions_pickups
    errors_dropoffs = actuals_dropoffs - predictions_dropoffs

    # Calculate metrics for pickups
    mae_pickups = np.mean(np.abs(errors_pickups))
    rmse_pickups = np.sqrt(np.mean(errors_pickups**2))
    actual_mean_pickups = np.mean(actuals_pickups)
    rel_mae_pickups = mae_pickups / actual_mean_pickups if actual_mean_pickups > 0 else np.nan

    # Calculate metrics for dropoffs
    mae_dropoffs = np.mean(np.abs(errors_dropoffs))
    rmse_dropoffs = np.sqrt(np.mean(errors_dropoffs**2))
    actual_mean_dropoffs = np.mean(actuals_dropoffs)
    rel_mae_dropoffs = mae_dropoffs / actual_mean_dropoffs if actual_mean_dropoffs > 0 else np.nan

    # Build result structure
    result = {
        "test_start_date": split_start.strftime("%Y-%m-%d"),
        "test_days": len(test_pickups),
        "results": {
            "pickups": {
                "actual": actuals_pickups.tolist(),
                "predicted": predictions_pickups.tolist(),
                "error": errors_pickups.tolist(),
                "metrics": {
                    "mae": float(mae_pickups),
                    "rmse": float(rmse_pickups),
                    "actual_mean": float(actual_mean_pickups),
                    "rel_mae": float(rel_mae_pickups) if not np.isnan(rel_mae_pickups) else None,
                },
            },
            "dropoffs": {
                "actual": actuals_dropoffs.tolist(),
                "predicted": predictions_dropoffs.tolist(),
                "error": errors_dropoffs.tolist(),
                "metrics": {
                    "mae": float(mae_dropoffs),
                    "rmse": float(rmse_dropoffs),
                    "actual_mean": float(actual_mean_dropoffs),
                    "rel_mae": float(rel_mae_dropoffs) if not np.isnan(rel_mae_dropoffs) else None,
                },
            },
        },
    }

    logger.info(
        f"    Pickups  - MAE: {mae_pickups:.2f}, RMSE: {rmse_pickups:.2f}, Rel MAE: {rel_mae_pickups:.2%}"
    )
    logger.info(
        f"    Dropoffs - MAE: {mae_dropoffs:.2f}, RMSE: {rmse_dropoffs:.2f}, Rel MAE: {rel_mae_dropoffs:.2%}"
    )

    return result


def _evaluate_station(
    station_id: str, df: pd.DataFrame, ctx: EvaluateProphetContext
) -> list[dict]:
    """
    Evaluate model for a single station across all CV splits.

    Args:
        station_id: Station identifier
        df: Station timeseries DataFrame
        ctx: EvaluateProphetContext with configuration

    Returns:
        List of dictionaries with results for each split
    """
    logger.info(f"=" * 60)
    logger.info(f"Evaluating station: {station_id}")
    logger.info(f"=" * 60)

    all_splits_results = []

    for split_idx, split_start_str in enumerate(ctx.test_split_start_dates):
        split_start = pd.to_datetime(split_start_str)

        # Evaluate this split
        split_result = _evaluate_split(station_id, df, split_idx, split_start, ctx)
        all_splits_results.append(split_result)

    logger.info(f"✓ Completed evaluation for {station_id} ({len(all_splits_results)} splits)")

    return all_splits_results


def _save_evaluation_plots(station_id: str, splits_results: list[dict], station_dir: Path) -> None:
    """
    Generate and save evaluation plots for a single station.

    Args:
        station_id: Station identifier
        splits_results: List of split results
        station_dir: Path to station output directory
    """
    import matplotlib.pyplot as plt
    from bluebikes_forecasting.plots import COLORS, plot_daily_longterm

    # Generate plot for each split
    for split_idx, split_result in enumerate(splits_results):
        for series_name in ["pickups", "dropoffs"]:
            # Extract data
            start_date = pd.to_datetime(split_result["test_start_date"])
            dates = pd.date_range(start=start_date, periods=split_result["test_days"], freq="D")

            actuals = split_result["results"][series_name]["actual"]
            predictions = split_result["results"][series_name]["predicted"]

            # Create DataFrame for plotting
            plot_df = pd.DataFrame({"actual": actuals, "forecast": predictions}, index=dates)

            # Set colors
            actual_color = COLORS[7] if series_name == "pickups" else COLORS[5]

            # Generate plot using shared plotting function
            plot_daily_longterm(
                plot_df,
                columns_to_plot=["actual", "forecast"],
                title=f"Split {split_idx + 1}: {series_name.title()} - Forecast vs Actual",
                ylabel="Morning Trips",
                color_list=[actual_color, COLORS[1]],
                linestyle_list=["-", "--"],
                legend_labels=["Actual", "Forecast"],
                figsize=(14, 3),
                show=False,
            )

            # Save plot
            plot_filename = f"split_{split_idx + 1}_{series_name}.png"
            plot_path = station_dir / plot_filename
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()

    logger.info(f"  Saved {len(splits_results) * 2} evaluation plots")


def _save_station_metrics_csv(
    station_id: str, splits_results: list[dict], station_dir: Path
) -> None:
    """
    Save per-station metrics summary CSV.

    Args:
        station_id: Station identifier
        splits_results: List of split results
        station_dir: Path to station output directory
    """
    rows = []

    for split_idx, split_result in enumerate(splits_results):
        pickups_metrics = split_result["results"]["pickups"]["metrics"]
        dropoffs_metrics = split_result["results"]["dropoffs"]["metrics"]

        rows.append(
            {
                "split": split_idx + 1,
                "pickups_mae": pickups_metrics["mae"],
                "pickups_rmse": pickups_metrics["rmse"],
                "pickups_actual_mean": pickups_metrics["actual_mean"],
                "pickups_rel_mae": pickups_metrics["rel_mae"],
                "dropoffs_mae": dropoffs_metrics["mae"],
                "dropoffs_rmse": dropoffs_metrics["rmse"],
                "dropoffs_actual_mean": dropoffs_metrics["actual_mean"],
                "dropoffs_rel_mae": dropoffs_metrics["rel_mae"],
            }
        )

    # Create DataFrame
    metrics_df = pd.DataFrame(rows)

    # Add average row
    avg_row = {
        "split": "avg",
        "pickups_mae": metrics_df["pickups_mae"].mean(),
        "pickups_rmse": metrics_df["pickups_rmse"].mean(),
        "pickups_actual_mean": metrics_df["pickups_actual_mean"].mean(),
        "pickups_rel_mae": metrics_df["pickups_rel_mae"].mean(),
        "dropoffs_mae": metrics_df["dropoffs_mae"].mean(),
        "dropoffs_rmse": metrics_df["dropoffs_rmse"].mean(),
        "dropoffs_actual_mean": metrics_df["dropoffs_actual_mean"].mean(),
        "dropoffs_rel_mae": metrics_df["dropoffs_rel_mae"].mean(),
    }

    metrics_df = pd.concat([metrics_df, pd.DataFrame([avg_row])], ignore_index=True)

    # Save to CSV
    csv_path = station_dir / f"{station_id}_metrics_summary.csv"
    metrics_df.to_csv(csv_path, index=False)

    logger.info(f"  Saved metrics CSV: {station_id}/{csv_path.name}")


def _save_station_results(
    station_id: str, splits_results: list[dict], ctx: EvaluateProphetContext
) -> None:
    """
    Save evaluation results for a single station to JSON, CSV summary, and generate plots.

    Args:
        station_id: Station identifier
        splits_results: List of split results
        ctx: EvaluateProphetContext with output paths
    """
    import json

    # Create station-specific subfolder
    station_dir = ctx.evaluation_dir / station_id
    station_dir.mkdir(parents=True, exist_ok=True)

    # Save JSON results
    json_path = station_dir / f"{station_id}_evaluation.json"
    with open(json_path, "w") as f:
        json.dump(splits_results, f, indent=2)

    logger.info(f"  Saved JSON: {station_id}/{json_path.name}")

    # Save CSV metrics summary
    _save_station_metrics_csv(station_id, splits_results, station_dir)

    # Generate and save plots
    _save_evaluation_plots(station_id, splits_results, station_dir)


def _extract_summary_row(station_id: str, split_result: dict) -> dict:
    """
    Extract summary metrics row from split result.

    Args:
        station_id: Station identifier
        split_result: Single split result dictionary

    Returns:
        Dictionary with flattened metrics
    """
    pickups_metrics = split_result["results"]["pickups"]["metrics"]
    dropoffs_metrics = split_result["results"]["dropoffs"]["metrics"]

    return {
        "station_id": station_id,
        "test_start_date": split_result["test_start_date"],
        "test_days": split_result["test_days"],
        "pickups_mae": pickups_metrics["mae"],
        "pickups_rmse": pickups_metrics["rmse"],
        "pickups_actual_mean": pickups_metrics["actual_mean"],
        "pickups_rel_mae": pickups_metrics["rel_mae"],
        "dropoffs_mae": dropoffs_metrics["mae"],
        "dropoffs_rmse": dropoffs_metrics["rmse"],
        "dropoffs_actual_mean": dropoffs_metrics["actual_mean"],
        "dropoffs_rel_mae": dropoffs_metrics["rel_mae"],
    }


def _save_summary_csv(summary_rows: list[dict], ctx: EvaluateProphetContext) -> None:
    """
    Save aggregate summary CSV.

    Args:
        summary_rows: List of summary metric dictionaries
        ctx: EvaluateProphetContext with output paths
    """
    logger.info("=" * 60)
    logger.info("Saving summary CSV")
    logger.info("=" * 60)

    summary_df = pd.DataFrame(summary_rows)
    summary_path = ctx.evaluation_dir / "evaluation_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    logger.info(f"✓ Saved summary: {summary_path}")


# ============================================================================
# Main public function
# ============================================================================


def evaluate_prophet(ctx: EvaluateProphetContext) -> None:
    """
    Evaluate Prophet models via cross-validation across multiple stations.

    Args:
        ctx: EvaluateProphetContext containing configuration and output paths
    """
    logger.info("Starting Prophet model evaluation")
    logger.info(f"Input directory: {ctx.station_timeseries_dir}")
    logger.info(f"Output directory: {ctx.evaluation_dir}")
    logger.info(f"Test splits: {len(ctx.test_split_start_dates)}")
    logger.info(f"Test period length: {ctx.test_split_days} days")
    logger.info(f"Retrain every: {ctx.retrain_every_days} days")

    # Find all station files
    csv_files = _find_station_files(ctx)

    # Process each station: load → evaluate → save
    summary_rows = []
    station_count = 0

    for csv_file in csv_files:
        # Load single station
        station_id, df = _load_single_station(csv_file)

        # Evaluate station
        splits_results = _evaluate_station(station_id, df, ctx)

        # Save station results immediately
        _save_station_results(station_id, splits_results, ctx)

        # Collect summary metrics
        for split_result in splits_results:
            summary_rows.append(_extract_summary_row(station_id, split_result))

        station_count += 1
        logger.info(f"Progress: {station_count}/{len(csv_files)} stations completed")

    # Save aggregate summary at the end
    _save_summary_csv(summary_rows, ctx)

    logger.info("=" * 60)
    logger.info("✓ Prophet evaluation completed")
    logger.info(f"  Evaluated {station_count} stations")
