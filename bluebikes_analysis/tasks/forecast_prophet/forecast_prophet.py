# tasks/forecast_prophet/forecast_prophet.py

import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from prophet import Prophet
import pickle

from .types import ForecastProphetContext

logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _load_station_timeseries(ctx: ForecastProphetContext) -> dict[str, pd.DataFrame]:
    """
    Load all station morning demand timeseries.

    Args:
        ctx: ForecastProphetContext with configuration

    Returns:
        Dictionary mapping station_id to timeseries DataFrame
    """
    logger.info("=" * 60)
    logger.info("Loading station timeseries")
    logger.info("=" * 60)

    if not ctx.station_timeseries_dir.exists():
        raise FileNotFoundError(
            f"Station timeseries directory not found: {ctx.station_timeseries_dir}\n"
            f"Please run generate_timeseries task first."
        )

    # Find all station CSV files
    csv_files = list(ctx.station_timeseries_dir.glob("*_morning_demand.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No station timeseries files found in {ctx.station_timeseries_dir}"
        )

    logger.info(f"Found {len(csv_files)} station timeseries files")

    # Load all station timeseries
    station_timeseries = {}
    for csv_file in csv_files:
        # Extract station_id from filename (e.g., "M32006_morning_demand.csv" -> "M32006")
        station_id = csv_file.stem.replace("_morning_demand", "")

        df = pd.read_csv(csv_file)
        df["date"] = pd.to_datetime(df["date"])

        station_timeseries[station_id] = df

    logger.info(f"✓ Loaded {len(station_timeseries)} station timeseries")

    # Validate inference_start_date against data availability
    inference_start = pd.to_datetime(ctx.inference_start_date)

    # Get latest date across all stations
    max_date = max(df["date"].max() for df in station_timeseries.values())

    if inference_start > max_date:
        raise ValueError(
            f"inference_start_date ({ctx.inference_start_date}) is after the last date "
            f"in the data ({max_date.date()}). No data available for inference."
        )

    logger.info(f"  Data date range: up to {max_date.date()}")

    return station_timeseries


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
        interval_width=0.80,  # For 80% prediction interval
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
        DataFrame with forecast (ds, yhat, yhat_lower, yhat_upper)
    """
    future = model.make_future_dataframe(periods=periods, freq="D")
    forecast = model.predict(future)

    # Keep only forecast period (not historical fitted values)
    forecast = forecast.tail(periods)

    # Keep relevant columns
    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()

    return forecast


def _save_model(
    model: Prophet, station_id: str, train_date: str, ctx: ForecastProphetContext
) -> None:
    """
    Save trained Prophet model to disk.

    Args:
        model: Fitted Prophet model
        station_id: Station identifier
        train_date: Date model was trained (YYYY-MM-DD)
        ctx: ForecastProphetContext with output paths
    """
    if not ctx.save_models or ctx.models_dir is None:
        return

    model_filename = f"{station_id}_trained_{train_date}.pkl"
    model_path = ctx.models_dir / model_filename

    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    logger.info(f"  Saved model: {model_filename}")


def _forecast_station(
    station_id: str, df: pd.DataFrame, ctx: ForecastProphetContext
) -> pd.DataFrame:
    """
    Generate forecasts for a single station (handles retraining logic).

    Args:
        station_id: Station identifier
        df: Station timeseries DataFrame
        ctx: ForecastProphetContext with configuration

    Returns:
        DataFrame with all forecasts for this station
    """
    logger.info(f"=" * 60)
    logger.info(f"Forecasting station: {station_id}")

    # Prepare data for Prophet (forecast pickups and dropoffs separately)
    pickups_data = _prepare_prophet_data(df, "morning_pickups")
    dropoffs_data = _prepare_prophet_data(df, "morning_dropoffs")

    # Determine inference date range
    inference_start = pd.to_datetime(ctx.inference_start_date)

    if ctx.inference_end_date == "end_of_data":
        inference_end = df["date"].max()
    else:
        inference_end = pd.to_datetime(ctx.inference_end_date)

    logger.info(f"  Inference period: {inference_start.date()} to {inference_end.date()}")

    # Case 1: No retraining - train once, forecast entire period
    if ctx.retrain_every_days == 0:
        # Train on all data before inference_start
        train_pickups = pickups_data[pickups_data["ds"] < inference_start]
        train_dropoffs = dropoffs_data[dropoffs_data["ds"] < inference_start]

        logger.info(f"  Training on {len(train_pickups)} days")

        # Train models
        model_pickups = _train_prophet_model(train_pickups)
        model_dropoffs = _train_prophet_model(train_dropoffs)

        # Generate forecasts
        periods = (inference_end - inference_start).days + 1
        forecast_pickups = _generate_forecast(model_pickups, periods)
        forecast_dropoffs = _generate_forecast(model_dropoffs, periods)

        # Save models if requested
        _save_model(
            model_pickups, f"{station_id}_pickups", inference_start.strftime("%Y-%m-%d"), ctx
        )
        _save_model(
            model_dropoffs, f"{station_id}_dropoffs", inference_start.strftime("%Y-%m-%d"), ctx
        )

        # Combine forecasts
        forecast = forecast_pickups.rename(
            columns={
                "yhat": "pickups_forecast",
                "yhat_lower": "pickups_lower",
                "yhat_upper": "pickups_upper",
            }
        )
        forecast = forecast.merge(
            forecast_dropoffs.rename(
                columns={
                    "yhat": "dropoffs_forecast",
                    "yhat_lower": "dropoffs_lower",
                    "yhat_upper": "dropoffs_upper",
                }
            ),
            on="ds",
        )

    # Case 2: Rolling retraining
    else:
        all_forecasts = []
        current_date = inference_start

        while current_date <= inference_end:
            # Train on all data up to current_date
            train_pickups = pickups_data[pickups_data["ds"] < current_date]
            train_dropoffs = dropoffs_data[dropoffs_data["ds"] < current_date]

            # Forecast next retrain_every_days (or remaining days)
            periods = min(ctx.retrain_every_days, (inference_end - current_date).days + 1)

            logger.info(
                f"  Training on data up to {current_date.date()}, forecasting {periods} days"
            )

            # Train models
            model_pickups = _train_prophet_model(train_pickups)
            model_dropoffs = _train_prophet_model(train_dropoffs)

            # Generate forecasts
            forecast_pickups = _generate_forecast(model_pickups, periods)
            forecast_dropoffs = _generate_forecast(model_dropoffs, periods)

            # Save models if requested
            _save_model(
                model_pickups, f"{station_id}_pickups", current_date.strftime("%Y-%m-%d"), ctx
            )
            _save_model(
                model_dropoffs, f"{station_id}_dropoffs", current_date.strftime("%Y-%m-%d"), ctx
            )

            # Combine this batch
            batch_forecast = forecast_pickups.rename(
                columns={
                    "yhat": "pickups_forecast",
                    "yhat_lower": "pickups_lower",
                    "yhat_upper": "pickups_upper",
                }
            )
            batch_forecast = batch_forecast.merge(
                forecast_dropoffs.rename(
                    columns={
                        "yhat": "dropoffs_forecast",
                        "yhat_lower": "dropoffs_lower",
                        "yhat_upper": "dropoffs_upper",
                    }
                ),
                on="ds",
            )

            all_forecasts.append(batch_forecast)

            # Move to next training window
            current_date += timedelta(days=ctx.retrain_every_days)

        # Concatenate all forecast batches
        forecast = pd.concat(all_forecasts, ignore_index=True)

    # Add station_id column
    forecast.insert(0, "station_id", station_id)

    logger.info(f"  ✓ Generated {len(forecast)} forecast days")

    return forecast


def _save_forecasts(forecasts_dict: dict[str, pd.DataFrame], ctx: ForecastProphetContext) -> None:
    """
    Save all station forecasts to CSV files.

    Args:
        forecasts_dict: Dictionary mapping station_id to forecast DataFrame
        ctx: ForecastProphetContext with output paths
    """
    logger.info("=" * 60)
    logger.info("Saving forecasts")
    logger.info("=" * 60)

    for station_id, forecast_df in forecasts_dict.items():
        output_path = ctx.forecasts_dir / f"{station_id}_forecast.csv"
        forecast_df.to_csv(output_path, index=False)

    logger.info(f"✓ Saved forecasts for {len(forecasts_dict)} stations")
    logger.info(f"  Output directory: {ctx.forecasts_dir}")


# ============================================================================
# Main public function
# ============================================================================


def forecast_prophet(ctx: ForecastProphetContext) -> None:
    """
    Generate Prophet forecasts for all stations.

    Args:
        ctx: ForecastProphetContext containing configuration and output paths
    """
    logger.info("Starting Prophet forecasting")
    logger.info(f"Input directory: {ctx.station_timeseries_dir}")
    logger.info(f"Output directory: {ctx.forecasts_dir}")
    logger.info(f"Inference start: {ctx.inference_start_date}")
    logger.info(f"Inference end: {ctx.inference_end_date}")
    logger.info(f"Retrain every: {ctx.retrain_every_days} days")

    # Load station timeseries
    station_timeseries = _load_station_timeseries(ctx)

    # Generate forecasts for each station
    forecasts_dict = {}
    for station_id, df in station_timeseries.items():
        logger.info(f"Processing station: {station_id}")
        forecast_df = _forecast_station(station_id, df, ctx)
        forecasts_dict[station_id] = forecast_df

    # Save all forecasts
    _save_forecasts(forecasts_dict, ctx)

    logger.info("=" * 60)
    logger.info("✓ Prophet forecasting completed")
    logger.info(f"  Forecasted {len(forecasts_dict)} stations")
