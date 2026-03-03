# tasks/generate_timeseries/generate_timeseries.py

import logging
from pathlib import Path
import pandas as pd
import numpy as np

from .types import GenerateTimeseriesContext

logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _copy_daily_timeseries(ctx: GenerateTimeseriesContext) -> None:
    """
    Copy daily aggregates to timeseries output directory.

    Args:
        ctx: GenerateTimeseriesContext with configuration
    """
    logger.info("=" * 60)
    logger.info("Copying daily system-wide timeseries")
    logger.info("=" * 60)

    # Check input file exists
    if not ctx.daily_aggregates_path.exists():
        raise FileNotFoundError(
            f"Daily aggregates file not found: {ctx.daily_aggregates_path}\n"
            f"Please run aggregate_trips task first."
        )

    # Load and save
    daily_df = pd.read_csv(ctx.daily_aggregates_path)
    output_path = ctx.system_timeseries_dir / "daily_trips_timeseries.csv"
    daily_df.to_csv(output_path, index=False)

    logger.info(f"✓ Copied daily timeseries: {len(daily_df)} days")
    logger.info(f"  Output: {output_path}")


def _load_hourly_aggregates(ctx: GenerateTimeseriesContext) -> pd.DataFrame:
    """
    Load hourly station aggregates.

    Args:
        ctx: GenerateTimeseriesContext with configuration

    Returns:
        DataFrame with hourly aggregates
    """
    logger.info("=" * 60)
    logger.info("Loading hourly station aggregates")
    logger.info("=" * 60)

    if not ctx.hourly_aggregates_path.exists():
        raise FileNotFoundError(
            f"Hourly aggregates file not found: {ctx.hourly_aggregates_path}\n"
            f"Please run aggregate_trips task first."
        )

    df = pd.read_csv(ctx.hourly_aggregates_path)
    df["date"] = pd.to_datetime(df["date"])

    logger.info(f"✓ Loaded {len(df)} hourly records")
    logger.info(f"  Stations: {df['station_id'].nunique()}")

    return df


def _fill_missing_hours(station_df_dict: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Fill missing hours with zeros for each station timeseries.

    Args:
        station_df_dict: Dictionary of station timeseries

    Returns:
        Dictionary with complete hourly timeseries (gaps filled with 0)
    """
    logger.info("Filling missing hours with zeros")

    for station_id, df in station_df_dict.items():
        df = df.set_index("timestamp")

        # Create complete hourly range
        full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="h")

        # Reindex and fill missing with 0
        df = df.reindex(full_range, fill_value=0).reset_index()
        df.columns = ["timestamp", "pickups", "dropoffs"]

        station_df_dict[station_id] = df

    logger.info("✓ Filled missing hours")

    return station_df_dict


def _generate_morning_demand(
    station_df_dict: dict[str, pd.DataFrame],
    df_hourly: pd.DataFrame,
    ctx: GenerateTimeseriesContext,
) -> dict[str, pd.DataFrame]:
    """
    Generate daily morning demand timeseries for each station.

    Args:
        station_df_dict: Dictionary of hourly station timeseries
        df_hourly: Original hourly dataframe for date range
        ctx: GenerateTimeseriesContext with morning hour configuration

    Returns:
        Dictionary mapping station_id to morning demand DataFrame
    """
    logger.info(
        f"Generating morning demand timeseries ({ctx.morning_start_hour}:00-{ctx.morning_end_hour}:59)"
    )

    # Get full date range
    min_date = df_hourly["date"].min()
    max_date = df_hourly["date"].max()
    full_date_range = pd.date_range(start=min_date, end=max_date, freq="D")

    morning_demand_dict = {}

    for station_id, df in station_df_dict.items():
        # Filter to morning hours
        filt = df["timestamp"].dt.hour.between(ctx.morning_start_hour, ctx.morning_end_hour)
        df_morning = df.loc[filt, :].copy()

        # Extract date and aggregate
        df_morning["date"] = df_morning["timestamp"].dt.date
        daily_morning = df_morning.groupby("date")[["pickups", "dropoffs"]].sum().reset_index()
        daily_morning.columns = ["date", "morning_pickups", "morning_dropoffs"]

        # Reindex to include all dates (fill missing with 0)
        daily_morning["date"] = pd.to_datetime(daily_morning["date"])
        daily_morning = daily_morning.set_index("date")
        daily_morning = daily_morning.reindex(full_date_range, fill_value=0).reset_index()
        daily_morning.columns = ["date", "morning_pickups", "morning_dropoffs"]

        morning_demand_dict[station_id] = daily_morning

    logger.info(f"✓ Generated morning demand for {len(morning_demand_dict)} stations")

    return morning_demand_dict


def _create_station_timeseries(
    df_hourly: pd.DataFrame, ctx: GenerateTimeseriesContext
) -> dict[str, pd.DataFrame]:
    """
    Create station-level morning demand timeseries with complete time range.

    Args:
        df_hourly: DataFrame with hourly aggregates
        ctx: GenerateTimeseriesContext with configuration

    Returns:
        Dictionary mapping station_id to morning demand DataFrame
    """
    logger.info("=" * 60)
    logger.info("Creating station-level timeseries")
    logger.info("=" * 60)

    # Create timestamp column
    df_hourly["timestamp"] = df_hourly["date"] + pd.to_timedelta(df_hourly["hour"], unit="h")

    # Aggregate by station and timestamp (sum across member/ebike categories)
    station_df_dict = {}
    for station_id, group in df_hourly.groupby("station_id"):
        station_ts = (
            group.groupby("timestamp")[["pickups", "dropoffs"]]
            .sum()
            .reset_index()
            .sort_values("timestamp")
        )
        station_df_dict[station_id] = station_ts

    logger.info(f"✓ Created timeseries for {len(station_df_dict)} stations")

    # Fill missing hours
    station_df_dict = _fill_missing_hours(station_df_dict)

    # Generate morning demand
    morning_demand_dict = _generate_morning_demand(station_df_dict, df_hourly, ctx)

    return morning_demand_dict


def _save_station_timeseries(
    morning_demand_dict: dict[str, pd.DataFrame], ctx: GenerateTimeseriesContext
) -> None:
    """
    Save each station's morning demand timeseries to separate CSV.

    Args:
        morning_demand_dict: Dictionary of morning demand DataFrames
        ctx: GenerateTimeseriesContext with output paths
    """
    logger.info("=" * 60)
    logger.info("Saving station timeseries")
    logger.info("=" * 60)

    for station_id, df in morning_demand_dict.items():
        output_path = ctx.station_timeseries_dir / f"{station_id}_morning_demand.csv"
        df.to_csv(output_path, index=False)

    logger.info(f"✓ Saved {len(morning_demand_dict)} station timeseries")
    logger.info(f"  Output directory: {ctx.station_timeseries_dir}")


# ============================================================================
# Main public function
# ============================================================================


def generate_timeseries(ctx: GenerateTimeseriesContext) -> None:
    """
    Generate time series from aggregated trip data.

    Args:
        ctx: GenerateTimeseriesContext containing configuration and output paths
    """
    logger.info("Starting timeseries generation")
    logger.info(f"Input directory: {ctx.interim_dir}")
    logger.info(f"Output directory: {ctx.timeseries_dir}")

    # Copy daily system-wide timeseries
    _copy_daily_timeseries(ctx)

    # Load hourly aggregates
    df_hourly = _load_hourly_aggregates(ctx)

    # Create station morning demand timeseries (includes filling gaps and filtering to morning)
    morning_demand_dict = _create_station_timeseries(df_hourly, ctx)

    # Save station timeseries
    _save_station_timeseries(morning_demand_dict, ctx)

    logger.info("=" * 60)
    logger.info("✓ Timeseries generation completed")
