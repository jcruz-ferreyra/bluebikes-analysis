# tasks/aggregate_trips/aggregate_trips.py

import logging
import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

from .types import AggregateTripsContext

logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _load_all_trip_csvs(ctx: AggregateTripsContext) -> dict[str, pd.DataFrame]:
    """
    Load all trip CSV files from raw directory.

    Args:
        ctx: AggregateTripsContext with configuration

    Returns:
        Dictionary mapping filename to DataFrame
    """
    csv_files = [f for f in os.listdir(ctx.raw_trips_dir) if f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {ctx.raw_trips_dir}")

    logger.info(f"Found {len(csv_files)} CSV files to load")

    trips_df_dict = {}
    for csv_file in csv_files:
        file_path = ctx.raw_trips_dir / csv_file
        df = pd.read_csv(file_path)
        file_key = csv_file.replace(".csv", "")
        trips_df_dict[file_key] = df
        logger.info(f"Loaded {csv_file}: {len(df):,} rows")

    return trips_df_dict


def _standardize_trip_data(trips_df_dict: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Standardize column names and keep only essential columns.

    Args:
        trips_df_dict: Dictionary of DataFrames with raw trip data

    Returns:
        Dictionary of DataFrames with standardized columns
    """
    # Column name mapping: old -> new
    column_mapping = {
        "starttime": "started_at",
        "stoptime": "ended_at",
        "start station id": "start_station_id",
        "end station id": "end_station_id",
        "start station latitude": "start_lat",
        "start station longitude": "start_lng",
        "end station latitude": "end_lat",
        "end station longitude": "end_lng",
        "usertype": "member_casual",
    }

    # Columns to keep (standardized names)
    keep_columns = [
        "started_at",
        "ended_at",
        "start_station_id",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual",
        "rideable_type",
    ]

    logger.info("Standardizing column names and filtering columns")

    for name, df in trips_df_dict.items():
        # Rename columns
        df.rename(columns=column_mapping, inplace=True)

        # Keep only columns that exist in this DataFrame
        cols_to_keep = [c for c in keep_columns if c in df.columns]
        trips_df_dict[name] = df[cols_to_keep]

    logger.info("✓ Column standardization complete")
    return trips_df_dict


def _parse_datetimes(trips_df_dict: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Parse datetime columns in all DataFrames.

    Args:
        trips_df_dict: Dictionary of DataFrames with trip data

    Returns:
        Dictionary of DataFrames with parsed datetimes
    """
    logger.info("Parsing datetime columns")

    for name, df in trips_df_dict.items():
        df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
        df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")

        # Log any parsing failures
        null_starts = df["started_at"].isnull().sum()
        null_ends = df["ended_at"].isnull().sum()

        if null_starts > 0 or null_ends > 0:
            logger.warning(f"{name}: {null_starts} null starts, {null_ends} null ends")

        trips_df_dict[name] = df

    logger.info("✓ Datetime parsing complete")
    return trips_df_dict


def _concatenate_trips(trips_df_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate all trip DataFrames into single DataFrame.

    Args:
        trips_df_dict: Dictionary of DataFrames

    Returns:
        Single concatenated DataFrame
    """
    logger.info("Concatenating all trip data")

    all_trips = pd.concat(trips_df_dict.values(), ignore_index=True)

    logger.info(f"✓ Concatenated into single DataFrame: {len(all_trips):,} rows")
    logger.info(
        f"  Date range: {all_trips['started_at'].min()} to {all_trips['started_at'].max()}"
    )

    return all_trips


def _load_and_prepare_trips(ctx: AggregateTripsContext) -> pd.DataFrame:
    """
    Load all trip CSVs, standardize, parse datetimes, and concatenate.

    Args:
        ctx: AggregateTripsContext with configuration

    Returns:
        Single DataFrame with all standardized trip data
    """
    logger.info("=" * 60)
    logger.info("Loading and preparing trip data")
    logger.info("=" * 60)

    trips_df_dict = _load_all_trip_csvs(ctx)
    trips_df_dict = _standardize_trip_data(trips_df_dict)
    trips_df_dict = _parse_datetimes(trips_df_dict)
    all_trips = _concatenate_trips(trips_df_dict)

    return all_trips


def _load_station_metadata(ctx: AggregateTripsContext) -> tuple[pd.DataFrame, list[str]]:
    """
    Load station metadata and create list of valid station IDs.

    Args:
        ctx: AggregateTripsContext with configuration

    Returns:
        Tuple of (stations_df, list of all valid station IDs)
    """
    logger.info("=" * 60)
    logger.info("Loading station metadata")
    logger.info("=" * 60)

    logger.info(f"Loading station metadata from {ctx.station_metadata_path}")

    if not ctx.station_metadata_path.exists():
        raise FileNotFoundError(
            f"Station metadata file not found: {ctx.station_metadata_path}\n"
            f"Please run download_stations_data task first."
        )

    stations_df = pd.read_csv(ctx.station_metadata_path)

    # Extract all valid station IDs (station_id, legacy_id, short_name)
    metadata_station_ids = list(
        set(stations_df["station_id"].dropna().astype(str).unique())
        | set(stations_df["short_name"].dropna().astype(str).unique())
    )

    logger.info(
        f"✓ Loaded {len(stations_df)} stations with {len(metadata_station_ids)} unique IDs"
    )

    return stations_df, metadata_station_ids


def _filter_maintenance_trips(
    all_trips: pd.DataFrame, metadata_station_ids: list[str]
) -> pd.DataFrame:
    """
    Remove trips involving maintenance stations (starting with 'X').

    Args:
        all_trips: DataFrame with all trip data
        metadata_station_ids: List of valid station IDs

    Returns:
        Filtered DataFrame without maintenance trips
    """
    initial_count = len(all_trips)

    # Find unknown station IDs (not in metadata)
    filt_unknown_start = ~all_trips["start_station_id"].isin(metadata_station_ids)
    filt_unknown_end = ~all_trips["end_station_id"].isin(metadata_station_ids)

    all_unknown_ids = set(
        all_trips.loc[filt_unknown_start, "start_station_id"].dropna().astype(str).unique()
    ) | set(all_trips.loc[filt_unknown_end, "end_station_id"].dropna().astype(str).unique())

    logger.info(f"Found {len(all_unknown_ids)} unknown station IDs")

    # Identify maintenance stations (starting with 'X')
    X_station_ids = [x for x in all_unknown_ids if str(x).startswith("X")]
    logger.info(f"Identified {len(X_station_ids)} maintenance stations (X-prefixed)")

    # Filter out trips involving maintenance stations
    filt_maintenance_start = all_trips["start_station_id"].isin(X_station_ids)
    filt_maintenance_end = all_trips["end_station_id"].isin(X_station_ids)

    all_trips = all_trips[~(filt_maintenance_start | filt_maintenance_end)].copy()

    removed_count = initial_count - len(all_trips)
    logger.info(
        f"✓ Removed {removed_count:,} maintenance trips ({removed_count/initial_count*100:.2f}%)"
    )

    return all_trips


def _calculate_trip_duration(all_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate trip duration in minutes.

    Args:
        all_trips: DataFrame with trip data

    Returns:
        DataFrame with min_duration column added
    """
    logger.info("Calculating trip durations")

    all_trips["min_duration"] = (
        all_trips["ended_at"] - all_trips["started_at"]
    ).dt.total_seconds() / 60

    logger.info("✓ Trip durations calculated")

    return all_trips


def _filter_outlier_trips(all_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Remove trips with unrealistic durations.

    Args:
        all_trips: DataFrame with trip data

    Returns:
        Filtered DataFrame
    """
    initial_count = len(all_trips)

    logger.info("Filtering outlier trips")

    # Remove trips with duration <1 min or >120 min
    all_trips = all_trips[
        (all_trips["min_duration"] >= 1) & (all_trips["min_duration"] <= 120)
    ].copy()

    duration_filtered = initial_count - len(all_trips)
    logger.info(f"  Removed {duration_filtered:,} trips with duration <1 min or >120 min")

    # Remove same-station round trips <3 min (false starts)
    same_station = all_trips["start_station_id"] == all_trips["end_station_id"]
    short_duration = all_trips["min_duration"] < 3

    false_starts = (same_station & short_duration).sum()
    all_trips = all_trips[~(same_station & short_duration)].copy()

    logger.info(f"  Removed {false_starts:,} same-station trips <3 min (false starts)")

    total_removed = initial_count - len(all_trips)
    logger.info(
        f"✓ Total outliers removed: {total_removed:,} ({total_removed/initial_count*100:.2f}%)"
    )

    return all_trips


def _clean_trip_data(all_trips: pd.DataFrame, metadata_station_ids: list[str]) -> pd.DataFrame:
    """
    Clean trip data by removing maintenance trips and outliers.

    Args:
        all_trips: DataFrame with all trip data
        metadata_station_ids: List of valid station IDs

    Returns:
        Cleaned DataFrame
    """
    logger.info("=" * 60)
    logger.info("Cleaning trip data")
    logger.info("=" * 60)

    initial_count = len(all_trips)

    all_trips = _filter_maintenance_trips(all_trips, metadata_station_ids)
    all_trips = _calculate_trip_duration(all_trips)
    all_trips = _filter_outlier_trips(all_trips)

    logger.info(
        f"✓ Cleaning complete: {len(all_trips):,} trips remaining ({len(all_trips)/initial_count*100:.2f}%)"
    )

    return all_trips


def _generate_daily_aggregates(all_trips: pd.DataFrame, ctx: AggregateTripsContext) -> None:
    """
    Generate system-wide daily trip counts and save to CSV.

    Args:
        all_trips: DataFrame with all trip data
        ctx: AggregateTripsContext with output paths
    """
    logger.info("=" * 60)
    logger.info("Generating daily system-wide aggregates")
    logger.info("=" * 60)

    # Extract date from started_at
    daily_trips = all_trips.copy()
    daily_trips["date"] = daily_trips["started_at"].dt.date

    # Group by date and count trips
    daily_aggregates = (
        daily_trips.groupby("date").size().sort_index().reset_index(name="trip_count")
    )
    
    # Ensure all dates are present (fill missing with 0)
    min_date = daily_aggregates["date"].min()
    max_date = daily_aggregates["date"].max()
    full_date_range = pd.date_range(start=min_date, end=max_date, freq='D').date
    
    # Create complete date range DataFrame
    complete_dates = pd.DataFrame({'date': full_date_range})
    
    # Merge and fill missing with 0
    daily_aggregates = complete_dates.merge(daily_aggregates, on='date', how='left')
    daily_aggregates['trip_count'] = daily_aggregates['trip_count'].fillna(0).astype(int)
    
    # Save to CSV
    output_path = ctx.processed_dir / "daily_aggregates.csv"
    daily_aggregates.to_csv(output_path, index=False)

    logger.info(f"✓ Saved daily aggregates: {len(daily_aggregates)} days")
    logger.info(f"  Output: {output_path}")
    logger.info(
        f"  Date range: {daily_aggregates['date'].min()} to {daily_aggregates['date'].max()}"
    )


def _load_stations_of_interest(ctx: AggregateTripsContext) -> list[str]:
    """
    Load list of station IDs of interest from JSON file.

    Args:
        ctx: AggregateTripsContext with configuration

    Returns:
        List of station short_name IDs
    """
    logger.info(f"Loading stations of interest from {ctx.stations_of_interest_path}")

    with open(ctx.stations_of_interest_path, "r") as f:
        stations_of_interest = json.load(f)

    if not isinstance(stations_of_interest, list):
        raise ValueError(
            f"stations_of_interest file must contain a JSON list, "
            f"got {type(stations_of_interest)}"
        )

    logger.info(f"✓ Loaded {len(stations_of_interest)} stations of interest")

    return stations_of_interest


def _generate_hourly_station_aggregates(
    all_trips: pd.DataFrame, ctx: AggregateTripsContext
) -> None:
    """
    Generate hourly pickups/dropoffs per station and save to CSV.

    Args:
        all_trips: DataFrame with trip data
        ctx: AggregateTripsContext with configuration
    """
    logger.info("=" * 60)
    logger.info("Generating hourly station-level aggregates")
    logger.info("=" * 60)

    # Load stations of interest
    stations_of_interest = _load_stations_of_interest(ctx)

    # Filter trips from hourly_start_date onwards
    hourly_start = datetime.strptime(ctx.hourly_start_date, "%Y-%m-%d")
    trips_filtered = all_trips[all_trips["started_at"] >= hourly_start].copy()

    logger.info(f"Filtered to {len(trips_filtered):,} trips from {ctx.hourly_start_date} onwards")

    # Extract temporal features
    trips_filtered["date"] = trips_filtered["started_at"].dt.date
    trips_filtered["hour"] = trips_filtered["started_at"].dt.hour

    # Convert categoricals to boolean
    trips_filtered["member"] = (trips_filtered["member_casual"] == "member").astype(int)
    trips_filtered["ebike"] = (trips_filtered["rideable_type"] == "electric_bike").astype(int)

    # Calculate pickups (group by start_station_id)
    pickups = (
        trips_filtered[trips_filtered["start_station_id"].isin(stations_of_interest)]
        .groupby(["start_station_id", "date", "hour", "member", "ebike"])
        .size()
        .reset_index(name="pickups")
        .rename(columns={"start_station_id": "station_id"})
    )

    logger.info(f"Calculated pickups: {len(pickups)} records")

    # Calculate dropoffs (group by end_station_id)
    dropoffs = (
        trips_filtered[trips_filtered["end_station_id"].isin(stations_of_interest)]
        .groupby(["end_station_id", "date", "hour", "member", "ebike"])
        .size()
        .reset_index(name="dropoffs")
        .rename(columns={"end_station_id": "station_id"})
    )

    logger.info(f"Calculated dropoffs: {len(dropoffs)} records")

    # Merge pickups and dropoffs
    hourly_aggregates = pd.merge(
        pickups, dropoffs, on=["station_id", "date", "hour", "member", "ebike"], how="outer"
    ).fillna(0)

    # Sort by station, date, hour
    hourly_aggregates = hourly_aggregates.sort_values(
        ["station_id", "date", "hour", "member", "ebike"]
    ).reset_index(drop=True)

    # Convert counts to integers
    hourly_aggregates["pickups"] = hourly_aggregates["pickups"].astype(int)
    hourly_aggregates["dropoffs"] = hourly_aggregates["dropoffs"].astype(int)

    # Save to CSV
    output_path = ctx.processed_dir / "hourly_station_aggregates.csv"
    hourly_aggregates.to_csv(output_path, index=False)

    logger.info(f"✓ Saved hourly station aggregates: {len(hourly_aggregates)} records")
    logger.info(f"  Output: {output_path}")
    logger.info(f"  Stations: {hourly_aggregates['station_id'].nunique()}")
    logger.info(
        f"  Date range: {hourly_aggregates['date'].min()} to {hourly_aggregates['date'].max()}"
    )


# ============================================================================
# Main public function
# ============================================================================


def aggregate_trips(ctx: AggregateTripsContext) -> None:
    """
    Aggregate Bluebikes trip data into analysis datasets.

    Args:
        ctx: AggregateTripsContext containing configuration and output paths
    """
    logger.info("Starting trip data aggregation")
    logger.info(f"Input directory: {ctx.raw_trips_dir}")
    logger.info(f"Output directory: {ctx.processed_dir}")

    # Load and prepare all trip data
    all_trips = _load_and_prepare_trips(ctx)

    # Load station metadata
    _, metadata_station_ids = _load_station_metadata(ctx)

    # Clean trip data
    all_trips = _clean_trip_data(all_trips, metadata_station_ids)

    # Generate daily aggregates
    _generate_daily_aggregates(all_trips, ctx)

    # Generate hourly station aggregates
    _generate_hourly_station_aggregates(all_trips, ctx)

    logger.info("=" * 60)
    logger.info("✓ Trip aggregation completed")
