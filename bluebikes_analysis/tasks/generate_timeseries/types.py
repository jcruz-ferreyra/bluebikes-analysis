# tasks/generate_timeseries/types.py

from dataclasses import dataclass
from pathlib import Path


@dataclass
class GenerateTimeseriesContext:
    """Context for generating time series from aggregated trip data."""

    morning_start_hour: int
    morning_end_hour: int
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate morning hours
        _validate_morning_hours(self.morning_start_hour, self.morning_end_hour)

    @property
    def interim_dir(self) -> Path:
        """Path to interim trip aggregates (input)."""
        return self.output_data_dir / "interim" / "trip_aggregates"

    @property
    def daily_aggregates_path(self) -> Path:
        """Path to daily aggregates CSV (input)."""
        return self.interim_dir / "daily_aggregates.csv"

    @property
    def hourly_aggregates_path(self) -> Path:
        """Path to hourly station aggregates CSV (input)."""
        return self.interim_dir / "hourly_station_aggregates.csv"

    @property
    def timeseries_dir(self) -> Path:
        """Path to processed timeseries output."""
        path = self.output_data_dir / "processed" / "timeseries"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def system_timeseries_dir(self) -> Path:
        """Path to system-level timeseries."""
        path = self.timeseries_dir / "system"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def station_timeseries_dir(self) -> Path:
        """Path to station-level timeseries."""
        path = self.timeseries_dir / "station"
        path.mkdir(parents=True, exist_ok=True)
        return path


def _validate_storage(storage: str) -> None:
    """
    Validate that the storage option is supported.

    Args:
        storage: Storage option ("local" or "drive")

    Raises:
        ValueError: If storage option is not valid
    """
    valid_storages = ["local", "drive"]
    if storage not in valid_storages:
        raise ValueError(f"output_storage must be one of {valid_storages}, got '{storage}'")


def _validate_morning_hours(start_hour: int, end_hour: int) -> None:
    """
    Validate morning hour range is valid.

    Args:
        start_hour: Start hour (0-23)
        end_hour: End hour (0-23)

    Raises:
        ValueError: If hours are invalid
    """
    if not (0 <= start_hour <= 23):
        raise ValueError(f"morning_start_hour must be 0-23, got {start_hour}")

    if not (0 <= end_hour <= 23):
        raise ValueError(f"morning_end_hour must be 0-23, got {end_hour}")

    if start_hour >= end_hour:
        raise ValueError(
            f"morning_start_hour ({start_hour}) must be less than "
            f"morning_end_hour ({end_hour})"
        )
