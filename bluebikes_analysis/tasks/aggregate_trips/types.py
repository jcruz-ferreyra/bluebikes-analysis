# tasks/aggregate_trips/types.py

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime


@dataclass
class AggregateTripsContext:
    """Context for aggregating Bluebikes trip data."""

    stations_of_interest_file: str
    hourly_start_date: str  # YYYY-MM-DD format
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate and parse hourly start date
        _validate_hourly_start_date(self.hourly_start_date)

        # Validate stations of interest file exists
        _validate_stations_file(self.stations_of_interest_path)

    @property
    def raw_trips_dir(self) -> Path:
        """Path to raw trip data CSVs."""
        return self.output_data_dir / "raw" / "trips"

    @property
    def raw_stations_dir(self) -> Path:
        """Path to raw station metadata."""
        return self.output_data_dir / "raw" / "stations"

    @property
    def stations_of_interest_path(self) -> Path:
        """Path to stations of interest JSON file."""
        return self.raw_stations_dir / self.stations_of_interest_file

    @property
    def station_metadata_path(self) -> Path:
        """Path to station information CSV."""
        return self.raw_stations_dir / "station_information.csv"

    @property
    def processed_dir(self) -> Path:
        """Path to processed data output."""
        path = self.output_data_dir / "interim" / "trip_aggregates"
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
        raise ValueError(
            f"output_storage must be one of {valid_storages}, got '{storage}'"
        )


def _validate_hourly_start_date(date_str: str) -> None:
    """
    Validate hourly start date is in correct format and >= 2023-04-01.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Raises:
        ValueError: If date format is invalid or before 2023-04-01
    """
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"hourly_start_date must be in YYYY-MM-DD format, got '{date_str}'"
        )

    min_date = datetime(2023, 4, 1)
    if date < min_date:
        raise ValueError(
            f"hourly_start_date must be 2023-04-01 or later (new station ID system), "
            f"got '{date_str}'"
        )


def _validate_stations_file(file_path: Path) -> None:
    """
    Validate that stations of interest file exists.

    Args:
        file_path: Path to stations of interest JSON file

    Raises:
        FileNotFoundError: If file does not exist
    """
    if not file_path.exists():
        raise FileNotFoundError(
            f"Stations of interest file not found: {file_path}\n"
            f"Please create this file with a JSON list of station short_name IDs."
        )