# tasks/aggregate_trips/types.py

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class AggregateTripsContext(BaseModel):
    """Context for aggregating Bluebikes trip data."""

    model_config = ConfigDict(extra="forbid")

    stations_of_interest_file: str
    hourly_start_date: str = "2023-04-01"  # YYYY-MM-DD format
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"

    @field_validator("hourly_start_date")
    @classmethod
    def _check_hourly_start_date(cls, value: str) -> str:
        # Hourly aggregation is only valid under the new station-ID system
        try:
            date = datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"hourly_start_date must be in YYYY-MM-DD format, got '{value}'")

        if date < datetime(2023, 4, 1):
            raise ValueError(
                f"hourly_start_date must be 2023-04-01 or later (new station ID system), "
                f"got '{value}'"
            )
        return value

    @model_validator(mode="after")
    def _check_inputs(self) -> "AggregateTripsContext":
        # Stations of interest file must exist before any loading starts
        if not self.stations_of_interest_path.exists():
            raise ValueError(
                f"Stations of interest file not found: {self.stations_of_interest_path}\n"
                f"Please create this file with a JSON list of station short_name IDs."
            )

        return self

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
