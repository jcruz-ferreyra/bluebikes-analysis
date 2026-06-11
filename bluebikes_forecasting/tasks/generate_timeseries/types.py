# tasks/generate_timeseries/types.py

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class GenerateTimeseriesContext(BaseModel):
    """Context for generating time series from aggregated trip data."""

    model_config = ConfigDict(extra="forbid")

    morning_start_hour: int = Field(ge=0, le=23)  # inclusive
    morning_end_hour: int = Field(ge=1, le=24)  # exclusive (e.g. 11 means up to 10:59)
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"

    @model_validator(mode="after")
    def _check_window_and_prepare_dirs(self) -> "GenerateTimeseriesContext":
        # Morning window must be non-empty (start inclusive, end exclusive)
        if self.morning_start_hour >= self.morning_end_hour:
            raise ValueError(
                f"morning_start_hour ({self.morning_start_hour}) must be less than "
                f"morning_end_hour ({self.morning_end_hour})"
            )

        # Validate output directory (same side effect as the old __post_init__)
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        return self

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
        path = self.output_data_dir / "processed" / "trips"
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
