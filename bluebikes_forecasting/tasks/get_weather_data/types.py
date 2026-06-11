# tasks/get_weather_data/types.py

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Datatype = Literal["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]


class GetWeatherDataContext(BaseModel):
    """Context for fetching NCEI weather data."""

    model_config = ConfigDict(extra="forbid")

    dataset: str
    station: str
    datatypes: list[Datatype] = Field(min_length=1)
    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format or "yesterday"
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"

    @field_validator("start_date")
    @classmethod
    def _check_start_date(cls, value: str) -> str:
        # Must be a real calendar date
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"start_date must be in YYYY-MM-DD format, got '{value}'")
        return value

    @field_validator("end_date")
    @classmethod
    def _check_end_date(cls, value: str) -> str:
        # "yesterday" resolves at fetch time; anything else must be a real date
        if value == "yesterday":
            return value
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"end_date must be in YYYY-MM-DD format or 'yesterday', got '{value}'"
            )
        return value

    @model_validator(mode="after")
    def _prepare_dirs(self) -> "GetWeatherDataContext":
        # Validate output directory (same side effect as the old __post_init__)
        self.output_data_dir.mkdir(parents=True, exist_ok=True)
        return self

    @property
    def weather_dir(self) -> Path:
        """Path to processed weather data output."""
        path = self.output_data_dir / "processed" / "weather"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def output_path(self) -> Path:
        """Path to output weather CSV."""
        return self.weather_dir / "daily_weather.csv"
