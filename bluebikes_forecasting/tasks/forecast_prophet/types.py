# tasks/forecast_prophet/types.py

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ForecastProphetContext(BaseModel):
    """Context for Prophet demand forecasting."""

    model_config = ConfigDict(extra="forbid")

    inference_start_date: str  # YYYY-MM-DD format
    inference_end_date: str = "end_of_data"  # YYYY-MM-DD format or "end_of_data"
    retrain_every_days: int = Field(default=0, ge=0)  # 0 = train once, no retraining
    save_models: bool = False
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"
    output_models_dir: Path | None = None

    @field_validator("inference_start_date")
    @classmethod
    def _check_start_date(cls, value: str) -> str:
        # Must be a real calendar date
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"inference_start_date must be in YYYY-MM-DD format, got '{value}'")
        return value

    @field_validator("inference_end_date")
    @classmethod
    def _check_end_date(cls, value: str) -> str:
        # "end_of_data" resolves at run time; anything else must be a real date
        if value == "end_of_data":
            return value
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"inference_end_date must be in YYYY-MM-DD format or 'end_of_data', got '{value}'"
            )
        return value

    @model_validator(mode="after")
    def _check_window(self) -> "ForecastProphetContext":
        # A concrete end date must come after the start date
        if self.inference_end_date != "end_of_data":
            start_date = datetime.strptime(self.inference_start_date, "%Y-%m-%d")
            end_date = datetime.strptime(self.inference_end_date, "%Y-%m-%d")
            if end_date <= start_date:
                raise ValueError(
                    f"inference_end_date must be after inference_start_date, "
                    f"got '{self.inference_end_date}' <= '{self.inference_start_date}'"
                )
        return self

    @property
    def timeseries_dir(self) -> Path:
        """Path to processed timeseries (input)."""
        return self.output_data_dir / "processed" / "trips"

    @property
    def station_timeseries_dir(self) -> Path:
        """Path to station-level timeseries (input)."""
        return self.timeseries_dir / "station"

    @property
    def forecasts_dir(self) -> Path:
        """Path to forecast outputs."""
        path = self.output_data_dir / "timeseries_results" / "forecasts" / "prophet"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def models_dir(self) -> Path | None:
        """Path to saved models (if save_models=True)."""
        if self.output_models_dir is None:
            return None
        path = self.output_models_dir / "prophet"
        if self.save_models:
            path.mkdir(parents=True, exist_ok=True)
        return path
