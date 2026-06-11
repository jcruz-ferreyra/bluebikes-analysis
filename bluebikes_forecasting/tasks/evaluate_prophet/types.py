# tasks/evaluate_prophet/types.py

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EvaluateProphetContext(BaseModel):
    """Context for Prophet model evaluation via cross-validation."""

    model_config = ConfigDict(extra="forbid")

    test_split_start_dates: list[str] = Field(min_length=1)  # YYYY-MM-DD format dates
    test_split_days: int = Field(gt=0)
    retrain_every_days: int = Field(default=0, ge=0)  # 0 = train once, no retraining
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"

    @field_validator("test_split_start_dates")
    @classmethod
    def _check_split_dates(cls, value: list[str]) -> list[str]:
        # Every split start must be a real calendar date
        for date_str in value:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"All test_split_start_dates must be in YYYY-MM-DD format, got '{date_str}'"
                )
        return value

    @model_validator(mode="after")
    def _prepare_dirs(self) -> "EvaluateProphetContext":
        # Validate output directory (same side effect as the old __post_init__)
        self.output_data_dir.mkdir(parents=True, exist_ok=True)
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
    def evaluation_dir(self) -> Path:
        """Path to evaluation outputs."""
        path = self.output_data_dir / "timeseries_results" / "evaluation" / "prophet"
        path.mkdir(parents=True, exist_ok=True)
        return path
