# tasks/forecast_prophet/types.py

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime


@dataclass
class ForecastProphetContext:
    """Context for Prophet demand forecasting."""

    inference_start_date: str  # YYYY-MM-DD format
    inference_end_date: str  # YYYY-MM-DD format or "end_of_data"
    retrain_every_days: int
    save_models: bool
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"
    output_models_dir: Path | None = None

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate inference dates
        _validate_inference_date(self.inference_start_date)
        _validate_inference_end_date(self.inference_end_date, self.inference_start_date)

        # Validate retrain frequency
        _validate_retrain_days(self.retrain_every_days)

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


def _validate_inference_date(date_str: str) -> None:
    """
    Validate inference start date format.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Raises:
        ValueError: If date format is invalid
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"inference_start_date must be in YYYY-MM-DD format, got '{date_str}'")


def _validate_inference_end_date(date_str: str, start_date_str: str) -> None:
    """
    Validate inference end date format and that it is after the start date.

    Args:
        date_str: Date string in YYYY-MM-DD format or "end_of_data"
        start_date_str: Start date string in YYYY-MM-DD format

    Raises:
        ValueError: If date format is invalid or end date is not after start date
    """
    if date_str == "end_of_data":
        return

    try:
        end_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"inference_end_date must be in YYYY-MM-DD format or 'end_of_data', got '{date_str}'"
        )

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    if end_date <= start_date:
        raise ValueError(
            f"inference_end_date must be after inference_start_date, got '{date_str}' <= '{start_date_str}'"
        )


def _validate_retrain_days(days: int) -> None:
    """
    Validate retrain frequency.

    Args:
        days: Number of days between retraining (0 = no retraining)

    Raises:
        ValueError: If days is negative
    """
    if days < 0:
        raise ValueError(f"retrain_every_days must be >= 0, got {days}")
