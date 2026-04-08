# tasks/evaluate_prophet/types.py

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime


@dataclass
class EvaluateProphetContext:
    """Context for Prophet model evaluation via cross-validation."""

    test_split_start_dates: list[str]  # List of YYYY-MM-DD format dates
    test_split_days: int
    retrain_every_days: int
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate test split dates
        _validate_test_split_dates(self.test_split_start_dates)

        # Validate test split days
        _validate_test_split_days(self.test_split_days)

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
    def evaluation_dir(self) -> Path:
        """Path to evaluation outputs."""
        path = self.output_data_dir / "timeseries_results" / "evaluation" / "prophet"
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


def _validate_test_split_dates(dates: list[str]) -> None:
    """
    Validate test split start dates format.

    Args:
        dates: List of date strings in YYYY-MM-DD format

    Raises:
        ValueError: If any date format is invalid or list is empty
    """
    if not dates:
        raise ValueError("test_split_start_dates cannot be empty")

    for date_str in dates:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"All test_split_start_dates must be in YYYY-MM-DD format, " f"got '{date_str}'"
            )


def _validate_test_split_days(days: int) -> None:
    """
    Validate test split duration.

    Args:
        days: Number of days in test period

    Raises:
        ValueError: If days is not positive
    """
    if days <= 0:
        raise ValueError(f"test_split_days must be positive, got {days}")


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
