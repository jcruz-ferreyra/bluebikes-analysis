# tasks/get_weather_data/types.py

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime


@dataclass
class GetWeatherDataContext:
    """Context for fetching NCEI weather data."""

    dataset: str
    station: str
    datatypes: list[str]
    start_date: str
    end_date: str
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate dates
        _validate_dates(self.start_date, self.end_date)

        # Validate datatypes
        _validate_datatypes(self.datatypes)

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


def _validate_dates(start_date: str, end_date: str) -> None:
    """
    Validate date format and range.

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD or "yesterday")

    Raises:
        ValueError: If dates are invalid
    """
    # Validate start_date format
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"start_date must be in YYYY-MM-DD format, got '{start_date}'")

    # Validate end_date format (allow "yesterday")
    if end_date != "yesterday":
        try:
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"end_date must be in YYYY-MM-DD format or 'yesterday', got '{end_date}'"
            )


def _validate_datatypes(datatypes: list[str]) -> None:
    """
    Validate that datatypes list is not empty.

    Args:
        datatypes: List of NCEI datatype codes

    Raises:
        ValueError: If datatypes is empty
    """
    if not datatypes:
        raise ValueError("datatypes list cannot be empty")

    valid_datatypes = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
    for dt in datatypes:
        if dt not in valid_datatypes:
            raise ValueError(f"Unknown datatype '{dt}'. Valid options: {valid_datatypes}")
