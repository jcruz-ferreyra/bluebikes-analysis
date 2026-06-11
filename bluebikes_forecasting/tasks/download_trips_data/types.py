# tasks/download_data/types.py

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DownloadTripsDataContext:
    """Context for downloading Bluebikes trip data."""

    main_url: str
    system_name: str
    start_date: str  # YYYYMM format
    end_date: str  # YYYYMM format
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate date format (YYYYMM)
        _validate_date_format(self.start_date, "start_date")
        _validate_date_format(self.end_date, "end_date")

        # Validate URL ends with /
        if not self.main_url.endswith("/"):
            self.main_url = self.main_url + "/"

    @property
    def raw_trips_dir(self) -> Path:
        """Path to raw trip data CSVs."""
        path = self.output_data_dir / "raw" / "trips"
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
        raise ValueError(f"output_storage must be one of {valid_storages}, " f"got '{storage}'")


def _validate_date_format(date_str: str, field_name: str) -> None:
    """
    Validate that a date string is in YYYYMM format.

    Args:
        date_str: Date string to validate
        field_name: Name of the field (for error message)

    Raises:
        ValueError: If date format is invalid
    """
    if len(date_str) != 6 or not date_str.isdigit():
        raise ValueError(f"{field_name} must be YYYYMM format, got '{date_str}'")
