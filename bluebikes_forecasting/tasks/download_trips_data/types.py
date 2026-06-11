# tasks/download_trips_data/types.py

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DownloadTripsDataContext(BaseModel):
    """Context for downloading Bluebikes trip data."""

    model_config = ConfigDict(extra="forbid")

    main_url: str
    system_name: str
    start_date: str = Field(pattern=r"^\d{6}$")  # YYYYMM format
    end_date: str = Field(pattern=r"^\d{6}$")  # YYYYMM format
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"

    @field_validator("main_url")
    @classmethod
    def _ensure_trailing_slash(cls, value: str) -> str:
        # Downstream URL building concatenates filenames directly
        return value if value.endswith("/") else value + "/"

    @property
    def raw_trips_dir(self) -> Path:
        """Path to raw trip data CSVs."""
        path = self.output_data_dir / "raw" / "trips"
        path.mkdir(parents=True, exist_ok=True)
        return path
