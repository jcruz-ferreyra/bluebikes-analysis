# tasks/download_stations_data/types.py

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class DownloadStationsDataContext(BaseModel):
    """Context for downloading Bluebikes station data via GBFS API."""

    model_config = ConfigDict(extra="forbid")

    version: str = "1.1"
    download_metadata: bool = False
    download_status: bool = False
    output_data_dir: Path
    output_storage: Literal["local", "drive"] = "local"

    @field_validator("version")
    @classmethod
    def _check_supported_version(cls, value: str) -> str:
        # Only the GBFS version used by Bluebikes' official website is parseable
        SUPPORTED_VERSION = "1.1"

        if value != SUPPORTED_VERSION:
            raise ValueError(
                f"\nUnsupported GBFS version: '{value}'\n\n"
                f"Currently, only version '{SUPPORTED_VERSION}' is supported for download and parsing.\n"
                f"This is the stable version currently used by Bluebikes' official website.\n\n"
                f"Available GBFS versions can be found at:\n"
                f"https://gbfs.lyft.com/gbfs/1.1/bos/en/gbfs_versions.json\n\n"
                f"To use version '{value}':\n"
                f"1. Remove this version check in types.py (_check_supported_version)\n"
                f"2. Update the JSON parsing logic in download_stations_data.py to handle the new format\n"
                f"3. Test thoroughly to ensure compatibility\n"
            )
        return value

    @model_validator(mode="after")
    def _check_downloads_and_prepare_dirs(self) -> "DownloadStationsDataContext":
        # At least one download option must be enabled
        if not self.download_metadata and not self.download_status:
            raise ValueError(
                "At least one download option must be enabled.\n"
                "Set 'download_metadata: true' or 'download_status: true' in config.yaml"
            )

        # Validate output directory (same side effect as the old __post_init__)
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        return self

    @property
    def base_url(self) -> str:
        """Base URL for GBFS API."""
        return f"https://gbfs.lyft.com/gbfs/{self.version}/bos/en"

    @property
    def station_information_url(self) -> str:
        """URL for station information (metadata)."""
        return f"{self.base_url}/station_information.json"

    @property
    def system_regions_url(self) -> str:
        """URL for system regions."""
        return f"{self.base_url}/system_regions.json"

    @property
    def station_status_url(self) -> str:
        """URL for station status."""
        return f"{self.base_url}/station_status.json"

    @property
    def stations_dir(self) -> Path:
        """Path to stations data directory."""
        path = self.output_data_dir / "raw" / "stations"
        status_path = path / "status"
        status_path.mkdir(parents=True, exist_ok=True)
        return path
