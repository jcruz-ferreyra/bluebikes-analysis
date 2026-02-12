# tasks/download_stations_data/types.py

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DownloadStationsDataContext:
    """Context for downloading Bluebikes station data via GBFS API."""

    version: str
    download_metadata: bool
    download_status: bool
    output_data_dir: Path
    output_storage: str = "local"  # "local" or "drive"

    def __post_init__(self):
        # Validate output directory
        self.output_data_dir.mkdir(parents=True, exist_ok=True)

        # Validate storage option
        _validate_storage(self.output_storage)

        # Validate version
        _validate_version(self.version)

        # Validate at least one download option is enabled
        _validate_anything_to_download(self.download_metadata, self.download_status)

        # Validate at least one download option is enabled
        if not self.download_metadata and not self.download_status:
            raise ValueError("At least one of download_metadata or download_status must be True")

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


def _validate_version(version: str) -> None:
    """
    Validate that the GBFS version is supported.

    Args:
        version: GBFS version string (e.g., "1.1", "2.3")

    Raises:
        ValueError: If version is not supported
    """
    SUPPORTED_VERSION = "1.1"

    if version != SUPPORTED_VERSION:
        error_msg = (
            f"\nUnsupported GBFS version: '{version}'\n\n"
            f"Currently, only version '{SUPPORTED_VERSION}' is supported for download and parsing.\n"
            f"This is the stable version currently used by Bluebikes' official website.\n\n"
            f"Available GBFS versions can be found at:\n"
            f"https://gbfs.lyft.com/gbfs/1.1/bos/en/gbfs_versions.json\n\n"
            f"To use version '{version}':\n"
            f"1. Remove this version check in types.py (_validate_version)\n"
            f"2. Update the JSON parsing logic in download_stations_data.py to handle the new format\n"
            f"3. Test thoroughly to ensure compatibility\n"
        )
        raise ValueError(error_msg)


def _validate_anything_to_download(download_metadata: bool, download_status: bool) -> None:
    """
    Validate that at least one download option is enabled.

    Args:
        download_metadata: Whether to download metadata
        download_status: Whether to download status

    Raises:
        ValueError: If both options are False
    """
    if not download_metadata and not download_status:
        raise ValueError(
            "At least one download option must be enabled.\n"
            "Set 'download_metadata: true' or 'download_status: true' in config.yaml"
        )
