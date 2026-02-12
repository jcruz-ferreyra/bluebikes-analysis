# tasks/download_stations_data/download_stations_data.py

import logging
import json
from pathlib import Path
from datetime import datetime
import requests
import csv

from .types import DownloadStationsDataContext

logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _fetch_json(url: str) -> dict | None:
    """
    Fetch JSON data from a URL.

    Args:
        url: URL to fetch from

    Returns:
        Parsed JSON as dictionary, or None if failed
    """
    try:
        logger.info(f"Fetching: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        logger.info(f"✓ Successfully fetched JSON from {url}")
        return data

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching {url}: {e}")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching {url}: {e}")
        return None

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error from {url}: {e}")
        return None

    except Exception as e:
        logger.error(f"Unexpected error fetching {url}: {e}")
        return None


def _parse_station_information(json_data: dict) -> list[dict]:
    """
    Parse station information JSON and extract relevant fields.

    Args:
        json_data: Raw JSON from station_information endpoint

    Returns:
        List of station dictionaries with filtered fields
    """
    stations = json_data.get("data", {}).get("stations", [])

    # Fields compatible with v2.3 (excluding rental_uris)
    filtered_fields = ["station_id", "name", "short_name", "lat", "lon", "capacity", "region_id"]

    filtered_stations = []
    for station in stations:
        filtered_station = {field: station.get(field) for field in filtered_fields}
        filtered_stations.append(filtered_station)

    logger.info(f"Parsed {len(filtered_stations)} stations")
    return filtered_stations


def _parse_system_regions(json_data: dict) -> dict[str, str]:
    """
    Parse system regions JSON into a mapping.

    Args:
        json_data: Raw JSON from system_regions endpoint

    Returns:
        Dictionary mapping region_id to region_name
    """
    regions_list = json_data.get("data", {}).get("regions", [])

    regions_map = {region["region_id"]: region["name"] for region in regions_list}

    logger.info(f"Parsed {len(regions_map)} regions")
    return regions_map


def _merge_regions_into_stations(stations: list[dict], regions: dict[str, str]) -> list[dict]:
    """
    Merge region names into station data.

    Args:
        stations: List of station dictionaries
        regions: Dictionary mapping region_id to region_name

    Returns:
        List of station dictionaries with region_name added
    """
    for station in stations:
        region_id = station.get("region_id")
        station["region_name"] = regions.get(region_id, "Unknown")

    logger.info(f"Merged region names into {len(stations)} stations")
    return stations


def _save_stations_to_csv(stations: list[dict], output_path: Path) -> None:
    """
    Save station data to CSV file.

    Args:
        stations: List of station dictionaries
        output_path: Path to output CSV file
    """
    if not stations:
        logger.warning("No stations to save")
        return

    # Get fieldnames from first station
    fieldnames = list(stations[0].keys())

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(stations)

        logger.info(f"✓ Saved {len(stations)} stations to {output_path.name}")

    except Exception as e:
        logger.error(f"Failed to save stations to CSV: {e}")
        raise


def _parse_station_status(json_data: dict) -> list[dict]:
    """
    Parse station status JSON and extract relevant fields.

    Args:
        json_data: Raw JSON from station_status endpoint

    Returns:
        List of station status dictionaries with filtered fields
    """
    stations = json_data.get("data", {}).get("stations", [])

    # Fields compatible with v2.3 (excluding vehicle_types_available)
    filtered_fields = [
        "station_id",
        "is_installed",
        "is_renting",
        "is_returning",
        "num_bikes_available",
        "num_ebikes_available",
        "num_bikes_disabled",
        "num_docks_available",
        "num_docks_disabled",
        "num_scooters_available",
        "num_scooters_unavailable",
        "last_reported",
    ]

    filtered_stations = []
    for station in stations:
        filtered_station = {field: station.get(field) for field in filtered_fields}

        # Add computed field: num_regular_bikes_available
        num_bikes = filtered_station.get("num_bikes_available", 0) or 0
        num_ebikes = filtered_station.get("num_ebikes_available", 0) or 0
        filtered_station["num_conventional_available"] = num_bikes - num_ebikes

        filtered_stations.append(filtered_station)

    logger.info(f"Parsed status for {len(filtered_stations)} stations")
    return filtered_stations


def _save_status_to_csv(status_data: list[dict], output_path: Path) -> None:
    """
    Save station status data to CSV file with timestamp.

    Args:
        status_data: List of station status dictionaries
        output_path: Path to output CSV file
    """
    if not status_data:
        logger.warning("No status data to save")
        return

    # Get fieldnames from first station
    fieldnames = list(status_data[0].keys())

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(status_data)

        logger.info(f"✓ Saved status for {len(status_data)} stations to {output_path.name}")

    except Exception as e:
        logger.error(f"Failed to save status to CSV: {e}")
        raise


def _download_metadata(ctx: DownloadStationsDataContext) -> None:
    """
    Download station information and regions, merge them, and save to CSV.

    Args:
        ctx: DownloadStationsDataContext with configuration
    """
    logger.info("=" * 60)
    logger.info("Downloading station metadata")
    logger.info("=" * 60)

    # 1. Fetch station_information.json
    station_info_data = _fetch_json(ctx.station_information_url)
    if station_info_data is None:
        logger.error("Failed to fetch station information")
        return

    # 2. Fetch system_regions.json
    regions_data = _fetch_json(ctx.system_regions_url)
    if regions_data is None:
        logger.error("Failed to fetch system regions")
        return

    # 3. Parse both
    stations = _parse_station_information(station_info_data)
    regions = _parse_system_regions(regions_data)

    # 4. Merge regions into stations
    stations_with_regions = _merge_regions_into_stations(stations, regions)

    # 5. Save to station_information.csv
    output_path = ctx.stations_dir / "station_information.csv"
    _save_stations_to_csv(stations_with_regions, output_path)

    logger.info("=" * 60)
    logger.info("✓ Metadata download completed")
    logger.info("=" * 60)


def _download_status(ctx: DownloadStationsDataContext) -> None:
    """
    Download current station status and save to timestamped CSV.

    Args:
        ctx: DownloadStationsDataContext with configuration
    """
    logger.info("=" * 60)
    logger.info("Downloading station status")
    logger.info("=" * 60)

    # 1. Fetch station_status.json
    status_data = _fetch_json(ctx.station_status_url)
    if status_data is None:
        logger.error("Failed to fetch station status")
        return

    # 2. Parse and filter fields
    status_list = _parse_station_status(status_data)

    # 3. Save to station_status_{timestamp}.csv
    import pytz
    timestamp = datetime.now(pytz.utc).strftime("%y%m%d_%H%M%S")
    output_path = ctx.stations_dir / f"station_status_{timestamp}.csv"
    _save_status_to_csv(status_list, output_path)

    logger.info("=" * 60)
    logger.info("✓ Status download completed")
    logger.info("=" * 60)


# ============================================================================
# Main public function
# ============================================================================


def download_stations_data(ctx: DownloadStationsDataContext) -> None:
    """
    Download Bluebikes station data (metadata and/or status).

    Args:
        ctx: DownloadStationsDataContext containing configuration and output paths
    """
    logger.info("Starting Bluebikes station data download")
    logger.info(f"Output directory: {ctx.stations_dir}")

    # Download metadata if requested
    if ctx.download_metadata:
        _download_metadata(ctx)

    # Download status if requested
    if ctx.download_status:
        _download_status(ctx)

    logger.info("✓ Station data download completed")
