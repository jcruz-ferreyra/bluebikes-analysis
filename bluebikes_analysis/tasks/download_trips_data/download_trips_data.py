# tasks/download_data/download_data.py

import logging
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
from tqdm import tqdm
import zipfile

from .types import DownloadDataContext

logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _generate_date_range(ctx: DownloadDataContext) -> list[str]:
    """
    Generate list of YYYYMM strings from start_date to end_date (inclusive).

    Args:
        ctx: DownloadDataContext with start_date and end_date

    Returns:
        List of date strings in YYYYMM format
    """
    start = datetime.strptime(ctx.start_date, "%Y%m")
    end = datetime.strptime(ctx.end_date, "%Y%m")

    date_list = []
    current = start

    while current <= end:
        date_list.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    logger.info(
        f"Generated date range: {len(date_list)} months from {ctx.start_date} to {ctx.end_date}"
    )
    return date_list


def _construct_file_url(ctx: DownloadDataContext, date_str: str) -> tuple[str, str]:
    """
    Construct URL for trip data file, trying standard pattern first then .csv.zip fallback.

    Args:
        ctx: DownloadDataContext with main_url and system_name
        date_str: Date string in YYYYMM format

    Returns:
        Tuple of (url, pattern_type) where pattern_type is "standard" or "csv"
    """
    # Try standard pattern first: YYYYMM-systemname-tripdata.zip
    standard_url = f"{ctx.main_url}{date_str}-{ctx.system_name}-tripdata.zip"

    # Fallback pattern: YYYYMM-systemname-tripdata.csv.zip
    csv_url = f"{ctx.main_url}{date_str}-{ctx.system_name}-tripdata.csv.zip"

    # Check which URL exists
    try:
        response = requests.head(standard_url, timeout=10)
        if response.status_code == 200:
            return (standard_url, "standard")
    except requests.RequestException:
        pass

    # Try fallback
    try:
        response = requests.head(csv_url, timeout=10)
        if response.status_code == 200:
            return (csv_url, "csv")
    except requests.RequestException:
        pass

    # Return standard as default (will fail in download, but we log it)
    return (standard_url, "standard")


def _download_file(url: str, destination: Path) -> bool:
    """
    Download file from URL with progress bar.

    Args:
        url: URL to download from
        destination: Local file path to save to

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading: {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with open(destination, "wb") as f:
            with tqdm(total=total_size, unit="B", unit_scale=True, desc=destination.name) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        logger.info(f"✓ Downloaded: {destination.name}")
        return True

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"File not found (404): {url}")
        else:
            logger.error(f"HTTP error downloading {url}: {e}")
        return False

    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return False


def _extract_zip(zip_path: Path, destination_dir: Path) -> bool:
    """
    Extract zip file and remove the zip after extraction.

    Args:
        zip_path: Path to zip file
        destination_dir: Directory to extract contents to

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Extracting: {zip_path.name}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_dir)

        # Remove zip file
        zip_path.unlink()
        logger.info(f"✓ Extracted and removed zip: {zip_path.name}")
        return True

    except Exception as e:
        logger.error(f"Failed to extract {zip_path}: {e}")
        return False


def _download_and_extract_trip_file(ctx: DownloadDataContext, date_str: str) -> str:
    """
    Download trip data file for a specific date, extract it, and clean up.

    Args:
        ctx: DownloadDataContext with configuration
        date_str: Date string in YYYYMM format

    Returns:
        Status string: "success", "failed", or "skipped"
    """
    logger.info("=" * 60)
    logger.info(f"Processing: {date_str}")

    # Check if CSV already exists
    expected_csv = ctx.raw_trips_dir / f"{date_str}-{ctx.system_name}-tripdata.csv"
    if expected_csv.exists():
        logger.info(f"✓ CSV already exists, skipping: {expected_csv.name}")
        logger.info("=" * 60)
        return "skipped"

    # Construct URL
    url, pattern = _construct_file_url(ctx, date_str)
    logger.info(f"Using URL pattern: {pattern}")

    # Download
    zip_path = ctx.raw_trips_dir / f"{date_str}-{ctx.system_name}-tripdata.zip"
    download_success = _download_file(url, zip_path)

    if not download_success:
        logger.warning(f"Skipping {date_str} due to download failure")
        logger.info("=" * 60)
        return "failed"

    # Extract and cleanup
    extract_success = _extract_zip(zip_path, ctx.raw_trips_dir)

    if not extract_success:
        logger.warning(f"Failed to extract {date_str}")
        logger.info("=" * 60)
        return "failed"

    logger.info("=" * 60)
    return "success"


# ============================================================================
# Main public function
# ============================================================================


def download_trips_data(ctx: DownloadDataContext) -> None:
    """
    Download all Bluebikes trip data files in the specified date range.

    Args:
        ctx: DownloadDataContext containing configuration and output paths
    """
    logger.info("Starting Bluebikes trip data download")
    logger.info(f"Output directory: {ctx.raw_trips_dir}")

    # Generate date range
    date_range = _generate_date_range(ctx)

    # Track results
    total_files = len(date_range)
    successful = 0
    failed = 0
    skipped = 0

    # Download and extract each file
    for date_str in date_range:
        result = _download_and_extract_trip_file(ctx, date_str)
        if result == "success":
            successful += 1
        elif result == "failed":
            failed += 1
        elif result == "skipped":
            skipped += 1

    # Summary
    logger.info("=" * 60)
    logger.info("Download Summary:")
    logger.info(f"  Total files: {total_files}")
    logger.info(f"  Successful: {successful}")
    logger.info(f"  Skipped (already exist): {skipped}")
    logger.info(f"  Failed: {failed}")
    logger.info("=" * 60)
    logger.info("✓ Download process completed")
