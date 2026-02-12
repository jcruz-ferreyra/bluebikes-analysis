# tasks/download_stations_data/__main__.py

from pathlib import Path

from bluebikes_analysis.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR
from bluebikes_analysis.utils import check_missing_keys, load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_analysis.tasks.download_stations_data import (
    DownloadStationsDataContext,
    download_stations_data,
)

logger.info("=" * 80)
logger.info("Starting download_stations_data task")
logger.info("=" * 80)

# Load config
CONFIG_PATH = Path(__file__).parent.resolve() / "config.yaml"
logger.info(f"Loading config from: {CONFIG_PATH}")
script_config = load_config(CONFIG_PATH)

# Validate config
required_keys = []
check_missing_keys(required_keys, script_config)

# Parse config
VERSION = script_config.get("version", "1.1")
DOWNLOAD_METADATA = script_config.get("download_metadata", False)
DOWNLOAD_STATUS = script_config.get("download_status", False)
OUTPUT_STORAGE = script_config.get("output_storage", "local")

# Determine output directory
if OUTPUT_STORAGE == "drive":
    if DRIVE_DATA_DIR is None:
        raise ValueError("DRIVE_DATA_DIR not configured. Check .env file or use 'local' storage.")
    OUTPUT_DATA_DIR = DRIVE_DATA_DIR
    logger.info(f"Using Drive storage: {OUTPUT_DATA_DIR}")
elif OUTPUT_STORAGE == "local":
    OUTPUT_DATA_DIR = LOCAL_DATA_DIR
    logger.info(f"Using local storage: {OUTPUT_DATA_DIR}")
else:
    raise ValueError(f"Invalid output_storage: '{OUTPUT_STORAGE}'. Use 'local' or 'drive'.")

logger.info(f"GBFS version: {VERSION}")
logger.info(f"Download metadata: {DOWNLOAD_METADATA}")
logger.info(f"Download status: {DOWNLOAD_STATUS}")

# Create context
context = DownloadStationsDataContext(
    version=VERSION,
    download_metadata=DOWNLOAD_METADATA,
    download_status=DOWNLOAD_STATUS,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
)

# Call main function
download_stations_data(context)

logger.info("=" * 80)
logger.info("✓ download_stations_data task completed successfully")
logger.info("=" * 80)