# tasks/download_data/__main__.py

from pathlib import Path

from bluebikes_analysis.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR
from bluebikes_analysis.utils import check_missing_keys, load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_analysis.tasks.download_trips_data import (
    DownloadTripsDataContext,
    download_trips_data,
)

logger.info("=" * 80)
logger.info("Starting download_data task")
logger.info("=" * 80)

# Load config
CONFIG_PATH = Path(__file__).parent.resolve() / "config.yaml"
logger.info(f"Loading config from: {CONFIG_PATH}")
script_config = load_config(CONFIG_PATH)

# Validate config
required_keys = ["main_url", "system_name", "start_date", "end_date"]
check_missing_keys(required_keys, script_config)

# Parse config
MAIN_URL = script_config["main_url"]
SYSTEM_NAME = script_config["system_name"]
START_DATE = script_config["start_date"]
END_DATE = script_config["end_date"]
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

logger.info(f"Main URL: {MAIN_URL}")
logger.info(f"System name: {SYSTEM_NAME}")
logger.info(f"Date range: {START_DATE} to {END_DATE}")

# Create context
context = DownloadTripsDataContext(
    main_url=MAIN_URL,
    system_name=SYSTEM_NAME,
    start_date=START_DATE,
    end_date=END_DATE,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
)

# Call main function
download_trips_data(context)

logger.info("=" * 80)
logger.info("✓ download_data task completed successfully")
logger.info("=" * 80)