# tasks/aggregate_trips/__main__.py

from pathlib import Path

from bluebikes_analysis.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR
from bluebikes_analysis.utils import check_missing_keys, load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_analysis.tasks.aggregate_trips import (
    AggregateTripsContext,
    aggregate_trips,
)

logger.info("=" * 80)
logger.info("Starting aggregate_trips task")
logger.info("=" * 80)

# Load config
CONFIG_PATH = Path(__file__).parent.resolve() / "config.yaml"
logger.info(f"Loading config from: {CONFIG_PATH}")
script_config = load_config(CONFIG_PATH)

# Validate config
required_keys = ["stations_of_interest_file"]
check_missing_keys(required_keys, script_config)

# Parse config
STATIONS_OF_INTEREST_FILE = script_config["stations_of_interest_file"]
HOURLY_START_DATE = script_config.get("hourly_start_date", "2023-04-01")
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

logger.info(f"Stations of interest file: {STATIONS_OF_INTEREST_FILE}")
logger.info(f"Hourly aggregation start date: {HOURLY_START_DATE}")

# Create context
context = AggregateTripsContext(
    stations_of_interest_file=STATIONS_OF_INTEREST_FILE,
    hourly_start_date=HOURLY_START_DATE,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
)

# Call main function
aggregate_trips(context)

logger.info("=" * 80)
logger.info("✓ aggregate_trips task completed successfully")
logger.info("=" * 80)