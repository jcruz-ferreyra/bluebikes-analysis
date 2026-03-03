# tasks/generate_timeseries/__main__.py

from pathlib import Path

from bluebikes_analysis.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR
from bluebikes_analysis.utils import check_missing_keys, load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_analysis.tasks.generate_timeseries import (
    GenerateTimeseriesContext,
    generate_timeseries,
)

logger.info("=" * 80)
logger.info("Starting generate_timeseries task")
logger.info("=" * 80)

# Load config
CONFIG_PATH = Path(__file__).parent.resolve() / "config.yaml"
logger.info(f"Loading config from: {CONFIG_PATH}")
script_config = load_config(CONFIG_PATH)

# Validate config
required_keys = ["morning_start_hour", "morning_end_hour"]
check_missing_keys(required_keys, script_config)

# Parse config
MORNING_START_HOUR = script_config["morning_start_hour"]
MORNING_END_HOUR = script_config["morning_end_hour"]
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

logger.info(f"Morning hours: {MORNING_START_HOUR}:00 - {MORNING_END_HOUR}:59")

# Create context
context = GenerateTimeseriesContext(
    morning_start_hour=MORNING_START_HOUR,
    morning_end_hour=MORNING_END_HOUR,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
)

# Call main function
generate_timeseries(context)

logger.info("=" * 80)
logger.info("✓ generate_timeseries task completed successfully")
logger.info("=" * 80)
