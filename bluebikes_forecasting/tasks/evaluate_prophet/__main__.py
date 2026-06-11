# tasks/evaluate_prophet/__main__.py

from pathlib import Path

from bluebikes_forecasting.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR
from bluebikes_forecasting.utils import check_missing_keys, load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_forecasting.tasks.evaluate_prophet import (
    EvaluateProphetContext,
    evaluate_prophet,
)

logger.info("=" * 80)
logger.info("Starting evaluate_prophet task")
logger.info("=" * 80)

# Load config
CONFIG_PATH = Path(__file__).parent.resolve() / "config.yaml"
logger.info(f"Loading config from: {CONFIG_PATH}")
script_config = load_config(CONFIG_PATH)

# Validate config
required_keys = ["test_split_start_dates", "test_split_days"]
check_missing_keys(required_keys, script_config)

# Parse config
TEST_SPLIT_START_DATES = script_config["test_split_start_dates"]
TEST_SPLIT_DAYS = script_config["test_split_days"]
RETRAIN_EVERY_DAYS = script_config.get("retrain_every_days", 0)
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

logger.info(f"Test splits: {len(TEST_SPLIT_START_DATES)} periods")
for i, date in enumerate(TEST_SPLIT_START_DATES, 1):
    logger.info(f"  Split {i}: {date} ({TEST_SPLIT_DAYS} days)")
logger.info(f"Retrain every: {RETRAIN_EVERY_DAYS} days")

# Create context
context = EvaluateProphetContext(
    test_split_start_dates=TEST_SPLIT_START_DATES,
    test_split_days=TEST_SPLIT_DAYS,
    retrain_every_days=RETRAIN_EVERY_DAYS,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
)

# Call main function
evaluate_prophet(context)

logger.info("=" * 80)
logger.info("✓ evaluate_prophet task completed successfully")
logger.info("=" * 80)
