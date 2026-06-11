# tasks/evaluate_prophet/__main__.py

from pathlib import Path

from bluebikes_forecasting.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR
from bluebikes_forecasting.utils import load_config, setup_logging

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

# Resolve the storage-dependent output directory; output_storage itself is
# validated by the context model, not here
output_storage = script_config.get("output_storage", "local")
if output_storage == "drive":
    if DRIVE_DATA_DIR is None:
        raise ValueError("DRIVE_DATA_DIR not configured. Check .env file or use 'local' storage.")
    output_data_dir = DRIVE_DATA_DIR
else:
    output_data_dir = LOCAL_DATA_DIR

# Create and validate context (the YAML dict feeds the model directly)
context = EvaluateProphetContext(**script_config, output_data_dir=output_data_dir)

logger.info(f"Using {context.output_storage} storage: {context.output_data_dir}")
logger.info(f"Test splits: {len(context.test_split_start_dates)} periods")
for i, date in enumerate(context.test_split_start_dates, 1):
    logger.info(f"  Split {i}: {date} ({context.test_split_days} days)")
logger.info(f"Retrain every: {context.retrain_every_days} days")

# Call main function
evaluate_prophet(context)

logger.info("=" * 80)
logger.info("✓ evaluate_prophet task completed successfully")
logger.info("=" * 80)
