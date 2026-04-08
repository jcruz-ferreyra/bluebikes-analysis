# tasks/forecast_prophet/__main__.py

from pathlib import Path

from bluebikes_analysis.config import (
    LOCAL_DATA_DIR,
    DRIVE_DATA_DIR,
    LOCAL_MODELS_DIR,
    DRIVE_MODELS_DIR,
)
from bluebikes_analysis.utils import check_missing_keys, load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_analysis.tasks.forecast_prophet import (
    ForecastProphetContext,
    forecast_prophet,
)

logger.info("=" * 80)
logger.info("Starting forecast_prophet task")
logger.info("=" * 80)

# Load config
CONFIG_PATH = Path(__file__).parent.resolve() / "config.yaml"
logger.info(f"Loading config from: {CONFIG_PATH}")
script_config = load_config(CONFIG_PATH)

# Validate config
required_keys = ["inference_start_date"]
check_missing_keys(required_keys, script_config)

# Parse config
INFERENCE_START_DATE = script_config["inference_start_date"]
INFERENCE_END_DATE = script_config.get("inference_end_date", "end_of_data")
RETRAIN_EVERY_DAYS = script_config.get("retrain_every_days", 0)
SAVE_MODELS = script_config.get("save_models", False)
OUTPUT_STORAGE = script_config.get("output_storage", "local")

# Determine output directories
if OUTPUT_STORAGE == "drive":
    if DRIVE_DATA_DIR is None:
        raise ValueError("DRIVE_DATA_DIR not configured. Check .env file or use 'local' storage.")
    OUTPUT_DATA_DIR = DRIVE_DATA_DIR
    OUTPUT_MODELS_DIR = DRIVE_MODELS_DIR if SAVE_MODELS else None
    logger.info(f"Using Drive storage: {OUTPUT_DATA_DIR}")
elif OUTPUT_STORAGE == "local":
    OUTPUT_DATA_DIR = LOCAL_DATA_DIR
    OUTPUT_MODELS_DIR = LOCAL_MODELS_DIR if SAVE_MODELS else None
    logger.info(f"Using local storage: {OUTPUT_DATA_DIR}")
else:
    raise ValueError(f"Invalid output_storage: '{OUTPUT_STORAGE}'. Use 'local' or 'drive'.")

logger.info(f"Inference start date: {INFERENCE_START_DATE}")
logger.info(f"Inference end date: {INFERENCE_END_DATE}")
logger.info(f"Retrain every: {RETRAIN_EVERY_DAYS} days")
logger.info(f"Save models: {SAVE_MODELS}")

# Create context
context = ForecastProphetContext(
    inference_start_date=INFERENCE_START_DATE,
    inference_end_date=INFERENCE_END_DATE,
    retrain_every_days=RETRAIN_EVERY_DAYS,
    save_models=SAVE_MODELS,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
    output_models_dir=OUTPUT_MODELS_DIR,
)

# Call main function
forecast_prophet(context)

logger.info("=" * 80)
logger.info("✓ forecast_prophet task completed successfully")
logger.info("=" * 80)
