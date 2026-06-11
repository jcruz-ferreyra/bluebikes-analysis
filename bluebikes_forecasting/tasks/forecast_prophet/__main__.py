# tasks/forecast_prophet/__main__.py

from pathlib import Path

from bluebikes_forecasting.config import (
    LOCAL_DATA_DIR,
    DRIVE_DATA_DIR,
    LOCAL_MODELS_DIR,
    DRIVE_MODELS_DIR,
)
from bluebikes_forecasting.utils import load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_forecasting.tasks.forecast_prophet import (
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

# Resolve the storage-dependent output directories; output_storage itself is
# validated by the context model, not here
output_storage = script_config.get("output_storage", "local")
save_models = script_config.get("save_models", False)
if output_storage == "drive":
    if DRIVE_DATA_DIR is None:
        raise ValueError("DRIVE_DATA_DIR not configured. Check .env file or use 'local' storage.")
    output_data_dir = DRIVE_DATA_DIR
    output_models_dir = DRIVE_MODELS_DIR if save_models else None
else:
    output_data_dir = LOCAL_DATA_DIR
    output_models_dir = LOCAL_MODELS_DIR if save_models else None

# Create and validate context (the YAML dict feeds the model directly)
context = ForecastProphetContext(
    **script_config, output_data_dir=output_data_dir, output_models_dir=output_models_dir
)

logger.info(f"Using {context.output_storage} storage: {context.output_data_dir}")
logger.info(f"Inference start date: {context.inference_start_date}")
logger.info(f"Inference end date: {context.inference_end_date}")
logger.info(f"Retrain every: {context.retrain_every_days} days")
logger.info(f"Save models: {context.save_models}")

# Call main function
forecast_prophet(context)

logger.info("=" * 80)
logger.info("✓ forecast_prophet task completed successfully")
logger.info("=" * 80)
