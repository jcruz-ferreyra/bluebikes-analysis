# tasks/get_weather_data/__main__.py

from pathlib import Path

from bluebikes_forecasting.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR, NCEI_APIKEY
from bluebikes_forecasting.utils import load_config, setup_logging

# Setup logging
script_name = Path(__file__).parent.name
logger = setup_logging(script_name, LOCAL_DATA_DIR)

# Import task components
from bluebikes_forecasting.tasks.get_weather_data import (
    GetWeatherDataContext,
    get_weather_data,
)

logger.info("=" * 80)
logger.info("Starting get_weather_data task")
logger.info("=" * 80)

# Check API key is configured
if not NCEI_APIKEY:
    raise ValueError(
        "NCEI_APIKEY not found in environment variables.\n"
        "Please add NCEI_APIKEY to your .env file.\n"
        "Get a free API token at: https://www.ncdc.noaa.gov/cdo-web/token"
    )

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
context = GetWeatherDataContext(**script_config, output_data_dir=output_data_dir)

logger.info(f"Using {context.output_storage} storage: {context.output_data_dir}")
logger.info(f"Dataset: {context.dataset}")
logger.info(f"Station: {context.station}")
logger.info(f"Datatypes: {', '.join(context.datatypes)}")
logger.info(f"Date range: {context.start_date} to {context.end_date}")

# Call main function
get_weather_data(context)

logger.info("=" * 80)
logger.info("✓ get_weather_data task completed successfully")
logger.info("=" * 80)
