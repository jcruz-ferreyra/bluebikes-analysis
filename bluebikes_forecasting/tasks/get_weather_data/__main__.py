# tasks/get_weather_data/__main__.py

from pathlib import Path

from bluebikes_forecasting.config import LOCAL_DATA_DIR, DRIVE_DATA_DIR, NCEI_APIKEY
from bluebikes_forecasting.utils import check_missing_keys, load_config, setup_logging

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

# Validate config
required_keys = ["dataset", "station", "datatypes", "start_date", "end_date"]
check_missing_keys(required_keys, script_config)

# Parse config
DATASET = script_config["dataset"]
STATION = script_config["station"]
DATATYPES = script_config["datatypes"]
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

logger.info(f"Dataset: {DATASET}")
logger.info(f"Station: {STATION}")
logger.info(f"Datatypes: {', '.join(DATATYPES)}")
logger.info(f"Date range: {START_DATE} to {END_DATE}")

# Create context
context = GetWeatherDataContext(
    dataset=DATASET,
    station=STATION,
    datatypes=DATATYPES,
    start_date=START_DATE,
    end_date=END_DATE,
    output_data_dir=OUTPUT_DATA_DIR,
    output_storage=OUTPUT_STORAGE,
)

# Call main function
get_weather_data(context)

logger.info("=" * 80)
logger.info("✓ get_weather_data task completed successfully")
logger.info("=" * 80)
