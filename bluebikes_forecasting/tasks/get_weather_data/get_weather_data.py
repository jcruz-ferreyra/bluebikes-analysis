# tasks/get_weather_data/get_weather_data.py

from datetime import date, timedelta
import logging
import time

import pandas as pd
import requests

from bluebikes_forecasting.config import NCEI_APIKEY

from .types import GetWeatherDataContext

logger = logging.getLogger(__name__)

# NCEI API configuration
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"


# ============================================================================
# Helper functions
# ============================================================================


def _fetch_year_range(
    dataset: str, station: str, datatypes: list[str], start: date, end: date, max_retries: int = 3
) -> list[dict]:
    """
    Fetch all records between two dates, paginating as needed with retry logic.

    Args:
        dataset: NCEI dataset ID
        station: Station ID
        datatypes: List of datatype codes
        start: Start date
        end: End date
        max_retries: Maximum retry attempts for 503 errors

    Returns:
        List of weather records
    """
    headers = {"token": NCEI_APIKEY}
    params = {
        "datasetid": dataset,
        "stationid": f"{dataset}:{station}",
        "datatypeid": ",".join(datatypes),
        "startdate": str(start),
        "enddate": str(end),
        "units": "standard",
        "limit": 1000,
        "includemetadata": "false",
    }

    records = []
    offset = 1

    while True:
        params["offset"] = offset

        # Retry logic for 503 errors
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{BASE_URL}/data", headers=headers, params=params)
                response.raise_for_status()
                break  # Success, exit retry loop
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503 and attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.warning(
                        f"  503 error at offset {offset}, retrying in {wait_time}s (attempt {attempt+1}/{max_retries})..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"HTTP error fetching data: {e}")
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                raise

        batch = response.json().get("results", [])

        if not batch:
            break

        records.extend(batch)
        offset += 1000

        # Small delay to avoid rate limiting
        time.sleep(0.2)

    return records


def _fetch_weather_data(ctx: GetWeatherDataContext) -> pd.DataFrame:
    """
    Fetch daily weather data from NCEI API.

    Args:
        ctx: GetWeatherDataContext with configuration

    Returns:
        Wide format DataFrame with date index and weather variables as columns
    """
    logger.info("=" * 60)
    logger.info("Fetching weather data from NCEI API")
    logger.info("=" * 60)

    # Parse dates
    start = date.fromisoformat(ctx.start_date)
    end = (
        date.today() - timedelta(days=1)
        if ctx.end_date == "yesterday"
        else date.fromisoformat(ctx.end_date)
    )

    logger.info(f"Date range: {start} to {end}")

    # Fetch data year by year
    records = []
    cursor = start

    while cursor <= end:
        year_end = min(date(cursor.year, 12, 31), end)
        logger.info(f"Fetching {cursor.year}...")

        batch = _fetch_year_range(ctx.dataset, ctx.station, ctx.datatypes, cursor, year_end)

        logger.info(f"  → {len(batch)} records")
        records.extend(batch)
        cursor = date(cursor.year + 1, 1, 1)

    if not records:
        raise ValueError(
            "No records returned — check dataset, station, and API token.\n"
            f"Dataset: {ctx.dataset}, Station: {ctx.station}"
        )

    logger.info(f"✓ Fetched {len(records)} total records")

    # Convert to DataFrame
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Pivot to wide format
    weather_df = df.pivot_table(index="date", columns="datatype", values="value", aggfunc="first")

    # Clean up column name
    weather_df.columns.name = None
    weather_df = weather_df.reset_index()

    logger.info(f"✓ Created DataFrame: {len(weather_df)} days × {len(weather_df.columns)} columns")
    logger.info(f"  Columns: {list(weather_df.columns)}")

    return weather_df


def _save_weather_data(weather_df: pd.DataFrame, ctx: GetWeatherDataContext) -> None:
    """
    Save weather data to CSV.

    Args:
        weather_df: DataFrame with weather data
        ctx: GetWeatherDataContext with output paths
    """
    logger.info("=" * 60)
    logger.info("Saving weather data")
    logger.info("=" * 60)

    weather_df.to_csv(ctx.output_path, index=False)

    logger.info(f"✓ Saved weather data: {len(weather_df)} days")
    logger.info(f"  Output: {ctx.output_path}")


# ============================================================================
# Main public function
# ============================================================================


def get_weather_data(ctx: GetWeatherDataContext) -> None:
    """
    Fetch daily weather data from NCEI API and save to CSV.

    Args:
        ctx: GetWeatherDataContext containing configuration and output paths
    """
    logger.info("Starting weather data fetch")
    logger.info(f"Dataset: {ctx.dataset}")
    logger.info(f"Station: {ctx.station}")
    logger.info(f"Datatypes: {', '.join(ctx.datatypes)}")
    logger.info(f"Output directory: {ctx.weather_dir}")

    # Fetch weather data
    weather_df = _fetch_weather_data(ctx)

    # Save to CSV
    _save_weather_data(weather_df, ctx)

    logger.info("=" * 60)
    logger.info("✓ Weather data fetch completed")
