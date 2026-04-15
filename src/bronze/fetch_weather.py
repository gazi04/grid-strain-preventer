import requests
import time
from itertools import product
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)
from src.common.schemas import BRONZE_WEATHER_SCHEMA
from src.common.logger import get_logger

logger = get_logger(__name__)

# ─── Grid Configuration ────────────────────────────────────────────────────────
# Bounding box derived from actual substation coordinates in bronze_substation.
# Grid spacing matches Open-Meteo's native 0.25° resolution — finer spacing
# would return interpolated data from the same underlying grid points anyway.

LAT_START  =  53.50
LAT_END    =  55.75
LON_START  =  -2.50
LON_END    =  -0.25
GRID_STEP  =   0.25

START_DATE = "2023-11-01"
END_DATE   = "2025-07-31"

API_URL     = "https://archive-api.open-meteo.com/v1/archive"
API_DELAY_S = 0.5

BRONZE_TABLE = "`grid-strain-preventer-catalog`.bronze.bronze_weather"

def generate_grid_points() -> list[tuple[float, float]]:
    """
    Generates all (lat, lon) grid points covering the Northern Powergrid region.
    Uses 0.25° spacing matching Open-Meteo's native resolution.
    """
    # Build sequences with rounding to avoid floating point drift
    # e.g. 53.50, 53.75, 54.00 ... 55.75
    steps_lat = round((LAT_END - LAT_START) / GRID_STEP) + 1
    steps_lon = round((LON_END - LON_START) / GRID_STEP) + 1

    lats = [round(LAT_START + i * GRID_STEP, 2) for i in range(steps_lat)]
    lons = [round(LON_START + i * GRID_STEP, 2) for i in range(steps_lon)]

    points = list(product(lats, lons))
    logger.info(f"Grid points generated: {len(points)} ({len(lats)} lat × {len(lons)} lon)")
    return points


def fetch_one_grid_point(lat: float, lon: float) -> list[dict]:
    """
    Fetches hourly temperature data for a single grid point from Open-Meteo.
    Returns a flat list of hourly records.
    """
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": START_DATE,
        "end_date":   END_DATE,
        "hourly":     "temperature_2m",
        "timezone":   "Europe/London",
    }

    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    timestamps   = data["hourly"]["time"]
    temperatures = data["hourly"]["temperature_2m"]

    return [
        {
            "grid_lat":              data["latitude"],
            "grid_lon":              data["longitude"],
            "elevation":             data["elevation"],
            "timezone":              data["timezone"],
            "timezone_abbreviation": data["timezone_abbreviation"],
            "utc_offset_seconds":    data["utc_offset_seconds"],
            "timestamp":             timestamps[i],
            "temperature_2m":        temperatures[i],
        }
        for i in range(len(timestamps))
    ]


def fetch_all_grid_points() -> list[dict]:
    """
    Iterates over all grid points, fetches weather data for each,
    and returns a combined flat list of all hourly records.
    """
    grid_points = generate_grid_points()
    all_records = []

    for i, (lat, lon) in enumerate(grid_points):
        logger.info(f"Fetching grid point {i + 1}/{len(grid_points)}: ({lat}, {lon})")
        try:
            records = fetch_one_grid_point(lat, lon)
            all_records.extend(records)
        except Exception as e:
            logger.error(f"Failed to fetch ({lat}, {lon}): {e}")

        time.sleep(API_DELAY_S)

    logger.info(f"Total records fetched across all grid points: {len(all_records):,}")
    return all_records

def ingest_weather_data(spark: SparkSession) -> None:
    """
    Fetches historical hourly temperature data from Open-Meteo for a 10×10
    grid covering the Northern Powergrid region and writes it to Bronze Delta.
    """
    all_records = fetch_all_grid_points()

    df = (
        spark.createDataFrame(all_records, schema=BRONZE_WEATHER_SCHEMA)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

    logger.info(f"Total rows to write: {df.count():,}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE)
    )

    logger.info(f"Weather ingestion complete → {BRONZE_TABLE}")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    ingest_weather_data(spark)
