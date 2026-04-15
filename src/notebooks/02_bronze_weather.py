# ============================================================
# Notebook: 02_bronze_weather.py
# Purpose:  Fetch historical weather data from Open-Meteo API
#           and load into Bronze Delta table.
#
# Pre-requisite: setup/create_bronze_table.sql must have been
#                run before executing this notebook.
# ============================================================

import sys
sys.path.insert(0, "/Workspace/Users/<your-email>/grid-strain-preventer/")

from src.bronze.fetch_weather import ingest_weather_data

ingest_weather_data(spark)

weather_df = spark.table("`grid-strain-preventer-catalog`.bronze.bronze_weather")

print(f"Total rows ingested: {weather_df.count():,}")
print(f"Date range covered:")
weather_df.selectExpr(
    "min(timestamp) as earliest",
    "max(timestamp) as latest"
).show(truncate=False)

print("Sample rows:")
weather_df.show(5, truncate=False)
