from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

BRONZE_SUBSTATION_SCHEMA = StructType([
    StructField("dataset_id",                          StringType(), True),
    StructField("dno_name",                            StringType(), True),
    StructField("dno_alias",                           StringType(), True),
    StructField("secondary_substation_id",             StringType(), True),
    StructField("secondary_substation_name",           StringType(), True),
    StructField("substation_geo_location",             StringType(), True),
    StructField("aggregated_device_count_active",      StringType(), True),
    StructField("primary_consumption_active_import",   StringType(), True),
    StructField("secondary_consumption_active_import", StringType(), True),
    StructField("total_consumption_active_import",     StringType(), True),
    StructField("aggregated_device_count_reactive",    StringType(), True),
    StructField("total_consumption_reactive_import",   StringType(), True),
    StructField("data_collection_log_timestamp",       StringType(), True),
    StructField("insert_time",                         StringType(), True),
    StructField("last_modified_time",                  StringType(), True),
])

BRONZE_WEATHER_SCHEMA = StructType([
    StructField("latitude",              DoubleType(), True),
    StructField("longitude",             DoubleType(), True),
    StructField("elevation",             DoubleType(), True),
    StructField("timezone",              StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("utc_offset_seconds",    LongType(),   True),
    StructField("timestamp",             StringType(), True),
    StructField("temperature_2m",        DoubleType(), True),
    StructField("_ingestion_timestamp",  StringType(), True),
    StructField("_location_name",        StringType(), True),
])

