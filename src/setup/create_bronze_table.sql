CREATE SCHEMA IF NOT EXISTS `grid-strain-preventer-catalog`.bronze
COMMENT 'Bronze layer: raw ingested data, no transformations';

CREATE SCHEMA IF NOT EXISTS `grid-strain-preventer-catalog`.silver
COMMENT 'Silver layer: cleansed and harmonized data';

CREATE SCHEMA IF NOT EXISTS `grid-strain-preventer-catalog`.gold
COMMENT 'Gold layer: business-ready aggregations and Grid Strain Index';

CREATE TABLE IF NOT EXISTS `grid-strain-preventer-catalog`.bronze.bronze_substation (
    dataset_id                          STRING,
    dno_name                            STRING,
    dno_alias                           STRING,
    secondary_substation_id             STRING,
    secondary_substation_name           STRING,
    substation_geo_location             STRING,
    aggregated_device_count_active      STRING,
    primary_consumption_active_import   STRING,
    secondary_consumption_active_import STRING,
    total_consumption_active_import     STRING,
    aggregated_device_count_reactive    STRING,
    total_consumption_reactive_import   STRING,
    data_collection_log_timestamp       STRING,
    insert_time                         STRING,
    last_modified_time                  STRING,
    _ingestion_timestamp                TIMESTAMP,
    _source_file                        STRING
)
USING DELTA
COMMENT 'Raw Northern Powergrid substation CSVs — no transformations applied';


CREATE TABLE IF NOT EXISTS `grid-strain-preventer-catalog`.bronze.bronze_weather (
    latitude                DOUBLE,
    longitude               DOUBLE,
    elevation               DOUBLE,
    timezone                STRING,
    timezone_abbreviation   STRING,
    utc_offset_seconds      LONG,
    timestamp               STRING,   -- raw string from API — cast to timestamp in Silver
    temperature_2m          DOUBLE,   -- °C at 2m above ground, hourly
    _ingestion_timestamp    TIMESTAMP,
    _location_name          STRING    -- "Newcastle" — useful when adding more locations later
)
USING DELTA
COMMENT 'Bronze layer: raw hourly temperature data from Open-Meteo API';
