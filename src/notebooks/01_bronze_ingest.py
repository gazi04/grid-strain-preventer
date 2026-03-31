# ============================================================
# Notebook: 01_bronze_ingest.py
# Purpose:  Ingest raw Northern Powergrid substation CSVs
#           from Unity Catalog Volume into Bronze Delta table.
#
# Pre-requisite: setup/01_create_catalog.sql must have been
#                run before executing this notebook.
# ============================================================

import sys
sys.path.insert(0, "/Workspace/Users/<your-email>/grid-strain-preventer/")

from src.bronze.ingest_meters import ingest_substation_data

ingest_substation_data(spark)

bronze_df = spark.table("`grid-strain-preventer-catalog`.bronze.bronze_substation")

print(f"Total rows ingested: {bronze_df.count():,}")
print(f"Distinct monthly files: {bronze_df.select('dataset_id').distinct().count()}")
bronze_df.select("dataset_id").distinct().orderBy("dataset_id").show(25, truncate=False)
