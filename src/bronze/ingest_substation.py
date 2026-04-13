from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.schemas import BRONZE_SUBSTATION_SCHEMA
from src.common.logger import get_logger

logger = get_logger(__name__)

RAW_VOLUME_PATH = "/Volumes/grid-strain-preventer-catalog/default/grid-strain-preventer-volume/"
BRONZE_TABLE    = "`grid-strain-preventer-catalog`.bronze.bronze_substation"

def ingest_substation_data(spark: SparkSession) -> None:
    """
    Reads all Northern Powergrid substation CSVs from the Unity Catalog Volume
    using a batch read and appends them into the Bronze Delta table.

    Uses spark.read (not readStream) — appropriate for bulk loading a fixed
    set of files already present in the Volume.
    """
    logger.info(f"Reading raw CSVs from: {RAW_VOLUME_PATH}")

    # ── Read all CSVs in the Volume in one batch ───────────────────────────────
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("nullValue", "NA")
        .option("enforceSchema", "true")
        .schema(BRONZE_SUBSTATION_SCHEMA)
        .load(RAW_VOLUME_PATH)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    total_rows = df.count()
    logger.info(f"Total rows read from CSVs: {total_rows:,}")

    # ── Write to Bronze Delta table ────────────────────────────────────────────
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE)
    )

    logger.info(f"Bronze ingestion complete → {BRONZE_TABLE}")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    ingest_substation_data(spark)
