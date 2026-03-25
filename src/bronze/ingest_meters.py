from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.schemas import BRONZE_SUBSTATION_SCHEMA
from src.common.logger import get_logger

logger = get_logger(__name__)

RAW_VOLUME_PATH = "/Volumes/grid-strain-preventer-catalog/default/grid-strain-preventer-volume/"
BRONZE_TABLE = "`grid-strain-preventer-catalog`.default.bronze_substation"
CHECKPOINT_PATH = "/Volumes/grid-strain-preventer-catalog/default/grid-strain-preventer-volume/_checkpoints/bronze_substation/"

def ingest_substation_data(spark: SparkSession) -> None:
    """
    Reads all Northern Powergrid substation CSVs from the Unity Catalog Volume
    using Auto Loader (cloudFiles) and appends them into the Bronze Delta table.

    Auto Loader tracks processed files via the checkpoint — if you add new monthly
    CSVs to the Volume later and re-run this, only the new files will be ingested.
    """
    logger.info(f"Reading raw CSVs from: {RAW_VOLUME_PATH}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("nullValue", "NA")
        .option("enforceSchema", "true")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
        .schema(BRONZE_SUBSTATION_SCHEMA)
        .load(RAW_VOLUME_PATH)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable(BRONZE_TABLE)
        .awaitTermination(timeout=300)
    )

    logger.info(f"Bronze ingestion complete → {BRONZE_TABLE}")



if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    ingest_substation_data(spark)
