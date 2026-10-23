"""Bronze: land the raw sources as tables, faithful to the source.

No business logic here. Closures keep every column as a string; businesses keep the
ArcGIS `attributes` and `geometry` structs exactly as the API returned them. Each row
carries ingestion metadata so any downstream row can be traced to its source file.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .storage import Store


def street_closures(spark: SparkSession, cfg: Config) -> DataFrame:
    df = (spark.read
          .option("header", True)
          .option("encoding", "UTF-8")
          .option("quote", '"')
          .option("escape", '"')          # the file uses RFC-4180 doubled quotes: תרס""ט
          .option("multiLine", True)
          .option("inferSchema", False)  # bronze stays string-typed; silver owns types
          .csv(cfg.closures_csv))
    return (df.withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))


def businesses(spark: SparkSession, cfg: Config) -> DataFrame:
    pages = (spark.read
             .option("multiLine", True)      # each page is one JSON document
             .option("pathGlobFilter", "businesses_page_*.json")
             .option("recursiveFileLookup", True)
             .json(cfg.businesses_dir))
    return (pages
            .select(F.col("_metadata.file_path").alias("_source_file"),
                    F.col("spatialReference.wkid").alias("_srid"),
                    F.explode("features").alias("f"))
            .select(F.col("f.attributes").alias("attributes"),
                    F.col("f.geometry").alias("geometry"),
                    "_srid", "_source_file")
            .withColumn("_run_id", F.regexp_extract("_source_file", r"run=([0-9TZ]+)", 1))
            .withColumn("_ingested_at", F.current_timestamp()))


def run(spark: SparkSession, cfg: Config) -> dict[str, DataFrame]:
    store = Store(spark, cfg)
    return {
        "street_closures": store.write(street_closures(spark, cfg), "bronze", "street_closures"),
        "businesses": store.write(businesses(spark, cfg), "bronze", "businesses"),
    }
