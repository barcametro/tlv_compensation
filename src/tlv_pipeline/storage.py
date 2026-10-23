"""Read/write tables by layer, independent of where they live.

  parquet  -> <base_path>/<layer>/<name>/          (local runs)
  delta    -> <catalog>.<schema>.<layer>_<name>    (Databricks, Unity Catalog managed tables)

Every table is fully rebuilt from the layer below it. The landing zone is the
immutable source of truth, so a re-run of any layer is idempotent by construction.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from .config import Config


class Store:
    def __init__(self, spark: SparkSession, cfg: Config):
        self.spark, self.cfg = spark, cfg
        if cfg.storage_format not in ("parquet", "delta"):
            raise ValueError(f"unsupported storage format: {cfg.storage_format}")

    def _table(self, layer: str, name: str) -> str:
        return f"{self.cfg.catalog}.{self.cfg.schema}.{layer}_{name}"

    def _path(self, layer: str, name: str) -> str:
        return os.path.join(self.cfg.base_path, layer, name)

    def write(self, df: DataFrame, layer: str, name: str) -> DataFrame:
        if self.cfg.storage_format == "delta":
            (df.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true").saveAsTable(self._table(layer, name)))
        else:
            df.write.mode("overwrite").parquet(self._path(layer, name))
        return self.read(layer, name)

    def read(self, layer: str, name: str) -> DataFrame:
        if self.cfg.storage_format == "delta":
            return self.spark.table(self._table(layer, name))
        return self.spark.read.parquet(self._path(layer, name))
