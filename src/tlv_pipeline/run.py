"""Run the pipeline end to end.

Local:       python -m tlv_pipeline.run              (from src/, or with src on PYTHONPATH)
Databricks:  the notebooks call the same layer functions; see notebooks/ and databricks.yml.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession

from . import bronze, config, gold, insights, quality, silver
from .storage import Store

EXPORTS = [  # small enough for CSV review; everything is also written as parquet / Delta
    ("silver", "businesses"), ("silver", "street_closures"), ("silver", "street_closures_rejected"),
    ("silver", "closure_streets"),
    ("gold", "compensation_business"), ("gold", "compensation_street"), ("gold", "compensation_category"),
    ("gold", "compensation_month"), ("gold", "compensation_closure_type"), ("gold", "closure_street_days"),
    ("gold", "dq_results"), ("gold", "dq_unmatched_streets"),
]


def local_spark() -> SparkSession:
    return (SparkSession.builder.master("local[*]").appName("tlv-compensation")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def export_csv(store: Store, out_dir: str) -> None:
    """UTF-8 with BOM so Excel renders Hebrew correctly."""
    os.makedirs(out_dir, exist_ok=True)
    for layer, name in EXPORTS:
        df = store.read(layer, name)
        for c, t in df.dtypes:  # arrays -> readable strings
            if t.startswith("array"):
                df = df.withColumn(c, df[c].cast("string"))
        df.toPandas().to_csv(os.path.join(out_dir, f"{layer}_{name}.csv"), index=False, encoding="utf-8-sig")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="conf/pipeline.yml")
    ap.add_argument("--root", default=None, help="project root (defaults to the config's parent)")
    ap.add_argument("--no-export", action="store_true")
    a = ap.parse_args(argv)

    cfg = config.load(a.config, root=a.root)
    spark = SparkSession.getActiveSession() or local_spark()
    t = time.time()
    for name, layer in (("bronze", bronze), ("silver", silver), ("gold", gold)):
        layer.run(spark, cfg)
        print(f"{name:<7} done  {time.time() - t:6.1f}s")
    quality.run(spark, cfg, raise_on_fail=True)
    print(f"quality passed  {time.time() - t:6.1f}s")
    insights.write(spark, cfg)
    print(f"insights written -> {cfg.base_path}/insights.json")
    dash = os.path.join(cfg.root, "dashboard")
    if os.path.isfile(os.path.join(dash, "build_dashboard.py")):
        sys.path.insert(0, dash)
        import build_dashboard
        print(f"dashboard rebuilt -> {build_dashboard.build(os.path.join(cfg.base_path, 'insights.json'))}")
    if not a.no_export and cfg.storage_format == "parquet":
        export_csv(Store(spark, cfg), os.path.join(cfg.base_path, "exports"))
        print(f"exported CSVs -> {os.path.join(cfg.base_path, 'exports')}")


if __name__ == "__main__":
    main()
