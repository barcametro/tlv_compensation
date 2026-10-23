# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — schema, landing volume, seed data
# MAGIC Creates the schema and the Unity Catalog Volume, then copies the repo's `landing/` files into the volume (only files that are not there yet, so data landed by real ingestion is never overwritten). Safe to re-run.

# COMMAND ----------

# Resolve the repo root without trusting the working directory (not guaranteed on serverless).
import os, sys

dbutils.widgets.text("repo_root", "")


def _repo_root() -> str:
    if dbutils.widgets.get("repo_root"):                 # job parameter; the bundle sets ${workspace.file_path}
        return dbutils.widgets.get("repo_root")
    for p in sys.path:                                   # a Git folder's root is on sys.path
        if os.path.isfile(os.path.join(p, "conf", "pipeline.yml")):
            return p
    try:                                                 # .../<repo>/notebooks/<this notebook>
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return "/Workspace" + os.path.dirname(os.path.dirname(nb))
    except Exception:
        return os.path.abspath("..")


REPO_ROOT = _repo_root()
assert os.path.isfile(os.path.join(REPO_ROOT, "conf", "pipeline.yml")), f"repo root not found: {REPO_ROOT}"
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from tlv_pipeline import databricks as dbx
cfg = dbx.cfg(dbutils, REPO_ROOT)
print("repo:", REPO_ROOT, "| target:", f"{cfg.catalog}.{cfg.schema}", "| landing:", os.path.dirname(cfg.businesses_dir))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{dbutils.widgets.get('volume')}")
volume = os.path.dirname(cfg.businesses_dir)

# COMMAND ----------

print(dbx.seed_landing(REPO_ROOT, volume))
inv = dbx.landing_inventory(volume)
print(inv)
assert inv["closures"] >= 1 and inv["businesses"] >= 2 and inv["reference"] >= 1, f"landing volume incomplete: {inv}"
