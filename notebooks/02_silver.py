# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — conform, validate, deduplicate
# MAGIC Typed closures with reject reasons, one row per closure per street, latest version of each business, categories mapped.

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

from tlv_pipeline import silver
for name, df in silver.run(spark, cfg).items():
    print(f"silver_{name}: {df.count():,} rows")
