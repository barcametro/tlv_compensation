# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality gate
# MAGIC Writes gold_dq_results and fails the job if any hard check fails, then prints the headline numbers.

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
spark.conf.set("spark.sql.session.timeZone", "UTC")    # as in local runs: dates never depend on workspace settings
print("repo:", REPO_ROOT, "| target:", f"{cfg.catalog}.{cfg.schema}", "| landing:", os.path.dirname(cfg.businesses_dir))

# COMMAND ----------

from tlv_pipeline import quality
dq = quality.run(spark, cfg, raise_on_fail=True)
display(dq)

# COMMAND ----------

from tlv_pipeline import insights
import json
print(json.dumps(insights.compute(spark, cfg)["kpi"], ensure_ascii=False, indent=2, default=str))
