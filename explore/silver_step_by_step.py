# Databricks notebook source
# MAGIC %md
# MAGIC # Silver, step by step
# MAGIC The same transformations as `src/tlv_pipeline/silver.py`, unrolled: every intermediate DataFrame is shown
# MAGIC (row count, columns, sample) and checked, and a few known records are traced through each step.
# MAGIC
# MAGIC **Read-only for the pipeline.** It reads the `bronze_*` tables in `workspace.tlv` and never writes there.
# MAGIC With `persist = yes`, each step is also saved as a table in `workspace.tlv_explore`, so you can query it
# MAGIC in the SQL editor next to this notebook (`explore/explore_queries.sql`).
# MAGIC
# MAGIC Run the pipeline first (job or notebooks `00`–`04`), then **Run all** here.

# COMMAND ----------

# Resolve the repo root without trusting the working directory (not guaranteed on serverless).
import os, sys

dbutils.widgets.text("repo_root", "")


def _repo_root() -> str:
    if dbutils.widgets.get("repo_root"):
        return dbutils.widgets.get("repo_root")
    for p in sys.path:
        if os.path.isfile(os.path.join(p, "conf", "pipeline.yml")):
            return p
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return "/Workspace" + os.path.dirname(os.path.dirname(nb))
    except Exception:
        return os.path.abspath("..")


REPO_ROOT = _repo_root()
assert os.path.isfile(os.path.join(REPO_ROOT, "conf", "pipeline.yml")), f"repo root not found: {REPO_ROOT}"
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from tlv_pipeline import databricks as dbx
cfg = dbx.cfg(dbutils, REPO_ROOT)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helpers
# MAGIC `step()` prints row count and every column with its type, shows a sample, and (optionally) saves the step
# MAGIC as `workspace.tlv_explore.<name>`. `check()` prints ✔ / ✘ instead of stopping. `trace()` shows only the
# MAGIC records we follow. `same_as()` proves the unrolled result equals the table the pipeline wrote.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from tlv_pipeline.storage import Store
from tlv_pipeline.normalize import clean_text, street_key, strip_wrapping_quotes
from tlv_pipeline.silver import trading_hours_factor, read_category_map

dbutils.widgets.text("explore_schema", "tlv_explore")
dbutils.widgets.dropdown("persist", "yes", ["yes", "no"])
EXPLORE = f"{cfg.catalog}.{dbutils.widgets.get('explore_schema')}"
PERSIST = dbutils.widgets.get("persist") == "yes"
if PERSIST:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {EXPLORE}")
store = Store(spark, cfg)

# records followed through every step
TRACE_CLOSURES = [1511, 2, 846, 1762, 1786, 603, 68]
#   1511 Ben Yehuda closure listed as Trumpeldor / Allenby (bounding cross-streets), factor 1.0
#   2    marathon 24/02/2023 04:00-14:00 -> trading-hours factor 0.5
#   846  Nahalat Binyamin evening pedestrian zone, started 2020 -> overlaps 2023
#   1762 / 1786  a true duplicate pair (same segment, streets, dates, type, hours)
#   603 / 68     same streets and dates, DIFFERENT segment -> both kept
TRACE_BUSINESSES = [68, 1011212, 1063304, 1018439]


def step(df, name, note="", n=10):
    cnt = df.count()
    print(f"■ {name}: {cnt:,} rows × {len(df.columns)} columns" + (f"   — {note}" if note else ""))
    for f in df.schema.fields:
        print(f"    {f.name:<28} {f.dataType.simpleString()}")
    if PERSIST:
        (df.write.mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable(f"{EXPLORE}.{name}"))
    display(df.limit(n))
    return df


def check(ok, msg):
    print(("✔ " if ok else "✘ ") + msg)


def trace(df, col, ids, cols=None):
    t = df.filter(F.col(col).isin(ids))
    display(t.select(*cols) if cols else t)


def same_as(df, layer, name):
    t = store.read(layer, name)
    cols = [c for c in t.columns if c != "_ingested_at"]
    a, b = df.select(cols), t.select(cols)
    only_here, only_table = a.exceptAll(b).count(), b.exceptAll(a).count()
    check(only_here == only_table == 0,
          f"identical to {layer}_{name}  ({only_here} rows only here, {only_table} only in the table)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — Closures
# MAGIC ### S01 · Source: `bronze_street_closures`
# MAGIC Every column is a string, exactly as in the CSV. Note `me_*` / `ad_*` (from / to street) and `id_rechov`
# MAGIC (the closed segment).

# COMMAND ----------

bronze_c = step(store.read("bronze", "street_closures"), "s01_bronze_closures", "raw CSV rows, all strings")
check(bronze_c.count() == 2006, "2,006 CSV records landed")
trace(bronze_c, "ID", [str(i) for i in TRACE_CLOSURES])

# COMMAND ----------

# MAGIC %md
# MAGIC ### S02 · Typing (`typed`)
# MAGIC `try_cast` / `try_to_timestamp` instead of `CAST`: under ANSI mode a bad value would crash the job; here it
# MAGIC becomes NULL and is counted later. Street names go through `street_key()` (quotes unified, spaces collapsed,
# MAGIC `"0"` → NULL). Same row count as the source, only the shape changes.

# COMMAND ----------

typed = bronze_c.select(
    F.expr("try_cast(ID AS INT)").alias("closure_id"),
    F.expr("try_cast(id_rechov AS INT)").alias("segment_id"),
    F.expr("try_cast(me_k_rechov AS INT)").alias("street_a_code"),
    street_key(F.col("me_shem_rechov")).alias("street_a"),
    F.expr("try_cast(ad_k_rechov AS INT)").alias("street_b_code"),
    street_key(F.col("ad_shem_rechov")).alias("street_b"),
    F.expr("try_to_timestamp(tr_from, 'dd/MM/yyyy H:mm')").alias("start_ts"),
    F.expr("try_to_timestamp(tr_to, 'dd/MM/yyyy H:mm')").alias("end_ts"),
    F.expr("try_cast(k_sug AS INT)").alias("closure_type_code"),
    clean_text(F.col("t_sug")).alias("closure_type"),
    F.expr("try_cast(k_sgira AS INT)").alias("closure_kind_code"),
    clean_text(F.col("shaot")).alias("hours_text"),
    F.col("sw_laila").alias("_sw_laila_raw"),
    "_source_file", "_ingested_at",
)
step(typed, "s02_typed", "same rows, typed columns")
check(typed.count() == bronze_c.count(), "typing keeps every row")
check(typed.filter(F.col("start_ts").isNull()).count() == 0, "every start date parsed (dd/MM/yyyy, not MM/dd)")
# raw vs typed, side by side, for the traced closures
display(bronze_c.filter(F.col("ID").isin([str(i) for i in TRACE_CLOSURES]))
        .select("ID", "tr_from", "tr_to", "me_k_rechov", "me_shem_rechov")
        .join(typed.select(F.col("closure_id").cast("string").alias("ID"), "start_ts", "end_ts", "street_a"), "ID"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### S03 · Street code 0 means "no street"
# MAGIC The name is NULL-ed whenever its code is 0.

# COMMAND ----------

before_zero = typed
typed = (typed
         .withColumn("street_a", F.when(F.col("street_a_code") == 0, None).otherwise(F.col("street_a")))
         .withColumn("street_b", F.when(F.col("street_b_code") == 0, None).otherwise(F.col("street_b"))))
step(typed, "s03_typed_no_zero_streets")
print("rows with a 0 street code:")
display(typed.filter((F.col("street_a_code") == 0) | (F.col("street_b_code") == 0))
        .select("closure_id", "street_a_code", "street_a", "street_b_code", "street_b").limit(10))
check(typed.filter(F.col("street_a").isNull() & F.col("street_b").isNull()).count() == 133,
      "133 closures have no street on either side")

# COMMAND ----------

# MAGIC %md
# MAGIC ### S04 · Derived columns (`clean`)
# MAGIC * `start_date` = date part of the start
# MAGIC * **brief rule:** missing end date → start date (`end_imputed` marks it)
# MAGIC * `night_flag` only for a clean `0`/`1`; anything else is the comma spill-over → `night_flag_corrupt`
# MAGIC * `trading_hours_factor` for Model B

# COMMAND ----------

clean = (typed
         .withColumn("start_date", F.to_date("start_ts"))
         .withColumn("end_imputed", F.col("end_ts").isNull())
         .withColumn("end_date", F.coalesce(F.to_date("end_ts"), F.col("start_date")))
         .withColumn("night_flag", F.when(F.trim("_sw_laila_raw") == "1", True)
                                    .when(F.trim("_sw_laila_raw") == "0", False))
         .withColumn("night_flag_corrupt",
                     F.col("_sw_laila_raw").isNotNull() & ~F.trim("_sw_laila_raw").isin("0", "1"))
         .withColumn("trading_hours_factor", trading_hours_factor("hours_text", cfg))
         .drop("_sw_laila_raw"))
step(clean, "s04_clean")
print("end date imputed from start date:")
display(clean.filter("end_imputed").select("closure_id", "start_ts", "end_ts", "start_date", "end_date"))
print("corrupted night flags (raw value was free text):")
display(typed.filter(~F.trim("_sw_laila_raw").isin("0", "1")).select("closure_id", "hours_text", "_sw_laila_raw").limit(5))
print("trading-hours factor for the traced closures:")
trace(clean, "closure_id", TRACE_CLOSURES, ["closure_id", "hours_text", "trading_hours_factor"])
check(clean.filter("end_imputed").count() == 6, "6 end dates imputed")
n_corrupt = clean.filter("night_flag_corrupt").count()
print(f"{n_corrupt} rows have a corrupt sw_laila here; the quality check counts the 152 of them that survive into silver_street_closures")

# COMMAND ----------

# MAGIC %md
# MAGIC ### S05 · Reject reasons (`flagged` → `rejected` + `valid`)
# MAGIC The first matching reason wins. Nothing is dropped silently: every row ends in exactly one of the two sets.

# COMMAND ----------

reason = (F.when(F.col("start_date").isNull(), "unparseable_start_date")
           .when(F.col("street_a").isNull() & F.col("street_b").isNull(), "no_valid_street")
           .when(F.col("end_date") < F.col("start_date"), "end_before_start"))
flagged = clean.withColumn("reject_reason", reason)
step(flagged, "s05_flagged")
display(flagged.groupBy("reject_reason").count().orderBy("reject_reason"))

rejected = flagged.filter(F.col("reject_reason").isNotNull())
valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
check(rejected.count() + valid.count() == clean.count(), "rejected + valid = all rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### S06 · Duplicate events (window)
# MAGIC Partition by the event key (**including the segment**), order by `closure_id`: `_rn = 1` is kept, the rest
# MAGIC point at the survivor via `duplicate_of`. Look at the two pairs below: 1762/1786 is a real duplicate;
# MAGIC 603/68 share streets and dates but are different segments, so both stay.

# COMMAND ----------

key = ["segment_id", "street_a", "street_b", "start_date", "end_date", "closure_type_code", "hours_text"]
w = Window.partitionBy(*key).orderBy("closure_id")
ranked = (valid.withColumn("_rn", F.row_number().over(w))
               .withColumn("duplicate_of", F.when(F.col("_rn") > 1, F.first("closure_id").over(w))))
step(ranked, "s06_ranked")
trace(ranked, "closure_id", [1762, 1786, 603, 68],
      ["closure_id", "segment_id", "street_a", "street_b", "start_date", "_rn", "duplicate_of"])

# what the old key (without segment) would have called duplicates
old_key = [k for k in key if k != "segment_id"]
w_old = Window.partitionBy(*old_key).orderBy("closure_id")
old_dups = valid.withColumn("_rn", F.row_number().over(w_old)).filter("_rn > 1").count()
check(ranked.filter("_rn > 1").count() == 8, f"8 true duplicates (the key without segment_id would reject {old_dups})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### S07 · Final closures: `deduped` and `rejected`
# MAGIC Every one of the 2,006 input rows is accounted for.

# COMMAND ----------

deduped = ranked.filter(F.col("_rn") == 1).drop("_rn", "duplicate_of")
dups = ranked.filter(F.col("_rn") > 1).drop("_rn").withColumn("reject_reason", F.lit("duplicate_event"))
rejected_all = rejected.unionByName(dups, allowMissingColumns=True)
step(deduped, "s07_street_closures", "→ silver_street_closures")
step(rejected_all, "s07_street_closures_rejected", "→ silver_street_closures_rejected")
display(rejected_all.groupBy("reject_reason").count())
check(deduped.count() + rejected_all.count() == bronze_c.count(),
      f"{deduped.count():,} clean + {rejected_all.count():,} rejected = {bronze_c.count():,} input rows")
check(deduped.filter("night_flag_corrupt").count() == 152, "152 corrupt sw_laila values in the clean closures")
same_as(deduped, "silver", "street_closures")
same_as(rejected_all, "silver", "street_closures_rejected")

# COMMAND ----------

# MAGIC %md
# MAGIC ### S08 · One row per closure per street (brief rule 5)
# MAGIC Two selects (street A, street B), stacked, NULL streets dropped. Follow 1511: one closure becomes a
# MAGIC Trumpeldor row and an Allenby row. Those are the **bounding cross-streets**; the closed street (Ben Yehuda)
# MAGIC is only in the free text.

# COMMAND ----------

base = ["closure_id", "start_date", "end_date", "closure_type", "closure_type_code",
        "hours_text", "trading_hours_factor"]
side_a = deduped.select(*base, F.lit("A").alias("street_role"),
                        F.col("street_a").alias("street_key"), F.col("street_a_code").alias("street_code"))
side_b = deduped.select(*base, F.lit("B").alias("street_role"),
                        F.col("street_b").alias("street_key"), F.col("street_b_code").alias("street_code"))
step(side_a, "s08a_side_a"); step(side_b, "s08b_side_b")
stacked = side_a.unionByName(side_b)
closure_streets = step(stacked.filter(F.col("street_key").isNotNull()), "s08_closure_streets", "→ silver_closure_streets")
check(stacked.count() == 2 * deduped.count(), "stacked = 2 × closures")
print(f"dropped {stacked.count() - closure_streets.count():,} NULL streets (closures with only one street)")
trace(closure_streets, "closure_id", [1511, 2], ["closure_id", "street_role", "street_key", "start_date", "end_date", "trading_hours_factor", "hours_text"])
same_as(closure_streets, "silver", "closure_streets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — Businesses
# MAGIC ### S09 · Source: `bronze_businesses`
# MAGIC One row per API feature, `attributes` and `geometry` still as structs. `_run_id` says which download it came from.

# COMMAND ----------

bronze_b = step(store.read("bronze", "businesses"), "s09_bronze_businesses", "raw ArcGIS structs", n=5)
display(bronze_b.groupBy("_run_id").count())
trace(bronze_b, "attributes.id_esek", TRACE_BUSINESSES, ["attributes", "_run_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### S10 · Flatten (`flat`)
# MAGIC Struct fields become typed columns: `tr_hakama` epoch ms → date, `date_import` string → timestamp,
# MAGIC `shimush` without stray quotes, street name kept for display and as the normalised join key.

# COMMAND ----------

a = "attributes"
flat = bronze_b.select(
    F.col(f"{a}.OBJECTID").cast("long").alias("object_id"),
    F.col(f"{a}.id_esek").cast("long").alias("business_id"),
    strip_wrapping_quotes(F.col(f"{a}.shimush")).alias("category_he"),
    clean_text(F.col(f"{a}.shem_rechov")).alias("street_name"),
    street_key(F.col(f"{a}.shem_rechov")).alias("street_key"),
    clean_text(F.col(f"{a}.ms_bayit")).alias("house_number"),
    F.to_date(F.timestamp_millis(F.col(f"{a}.tr_hakama").cast("long"))).alias("established_date"),
    (F.col(f"{a}.sw_taun_rishui").cast("int") == 1).alias("requires_license"),
    F.col(f"{a}.shetach").cast("int").alias("area_sqm"),
    F.col(f"{a}.x_coord").cast("double").alias("x_itm"),
    F.col(f"{a}.y_coord").cast("double").alias("y_itm"),
    F.expr(f"try_to_timestamp({a}.date_import, 'dd/MM/yyyy HH:mm:ss')").alias("import_ts"),
    "_run_id", "_ingested_at", "_source_file",
)
step(flat, "s10_flat")
display(bronze_b.filter(F.col("attributes.id_esek").isin(TRACE_BUSINESSES))
        .select(F.col("attributes.id_esek").alias("business_id"), F.col("attributes.tr_hakama").alias("tr_hakama_raw"),
                F.col("attributes.date_import").alias("date_import_raw"))
        .join(flat.select("business_id", "established_date", "import_ts"), "business_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### S11 · Latest version per business (brief cleaning rule)
# MAGIC `row_number()` per `business_id`, newest import first, then newest download, then highest OBJECTID.
# MAGIC With one download every business has a single version (`_rn = 1`); a second download would add `_rn = 2` rows.

# COMMAND ----------

wb = Window.partitionBy("business_id").orderBy(
    F.col("import_ts").desc_nulls_last(), F.col("_run_id").desc(), F.col("object_id").desc())
ranked_b = flat.filter(F.col("business_id").isNotNull()).withColumn("_rn", F.row_number().over(wb))
step(ranked_b, "s11_ranked_businesses")
display(ranked_b.groupBy("_rn").count())
latest = ranked_b.filter(F.col("_rn") == 1).drop("_rn")
check(latest.count() == latest.select("business_id").distinct().count(), "one row per business_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### S12 · Category map (`cats`)
# MAGIC Read from `conf/category_map.csv` in the repo; `model_b_eligible` 1/0 → boolean, empty group → NULL.

# COMMAND ----------

categories = step(read_category_map(spark, cfg.category_map), "s12a_category_map_raw")
cats = categories.select(
    clean_text(F.col("category_he")).alias("category_he"), "category_en",
    (F.col("model_b_eligible").cast("int") == 1).alias("model_b_eligible"),
    F.when(F.col("exclusion_group") == "", None).otherwise(F.col("exclusion_group")).alias("exclusion_group"))
step(cats, "s12_cats")
display(cats.groupBy("model_b_eligible", "exclusion_group").count().orderBy("model_b_eligible", "exclusion_group"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### S13 · Join categories → `silver_businesses`
# MAGIC Left join, so no business is lost. Unknown categories would be labelled `Unmapped` and kept eligible.

# COMMAND ----------

businesses = (latest.join(F.broadcast(cats), "category_he", "left")
              .withColumn("category_mapped", F.col("category_en").isNotNull())
              .withColumn("category_en", F.coalesce("category_en", F.lit("Unmapped")))
              .withColumn("model_b_eligible", F.coalesce("model_b_eligible", F.lit(True))))
step(businesses, "s13_businesses", "→ silver_businesses")
check(businesses.count() == latest.count(), "left join kept every business")
check(businesses.filter(~F.col("category_mapped")).count() == 0, "every category mapped")
trace(businesses, "business_id", TRACE_BUSINESSES,
      ["business_id", "street_name", "category_en", "model_b_eligible", "exclusion_group", "area_sqm", "established_date"])
same_as(businesses, "silver", "businesses")
