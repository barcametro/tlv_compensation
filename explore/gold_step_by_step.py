# Databricks notebook source
# MAGIC %md
# MAGIC # Gold, step by step
# MAGIC The same transformations as `src/tlv_pipeline/gold.py`, unrolled: every intermediate DataFrame is shown
# MAGIC (row count, columns, sample) and checked, and a few closures, streets and businesses are traced from
# MAGIC Silver to the final compensation.
# MAGIC
# MAGIC **Read-only for the pipeline.** It reads `silver_*` tables in `workspace.tlv` and never writes there.
# MAGIC With `persist = yes`, each step is saved in `workspace.tlv_explore` for the SQL editor.

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
# MAGIC Same as in the Silver notebook: `step()` shows count + columns + sample (and saves to `tlv_explore`),
# MAGIC `check()` prints ✔ / ✘, `trace()` filters to the followed records, `same_as()` compares with the real table.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from tlv_pipeline.storage import Store

dbutils.widgets.text("explore_schema", "tlv_explore")
dbutils.widgets.dropdown("persist", "yes", ["yes", "no"])
EXPLORE = f"{cfg.catalog}.{dbutils.widgets.get('explore_schema')}"
PERSIST = dbutils.widgets.get("persist") == "yes"
if PERSIST:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {EXPLORE}")
store = Store(spark, cfg)

TRACE_CLOSURES = [1511, 2, 846]
#   1511 Ben Yehuda works listed as Trumpeldor / Allenby, 4 Jan – 30 Nov 2023, factor 1.0
#   2    marathon on 24 Feb 2023, 04:00-14:00 -> factor 0.5
#   846  evening pedestrian zone from June 2020 to Dec 2023 -> clipped to 2023
TRACE_STREETS = ["אלנבי", "אבן גבירול"]
TRACE_BUSINESSES = [1011212, 68, 1063304, 1018439]
#   1011212 cafe on Allenby, 96 sqm -> 9,600/day, below the cap
#   68      office on Ibn Gabirol, 150 sqm -> capped at 10,000/day; established 2025 -> Model B 0
#   1063304 artist studio on Herzl, established 6 Jun 2023 -> Model B only from that day
#   1018439 transformer room on Ibn Gabirol -> non-trading, Model B 0


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
# MAGIC ## G01 · Sources
# MAGIC `silver_closure_streets` (one row per closure per affected street) and `silver_businesses` (one row per business).

# COMMAND ----------

closure_streets = step(store.read("silver", "closure_streets"), "g01_closure_streets", "input from silver")
businesses = step(store.read("silver", "businesses"), "g01_businesses", "input from silver", n=5)
trace(closure_streets, "closure_id", TRACE_CLOSURES,
      ["closure_id", "street_role", "street_key", "start_date", "end_date", "trading_hours_factor"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## G02 · `in_year` — overlap with 2023 (brief rule 6)
# MAGIC Keep a closure if it **starts on or before 31 Dec and ends on or after 1 Jan**. Filtering on "starts in 2023"
# MAGIC would lose closures that began earlier, like 846 (June 2020).

# COMMAND ----------

y0, y1 = F.lit(f"{cfg.year}-01-01").cast("date"), F.lit(f"{cfg.year}-12-31").cast("date")
in_year = closure_streets.filter((F.col("start_date") <= y1) & (F.col("end_date") >= y0))
step(in_year, "g02_in_year", "closure-streets overlapping 2023")

started_in_year = closure_streets.filter(F.year("start_date") == cfg.year)
n_overlap = in_year.select("closure_id").distinct().count()
n_started = started_in_year.select("closure_id").distinct().count()
print(f"closures overlapping {cfg.year}: {n_overlap}   |   closures that START in {cfg.year}: {n_started}")
print("overlapping closures that started BEFORE the year (a start-year filter would drop these):")
display(in_year.filter(F.col("start_date") < y0)
        .groupBy("closure_id", "closure_type", "start_date", "end_date").agg(F.collect_list("street_key").alias("streets"))
        .orderBy("start_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## G03 · `days` — one row per closure-street per day, clipped to the year
# MAGIC `sequence(greatest(start, 1 Jan), least(end, 31 Dec))` builds the list of dates; `explode` makes a row per date.
# MAGIC The helper columns `clip_from`, `clip_to`, `n_days` exist only here, to make the clipping visible.

# COMMAND ----------

clipped = (in_year.withColumn("clip_from", F.greatest("start_date", y0))
                  .withColumn("clip_to", F.least("end_date", y1))
                  .withColumn("n_days", F.datediff("clip_to", "clip_from") + 1))
print("clipping for the traced closures:")
trace(clipped, "closure_id", TRACE_CLOSURES,
      ["closure_id", "street_key", "start_date", "end_date", "clip_from", "clip_to", "n_days"])

days = in_year.withColumn(
    "day", F.explode(F.sequence(F.greatest("start_date", y0), F.least("end_date", y1),
                                F.expr("INTERVAL 1 DAY"))))
step(days, "g03_days", "exploded to days")
check(days.count() == clipped.agg(F.sum("n_days")).first()[0], "row count = sum of clipped day counts")
display(days.filter(F.col("closure_id").isin(TRACE_CLOSURES))
        .groupBy("closure_id", "street_key").agg(F.count("*").alias("days"), F.min("day").alias("first"), F.max("day").alias("last")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## G04 · `street_days` — one row per street per day (brief rule 2)
# MAGIC Overlapping closures on the same street and day collapse into one row **before** businesses are joined, so no
# MAGIC business can be paid twice for a day. Example: Ibn Gabirol on 24 Feb 2023 has 7 closures and becomes 1 row.

# COMMAND ----------

street_days = (days.groupBy("street_key", "day")
               .agg(F.countDistinct("closure_id").alias("closures_that_day"),
                    F.max("trading_hours_factor").alias("trading_hours_factor"),
                    F.max_by("closure_type", F.struct("trading_hours_factor", "closure_id"))
                     .alias("primary_closure_type"),
                    F.sort_array(F.collect_set("closure_id")).alias("closure_ids")))
step(street_days, "g04_street_days", "→ gold_closure_street_days")
print("before grouping (Ibn Gabirol, 24 Feb 2023):")
display(days.filter((F.col("street_key") == "אבן גבירול") & (F.col("day") == "2023-02-24"))
        .select("closure_id", "closure_type", "trading_hours_factor", "street_role"))
print("after grouping:")
display(street_days.filter((F.col("street_key") == "אבן גבירול") & (F.col("day") == "2023-02-24")))
check(street_days.count() == street_days.select("street_key", "day").distinct().count(), "(street, day) is unique")
display(street_days.filter(F.col("street_key").isin(TRACE_STREETS)).groupBy("street_key").count())
same_as(street_days, "gold", "closure_street_days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G05 · `business_closure_days` — one row per business per affected day
# MAGIC Brief rule 1 is the **inner join on `street_key`**: each business gets one row for every closed day of its
# MAGIC street. Then the amounts:
# MAGIC * `full_day = least(100 × area, 10,000)` — rules 3 and 4
# MAGIC * `comp_model_a = full_day` (decimal, per day)
# MAGIC * `comp_model_b = full_day × factor` if the category trades **and** the business existed that day, else 0

# COMMAND ----------

full_day = F.least(F.lit(cfg.rate) * F.coalesce(F.col("area_sqm"), F.lit(0)), F.lit(cfg.cap))
existed = F.col("established_date").isNull() | (F.col("established_date") <= F.col("day"))
b = businesses.filter(F.col("street_key").isNotNull()).select(
    "business_id", "street_key", "street_name", "category_he", "category_en",
    "model_b_eligible", "established_date", "area_sqm")
step(b, "g05a_business_cols", "businesses with a street, only the columns gold needs")

joined = b.join(street_days, "street_key", "inner")
step(joined, "g05b_joined", "rule 1: business × closed days of its street")
check(joined.select("business_id").distinct().count() == 12165, "12,165 businesses sit on a street closed in 2023")

bcd = (joined
       .withColumn("existed_on_day", existed)
       .withColumn("cap_binding", F.lit(cfg.rate) * F.coalesce("area_sqm", F.lit(0)) > cfg.cap)
       .withColumn("comp_model_a", full_day.cast("decimal(12,2)"))
       .withColumn("comp_model_b",
                   F.when(F.col("model_b_eligible") & F.col("existed_on_day"),
                          full_day * F.col("trading_hours_factor"))
                    .otherwise(0).cast("decimal(12,2)")))
step(bcd, "g05_business_closure_days", "→ gold_business_closure_days")

print("the amount, unpacked, for the traced businesses (first closed day of each):")
explain = (bcd.filter(F.col("business_id").isin(TRACE_BUSINESSES))
           .withColumn("uncapped", F.lit(cfg.rate) * F.col("area_sqm"))
           .withColumn("_r", F.row_number().over(Window.partitionBy("business_id").orderBy("day")))
           .filter("_r = 1")
           .select("business_id", "street_name", "category_en", "area_sqm", "uncapped", "cap_binding", "comp_model_a",
                   "model_b_eligible", "established_date", "day", "existed_on_day", "trading_hours_factor", "comp_model_b"))
display(explain)
print("business 1063304 around its establishment date (Model B starts on 6 Jun 2023):")
display(bcd.filter((F.col("business_id") == 1063304) & F.col("day").between("2023-06-03", "2023-06-09"))
        .select("business_id", "day", "established_date", "existed_on_day", "comp_model_a", "comp_model_b").orderBy("day"))
print("marathon day: the factor halves Model B")
display(bcd.filter((F.col("day") == "2023-02-24") & (F.col("trading_hours_factor") < 1))
        .select("business_id", "street_name", "area_sqm", "trading_hours_factor", "comp_model_a", "comp_model_b").limit(5))

check(bcd.groupBy("business_id", "day").count().filter("count > 1").count() == 0, "rule 2: one row per business per day")
check(bcd.filter(F.col("comp_model_a") > cfg.cap).count() == 0, "rule 4: no day above the cap")
check(bcd.filter(F.col("comp_model_b") > F.col("comp_model_a")).count() == 0, "Model B never above Model A")
same_as(bcd, "gold", "business_closure_days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G06 · `compensation_business` — required output 1
# MAGIC `per` = groupBy business over the days; `attrs` = descriptive columns joined back; then per-business metrics.

# COMMAND ----------

per = (bcd.groupBy("business_id")
          .agg(F.count("*").alias("days_affected"),
               F.min("day").alias("first_day"), F.max("day").alias("last_day"),
               F.sum("comp_model_a").alias("total_model_a"),
               F.sum("comp_model_b").alias("total_model_b"),
               F.max("cap_binding").alias("cap_binding")))
step(per, "g06a_per_business", "days and totals per business")
attrs = businesses.select("business_id", "street_name", "street_key", "house_number",
                          "category_he", "category_en", "model_b_eligible", "exclusion_group",
                          "established_date", "area_sqm", "x_itm", "y_itm")
out = per.join(attrs, "business_id", "left")
area = F.when(F.col("area_sqm") > 0, F.col("area_sqm"))
comp_business = (out.withColumn("avg_daily_model_a", (F.col("total_model_a") / F.col("days_affected")).cast("decimal(12,2)"))
                    .withColumn("cost_per_sqm_model_a", (F.col("total_model_a") / area).cast("decimal(14,2)"))
                    .withColumn("cost_per_sqm_model_b", (F.col("total_model_b") / area).cast("decimal(14,2)"))
                    .withColumn("model_b_vs_a_pct",
                                F.round(100 * F.col("total_model_b") / F.when(F.col("total_model_a") > 0,
                                                                              F.col("total_model_a")), 1)))
step(comp_business, "g06_compensation_business", "→ gold_compensation_business")
trace(comp_business, "business_id", TRACE_BUSINESSES,
      ["business_id", "street_name", "category_en", "area_sqm", "days_affected", "first_day", "last_day",
       "total_model_a", "total_model_b", "cap_binding", "established_date"])
print("check by hand: 1011212 = 96 sqm × 100 × days;  68 = 10,000 × 365")
check(comp_business.count() == 12165, "12,165 businesses")
same_as(comp_business, "gold", "compensation_business")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G07 · `compensation_street` — required output 2
# MAGIC `closed` = days per street; `agg` = per-business totals rolled up by street; joined and given per-street metrics.

# COMMAND ----------

closed = street_days.groupBy("street_key").agg(F.count("*").alias("closed_days"))
step(closed, "g07a_closed_days")
agg = (comp_business.groupBy("street_key")
       .agg(F.min("street_name").alias("street_name"),
            F.count("*").alias("businesses_affected"),
            F.sum(F.when(F.col("model_b_eligible"), 1).otherwise(0)).alias("businesses_eligible_model_b"),
            F.sum("area_sqm").alias("total_area_sqm"),
            F.sum("total_model_a").alias("total_model_a"),
            F.sum("total_model_b").alias("total_model_b"),
            F.sum(F.col("cap_binding").cast("int")).alias("businesses_at_cap")))
step(agg, "g07b_street_totals")
s_area = F.when(F.col("total_area_sqm") > 0, F.col("total_area_sqm"))
comp_street = (agg.join(closed, "street_key", "left")
                  .withColumn("cost_per_sqm_model_a", (F.col("total_model_a") / s_area).cast("decimal(14,2)"))
                  .withColumn("cost_per_sqm_model_b", (F.col("total_model_b") / s_area).cast("decimal(14,2)"))
                  .withColumn("avg_per_business_model_a",
                              (F.col("total_model_a") / F.col("businesses_affected")).cast("decimal(14,2)")))
step(comp_street, "g07_compensation_street", "→ gold_compensation_street")
display(comp_street.orderBy(F.desc("total_model_a")).limit(10))
trace(comp_street, "street_key", TRACE_STREETS)
check(comp_street.count() == 94, "94 streets compensated")
same_as(comp_street, "gold", "compensation_street")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G08 · Rollups: category, month, closure type

# COMMAND ----------

comp_category = (comp_business.groupBy("category_en", "category_he", "model_b_eligible", "exclusion_group")
                 .agg(F.count("*").alias("businesses"), F.sum("area_sqm").alias("total_area_sqm"),
                      F.sum("total_model_a").alias("total_model_a"), F.sum("total_model_b").alias("total_model_b")))
step(comp_category, "g08a_compensation_category"); same_as(comp_category, "gold", "compensation_category")

comp_month = (bcd.groupBy(F.date_trunc("month", "day").cast("date").alias("month"))
              .agg(F.countDistinct("business_id").alias("businesses_affected"),
                   F.countDistinct("street_key").alias("streets_closed"),
                   F.count("*").alias("business_days"),
                   F.sum("comp_model_a").alias("total_model_a"),
                   F.sum("comp_model_b").alias("total_model_b")))
step(comp_month, "g08b_compensation_month"); same_as(comp_month, "gold", "compensation_month")
display(comp_month.orderBy("month"))

comp_type = (bcd.groupBy("primary_closure_type")
             .agg(F.countDistinct("street_key").alias("streets"),
                  F.count("*").alias("business_days"),
                  F.sum("comp_model_a").alias("total_model_a"),
                  F.sum("comp_model_b").alias("total_model_b")))
step(comp_type, "g08c_compensation_closure_type"); same_as(comp_type, "gold", "compensation_closure_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G09 · Reconciliation and the Model A → Model B bridge
# MAGIC The three grains must give the same Model A total. Then the bridge: cumulative sums over business-days,
# MAGIC switching on one Model B condition at a time.

# COMMAND ----------

t_days = bcd.agg(F.sum("comp_model_a")).first()[0]
t_biz = comp_business.agg(F.sum("total_model_a")).first()[0]
t_str = comp_street.agg(F.sum("total_model_a")).first()[0]
print(f"business-days {t_days:,}   per-business {t_biz:,}   per-street {t_str:,}")
check(t_days == t_biz == t_str, "totals reconcile across all three grains")

bridge = bcd.agg(
    F.sum("comp_model_a").alias("1_model_a"),
    F.sum(F.when(F.col("model_b_eligible"), F.col("comp_model_a"))).alias("2_only_trading_premises"),
    F.sum(F.when(F.col("model_b_eligible") & F.col("existed_on_day"), F.col("comp_model_a"))).alias("3_and_only_after_established"),
    F.sum("comp_model_b").alias("4_and_weighted_by_trading_hours_model_b"))
display(bridge)
