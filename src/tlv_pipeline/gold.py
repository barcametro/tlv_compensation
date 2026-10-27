"""Gold: 2023 compensation, computed at the business-day grain and rolled up.

Grain is declared explicitly:
  closure_street_days       one row per (street, day) in the year
  business_closure_days     one row per (business, day)   <- rule 2: once per business per day
  compensation_business     one row per business
  compensation_street       one row per street

Two compensation models are computed side by side:

  Model A — the brief, literally.
      daily = min(rate * area_sqm, cap) on every day the business's street appears
      in any closure. Every business type, every hour of closure, counts in full.

  Model B — the brief, adjusted for three things the literal rule ignores:
      1. Non-trading premises are not compensated: transformer rooms, warehouses,
         free parking, closed units, municipal departments, places of worship
         (see conf/category_map.csv, column model_b_eligible).
      2. A business is only compensated from its establishment date onward. The API
         is a current snapshot, so it contains businesses that did not exist in 2023.
      3. The daily amount is scaled by the share of trading hours (08:00-20:00) the
         closure actually covers. A 04:00-14:00 marathon closure pays 50%.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .storage import Store


def closure_street_days(closure_streets: DataFrame, cfg: Config) -> DataFrame:
    y0, y1 = F.lit(f"{cfg.year}-01-01").cast("date"), F.lit(f"{cfg.year}-12-31").cast("date")
    # filter on OVERLAP with the year, not on start year: closures that began in 2020
    # and ran through 2023 are the largest ones in the file
    in_year = closure_streets.filter((F.col("start_date") <= y1) & (F.col("end_date") >= y0))
    days = in_year.withColumn(
        "day", F.explode(F.sequence(F.greatest("start_date", y0), F.least("end_date", y1),
                                    F.expr("INTERVAL 1 DAY"))))
    return (days.groupBy("street_key", "day")
                .agg(F.countDistinct("closure_id").alias("closures_that_day"),
                     F.max("trading_hours_factor").alias("trading_hours_factor"),
                     F.max_by("closure_type", F.struct("trading_hours_factor", "closure_id"))     # ties -> highest id
                      .alias("primary_closure_type"),
                     F.sort_array(F.collect_set("closure_id")).alias("closure_ids")))


def business_closure_days(businesses: DataFrame, street_days: DataFrame, cfg: Config) -> DataFrame:
    full_day = F.least(F.lit(cfg.rate) * F.coalesce(F.col("area_sqm"), F.lit(0)), F.lit(cfg.cap))
    existed = F.col("established_date").isNull() | (F.col("established_date") <= F.col("day"))
    b = businesses.filter(F.col("street_key").isNotNull()).select(
        "business_id", "street_key", "street_name", "category_he", "category_en",
        "model_b_eligible", "established_date", "area_sqm")
    return (b.join(street_days, "street_key", "inner")
             .withColumn("existed_on_day", existed)
             .withColumn("cap_binding", F.lit(cfg.rate) * F.coalesce("area_sqm", F.lit(0)) > cfg.cap)
             .withColumn("comp_model_a", full_day.cast("decimal(12,2)"))
             .withColumn("comp_model_b",
                         F.when(F.col("model_b_eligible") & F.col("existed_on_day"),
                                full_day * F.col("trading_hours_factor"))
                          .otherwise(0).cast("decimal(12,2)")))


def compensation_business(bcd: DataFrame, businesses: DataFrame) -> DataFrame:
    per = (bcd.groupBy("business_id")
              .agg(F.count("*").alias("days_affected"),
                   F.min("day").alias("first_day"), F.max("day").alias("last_day"),
                   F.sum("comp_model_a").alias("total_model_a"),
                   F.sum("comp_model_b").alias("total_model_b"),
                   F.max("cap_binding").alias("cap_binding")))
    attrs = businesses.select("business_id", "street_name", "street_key", "house_number",
                              "category_he", "category_en", "model_b_eligible", "exclusion_group",
                              "established_date", "area_sqm", "x_itm", "y_itm")
    out = per.join(attrs, "business_id", "left")
    area = F.when(F.col("area_sqm") > 0, F.col("area_sqm"))
    return (out.withColumn("avg_daily_model_a", (F.col("total_model_a") / F.col("days_affected")).cast("decimal(12,2)"))
               .withColumn("cost_per_sqm_model_a", (F.col("total_model_a") / area).cast("decimal(14,2)"))
               .withColumn("cost_per_sqm_model_b", (F.col("total_model_b") / area).cast("decimal(14,2)"))
               .withColumn("model_b_vs_a_pct",
                           F.round(100 * F.col("total_model_b") / F.when(F.col("total_model_a") > 0,
                                                                         F.col("total_model_a")), 1)))


def compensation_street(comp_business: DataFrame, street_days: DataFrame) -> DataFrame:
    closed = street_days.groupBy("street_key").agg(F.count("*").alias("closed_days"))
    agg = (comp_business.groupBy("street_key")
           .agg(F.min("street_name").alias("street_name"),
                F.count("*").alias("businesses_affected"),
                F.sum(F.when(F.col("model_b_eligible"), 1).otherwise(0)).alias("businesses_eligible_model_b"),
                F.sum("area_sqm").alias("total_area_sqm"),
                F.sum("total_model_a").alias("total_model_a"),
                F.sum("total_model_b").alias("total_model_b"),
                F.sum(F.col("cap_binding").cast("int")).alias("businesses_at_cap")))
    area = F.when(F.col("total_area_sqm") > 0, F.col("total_area_sqm"))
    return (agg.join(closed, "street_key", "left")
               .withColumn("cost_per_sqm_model_a", (F.col("total_model_a") / area).cast("decimal(14,2)"))
               .withColumn("cost_per_sqm_model_b", (F.col("total_model_b") / area).cast("decimal(14,2)"))
               .withColumn("avg_per_business_model_a",
                           (F.col("total_model_a") / F.col("businesses_affected")).cast("decimal(14,2)")))


def compensation_category(comp_business: DataFrame) -> DataFrame:
    return (comp_business.groupBy("category_en", "category_he", "model_b_eligible", "exclusion_group")
            .agg(F.count("*").alias("businesses"), F.sum("area_sqm").alias("total_area_sqm"),
                 F.sum("total_model_a").alias("total_model_a"), F.sum("total_model_b").alias("total_model_b")))


def compensation_month(bcd: DataFrame) -> DataFrame:
    return (bcd.groupBy(F.date_trunc("month", "day").cast("date").alias("month"))
               .agg(F.countDistinct("business_id").alias("businesses_affected"),
                    F.countDistinct("street_key").alias("streets_closed"),
                    F.count("*").alias("business_days"),
                    F.sum("comp_model_a").alias("total_model_a"),
                    F.sum("comp_model_b").alias("total_model_b")))


def compensation_closure_type(bcd: DataFrame) -> DataFrame:
    return (bcd.groupBy("primary_closure_type")
               .agg(F.countDistinct("street_key").alias("streets"),
                    F.count("*").alias("business_days"),
                    F.sum("comp_model_a").alias("total_model_a"),
                    F.sum("comp_model_b").alias("total_model_b")))


def run(spark: SparkSession, cfg: Config) -> dict[str, DataFrame]:
    store = Store(spark, cfg)
    businesses = store.read("silver", "businesses")
    sd = store.write(closure_street_days(store.read("silver", "closure_streets"), cfg),
                     "gold", "closure_street_days")
    bcd = store.write(business_closure_days(businesses, sd, cfg), "gold", "business_closure_days")
    cb = store.write(compensation_business(bcd, businesses), "gold", "compensation_business")
    return {
        "closure_street_days": sd, "business_closure_days": bcd, "compensation_business": cb,
        "compensation_street": store.write(compensation_street(cb, sd), "gold", "compensation_street"),
        "compensation_category": store.write(compensation_category(cb), "gold", "compensation_category"),
        "compensation_month": store.write(compensation_month(bcd), "gold", "compensation_month"),
        "compensation_closure_type": store.write(compensation_closure_type(bcd), "gold", "compensation_closure_type"),
    }
