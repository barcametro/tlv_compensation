"""Silver: typed, cleaned, deduplicated, conformed.

Every cleaning rule from the brief is implemented here, plus the data defects found
while profiling the real files (see README, "What the data actually looks like").
All parsing uses try_* functions: Spark 4 and Databricks run ANSI mode by default,
where a single unparseable value would otherwise fail the whole job.
"""
from __future__ import annotations

import csv

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Config
from .normalize import clean_text, street_key, strip_wrapping_quotes
from .storage import Store

_HOURS = r"'^\\s*(\\d{1,2}):(\\d{2})\\s*-\\s*(\\d{1,2}):(\\d{2})'"


def trading_hours_factor(col: str, cfg: Config) -> F.Column:
    """Share of trading hours (default 08:00-20:00) covered by a daily closure window.

    '04:00-14:00' -> 0.5;  '07:00-07:00' (24h) -> 1.0;  '20:00-14:00' wraps midnight -> 0.5.
    Unparseable or missing hours -> 1.0 (benefit of the doubt to the business).
    Used only by compensation Model B.
    """
    t0, t1, span = cfg.trading_start_min, cfg.trading_end_min, cfg.trading_minutes
    s = f"(try_cast(regexp_extract({col}, {_HOURS}, 1) AS INT) * 60 + try_cast(regexp_extract({col}, {_HOURS}, 2) AS INT))"
    e = f"(try_cast(regexp_extract({col}, {_HOURS}, 3) AS INT) * 60 + try_cast(regexp_extract({col}, {_HOURS}, 4) AS INT))"
    return F.expr(f"""
        CASE
          WHEN {s} IS NULL OR {e} IS NULL THEN 1.0
          WHEN {s} = {e} THEN 1.0
          WHEN {e} > {s} THEN greatest(0, least({e}, {t1}) - greatest({s}, {t0})) / {span}
          ELSE (greatest(0, {t1} - greatest({s}, {t0})) + greatest(0, least({e}, {t1}) - {t0})) / {span}
        END""")


# --------------------------------------------------------------------------- closures
def street_closures(bronze: DataFrame, cfg: Config) -> tuple[DataFrame, DataFrame]:
    """Returns (clean closures, rejected closures)."""
    typed = bronze.select(
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
    # a street code of 0 means "no street"
    typed = (typed
             .withColumn("street_a", F.when(F.col("street_a_code") == 0, None).otherwise(F.col("street_a")))
             .withColumn("street_b", F.when(F.col("street_b_code") == 0, None).otherwise(F.col("street_b"))))

    clean = (typed
             .withColumn("start_date", F.to_date("start_ts"))
             # rule: if the end date is missing, use the start date
             .withColumn("end_imputed", F.col("end_ts").isNull())
             .withColumn("end_date", F.coalesce(F.to_date("end_ts"), F.col("start_date")))
             # embedded commas in `shaot` pushed free text into sw_laila on ~160 rows
             .withColumn("night_flag", F.when(F.trim("_sw_laila_raw") == "1", True)
                                        .when(F.trim("_sw_laila_raw") == "0", False))
             .withColumn("night_flag_corrupt",
                         F.col("_sw_laila_raw").isNotNull() & ~F.trim("_sw_laila_raw").isin("0", "1"))
             .withColumn("trading_hours_factor", trading_hours_factor("hours_text", cfg))
             .drop("_sw_laila_raw"))

    reason = (F.when(F.col("start_date").isNull(), "unparseable_start_date")
               .when(F.col("street_a").isNull() & F.col("street_b").isNull(), "no_valid_street")
               .when(F.col("end_date") < F.col("start_date"), "end_before_start"))
    flagged = clean.withColumn("reject_reason", reason)

    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    # the same event is sometimes entered twice (identical except for ID); keep the first
    key = ["street_a", "street_b", "start_date", "end_date", "closure_type_code", "hours_text"]
    w = Window.partitionBy(*key).orderBy("closure_id")
    valid = (valid.withColumn("_rn", F.row_number().over(w))
                  .withColumn("duplicate_of", F.when(F.col("_rn") > 1, F.first("closure_id").over(w))))
    deduped = valid.filter(F.col("_rn") == 1).drop("_rn", "duplicate_of")
    dups = (valid.filter(F.col("_rn") > 1).drop("_rn")
                 .withColumn("reject_reason", F.lit("duplicate_event")))
    rejected = rejected.unionByName(dups, allowMissingColumns=True)
    return deduped, rejected


def closure_streets(closures: DataFrame) -> DataFrame:
    """One row per closure per affected street, so each street matches independently."""
    base = ["closure_id", "start_date", "end_date", "closure_type", "closure_type_code",
            "hours_text", "trading_hours_factor"]
    a = closures.select(*base, F.lit("A").alias("street_role"),
                        F.col("street_a").alias("street_key"), F.col("street_a_code").alias("street_code"))
    b = closures.select(*base, F.lit("B").alias("street_role"),
                        F.col("street_b").alias("street_key"), F.col("street_b_code").alias("street_code"))
    return a.unionByName(b).filter(F.col("street_key").isNotNull())


# --------------------------------------------------------------------------- businesses
def businesses(bronze: DataFrame, categories: DataFrame) -> DataFrame:
    a = "attributes"
    flat = bronze.select(
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
    # rule: remove duplicate businesses by business id, keeping the latest record.
    # Ties on import_ts (every row of one API snapshot shares it) are broken by the
    # most recent landing run, then OBJECTID, so the choice is deterministic.
    w = Window.partitionBy("business_id").orderBy(
        F.col("import_ts").desc_nulls_last(), F.col("_run_id").desc(), F.col("object_id").desc())
    latest = (flat.filter(F.col("business_id").isNotNull())
                  .withColumn("_rn", F.row_number().over(w))
                  .filter(F.col("_rn") == 1).drop("_rn"))

    cats = categories.select(
        clean_text(F.col("category_he")).alias("category_he"), "category_en",
        (F.col("model_b_eligible").cast("int") == 1).alias("model_b_eligible"),
        F.when(F.col("exclusion_group") == "", None).otherwise(F.col("exclusion_group")).alias("exclusion_group"))
    return (latest.join(F.broadcast(cats), "category_he", "left")
                  .withColumn("category_mapped", F.col("category_en").isNotNull())
                  .withColumn("category_en", F.coalesce("category_en", F.lit("Unmapped")))
                  .withColumn("model_b_eligible", F.coalesce("model_b_eligible", F.lit(True))))


def read_category_map(spark: SparkSession, path: str) -> DataFrame:
    """The category map is configuration (191 rows), read from the repo with Python's csv
    module. Same file locally and on Databricks (/Workspace/...), so editing
    conf/category_map.csv is the only way to change Model B eligibility."""
    with open(path, encoding="utf-8-sig", newline="") as fh:
        rows = [(r["category_he"], r["category_en"], r["model_b_eligible"], r["exclusion_group"])
                for r in csv.DictReader(fh)]
    return spark.createDataFrame(
        rows, "category_he string, category_en string, model_b_eligible string, exclusion_group string")


def run(spark: SparkSession, cfg: Config) -> dict[str, DataFrame]:
    store = Store(spark, cfg)
    categories = read_category_map(spark, cfg.category_map)
    clean, rejected = street_closures(store.read("bronze", "street_closures"), cfg)
    out = {"street_closures": store.write(clean, "silver", "street_closures"),
           "street_closures_rejected": store.write(rejected, "silver", "street_closures_rejected")}
    out["closure_streets"] = store.write(closure_streets(out["street_closures"]), "silver", "closure_streets")
    out["businesses"] = store.write(businesses(store.read("bronze", "businesses"), categories),
                                    "silver", "businesses")
    return out
