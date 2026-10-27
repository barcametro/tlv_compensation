"""Data quality: assertions across all layers, written as a table.

Each check is a row (check, layer, observed, expected, status, detail) so results
can be trended over runs and shown on the dashboard. `fail` statuses stop the run
when raise_on_fail=True (the scheduled job uses this as its gate).
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .storage import Store


class DataQualityError(RuntimeError):
    pass


def _expected_business_count(spark: SparkSession, cfg: Config) -> int | None:
    try:
        m = (spark.read.option("multiLine", True).option("recursiveFileLookup", True)
             .option("pathGlobFilter", "manifest.json")   # not "_manifest": Spark hides files starting with _
             .json(cfg.businesses_dir))
    except Exception:  # no manifest landed (e.g. data uploaded by hand)
        return None
    if m.isEmpty() or "expected_count" not in m.columns:   # DataFrame API: works on serverless
        return None
    return m.orderBy(F.col("run_utc").desc()).first()["expected_count"]


def unmatched_streets(store: Store, cfg: Config) -> DataFrame:
    """Streets closed in the year that match no business street — reported, never fuzzy-matched."""
    biz = store.read("silver", "businesses").select("street_key").distinct().withColumn("_m", F.lit(1))
    sd = store.read("gold", "closure_street_days").groupBy("street_key").agg(F.count("*").alias("closed_days"))
    return sd.join(biz, "street_key", "left").filter(F.col("_m").isNull()).drop("_m")


def run(spark: SparkSession, cfg: Config, raise_on_fail: bool = True) -> DataFrame:
    store = Store(spark, cfg)
    bz_b, bz_c = store.read("bronze", "businesses"), store.read("bronze", "street_closures")
    sv_b, sv_c = store.read("silver", "businesses"), store.read("silver", "street_closures")
    rej = store.read("silver", "street_closures_rejected")
    sd, bcd = store.read("gold", "closure_street_days"), store.read("gold", "business_closure_days")
    cb, cs = store.read("gold", "compensation_business"), store.read("gold", "compensation_street")

    rows: list[tuple] = []

    def check(name, layer, observed, expected, ok, detail="", severity="fail"):
        status = "pass" if ok else severity
        rows.append((name, layer, str(observed), str(expected), status, detail))

    # Bronze keeps every landed run, so compare the LATEST run with the LATEST manifest.
    # Counting all of bronze would fail the gate as soon as a second download lands.
    exp = _expected_business_count(spark, cfg)
    latest_run = bz_b.agg(F.max("_run_id")).first()[0]
    n_bz = bz_b.filter(F.col("_run_id") == latest_run).count() if latest_run else bz_b.count()
    if exp is None:
        check("businesses landed == server count (latest run)", "bronze", n_bz, "manifest missing", False,
              "no download manifest found; completeness cannot be verified", severity="warn")
    else:
        check("businesses landed == server count (latest run)", "bronze", n_bz, exp, n_bz == exp,
              f"run {latest_run}; the API caps responses at 2000, a single call returns ~5% of rows")
    check("closures rows landed > 0", "bronze", bz_c.count(), "> 0", bz_c.count() > 0)

    n_sv, n_ids = sv_b.count(), sv_b.select("business_id").distinct().count()
    check("business_id unique", "silver", n_sv - n_ids, 0, n_sv == n_ids)
    unmapped = sv_b.filter(~F.col("category_mapped")).count()
    check("business categories mapped", "silver", unmapped, 0, unmapped == 0, severity="warn")
    no_area = sv_b.filter(F.col("area_sqm").isNull() | (F.col("area_sqm") <= 0)).count()
    check("businesses with area <= 0 or null", "silver", no_area, "info", True, "they receive 0 compensation")

    bad_dates = sv_c.filter(F.col("start_date").isNull()).count()
    check("closure start dates parsed", "silver", bad_dates, 0, bad_dates == 0)
    for reason in ("no_valid_street", "duplicate_event", "unparseable_start_date"):
        n = rej.filter(F.col("reject_reason") == reason).count()
        check(f"closures rejected: {reason}", "silver", n, "info", True)
    imputed = sv_c.filter("end_imputed").count()
    check("closures with end date imputed from start", "silver", imputed, "info", True)
    corrupt = sv_c.filter("night_flag_corrupt").count()
    check("sw_laila values corrupted by unescaped commas", "silver", corrupt, "info", True,
          "night_flag set to NULL on these rows")

    streets = sd.select("street_key").distinct().count()
    um = unmatched_streets(store, cfg).count()
    rate = (streets - um) / streets if streets else 0
    check(f"{cfg.year} closure streets matched to businesses", "gold", f"{rate:.1%}",
          f">= {cfg.min_street_match_rate:.0%}", rate >= cfg.min_street_match_rate,
          f"{um} unmatched of {streets}", severity="warn")

    dup_bd = bcd.groupBy("business_id", "day").count().filter("count > 1").count()
    check("one row per business per day", "gold", dup_bd, 0, dup_bd == 0, "brief, rule 2")
    out_of_year = bcd.filter(F.year("day") != cfg.year).count()
    check(f"all days inside {cfg.year}", "gold", out_of_year, 0, out_of_year == 0, "brief, rule 6")
    over_cap = bcd.filter((F.col("comp_model_a") > cfg.cap) | (F.col("comp_model_a") < 0)).count()
    check("daily compensation within [0, cap]", "gold", over_cap, 0, over_cap == 0, "brief, rule 4")
    b_over_a = bcd.filter(F.col("comp_model_b") > F.col("comp_model_a")).count()
    check("model B never exceeds model A", "gold", b_over_a, 0, b_over_a == 0)

    t_days = bcd.agg(F.sum("comp_model_a")).first()[0] or 0
    t_biz = cb.agg(F.sum("total_model_a")).first()[0] or 0
    t_str = cs.agg(F.sum("total_model_a")).first()[0] or 0
    check("totals reconcile: business-days = per-business = per-street", "gold",
          f"{t_days} / {t_biz} / {t_str}", "equal", t_days == t_biz == t_str)

    run_at = datetime.now(timezone.utc)
    df = spark.createDataFrame([Row(check=r[0], layer=r[1], observed=r[2], expected=r[3],
                                    status=r[4], detail=r[5], run_at=run_at) for r in rows])
    store.write(df, "gold", "dq_results")
    store.write(unmatched_streets(store, cfg), "gold", "dq_unmatched_streets")

    failed = [r for r in rows if r[4] == "fail"]
    if failed and raise_on_fail:
        raise DataQualityError("; ".join(f"{r[0]} (observed {r[2]}, expected {r[3]})" for r in failed))
    return df
