"""Headline metrics and chart series, recomputed from gold on every run.

Feeds the README and the dashboard. Everything here is derived from gold tables only,
so it is reproducible and stays consistent with the business outputs.
"""
from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Config
from .storage import Store

CLOSURE_TYPE_EN = {
    'נת"ע – הרכבת הקלה': "Light rail works (NTA)", "אחר": "Other (long works)",
    "שיפור תשתיות": "Infrastructure upgrade", "ארוע מרתון": "Marathon event",
    "הנחת קו מים": "Water main", "קרצוף וסלילה": "Milling & paving",
}


def _f(x) -> float:
    return float(x or 0)


def compute(spark: SparkSession, cfg: Config) -> dict:
    s = Store(spark, cfg)
    bcd, cb, cs = (s.read("gold", n) for n in ("business_closure_days", "compensation_business", "compensation_street"))
    sd, mo, ct = (s.read("gold", n) for n in ("closure_street_days", "compensation_month", "compensation_closure_type"))
    cat, dq = s.read("gold", "compensation_category"), s.read("gold", "dq_results")
    y_end = F.lit(f"{cfg.year}-12-31").cast("date")

    k = cb.agg(F.count("*").alias("n"), F.sum("total_model_a").alias("a"), F.sum("total_model_b").alias("b"),
               F.sum("area_sqm").alias("area"), F.sum(F.col("cap_binding").cast("int")).alias("capped"),
               F.sum(F.when(F.col("cap_binding"), F.col("total_model_a"))).alias("capped_a"),
               F.percentile_approx("total_model_a", 0.5).alias("median_a"),
               F.percentile_approx("days_affected", 0.5).alias("median_days")).first()
    steps = bcd.agg(
        F.sum("comp_model_a").alias("a"),
        F.sum(F.when(F.col("model_b_eligible"), F.col("comp_model_a"))).alias("eligible"),
        F.sum(F.when(F.col("model_b_eligible") & F.col("existed_on_day"), F.col("comp_model_a"))).alias("existed"),
        F.sum("comp_model_b").alias("b"), F.count("*").alias("business_days")).first()
    late = cb.filter(F.col("established_date") > y_end).agg(F.count("*").alias("n"),
                                                           F.sum("total_model_a").alias("a")).first()
    top = cs.orderBy(F.desc("total_model_a")).limit(10).collect()
    a_total = _f(k["a"])
    top5 = sum(_f(r["total_model_a"]) for r in top[:5])
    biggest = cb.orderBy(F.desc("total_model_a"), "business_id").first()

    return {
        "year": cfg.year,
        "kpi": {
            "businesses_affected": k["n"], "streets_compensated": cs.count(),
            "street_days": sd.count(), "business_days": steps["business_days"],
            "total_model_a": a_total, "total_model_b": _f(k["b"]),
            "model_b_pct_of_a": round(100 * _f(k["b"]) / a_total, 1) if a_total else None,
            "cost_per_sqm_model_a": round(a_total / _f(k["area"]), 0) if k["area"] else None,
            "businesses_at_cap": k["capped"], "pct_businesses_at_cap": round(100 * k["capped"] / k["n"], 1),
            "pct_spend_at_cap": round(100 * _f(k["capped_a"]) / a_total, 1),
            "median_business_model_a": _f(k["median_a"]), "median_days_affected": k["median_days"],
            "top5_street_share_pct": round(100 * top5 / a_total, 1),
            "established_after_year": {"businesses": late["n"], "total_model_a": _f(late["a"])},
            "largest_single_business": {"business_id": biggest["business_id"], "street": biggest["street_name"],
                                        "category": biggest["category_en"], "area_sqm": biggest["area_sqm"],
                                        "days": biggest["days_affected"], "total_model_a": _f(biggest["total_model_a"])},
        },
        "model_a_to_b": [
            {"step": "Model A — literal rule", "value": _f(steps["a"])},
            {"step": "Non-trading premises", "value": _f(steps["eligible"]) - _f(steps["a"])},
            {"step": "Days before business existed", "value": _f(steps["existed"]) - _f(steps["eligible"])},
            {"step": "Hours outside trading time", "value": _f(steps["b"]) - _f(steps["existed"])},
            {"step": "Model B", "value": _f(steps["b"])},
        ],
        "monthly": [{"month": str(r["month"])[:7], "a": _f(r["total_model_a"]), "b": _f(r["total_model_b"]),
                     "businesses": r["businesses_affected"]} for r in mo.orderBy("month").collect()],
        "top_streets": [{"street": r["street_name"], "closed_days": r["closed_days"],
                         "businesses": r["businesses_affected"], "a": _f(r["total_model_a"]),
                         "b": _f(r["total_model_b"]), "cost_per_sqm_a": _f(r["cost_per_sqm_model_a"])} for r in top],
        "closure_types": [{"type_he": r["primary_closure_type"],
                           "type": CLOSURE_TYPE_EN.get(r["primary_closure_type"], r["primary_closure_type"]),
                           "streets": r["streets"], "a": _f(r["total_model_a"]), "b": _f(r["total_model_b"])}
                          for r in ct.orderBy(F.desc("total_model_a")).collect()],
        "non_trading": [{"group": r["exclusion_group"], "businesses": r["businesses"], "a": _f(r["a"])}
                        for r in cat.filter(~F.col("model_b_eligible")).groupBy("exclusion_group")
                                    .agg(F.sum("businesses").alias("businesses"), F.sum("total_model_a").alias("a"))
                                    .orderBy(F.desc("a")).collect()],
        "top_categories": [{"category": r["category_en"], "eligible": r["model_b_eligible"], "businesses": r["businesses"],
                            "a": _f(r["total_model_a"]), "b": _f(r["total_model_b"])}
                           for r in cat.orderBy(F.desc("total_model_a")).limit(12).collect()],
        "dq": [{"check": r["check"], "layer": r["layer"], "observed": r["observed"], "expected": r["expected"],
                "status": r["status"]} for r in dq.collect()],
    }


def write(spark: SparkSession, cfg: Config, path: str | None = None) -> dict:
    data = compute(spark, cfg)
    path = path or os.path.join(cfg.base_path, "insights.json")
    if not path.startswith("/Volumes"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, default=str)
    return data
