"""Unit tests for the business rules, on tiny hand-built inputs where the right answer is obvious.

Run:  PYTHONPATH=src pytest -q tests/
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from tlv_pipeline import config, gold, silver
from tlv_pipeline.normalize import street_key, strip_wrapping_quotes

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session")
def cfg():
    return config.load("conf/pipeline.yml", root=ROOT)


def closures_bronze(spark, rows):
    cols = ["ID", "id_rechov", "me_k_rechov", "me_shem_rechov", "ad_k_rechov", "ad_shem_rechov",
            "tr_from", "tr_to", "k_sug", "t_sug", "k_sgira", "shaot", "sw_laila"]
    df = spark.createDataFrame([tuple(r) for r in rows], cols)
    return df.withColumn("_source_file", F.lit("t")).withColumn("_ingested_at", F.current_timestamp())


# ------------------------------------------------------------------ normalisation
def test_street_key_unifies_quotes_and_treats_zero_as_missing(spark):
    df = spark.createDataFrame([("של״ג",), ('של"ג',), ("  המלך   ג׳ורג׳ ",), ("0",), ("",)], ["s"])
    got = [r[0] for r in df.select(street_key(F.col("s"))).collect()]
    assert got == ['של"ג', 'של"ג', "המלך ג'ורג'", None, None]


def test_strip_wrapping_quotes_keeps_internal_quotes(spark):
    df = spark.createDataFrame([('"משרד השמה וכ"א"',), ('תמח"ת',)], ["s"])
    assert [r[0] for r in df.select(strip_wrapping_quotes(F.col("s"))).collect()] == ['משרד השמה וכ"א', 'תמח"ת']


# ------------------------------------------------------------------ trading-hours factor (model B)
@pytest.mark.parametrize("hours,expected", [
    ("04:00-14:00 מרתון", 0.5),        # overlaps 08-14 of 08-20
    ("07:00-07:00", 1.0),              # 24h
    ("21:00-05:00", 0.0),              # night only
    ("20:00-14:00", 0.5),              # wraps midnight -> covers 08-14
    ("17:00-09:00", 4 / 12),           # 17-20 plus 08-09
    ("מסיבת פורים משעה 08:00-14:00", 0.5),  # window after leading text
    ("", 1.0), (None, 1.0),            # unknown -> benefit of the doubt
])
def test_trading_hours_factor(spark, cfg, hours, expected):
    df = spark.createDataFrame([(hours,)], "h string")
    got = df.select(silver.trading_hours_factor("h", cfg)).first()[0]
    assert got == pytest.approx(expected)


# ------------------------------------------------------------------ closures cleaning
def test_closure_cleaning_rules(spark, cfg):
    raw = closures_bronze(spark, [
        ("1", "1", "10", "דיזנגוף", "20", "בן יהודה", "01/02/2023 0:00", "03/02/2023 0:00", "5", "ארוע", "2", "09:00-17:00", "0"),
        ("2", "2", "10", "דיזנגוף", "0", "0", "05/02/2023 0:00", "", "5", "ארוע", "2", "", "1"),           # no end date
        ("3", "3", "0", "0", "0", "0", "05/02/2023 0:00", "05/02/2023 0:00", "4", "סלילה", "1", "", "1"),  # no street
        ("4", "1", "10", "דיזנגוף", "20", "בן יהודה", "01/02/2023 0:00", "03/02/2023 0:00", "5", "ארוע", "2", "09:00-17:00", "0"),  # dup of 1
        ("5", "9", "10", "דיזנגוף", "20", "בן יהודה", "01/02/2023 0:00", "03/02/2023 0:00", "5", "ארוע", "2", "09:00-17:00", "0"),  # other segment: kept
    ])
    clean, rejected = silver.street_closures(raw, cfg)
    c = {r.closure_id: r for r in clean.collect()}
    assert set(c) == {1, 2, 5}
    assert c[1].start_date == dt.date(2023, 2, 1) and c[1].end_date == dt.date(2023, 2, 3)  # dd/MM, not MM/dd
    assert c[2].end_date == c[2].start_date and c[2].end_imputed                            # missing end -> start
    assert c[2].street_b is None                                                            # code 0 -> no street
    reasons = {r.closure_id: r.reject_reason for r in rejected.collect()}
    assert reasons == {3: "no_valid_street", 4: "duplicate_event"}


def test_both_streets_become_independent_rows(spark, cfg):
    raw = closures_bronze(spark, [
        ("1", "1", "10", "דיזנגוף", "20", "בן יהודה", "01/02/2023 0:00", "01/02/2023 0:00", "5", "ארוע", "2", "", "0")])
    clean, _ = silver.street_closures(raw, cfg)
    streets = sorted(r.street_key for r in silver.closure_streets(clean).collect())
    assert streets == sorted(["דיזנגוף", "בן יהודה"])


# ------------------------------------------------------------------ businesses
def test_business_dedup_keeps_latest_import(spark):
    def row(oid, imp, area):
        return Row(attributes=Row(OBJECTID=oid, id_esek=7, shimush="משרדים", shem_rechov="דיזנגוף", ms_bayit="1",
                                  tr_hakama=1356998400000, sw_taun_rishui=1, shetach=area, x_coord=1.0, y_coord=2.0,
                                  date_import=imp),
                   geometry=Row(x=1.0, y=2.0), _srid=2039, _source_file="t", _run_id="r1",
                   _ingested_at=dt.datetime(2026, 1, 1))
    bronze = spark.createDataFrame([row(1, "01/01/2025 00:00:00", 50), row(2, "01/06/2026 00:00:00", 80)])
    cats = spark.createDataFrame([("משרדים", "Offices", "1", "")],
                                 ["category_he", "category_en", "model_b_eligible", "exclusion_group"])
    out = silver.businesses(bronze, cats).collect()
    assert len(out) == 1 and out[0].area_sqm == 80 and out[0].established_date == dt.date(2013, 1, 1)


# ------------------------------------------------------------------ compensation rules
def test_compensation_rules(spark, cfg):
    cs = spark.createDataFrame([
        # straddles the year: only 30-31 Dec 2022 are outside, 2 days count in 2023
        (1, dt.date(2022, 12, 30), dt.date(2023, 1, 2), "ארוע", 5, "", 1.0, "A", "דיזנגוף", 10),
        # a second closure on the same street, same day -> still one payment that day
        (2, dt.date(2023, 1, 1), dt.date(2023, 1, 1), "ארוע", 5, "", 0.5, "A", "דיזנגוף", 10),
        # different year entirely -> ignored
        (3, dt.date(2024, 5, 1), dt.date(2024, 5, 3), "ארוע", 5, "", 1.0, "A", "דיזנגוף", 10),
    ], "closure_id int, start_date date, end_date date, closure_type string, closure_type_code int, "
       "hours_text string, trading_hours_factor double, street_role string, street_key string, street_code int")
    biz = spark.createDataFrame([
        (1, "דיזנגוף", "דיזנגוף", "משרדים", "Offices", True, dt.date(2010, 1, 1), 25),    # 25 sqm -> 2,500/day
        (2, "דיזנגוף", "דיזנגוף", "משרדים", "Offices", True, dt.date(2010, 1, 1), 400),   # capped at 10,000/day
        (3, "דיזנגוף", "דיזנגוף", "חדר טרנספורמציה", "Transformer Room", False, None, 30),  # model B: excluded
        (4, "דיזנגוף", "דיזנגוף", "משרדים", "Offices", True, dt.date(2023, 1, 2), 10),    # opened on day 2
    ], "business_id long, street_key string, street_name string, category_he string, category_en string, "
       "model_b_eligible boolean, established_date date, area_sqm int")

    sd = gold.closure_street_days(cs, cfg)
    assert sd.count() == 2                                                  # 1 and 2 Jan 2023 only
    bcd = gold.business_closure_days(biz, sd, cfg)
    assert bcd.groupBy("business_id", "day").count().filter("count > 1").count() == 0
    t = {r.business_id: r for r in bcd.groupBy("business_id").agg(
        F.sum("comp_model_a").alias("a"), F.sum("comp_model_b").alias("b")).collect()}
    assert float(t[1].a) == 2 * 25 * 100          # the brief's example shape: days * sqm * 100
    assert float(t[2].a) == 2 * 10_000            # cap
    assert float(t[3].a) == 2 * 3_000 and float(t[3].b) == 0
    assert float(t[4].a) == 2 * 1_000 and float(t[4].b) == 1_000   # only 2 Jan counts in model B


# ------------------------------------------------------------------ end to end vs an independent oracle
@pytest.mark.skipif(not os.path.isdir(os.path.join(ROOT, "output", "gold")), reason="run the pipeline first")
def test_pipeline_matches_independent_oracle():
    res = subprocess.run([sys.executable, "-I", os.path.join(ROOT, "tests", "independent_oracle.py")],
                         capture_output=True, text=True, check=True)
    oracle = json.loads(res.stdout.strip().splitlines()[-1])
    import pandas as pd
    g = pd.read_parquet(os.path.join(ROOT, "output", "gold", "compensation_business"))
    assert len(g) == oracle["businesses_affected"]
    assert float(g.total_model_a.sum()) == pytest.approx(oracle["total_model_a"], abs=0.01)
    assert float(g.total_model_b.sum()) == pytest.approx(oracle["total_model_b"], abs=0.01)
