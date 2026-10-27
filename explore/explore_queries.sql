-- Companion to explore/silver_step_by_step and explore/gold_step_by_step.
-- Run the notebooks with persist = yes first; every step is then a table in workspace.tlv_explore.
-- Open this file in the SQL editor and run one query at a time, next to the notebook.

-- ============================================================ SILVER · closures
-- S01 → S02: the raw CSV strings and the typed values, for the traced closures
SELECT ID, tr_from, tr_to, me_k_rechov, me_shem_rechov, ad_k_rechov, ad_shem_rechov, id_rechov, shaot, sw_laila
FROM workspace.tlv_explore.s01_bronze_closures WHERE ID IN ('1511', '2', '846');

SELECT closure_id, segment_id, start_ts, end_ts, street_a_code, street_a, street_b_code, street_b, hours_text
FROM workspace.tlv_explore.s02_typed WHERE closure_id IN (1511, 2, 846);

-- S03: rows where a street code is 0 (the name becomes NULL)
SELECT closure_id, street_a_code, street_a, street_b_code, street_b
FROM workspace.tlv_explore.s03_typed_no_zero_streets
WHERE street_a_code = 0 OR street_b_code = 0 LIMIT 20;

-- S04: how the hours text turns into the Model B factor
SELECT trading_hours_factor, COUNT(*) AS closures, MIN(hours_text) AS example
FROM workspace.tlv_explore.s04_clean GROUP BY trading_hours_factor ORDER BY closures DESC;

-- S05: why rows are rejected
SELECT COALESCE(reject_reason, '(valid)') AS reason, COUNT(*) FROM workspace.tlv_explore.s05_flagged GROUP BY 1;

-- S06: a true duplicate (1762/1786) vs same streets on a different segment (603/68)
SELECT closure_id, segment_id, street_a, street_b, start_date, end_date, _rn, duplicate_of
FROM workspace.tlv_explore.s06_ranked WHERE closure_id IN (1762, 1786, 603, 68) ORDER BY start_date, closure_id;

-- S07: every input row accounted for
SELECT (SELECT COUNT(*) FROM workspace.tlv_explore.s07_street_closures)          AS clean,
       (SELECT COUNT(*) FROM workspace.tlv_explore.s07_street_closures_rejected) AS rejected,
       (SELECT COUNT(*) FROM workspace.tlv_explore.s01_bronze_closures)          AS input_rows;

-- S08: one closure, two street rows (the bounding cross-streets)
SELECT closure_id, street_role, street_key, start_date, end_date, trading_hours_factor, hours_text
FROM workspace.tlv_explore.s08_closure_streets WHERE closure_id IN (1511, 2) ORDER BY closure_id, street_role;

-- ============================================================ SILVER · businesses
-- S09 → S10: raw struct vs flattened columns
SELECT attributes.id_esek, attributes.shimush, attributes.shem_rechov, attributes.tr_hakama, attributes.shetach, attributes.date_import
FROM workspace.tlv_explore.s09_bronze_businesses WHERE attributes.id_esek IN (68, 1011212, 1063304, 1018439);

SELECT business_id, category_he, street_name, street_key, established_date, area_sqm, import_ts, _run_id
FROM workspace.tlv_explore.s10_flat WHERE business_id IN (68, 1011212, 1063304, 1018439);

-- S11: versions per business (all 1 with a single download)
SELECT _rn, COUNT(*) FROM workspace.tlv_explore.s11_ranked_businesses GROUP BY _rn;

-- S12 → S13: eligibility after the category join
SELECT model_b_eligible, exclusion_group, COUNT(*) AS businesses
FROM workspace.tlv_explore.s13_businesses GROUP BY ALL ORDER BY businesses DESC;

-- ============================================================ GOLD
-- G02: closures overlapping 2023 that started earlier (a start-year filter would drop them)
SELECT DISTINCT closure_id, closure_type, start_date, end_date
FROM workspace.tlv_explore.g02_in_year WHERE start_date < DATE'2023-01-01' ORDER BY start_date;

-- G03: one row per closure-street per day, clipped to 2023
SELECT closure_id, street_key, COUNT(*) AS days, MIN(day) AS first_day, MAX(day) AS last_day
FROM workspace.tlv_explore.g03_days WHERE closure_id IN (1511, 2, 846) GROUP BY ALL;

-- G03 → G04: seven closures on Ibn Gabirol on 24 Feb 2023 become one street-day
SELECT closure_id, closure_type, trading_hours_factor FROM workspace.tlv_explore.g03_days
WHERE street_key = 'אבן גבירול' AND day = DATE'2023-02-24';
SELECT * FROM workspace.tlv_explore.g04_street_days WHERE street_key = 'אבן גבירול' AND day = DATE'2023-02-24';

-- G05: the daily amount for each traced business, first affected day
SELECT business_id, street_name, category_en, area_sqm, 100 * area_sqm AS uncapped, cap_binding, comp_model_a,
       model_b_eligible, established_date, day, existed_on_day, trading_hours_factor, comp_model_b
FROM workspace.tlv_explore.g05_business_closure_days
WHERE business_id IN (68, 1011212, 1063304, 1018439)
QUALIFY ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY day) = 1;

-- G05: Model B switches on the day business 1063304 was established
SELECT day, established_date, existed_on_day, comp_model_a, comp_model_b
FROM workspace.tlv_explore.g05_business_closure_days
WHERE business_id = 1063304 AND day BETWEEN DATE'2023-06-03' AND DATE'2023-06-09' ORDER BY day;

-- G05 → G06: summing the days gives the per-business row (recompute it by hand)
SELECT business_id, COUNT(*) AS days, SUM(comp_model_a) AS a, SUM(comp_model_b) AS b
FROM workspace.tlv_explore.g05_business_closure_days WHERE business_id IN (68, 1011212, 1063304, 1018439) GROUP BY 1;
SELECT business_id, days_affected, total_model_a, total_model_b
FROM workspace.tlv.gold_compensation_business WHERE business_id IN (68, 1011212, 1063304, 1018439);

-- G07: Allenby and Ibn Gabirol in the per-street output
SELECT * FROM workspace.tlv_explore.g07_compensation_street WHERE street_key IN ('אלנבי', 'אבן גבירול');

-- Clean up when finished (removes only the exploration copies, never the pipeline tables)
-- DROP SCHEMA workspace.tlv_explore CASCADE;
