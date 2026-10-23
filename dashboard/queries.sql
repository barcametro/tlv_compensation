-- Databricks AI/BI dashboard datasets. Replace workspace.tlv with your catalog.schema.

-- KPI tiles
SELECT COUNT(*)                                   AS businesses_affected,
       SUM(total_model_a)                         AS total_model_a_ils,
       SUM(total_model_b)                         AS total_model_b_ils,
       ROUND(100 * SUM(total_model_b) / SUM(total_model_a), 1) AS model_b_pct_of_a,
       ROUND(SUM(total_model_a) / SUM(area_sqm), 0)            AS cost_per_sqm_model_a
FROM workspace.tlv.gold_compensation_business;

-- Output 1: annual compensation per business
SELECT business_id, street_name, house_number, category_en, area_sqm, days_affected,
       total_model_a, total_model_b, cost_per_sqm_model_a, cap_binding
FROM workspace.tlv.gold_compensation_business
ORDER BY total_model_a DESC;

-- Output 2: annual compensation per street
SELECT street_name, closed_days, businesses_affected, total_area_sqm,
       total_model_a, total_model_b, cost_per_sqm_model_a, businesses_at_cap
FROM workspace.tlv.gold_compensation_street
ORDER BY total_model_a DESC;

-- Monthly trend, both models
SELECT month, total_model_a, total_model_b, businesses_affected, streets_closed
FROM workspace.tlv.gold_compensation_month ORDER BY month;

-- By closure type
SELECT primary_closure_type, streets, business_days, total_model_a, total_model_b
FROM workspace.tlv.gold_compensation_closure_type ORDER BY total_model_a DESC;

-- Where Model A pays non-trading premises
SELECT exclusion_group, SUM(businesses) AS businesses, SUM(total_model_a) AS total_model_a
FROM workspace.tlv.gold_compensation_category
WHERE NOT model_b_eligible GROUP BY exclusion_group ORDER BY total_model_a DESC;

-- Businesses compensated for 2023 that did not exist yet
SELECT COUNT(*) AS businesses, SUM(total_model_a) AS total_model_a
FROM workspace.tlv.gold_compensation_business
WHERE established_date > DATE'2023-12-31';

-- Data quality status of the latest run
SELECT `check`, layer, observed, expected, status, detail
FROM workspace.tlv.gold_dq_results ORDER BY status DESC, layer;
