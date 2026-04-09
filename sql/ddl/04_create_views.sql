-- -----------------------------------------------------------------------------
-- File: 04_create_views.sql
-- Purpose:
--     Create reusable analytical and reporting views for the macro lab project.
--
-- Views created:
--     - macro_lab.vw_macro_monthly_panel
--     - macro_lab.vw_latest_macro_snapshot
--     - macro_lab.vw_forecast_comparison
--     - macro_lab.vw_data_availability
--
-- Notes:
--     These views support SQL analysis, export workflows, validation,
--     and reporting layers.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Monthly macro panel
-- Provides a wide monthly panel for core indicators by entity and month.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW macro_lab.vw_macro_monthly_panel AS
SELECT
    c.country_code,
    c.country_name,
    f.observation_date AS year_month,
    MAX(CASE WHEN i.indicator_code = 'inflation_yoy' THEN f.observation_value END) AS inflation_yoy,
    MAX(CASE WHEN i.indicator_code = 'unemployment_rate' THEN f.observation_value END) AS unemployment_rate,
    MAX(CASE WHEN i.indicator_code = 'industrial_production_index' THEN f.observation_value END) AS industrial_production_index,
    MAX(CASE WHEN i.indicator_code = 'policy_rate' THEN f.observation_value END) AS policy_rate,
    MAX(CASE WHEN i.indicator_code = 'try_eur_eom' THEN f.observation_value END) AS try_eur_eom,
    MAX(CASE WHEN i.indicator_code = 'try_usd_eom' THEN f.observation_value END) AS try_usd_eom,
    MAX(CASE WHEN i.indicator_code = 'sentiment_index' THEN f.observation_value END) AS sentiment_index
FROM macro_lab.fact_macro_observation f
JOIN macro_lab.dim_country c
    ON f.country_id = c.country_id
JOIN macro_lab.dim_indicator i
    ON f.indicator_id = i.indicator_id
WHERE f.frequency_code = 'M'
GROUP BY
    c.country_code,
    c.country_name,
    f.observation_date;

-- -----------------------------------------------------------------------------
-- Latest macro snapshot
-- Returns the latest available monthly row for each entity.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW macro_lab.vw_latest_macro_snapshot AS
WITH ranked_snapshot AS (
    SELECT
        v.*,
        ROW_NUMBER() OVER (
            PARTITION BY v.country_code
            ORDER BY v.year_month DESC
        ) AS row_num
    FROM macro_lab.vw_macro_monthly_panel v
)
SELECT
    country_code,
    country_name,
    year_month,
    inflation_yoy,
    unemployment_rate,
    industrial_production_index,
    policy_rate,
    try_eur_eom,
    try_usd_eom,
    sentiment_index
FROM ranked_snapshot
WHERE row_num = 1;

-- -----------------------------------------------------------------------------
-- Forecast comparison
-- Joins forecast outputs with run metadata for model comparison and reporting.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW macro_lab.vw_forecast_comparison AS
SELECT
    fr.forecast_result_id,
    mr.run_id,
    mr.run_label,
    mr.model_name,
    mr.task_type,
    mr.target_name,
    mr.forecast_horizon_months,
    mr.country_code,
    mr.backtest_type,
    fr.prediction_date,
    fr.actual_value,
    fr.predicted_value,
    fr.prediction_interval_lower,
    fr.prediction_interval_upper,
    fr.class_actual,
    fr.class_predicted,
    fr.set_type,
    mr.run_timestamp
FROM macro_lab.fact_forecast_result fr
JOIN macro_lab.model_run_log mr
    ON fr.run_id = mr.run_id;

-- -----------------------------------------------------------------------------
-- Data availability view
-- Summarizes coverage and row counts by entity and indicator.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW macro_lab.vw_data_availability AS
SELECT
    c.country_code,
    c.country_name,
    i.indicator_code,
    i.indicator_name,
    MIN(f.observation_date) AS first_observation_date,
    MAX(f.observation_date) AS last_observation_date,
    COUNT(*) AS observation_count
FROM macro_lab.fact_macro_observation f
JOIN macro_lab.dim_country c
    ON f.country_id = c.country_id
JOIN macro_lab.dim_indicator i
    ON f.indicator_id = i.indicator_id
GROUP BY
    c.country_code,
    c.country_name,
    i.indicator_code,
    i.indicator_name;