-- -----------------------------------------------------------------------------
-- File: 03_create_facts.sql
-- Purpose:
--     Create fact and log tables for macro observations, engineered features,
--     forecast results, model runs, and pipeline refresh runs.
--
-- Tables created:
--     - macro_lab.fact_macro_observation
--     - macro_lab.fact_feature_snapshot
--     - macro_lab.model_run_log
--     - macro_lab.fact_forecast_result
--     - macro_lab.pipeline_run_log
--
-- Notes:
--     These tables support the core data pipeline, modeling workflow,
--     reproducibility, and reporting layers of the macro lab project.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Raw normalized macro observations
-- Stores one observation per country/entity, indicator, frequency, and date.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.fact_macro_observation (
    observation_id BIGSERIAL PRIMARY KEY,
    country_id SMALLINT NOT NULL REFERENCES macro_lab.dim_country(country_id),
    indicator_id INTEGER NOT NULL REFERENCES macro_lab.dim_indicator(indicator_id),
    source_id SMALLINT NOT NULL REFERENCES macro_lab.dim_source(source_id),
    frequency_code TEXT NOT NULL REFERENCES macro_lab.dim_frequency(frequency_code),
    observation_date DATE NOT NULL,
    observation_value NUMERIC(18, 6) NOT NULL,
    value_status TEXT,
    is_preliminary BOOLEAN NOT NULL DEFAULT FALSE,
    retrieved_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_macro_observation UNIQUE (
        country_id,
        indicator_id,
        source_id,
        frequency_code,
        observation_date
    )
);

-- -----------------------------------------------------------------------------
-- Engineered feature snapshot table
-- Stores feature values by month for traceability and debugging.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.fact_feature_snapshot (
    feature_snapshot_id BIGSERIAL PRIMARY KEY,
    country_id SMALLINT NOT NULL REFERENCES macro_lab.dim_country(country_id),
    year_month DATE NOT NULL,
    feature_set_name TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_value NUMERIC(18, 6),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_feature_snapshot UNIQUE (
        country_id,
        year_month,
        feature_set_name,
        feature_name
    )
);

-- -----------------------------------------------------------------------------
-- Model run log
-- Stores metadata about individual model runs, including target, horizon,
-- split windows, and backtesting style.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.model_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    model_name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    target_name TEXT NOT NULL,
    forecast_horizon_months INTEGER NOT NULL CHECK (forecast_horizon_months > 0),
    country_code TEXT NOT NULL,
    train_start_date DATE,
    train_end_date DATE,
    validation_start_date DATE,
    validation_end_date DATE,
    test_start_date DATE,
    test_end_date DATE,
    backtest_type TEXT,
    notes TEXT,
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Forecast result table
-- Stores actual and predicted values for regression and classification tasks.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.fact_forecast_result (
    forecast_result_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES macro_lab.model_run_log(run_id),
    country_id SMALLINT NOT NULL REFERENCES macro_lab.dim_country(country_id),
    target_name TEXT NOT NULL,
    forecast_horizon_months INTEGER NOT NULL CHECK (forecast_horizon_months > 0),
    prediction_date DATE NOT NULL,
    actual_value NUMERIC(18, 6),
    predicted_value NUMERIC(18, 6),
    prediction_interval_lower NUMERIC(18, 6),
    prediction_interval_upper NUMERIC(18, 6),
    class_actual INTEGER,
    class_predicted INTEGER,
    set_type TEXT NOT NULL CHECK (
        set_type IN ('train', 'validation', 'test', 'backtest')
    ),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Pipeline run log
-- Stores metadata about end-to-end pipeline refresh runs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.pipeline_run_log (
    pipeline_run_id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    run_started_at TIMESTAMP NOT NULL,
    run_finished_at TIMESTAMP,
    run_status TEXT NOT NULL,
    triggered_by TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (run_status IN ('started', 'success', 'failed'))
);