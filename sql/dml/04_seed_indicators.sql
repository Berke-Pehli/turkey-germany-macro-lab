-- -----------------------------------------------------------------------------
-- File: 04_seed_indicators.sql
-- Purpose:
--     Seed core macro indicator metadata for the macro lab project.
-- -----------------------------------------------------------------------------

INSERT INTO macro_lab.dim_indicator (
    indicator_code,
    indicator_name,
    source_id,
    default_frequency_code,
    unit,
    category,
    transformation_note
)
VALUES
    (
        'inflation_yoy',
        'Inflation Year-over-Year',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'EUROSTAT'),
        'M',
        'percent',
        'inflation',
        'Used for Germany and euro area directly; Türkiye mapping now comes from OECD ingestion.'
    ),
    (
        'unemployment_rate',
        'Unemployment Rate',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'EUROSTAT'),
        'M',
        'percent',
        'labor',
        'Monthly unemployment rate.'
    ),
    (
        'industrial_production_index',
        'Industrial Production Index',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'EUROSTAT'),
        'M',
        'index',
        'activity',
        'Monthly industrial production index.'
    ),
    (
        'policy_rate',
        'Policy Rate',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'ECB'),
        'M',
        'percent',
        'policy_rate',
        'Stored as monthly end-of-period policy stance after transformation.'
    ),
    (
        'try_eur_eom',
        'TRY per EUR End-of-Month',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'EVDS'),
        'M',
        'exchange_rate',
        'exchange_rate',
        'Monthly end-of-month TRY/EUR exchange rate.'
    ),
    (
        'try_usd_eom',
        'TRY per USD End-of-Month',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'EVDS'),
        'M',
        'exchange_rate',
        'exchange_rate',
        'Monthly end-of-month TRY/USD exchange rate.'
    ),
    (
        'sentiment_index',
        'Sentiment Index',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'OECD'),
        'M',
        'index',
        'sentiment',
        'Monthly business confidence indicator from OECD.'
    ),
    (
        'consumer_confidence_index',
        'Consumer Confidence Index',
        (SELECT source_id FROM macro_lab.dim_source WHERE source_code = 'OECD'),
        'M',
        'index',
        'sentiment',
        'Monthly consumer confidence indicator from OECD.'
    )
ON CONFLICT (indicator_code) DO NOTHING;