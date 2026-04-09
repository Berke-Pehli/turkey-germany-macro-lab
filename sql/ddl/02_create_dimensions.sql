-- -----------------------------------------------------------------------------
-- File: 02_create_dimensions.sql
-- Purpose:
--     Create dimension tables for sources, countries/entities, frequencies,
--     and indicators used in the macro lab project.
--
-- Tables created:
--     - macro_lab.dim_source
--     - macro_lab.dim_country
--     - macro_lab.dim_frequency
--     - macro_lab.dim_indicator
--
-- Notes:
--     These dimension tables provide the metadata foundation for the raw
--     observation and modeling layers.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Source dimension
-- Stores metadata about data providers such as Eurostat, ECB, and EVDS.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.dim_source (
    source_id SMALLSERIAL PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    base_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Country/entity dimension
-- Stores project entities such as Türkiye, Germany, and the Euro Area.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.dim_country (
    country_id SMALLSERIAL PRIMARY KEY,
    country_code TEXT NOT NULL UNIQUE,
    country_name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Frequency dimension
-- Standardizes supported data frequencies.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.dim_frequency (
    frequency_code TEXT PRIMARY KEY,
    frequency_name TEXT NOT NULL,
    description TEXT
);

-- -----------------------------------------------------------------------------
-- Indicator dimension
-- Stores metadata about indicators such as inflation, unemployment,
-- industrial production, policy rates, exchange rates, and sentiment.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS macro_lab.dim_indicator (
    indicator_id SERIAL PRIMARY KEY,
    indicator_code TEXT NOT NULL UNIQUE,
    indicator_name TEXT NOT NULL,
    source_id SMALLINT NOT NULL REFERENCES macro_lab.dim_source(source_id),
    default_frequency_code TEXT NOT NULL REFERENCES macro_lab.dim_frequency(frequency_code),
    unit TEXT,
    category TEXT NOT NULL,
    transformation_note TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);