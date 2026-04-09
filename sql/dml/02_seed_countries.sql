-- -----------------------------------------------------------------------------
-- File: 02_seed_countries.sql
-- Purpose:
--     Seed countries/entities used in the macro lab project.
-- -----------------------------------------------------------------------------

INSERT INTO macro_lab.dim_country (
    country_code,
    country_name,
    entity_type
)
VALUES
    ('TR', 'Türkiye', 'country'),
    ('DE', 'Germany', 'country'),
    ('EA', 'Euro Area', 'region')
ON CONFLICT (country_code) DO NOTHING;