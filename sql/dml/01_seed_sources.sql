-- -----------------------------------------------------------------------------
-- File: 01_seed_sources.sql
-- Purpose:
--     Seed data sources used in the macro lab project.
-- -----------------------------------------------------------------------------

INSERT INTO macro_lab.dim_source (
    source_code,
    source_name,
    source_type,
    base_url
)
VALUES
    ('EUROSTAT', 'Eurostat', 'api', 'https://ec.europa.eu/eurostat'),
    ('ECB', 'European Central Bank', 'api', 'https://data.ecb.europa.eu'),
    ('EVDS', 'Central Bank of the Republic of Türkiye EVDS', 'api', 'https://evds2.tcmb.gov.tr'),
    ('CBRT', 'Central Bank of the Republic of Türkiye', 'api', 'https://www.tcmb.gov.tr'),
    ('OECD', 'Organisation for Economic Co-operation and Development', 'api', 'https://sdmx.oecd.org')
ON CONFLICT (source_code) DO NOTHING;