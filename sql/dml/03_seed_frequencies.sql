-- -----------------------------------------------------------------------------
-- File: 03_seed_frequencies.sql
-- Purpose:
--     Seed supported data frequencies.
-- -----------------------------------------------------------------------------

INSERT INTO macro_lab.dim_frequency (
    frequency_code,
    frequency_name,
    description
)
VALUES
    ('D', 'Daily', 'Daily frequency'),
    ('M', 'Monthly', 'Monthly frequency')
ON CONFLICT (frequency_code) DO NOTHING;