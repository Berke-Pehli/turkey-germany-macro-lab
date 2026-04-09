-- -----------------------------------------------------------------------------
-- File: 01_create_schema.sql
-- Purpose:
--     Create the dedicated PostgreSQL schema for the macro lab project.
--
-- Notes:
--     All project database objects will live under the `macro_lab` schema
--     instead of the default `public` schema.
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS macro_lab;