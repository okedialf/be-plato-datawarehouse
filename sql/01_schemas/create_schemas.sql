-- =============================================================================
-- EMIS Data Warehouse: Schema Creation
-- Creates the three core schemas: stg, dw, dw_audit
-- Run this once on a fresh Cloud SQL PostgreSQL instance.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS stg;
COMMENT ON SCHEMA stg IS 'Staging schema: raw extracts and flattened tables before DW load';

CREATE SCHEMA IF NOT EXISTS dw;
COMMENT ON SCHEMA dw IS 'Data Warehouse schema: conformed SCD2 dimensions and fact tables';

CREATE SCHEMA IF NOT EXISTS dw_audit;
COMMENT ON SCHEMA dw_audit IS 'Audit schema: ETL run logs, step logs, error logs, row counts';
