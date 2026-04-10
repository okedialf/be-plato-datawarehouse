-- =============================================================================
-- EMIS Data Warehouse: Audit / Control Tables
-- Schema: dw_audit
-- Run once on initial setup.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw_audit.etl_run
--    One row per nightly ETL pipeline execution.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_audit.etl_run (
    run_id          SERIAL                      NOT NULL,
    pipeline        VARCHAR(100)                NOT NULL,
    as_of_date      DATE                        NOT NULL,
    start_ts        TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    end_ts          TIMESTAMP WITH TIME ZONE,
    status          VARCHAR(20)                 NOT NULL DEFAULT 'RUNNING',
    row_counts_json JSONB,
    error_message   TEXT,
    CONSTRAINT etl_run_pkey PRIMARY KEY (run_id)
);
COMMENT ON TABLE  dw_audit.etl_run               IS 'One row per ETL pipeline run. Status: RUNNING, SUCCESS, FAILED.';
COMMENT ON COLUMN dw_audit.etl_run.pipeline      IS 'Pipeline name e.g. nightly_schools_etl';
COMMENT ON COLUMN dw_audit.etl_run.as_of_date    IS 'The business date this run processes (usually CURRENT_DATE)';
COMMENT ON COLUMN dw_audit.etl_run.row_counts_json IS 'JSON object with row counts per step e.g. {"schools_raw":60000,"schools_dim_inserted":12}';
COMMENT ON COLUMN dw_audit.etl_run.status        IS 'RUNNING | SUCCESS | FAILED';


-- -----------------------------------------------------------------------------
-- 2. dw_audit.etl_step_log
--    One row per step within a pipeline run. Enables granular debugging.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_audit.etl_step_log (
    step_log_id  SERIAL                    NOT NULL,
    run_id       INTEGER                   NOT NULL,
    step_name    VARCHAR(100)              NOT NULL,
    start_ts     TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    end_ts       TIMESTAMP WITH TIME ZONE,
    status       VARCHAR(20)               NOT NULL DEFAULT 'RUNNING',
    rows_affected INTEGER,
    message      TEXT,
    CONSTRAINT etl_step_log_pkey    PRIMARY KEY (step_log_id),
    CONSTRAINT etl_step_log_run_fk  FOREIGN KEY (run_id) REFERENCES dw_audit.etl_run (run_id)
);
COMMENT ON TABLE  dw_audit.etl_step_log              IS 'Granular step-level log for each ETL run.';
COMMENT ON COLUMN dw_audit.etl_step_log.step_name    IS 'e.g. extract_schools_raw, flatten_schools, scd2_schools_dim, load_school_fact';
COMMENT ON COLUMN dw_audit.etl_step_log.rows_affected IS 'Rows inserted/updated/deleted in this step';


-- -----------------------------------------------------------------------------
-- 3. dw_audit.etl_watermark
--    Tracks the last successful extraction timestamp per source table.
--    Used for incremental loads if/when full refresh is replaced.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_audit.etl_watermark (
    source_name         VARCHAR(100)              NOT NULL,
    last_success_ts     TIMESTAMP WITH TIME ZONE,
    last_run_id         INTEGER,
    CONSTRAINT etl_watermark_pkey PRIMARY KEY (source_name)
);
COMMENT ON TABLE  dw_audit.etl_watermark              IS 'Watermark table for incremental load support. Tracks last successful pull per source.';
COMMENT ON COLUMN dw_audit.etl_watermark.source_name  IS 'Source table name e.g. public.schools, public.school_location_details';


-- -----------------------------------------------------------------------------
-- 4. dw_audit.etl_error_log
--    Row-level errors captured during SCD2 or transformation steps.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_audit.etl_error_log (
    error_id      SERIAL                    NOT NULL,
    run_id        INTEGER                   NOT NULL,
    step_name     VARCHAR(100)              NOT NULL,
    source_id     BIGINT,
    error_message TEXT                      NOT NULL,
    payload_json  JSONB,
    logged_at     TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_error_log_pkey   PRIMARY KEY (error_id),
    CONSTRAINT etl_error_log_run_fk FOREIGN KEY (run_id) REFERENCES dw_audit.etl_run (run_id)
);
COMMENT ON TABLE  dw_audit.etl_error_log              IS 'Row-level error log. Each row is one rejected or failed record.';
COMMENT ON COLUMN dw_audit.etl_error_log.source_id    IS 'OLTP source id of the failing record, if applicable';
COMMENT ON COLUMN dw_audit.etl_error_log.payload_json IS 'JSON snapshot of the failing row for debugging';


-- -----------------------------------------------------------------------------
-- 5. dw_audit.dq_check_log
--    Results of data quality checks run after each ETL cycle.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw_audit.dq_check_log (
    dq_id        SERIAL                    NOT NULL,
    run_id       INTEGER                   NOT NULL,
    check_name   VARCHAR(200)              NOT NULL,
    status       VARCHAR(20)              NOT NULL,
    detail       TEXT,
    logged_at    TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    CONSTRAINT dq_check_log_pkey   PRIMARY KEY (dq_id),
    CONSTRAINT dq_check_log_run_fk FOREIGN KEY (run_id) REFERENCES dw_audit.etl_run (run_id)
);
COMMENT ON TABLE  dw_audit.dq_check_log            IS 'Data quality check results per ETL run.';
COMMENT ON COLUMN dw_audit.dq_check_log.check_name IS 'e.g. null_emis_numbers, schools_dim_no_current_row, fact_row_count_zero';
COMMENT ON COLUMN dw_audit.dq_check_log.status     IS 'PASS | WARN | FAIL';


-- Seed watermark rows for known sources
INSERT INTO dw_audit.etl_watermark (source_name, last_success_ts, last_run_id)
VALUES
    ('public.schools',                   NULL, NULL),
    ('public.school_location_details',   NULL, NULL),
    ('administrative_units.admin_units', NULL, NULL)
ON CONFLICT (source_name) DO NOTHING;
