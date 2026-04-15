-- =============================================================================
-- EMIS DW Audit Schema: Extend dq_check_log for Multi-Mart Support
-- Run once before the enrolment mart ETL.
-- Adds mart, check_sql, result, row_count, severity, run_date, notes columns.
-- The original run_id FK is preserved for backwards compatibility.
-- =============================================================================

-- Add new columns if they don't already exist
ALTER TABLE dw_audit.dq_check_log
    ADD COLUMN IF NOT EXISTS run_date   DATE         DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS mart       VARCHAR(50)  DEFAULT 'SCHOOLS',
    ADD COLUMN IF NOT EXISTS check_sql  TEXT,
    ADD COLUMN IF NOT EXISTS result     VARCHAR(20),
    ADD COLUMN IF NOT EXISTS row_count  BIGINT       DEFAULT 0,
    ADD COLUMN IF NOT EXISTS severity   VARCHAR(20)  DEFAULT 'WARNING',
    ADD COLUMN IF NOT EXISTS notes      TEXT;

-- Back-fill result from existing status column
UPDATE dw_audit.dq_check_log
SET result = status
WHERE result IS NULL AND status IS NOT NULL;

-- Make run_id optional (enrolment ETL writes without a run_id per-check)
ALTER TABLE dw_audit.dq_check_log
    ALTER COLUMN run_id DROP NOT NULL;

COMMENT ON COLUMN dw_audit.dq_check_log.mart      IS 'Data mart this check applies to: SCHOOLS, ENROLMENT, TEACHERS etc.';
COMMENT ON COLUMN dw_audit.dq_check_log.check_sql IS 'Brief description of the SQL logic used in this check.';
COMMENT ON COLUMN dw_audit.dq_check_log.result    IS 'PASS | WARNING | FAIL | INFO';
COMMENT ON COLUMN dw_audit.dq_check_log.row_count IS 'Number of rows that triggered this check result.';
COMMENT ON COLUMN dw_audit.dq_check_log.severity  IS 'CRITICAL | WARNING | INFO';
COMMENT ON COLUMN dw_audit.dq_check_log.run_date  IS 'Calendar date of the ETL run.';
COMMENT ON COLUMN dw_audit.dq_check_log.notes     IS 'Human-readable explanation of what this check verifies.';

-- Add watermark entries for enrolment sources
INSERT INTO dw_audit.etl_watermark (source_name, last_success_ts, last_run_id)
VALUES
    ('src.enrolments',            NULL, NULL),
    ('src.learners',              NULL, NULL),
    ('src.learner_disabilities',  NULL, NULL),
    ('src.learner_promotions',    NULL, NULL),
    ('src.learner_transitions',   NULL, NULL)
ON CONFLICT (source_name) DO NOTHING;
