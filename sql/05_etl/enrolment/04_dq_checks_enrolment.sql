-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 4 — Data Quality Checks
-- Writes results to dw_audit.dq_check_log.
-- status column mirrors result for backwards compatibility with original schema.
-- =============================================================================

BEGIN;

-- ── DQ-1: Orphaned fact rows (learner_id FK broken) ───────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-1: Fact orphaned learner_id',
    'enrolment_fact.learner_id NOT IN learner_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'enrolment_fact rows with no matching learner_dim row'
FROM dw.enrolment_fact ef
WHERE NOT EXISTS (SELECT 1 FROM dw.learner_dim ld WHERE ld.id = ef.learner_id);

-- ── DQ-2: Orphaned fact rows (school_id FK broken) ────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-2: Fact orphaned school_id',
    'enrolment_fact.school_id NOT IN schools_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'enrolment_fact rows with no matching schools_dim row'
FROM dw.enrolment_fact ef
WHERE NOT EXISTS (SELECT 1 FROM dw.schools_dim sd WHERE sd.id = ef.school_id);

-- ── DQ-3: Orphaned fact rows (date_id FK broken) ──────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-3: Fact orphaned date_id',
    'enrolment_fact.date_id NOT IN date_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'enrolment_fact rows with no matching date_dim row'
FROM dw.enrolment_fact ef
WHERE NOT EXISTS (SELECT 1 FROM dw.date_dim dd WHERE dd.id = ef.date_id);

-- ── DQ-4: learner_dim — no duplicate current rows per learner ─────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-4: learner_dim duplicate current rows',
    'COUNT(*) > 1 per source_id WHERE is_current=TRUE',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'learners with more than one is_current=TRUE row in learner_dim'
FROM (
    SELECT source_id FROM dw.learner_dim
    WHERE is_current = TRUE
    GROUP BY source_id HAVING COUNT(*) > 1
) dup;

-- ── DQ-5: NULL grade_id ───────────────────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-5: NULL grade_id in enrolment_fact',
    'enrolment_fact.grade_id IS NULL',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Enrolments where grade could not be resolved'
FROM dw.enrolment_fact WHERE grade_id IS NULL;

-- ── DQ-6: NULL enrolment_type_id ─────────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-6: NULL enrolment_type_id in enrolment_fact',
    'enrolment_fact.enrolment_type_id IS NULL',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Enrolments where type could not be resolved from staging'
FROM dw.enrolment_fact WHERE enrolment_type_id IS NULL;

-- ── DQ-7: Learners without NIN ───────────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-7: Learners without NIN',
    'learner_dim.nin IS NULL WHERE is_current=TRUE',
    'INFO', 'INFO',
    COUNT(*), 'INFO',
    'Current learners without a National ID Number recorded'
FROM dw.learner_dim WHERE is_current = TRUE AND nin IS NULL;

-- ── DQ-8: Enrolment count vs staging count ────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-8: Enrolment fact vs staging count',
    'COUNT(enrolment_fact) vs COUNT(enrolments_flat active)',
    CASE WHEN ABS(fact_count - flat_count)::FLOAT / NULLIF(flat_count,0) < 0.01
         THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN ABS(fact_count - flat_count)::FLOAT / NULLIF(flat_count,0) < 0.01
         THEN 'PASS' ELSE 'WARNING' END,
    ABS(fact_count - flat_count), 'WARNING',
    'More than 1% difference between staging active rows and loaded fact rows'
FROM (
    SELECT
        (SELECT COUNT(*) FROM dw.enrolment_fact)                           AS fact_count,
        (SELECT COUNT(*) FROM stg.enrolments_flat WHERE is_active = TRUE)  AS flat_count
) counts;

-- ── DQ-9: SCD2 version gaps ───────────────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'ENROLMENT', 'DQ-9: learner_dim SCD2 version gaps',
    'Gap between expiration_date+1 and next effective_date',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Learner dim version history gaps'
FROM (
    SELECT a.source_id
    FROM dw.learner_dim a
    JOIN dw.learner_dim b
        ON  b.source_id      = a.source_id
        AND b.effective_date > a.effective_date
        AND NOT EXISTS (
            SELECT 1 FROM dw.learner_dim c
            WHERE c.source_id      = a.source_id
              AND c.effective_date  > a.effective_date
              AND c.effective_date  < b.effective_date
        )
    WHERE a.expiration_date + 1 <> b.effective_date
) gaps;

-- ── Summary ───────────────────────────────────────────────────────────────────
DO $$
DECLARE
    v_pass INTEGER; v_warn INTEGER; v_fail INTEGER; v_info INTEGER;
BEGIN
    SELECT
        COUNT(*) FILTER (WHERE result = 'PASS'),
        COUNT(*) FILTER (WHERE result = 'WARNING'),
        COUNT(*) FILTER (WHERE result = 'FAIL'),
        COUNT(*) FILTER (WHERE result = 'INFO')
    INTO v_pass, v_warn, v_fail, v_info
    FROM dw_audit.dq_check_log
    WHERE run_date = CURRENT_DATE AND mart = 'ENROLMENT';

    RAISE NOTICE 'Enrolment DQ: PASS=% | WARNING=% | FAIL=% | INFO=%',
        v_pass, v_warn, v_fail, v_info;

    IF v_fail > 0 THEN
        RAISE EXCEPTION 'Enrolment DQ: % CRITICAL check(s) failed.', v_fail;
    END IF;
END;
$$;

COMMIT;
