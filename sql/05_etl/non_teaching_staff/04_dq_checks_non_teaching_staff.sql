-- =============================================================================
-- EMIS Non-Teaching Staff Mart ETL: Step 4 — Data Quality Checks
-- =============================================================================

BEGIN;

-- ── DQ-1: Orphaned fact rows (staff_id FK broken) ────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-1: Fact orphaned staff_id',
    'non_teaching_staff_fact.staff_id NOT IN non_teaching_staff_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'non_teaching_staff_fact rows with no matching dim row'
FROM dw.non_teaching_staff_fact f
WHERE NOT EXISTS (SELECT 1 FROM dw.non_teaching_staff_dim d WHERE d.id = f.staff_id);

-- ── DQ-2: Orphaned fact rows (school_id FK broken) ───────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-2: Fact orphaned school_id',
    'non_teaching_staff_fact.school_id NOT IN schools_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'non_teaching_staff_fact rows with no matching schools_dim row'
FROM dw.non_teaching_staff_fact f
WHERE NOT EXISTS (SELECT 1 FROM dw.schools_dim sd WHERE sd.id = f.school_id);

-- ── DQ-3: Orphaned fact rows (date_id FK broken) ─────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-3: Fact orphaned date_id',
    'non_teaching_staff_fact.date_id NOT IN date_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'non_teaching_staff_fact rows with no matching date_dim row'
FROM dw.non_teaching_staff_fact f
WHERE NOT EXISTS (SELECT 1 FROM dw.date_dim dd WHERE dd.id = f.date_id);

-- ── DQ-4: Duplicate current rows in dim ──────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-4: dim duplicate current rows',
    'COUNT(*) > 1 per source_id WHERE is_current=TRUE',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'Staff with more than one is_current=TRUE row in non_teaching_staff_dim'
FROM (
    SELECT source_id FROM dw.non_teaching_staff_dim
    WHERE is_current = TRUE
    GROUP BY source_id HAVING COUNT(*) > 1
) dup;

-- ── DQ-5: Staff with no school deployment ────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-5: Staff in dim with no fact rows',
    'non_teaching_staff_dim current rows with no matching fact',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Current staff in dim with no school deployment in fact'
FROM dw.non_teaching_staff_dim d
WHERE d.is_current = TRUE
  AND NOT EXISTS (SELECT 1 FROM dw.non_teaching_staff_fact f WHERE f.staff_id = d.id);

-- ── DQ-6: Staff with no category ─────────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-6: Staff with no category',
    'non_teaching_staff_dim.category IS NULL WHERE is_current=TRUE',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Current staff with no category recorded (ADMINISTRATIVE/SUPPORT)'
FROM dw.non_teaching_staff_dim
WHERE is_current = TRUE AND category IS NULL;

-- ── DQ-7: Staff with no role ─────────────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-7: Staff with no role',
    'non_teaching_staff_dim.role IS NULL WHERE is_current=TRUE',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Current staff with no specific role recorded (BURSAR, COOK etc.)'
FROM dw.non_teaching_staff_dim
WHERE is_current = TRUE AND role IS NULL;

-- ── DQ-8: Fact vs flat count variance ────────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'NON_TEACHING_STAFF', 'DQ-8: Fact vs staging count',
    'ABS(fact - flat) / flat < 1%',
    CASE WHEN ABS(fact_count - flat_count)::FLOAT / NULLIF(flat_count,0) < 0.01
         THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN ABS(fact_count - flat_count)::FLOAT / NULLIF(flat_count,0) < 0.01
         THEN 'PASS' ELSE 'WARNING' END,
    ABS(fact_count - flat_count), 'WARNING',
    'More than 1% difference between staging rows and loaded fact rows'
FROM (
    SELECT
        (SELECT COUNT(*) FROM dw.non_teaching_staff_fact)      AS fact_count,
        (SELECT COUNT(*) FROM stg.non_teaching_staff_flat)     AS flat_count
) counts;

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
    WHERE run_date = CURRENT_DATE AND mart = 'NON_TEACHING_STAFF';

    RAISE NOTICE 'Non-Teaching Staff DQ: PASS=% | WARNING=% | FAIL=% | INFO=%',
        v_pass, v_warn, v_fail, v_info;

    IF v_fail > 0 THEN
        RAISE EXCEPTION 'Non-Teaching Staff DQ: % CRITICAL check(s) failed.', v_fail;
    END IF;
END;
$$;

COMMIT;
