-- =============================================================================
-- EMIS Teachers Mart ETL: Step 4 — Data Quality Checks
-- Writes results to dw_audit.dq_check_log.
-- status column mirrors result (NOT NULL constraint on original schema).
-- =============================================================================

BEGIN;

-- ── DQ-1: Orphaned hr_fact rows (teacher_id FK broken) ───────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-1: Fact orphaned teacher_id',
    'hr_fact.teacher_id NOT IN teacher_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'hr_fact rows with no matching teacher_dim row'
FROM dw.hr_fact f
WHERE NOT EXISTS (SELECT 1 FROM dw.teacher_dim td WHERE td.id = f.teacher_id);

-- ── DQ-2: Orphaned hr_fact rows (school_id FK broken) ────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-2: Fact orphaned school_id',
    'hr_fact.school_id NOT IN schools_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'hr_fact rows with no matching schools_dim row'
FROM dw.hr_fact f
WHERE NOT EXISTS (SELECT 1 FROM dw.schools_dim sd WHERE sd.id = f.school_id);

-- ── DQ-3: Orphaned hr_fact rows (date_id FK broken) ──────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-3: Fact orphaned date_id',
    'hr_fact.date_id NOT IN date_dim',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'hr_fact rows with no matching date_dim row'
FROM dw.hr_fact f
WHERE NOT EXISTS (SELECT 1 FROM dw.date_dim dd WHERE dd.id = f.date_id);

-- ── DQ-4: teacher_dim — no duplicate current rows per teacher ─────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-4: teacher_dim duplicate current rows',
    'COUNT(*) > 1 per source_id WHERE is_current=TRUE',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*), 'CRITICAL',
    'Teachers with more than one is_current=TRUE row in teacher_dim'
FROM (
    SELECT source_id FROM dw.teacher_dim
    WHERE is_current = TRUE
    GROUP BY source_id HAVING COUNT(*) > 1
) dup;

-- ── DQ-5: Teachers with no school in hr_fact ──────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-5: Teachers in dim with no hr_fact rows',
    'teacher_dim current rows with no matching hr_fact',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Current teachers in teacher_dim with no deployment record in hr_fact'
FROM dw.teacher_dim td
WHERE td.is_current = TRUE
  AND NOT EXISTS (SELECT 1 FROM dw.hr_fact f WHERE f.teacher_id = td.id);

-- ── DQ-6: Teachers without IPPS number ───────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-6: Teachers without IPPS number',
    'teacher_dim.ipps_number IS NULL WHERE is_current=TRUE',
    'INFO', 'INFO',
    COUNT(*), 'INFO',
    'Current teachers without an IPPS payroll number'
FROM dw.teacher_dim
WHERE is_current = TRUE AND ipps_number IS NULL;

-- ── DQ-7: Teachers with NULL discipline ──────────────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-7: Teachers with NULL discipline',
    'teacher_dim.discipline IS NULL WHERE is_current=TRUE',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END,
    COUNT(*), 'WARNING',
    'Current teachers with no subjects recorded — discipline cannot be derived'
FROM dw.teacher_dim
WHERE is_current = TRUE AND discipline IS NULL;

-- ── DQ-8: hr_fact vs teachers_flat count variance ────────────────────────────
INSERT INTO dw_audit.dq_check_log
    (run_date, mart, check_name, check_sql, result, status, row_count, severity, notes)
SELECT
    CURRENT_DATE, 'TEACHERS', 'DQ-8: hr_fact vs teachers_flat count',
    'ABS(hr_fact count - teachers_flat count) / teachers_flat < 1%',
    CASE WHEN ABS(fact_count - flat_count)::FLOAT / NULLIF(flat_count,0) < 0.01
         THEN 'PASS' ELSE 'WARNING' END,
    CASE WHEN ABS(fact_count - flat_count)::FLOAT / NULLIF(flat_count,0) < 0.01
         THEN 'PASS' ELSE 'WARNING' END,
    ABS(fact_count - flat_count), 'WARNING',
    'More than 1% difference between staging rows and loaded fact rows'
FROM (
    SELECT
        (SELECT COUNT(*) FROM dw.hr_fact)          AS fact_count,
        (SELECT COUNT(*) FROM stg.teachers_flat)   AS flat_count
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
    WHERE run_date = CURRENT_DATE AND mart = 'TEACHERS';

    RAISE NOTICE 'Teachers DQ: PASS=% | WARNING=% | FAIL=% | INFO=%',
        v_pass, v_warn, v_fail, v_info;

    IF v_fail > 0 THEN
        RAISE EXCEPTION 'Teachers DQ: % CRITICAL check(s) failed.', v_fail;
    END IF;
END;
$$;

COMMIT;
