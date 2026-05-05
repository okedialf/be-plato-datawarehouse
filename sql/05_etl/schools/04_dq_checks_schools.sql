-- =============================================================================
-- EMIS Schools Mart ETL: DQ Checks
-- Validates dw.schools_dim and dw.school_fact after each nightly ETL run.
-- Results written to dw_audit.dq_check_log.
-- Schema: run_id, check_name, status (PASS/WARN/FAIL), detail
-- =============================================================================

DO $$
DECLARE
    v_run_id    INTEGER;
    v_count     BIGINT;
    v_status    VARCHAR(20);
    v_detail    TEXT;
BEGIN
    INSERT INTO dw_audit.etl_run (pipeline, as_of_date, status, start_ts)
    VALUES ('schools_dq', CURRENT_DATE, 'DQ', NOW())
    RETURNING run_id INTO v_run_id;

    -- CHECK 1: schools_dim row count
    SELECT COUNT(*) INTO v_count FROM dw.schools_dim WHERE is_current = TRUE;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL'
                     WHEN v_count < 80000 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('dw.schools_dim current rows: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'schools_dim_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 2: schools with no EMIS number
    SELECT COUNT(*) INTO v_count
    FROM dw.schools_dim
    WHERE is_current = TRUE AND (emis_number IS NULL OR emis_number = '');
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Schools with no EMIS number: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'schools_dim_no_emis_number', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 3: schools with no admin unit resolved
    SELECT COUNT(*) INTO v_count
    FROM dw.schools_dim
    WHERE is_current = TRUE AND admin_unit_id IS NULL;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Schools with unresolved admin unit: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'schools_dim_no_admin_unit', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 4: school_fact row count
    SELECT COUNT(*) INTO v_count FROM dw.school_fact;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.school_fact rows: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'school_fact_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 5: school_fact rows with unresolved school_id
    SELECT COUNT(*) INTO v_count
    FROM dw.school_fact sf
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.schools_dim sd WHERE sd.id = sf.school_id
    );
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('school_fact rows with unresolved school_id: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'school_fact_unresolved_school', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 6: schools in dim but not in fact
    SELECT COUNT(*) INTO v_count
    FROM dw.schools_dim sd
    WHERE sd.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM dw.school_fact sf WHERE sf.school_id = sd.id
      );
    v_status := CASE WHEN v_count > 1000 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Schools in dim but not in fact: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'schools_dim_not_in_fact', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 7: duplicate emis numbers in current rows
    SELECT COUNT(*) INTO v_count
    FROM (
        SELECT emis_number
        FROM dw.schools_dim
        WHERE is_current = TRUE
        GROUP BY emis_number
        HAVING COUNT(*) > 1
    ) dups;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Duplicate EMIS numbers in current schools_dim rows: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'schools_dim_duplicate_emis', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    RAISE NOTICE '=== SCHOOLS DQ CHECKS COMPLETE ===';
END; $$;
