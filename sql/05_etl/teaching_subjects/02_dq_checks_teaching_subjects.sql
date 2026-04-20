-- =============================================================================
-- EMIS Teaching Subjects Mart ETL: Step 2 — Data Quality Checks
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
    -- Insert a fresh ETL run row
    INSERT INTO dw_audit.etl_run (pipeline, as_of_date, status, start_ts)
    VALUES ('teaching_subjects_dq', CURRENT_DATE, 'DQ', NOW())
    RETURNING run_id INTO v_run_id;

    -- CHECK 1: subject_dim row count
    SELECT COUNT(*) INTO v_count FROM dw.subject_dim;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.subject_dim: %s rows loaded', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'subjects_dim_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 2: Both primary and secondary subjects present
    SELECT COUNT(DISTINCT subject_level) INTO v_count FROM dw.subject_dim;
    v_status := CASE WHEN v_count < 2 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('subject_dim distinct levels: %s (expected PRIMARY and SECONDARY)', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'subjects_both_levels_present', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 3: teaching_subject_fact row count
    SELECT COUNT(*) INTO v_count FROM dw.teaching_subject_fact;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.teaching_subject_fact: %s rows loaded', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'teaching_subject_fact_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 4: Unresolved teacher FK (teacher_id pointing to no teacher_dim row)
    SELECT COUNT(*) INTO v_count
    FROM dw.teaching_subject_fact tsf
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.teacher_dim td WHERE td.id = tsf.teacher_id
    );
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('Fact rows with unresolved teacher_id: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'teaching_subject_unresolved_teacher', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 5: Unresolved school FK
    SELECT COUNT(*) INTO v_count
    FROM dw.teaching_subject_fact tsf
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.schools_dim sd WHERE sd.id = tsf.school_id
    );
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('Fact rows with unresolved school_id: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'teaching_subject_unresolved_school', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 6: Subjects with no teachers assigned
    SELECT COUNT(*) INTO v_count
    FROM dw.subject_dim sd
    WHERE sd.is_active_yn = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM dw.teaching_subject_fact tsf
          WHERE tsf.subject_id = sd.id
      );
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Active subjects with no teachers assigned: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'subjects_with_no_teachers', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 7: Teachers with subjects but no school (school_id NULL in staging)
    SELECT COUNT(*) INTO v_count
    FROM stg.teaching_subject_flat
    WHERE school_id IS NULL;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Staging rows with NULL school_id (teacher has no school): %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'teaching_subject_no_school', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 8: Science vs Arts breakdown — INFO only
    SELECT COUNT(*) INTO v_count
    FROM dw.teaching_subject_fact tsf
    JOIN dw.subject_dim sd ON sd.id = tsf.subject_id
    WHERE sd.is_science_subject = TRUE;
    v_status := 'PASS';
    v_detail := format('Science subject assignments: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'teaching_subject_science_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    RAISE NOTICE '=== TEACHING SUBJECTS DQ CHECKS COMPLETE ===';
END; $$;
