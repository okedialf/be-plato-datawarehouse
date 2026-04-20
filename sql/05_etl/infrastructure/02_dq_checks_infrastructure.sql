-- =============================================================================
-- EMIS Infrastructure Mart ETL: Step 2 — Data Quality Checks
-- Results written to dw_audit.dq_check_log.
-- =============================================================================

DO $$
DECLARE
    v_run_id    INTEGER;
    v_count     BIGINT;
    v_status    VARCHAR(20);
    v_detail    TEXT;
BEGIN
    INSERT INTO dw_audit.etl_run (pipeline, as_of_date, status, start_ts)
    VALUES ('infrastructure_dq', CURRENT_DATE, 'DQ', NOW())
    RETURNING run_id INTO v_run_id;

    -- CHECK 1: Infrastructure type dim row count
    SELECT COUNT(*) INTO v_count FROM dw.infrastructure_type_dim;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.infrastructure_type_dim: %s rows', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_type_dim_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 2: Fact table row count
    SELECT COUNT(*) INTO v_count FROM dw.infrastructure_fact;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.infrastructure_fact: %s rows', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_fact_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 3: Unresolved school FK
    SELECT COUNT(*) INTO v_count
    FROM dw.infrastructure_fact f
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.schools_dim sd WHERE sd.id = f.school_id
    );
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('Fact rows with unresolved school_id: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_unresolved_school', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 4: Unresolved infrastructure type FK
    SELECT COUNT(*) INTO v_count
    FROM dw.infrastructure_fact f
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.infrastructure_type_dim td WHERE td.id = f.infra_type_id
    );
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('Fact rows with unresolved infra_type_id: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_unresolved_type', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 5: Completion status breakdown (INFO)
    SELECT COUNT(*) INTO v_count
    FROM dw.infrastructure_fact WHERE completion_status = 'COMPLETE';
    v_status := 'PASS';
    v_detail := format('COMPLETE structures: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_complete_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 6: Structures in POOR or DILAPIDATED condition (INFO)
    SELECT COUNT(*) INTO v_count
    FROM dw.infrastructure_fact
    WHERE structure_condition IN ('POOR', 'DILAPIDATED');
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Structures in POOR or DILAPIDATED condition: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_poor_condition_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 7: NULL total_number (should always be recorded)
    SELECT COUNT(*) INTO v_count
    FROM dw.infrastructure_fact WHERE total_number IS NULL;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Fact rows with NULL total_number: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_null_total_number', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 8: Staging vs fact count variance
    SELECT COUNT(*) INTO v_count
    FROM stg.infrastructure_raw ir
    WHERE ir.school_id IS NOT NULL
      AND ir.building_id IS NOT NULL
      AND ir.academic_year IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM dw.infrastructure_fact f
          WHERE f.school_source_id  = ir.school_id
            AND f.building_source_id = ir.building_id
            AND f.academic_year      = ir.academic_year
            AND f.term               = ir.term
      );
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Staging rows not loaded to fact (no school_dim match): %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_staging_not_loaded', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 9: Lab coverage — schools with no lab structure recorded (INFO)
    SELECT COUNT(*) INTO v_count
    FROM dw.schools_dim sd
    WHERE sd.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM dw.infrastructure_fact f
          JOIN dw.infrastructure_type_dim td ON td.id = f.infra_type_id
          WHERE f.school_id  = sd.id
            AND td.is_lab_yn = TRUE
      );
    v_status := 'PASS';
    v_detail := format('Schools with no lab infrastructure recorded: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'infra_schools_no_lab', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    RAISE NOTICE '=== INFRASTRUCTURE DQ CHECKS COMPLETE ===';
END; $$;
