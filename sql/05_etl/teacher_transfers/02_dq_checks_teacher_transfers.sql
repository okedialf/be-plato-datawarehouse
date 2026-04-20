-- =============================================================================
-- EMIS Teacher Transfers Mart ETL: Step 2 — Data Quality Checks
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
    VALUES ('teacher_transfers_dq', CURRENT_DATE, 'DQ', NOW())
    RETURNING run_id INTO v_run_id;

    -- CHECK 1: Total transfers extracted
    SELECT COUNT(*) INTO v_count FROM stg.teacher_transfers_raw;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('stg.teacher_transfers_raw total rows: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_staging_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 2: Approved vs pending vs rejected breakdown (INFO)
    SELECT COUNT(*) INTO v_count
    FROM stg.teacher_transfers_raw WHERE transfer_status = 'APPROVED';
    v_status := CASE WHEN v_count = 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Approved transfers in staging: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_approved_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 3: Fact table row count
    SELECT COUNT(*) INTO v_count FROM dw.teacher_transfer_fact;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.teacher_transfer_fact rows loaded: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfer_fact_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 4: Unresolved teacher FK
    SELECT COUNT(*) INTO v_count
    FROM dw.teacher_transfer_fact f
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.teacher_dim td WHERE td.id = f.teacher_id
    );
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('Fact rows with unresolved teacher_id FK: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_unresolved_teacher', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 5: Transfers with no outgoing school resolved
    SELECT COUNT(*) INTO v_count
    FROM dw.teacher_transfer_fact
    WHERE out_school_id IS NULL;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Transfers with unresolved outgoing school: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_unresolved_out_school', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 6: Transfers with no district resolved
    SELECT COUNT(*) INTO v_count
    FROM dw.teacher_transfer_fact
    WHERE out_district_id IS NULL AND in_district_id IS NULL;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Transfers with no district resolved: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_no_district', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 7: Inter-district transfer count (INFO)
    SELECT COUNT(*) INTO v_count
    FROM dw.teacher_transfer_fact WHERE is_inter_district = TRUE;
    v_status := 'PASS';
    v_detail := format('Inter-district transfers: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_inter_district_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 8: Implausible days_at_station (negative or > 20 years)
    SELECT COUNT(*) INTO v_count
    FROM dw.teacher_transfer_fact
    WHERE days_at_outgoing_station IS NOT NULL
      AND (days_at_outgoing_station < 0 OR days_at_outgoing_station > 7300);
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Transfers with implausible days_at_station (<0 or >20yrs): %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_implausible_days', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 9: Transfers where reporting_date < posting_date (data error)
    SELECT COUNT(*) INTO v_count
    FROM dw.teacher_transfer_fact
    WHERE reporting_date IS NOT NULL
      AND posting_date   IS NOT NULL
      AND reporting_date < posting_date;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Transfers where reporting_date < posting_date: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_date_logic_error', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 10: Transfers approved in staging but missing from fact
    SELECT COUNT(*) INTO v_count
    FROM stg.teacher_transfers_raw r
    WHERE r.transfer_status = 'APPROVED'
      AND r.person_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM dw.teacher_transfer_fact f
          WHERE f.transfer_source_id = r.transfer_id
      );
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Approved transfers in staging not loaded to fact (no teacher_dim match): %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'transfers_approved_not_loaded', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    RAISE NOTICE '=== TEACHER TRANSFERS DQ CHECKS COMPLETE ===';
END; $$;
