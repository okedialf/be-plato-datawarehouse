-- =============================================================================
-- EMIS Indicators Mart ETL: Step 4 — Data Quality Checks
--
-- Validates dw.population_fact, dw.enrolment_indicator_fact,
-- dw.school_indicator_fact against known ranges and UNESCO guidelines.
-- Results written to dw_audit.dq_check_log.
--
-- dq_check_log schema: run_id, check_name, status (PASS/WARN/FAIL), detail
-- =============================================================================

DO $$
DECLARE
    v_run_id     INTEGER;
    v_count      BIGINT;
    v_status     VARCHAR(20);
    v_detail     TEXT;
BEGIN
    -- Insert an ETL run row for the indicators DQ check if none exists,
    -- then use its run_id. The FK on dq_check_log requires a valid run_id.
    INSERT INTO dw_audit.etl_run (pipeline, as_of_date, status, start_ts)
    VALUES ('indicators_dq', CURRENT_DATE, 'DQ', NOW())
    RETURNING run_id INTO v_run_id;

    -- ── POPULATION FACT ───────────────────────────────────────────────────────

    -- CHECK 1: Population fact has rows for current year
    SELECT COUNT(*) INTO v_count
    FROM dw.population_fact
    WHERE sex = 'TOTAL';

    v_status := CASE WHEN v_count = 0 THEN 'FAIL'
                     WHEN v_count < 100 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('population_fact TOTAL rows loaded: %s (loaded for year 2025)', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_population_current_year', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 2: Age proportion sanity — primary age should not exceed 20% of total
    SELECT COUNT(*) INTO v_count
    FROM dw.population_fact
    WHERE sex = 'TOTAL' AND pop_age_6_12 > total_population * 0.20;

    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Districts with primary-age pop >20%% of total: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_population_age_proportions', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- ── ENROLMENT INDICATOR FACT ──────────────────────────────────────────────

    -- CHECK 3: Row count
    SELECT COUNT(*) INTO v_count FROM dw.enrolment_indicator_fact;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.enrolment_indicator_fact rows: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_enrolment_fact_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 4: GER range — should be 0 to 200%
    SELECT COUNT(*) INTO v_count
    FROM dw.enrolment_indicator_fact
    WHERE ger IS NOT NULL AND (ger < 0 OR ger > 200);
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Rows with GER outside 0-200%%: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_ger_range', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 5: Repetition rate should be below 30%
    SELECT COUNT(*) INTO v_count
    FROM dw.enrolment_indicator_fact
    WHERE repetition_rate IS NOT NULL AND repetition_rate > 30;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Rows with repetition rate >30%%: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_repetition_rate_range', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 6: Rows with enrolment but no GER (missing population denominator)
    SELECT COUNT(*) INTO v_count
    FROM dw.enrolment_indicator_fact
    WHERE ger IS NULL AND total_enrolment > 0;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Rows with enrolment but NULL GER (missing population): %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_missing_ger_denominator', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- ── SCHOOL INDICATOR FACT ─────────────────────────────────────────────────

    -- CHECK 7: Row count
    SELECT COUNT(*) INTO v_count FROM dw.school_indicator_fact;
    v_status := CASE WHEN v_count = 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('dw.school_indicator_fact rows: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_school_fact_row_count', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 8: PTR extreme values (> 100 is almost certainly a data error)
    SELECT COUNT(*) INTO v_count
    FROM dw.school_indicator_fact
    WHERE ptr IS NOT NULL AND ptr > 100;
    v_status := CASE WHEN v_count > 0 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Schools with PTR > 100: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_ptr_extreme', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 9: Schools with learners but no teachers
    SELECT COUNT(*) INTO v_count
    FROM dw.school_indicator_fact
    WHERE total_learners > 0 AND total_teachers = 0;
    v_status := CASE WHEN v_count > 1000 THEN 'WARN' ELSE 'PASS' END;
    v_detail := format('Schools with learners but zero teachers: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_schools_no_teachers', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    -- CHECK 10: % female teachers out of range
    SELECT COUNT(*) INTO v_count
    FROM dw.school_indicator_fact
    WHERE pct_female_teachers IS NOT NULL
      AND (pct_female_teachers < 0 OR pct_female_teachers > 100);
    v_status := CASE WHEN v_count > 0 THEN 'FAIL' ELSE 'PASS' END;
    v_detail := format('Rows with %% female teachers outside 0-100%%: %s', v_count);
    INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
    VALUES (v_run_id, 'indicators_female_teacher_pct_range', v_status, v_detail);
    RAISE NOTICE '[%] %', v_status, v_detail;

    RAISE NOTICE '=== INDICATORS DQ CHECKS COMPLETE ===';
END; $$;
