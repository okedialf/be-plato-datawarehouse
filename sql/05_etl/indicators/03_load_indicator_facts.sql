-- =============================================================================
-- EMIS Indicators Mart ETL: Step 3 — Load DW Fact Tables
--
-- Loads from staging into the three DW fact tables:
--   stg.population_raw           → dw.population_fact
--   stg.enrolment_indicator_raw  → dw.enrolment_indicator_fact
--   stg.school_indicator_raw     → dw.school_indicator_fact
--
-- Uses temp table JOIN pattern (not correlated subqueries) for performance.
-- Safe to re-run (ON CONFLICT DO NOTHING / DO UPDATE).
-- =============================================================================

BEGIN;

-- ── Step 3a: Load dw.population_fact ─────────────────────────────────────────
INSERT INTO dw.population_fact (
    district_id,
    district_name,
    year,
    sex,
    total_population,
    pop_age_3_5,
    pop_age_6_12,
    pop_age_13_16,
    pop_age_17_18,
    pop_age_6,
    data_source
)
SELECT
    -- Resolve district surrogate from admin_units_dim
    au.id                           AS district_id,
    pr.district_name,
    pr.year,
    pr.sex,
    pr.total_population,
    pr.pop_age_3_5,
    pr.pop_age_6_12,
    pr.pop_age_13_16,
    pr.pop_age_17_18,
    pr.pop_age_6,
    pr.data_source
FROM stg.population_raw pr
JOIN dw.admin_units_dim au
    ON au.source_id   = pr.district_source_id
    AND au.admin_unit_type = 'District'
    AND au.current_status = TRUE
WHERE pr.district_source_id IS NOT NULL
ON CONFLICT (district_id, year, sex) DO UPDATE
    SET
        total_population = EXCLUDED.total_population,
        pop_age_3_5      = EXCLUDED.pop_age_3_5,
        pop_age_6_12     = EXCLUDED.pop_age_6_12,
        pop_age_13_16    = EXCLUDED.pop_age_13_16,
        pop_age_17_18    = EXCLUDED.pop_age_17_18,
        pop_age_6        = EXCLUDED.pop_age_6,
        load_time        = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.population_fact;
    RAISE NOTICE 'dw.population_fact: % rows', v;
END; $$;


-- ── Step 3b: Load dw.enrolment_indicator_fact ─────────────────────────────────
-- Resolve date_id for Term 1 start date per academic year

CREATE TEMP TABLE tmp_term1_dates AS
SELECT DISTINCT
    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM aytp.start_date)::INTEGER
    )                   AS academic_year,
    dd.id               AS date_id,
    aytp.start_date
FROM public.academic_year_teaching_periods aytp
JOIN public.setting_academic_years   ay ON ay.id = aytp.academic_year_id
JOIN public.setting_teaching_periods tp ON tp.id = aytp.teaching_period_id
JOIN dw.date_dim dd ON dd.system_date = aytp.start_date
WHERE aytp.status = 'active'
  AND aytp.school_id IS NULL
  AND tp.name = '1';   -- Term 1

CREATE INDEX ON tmp_term1_dates (academic_year);

INSERT INTO dw.enrolment_indicator_fact (
    district_id,
    date_id,
    school_level,
    academic_year,
    gender,
    total_enrolment,
    official_age_enrolment,
    new_entrants_p1,
    new_entrants_p1_official_age,
    repeaters,
    learners_with_disability,
    school_age_population,
    population_age_6,
    ger,
    ner,
    gir,
    repetition_rate,
    sne_inclusion_rate
)
SELECT
    eir.district_id,
    COALESCE(td.date_id, dd_fallback.id)    AS date_id,
    eir.school_level,
    eir.academic_year,
    eir.gender,
    eir.total_enrolment,
    eir.official_age_enrolment,
    eir.new_entrants_p1,
    eir.new_entrants_p1_official_age,
    eir.repeaters,
    eir.learners_with_disability,
    eir.school_age_population,
    eir.population_age_6,
    eir.ger,
    eir.ner,
    eir.gir,
    eir.repetition_rate,
    eir.sne_inclusion_rate
FROM stg.enrolment_indicator_raw eir
LEFT JOIN tmp_term1_dates td
    ON td.academic_year = eir.academic_year
-- Fallback: use Feb 1 of the academic year if Term 1 date not found
LEFT JOIN dw.date_dim dd_fallback
    ON dd_fallback.system_date = MAKE_DATE(eir.academic_year, 2, 1)
WHERE eir.district_id IS NOT NULL
ON CONFLICT (district_id, school_level, academic_year, gender) DO UPDATE
    SET
        total_enrolment             = EXCLUDED.total_enrolment,
        official_age_enrolment      = EXCLUDED.official_age_enrolment,
        new_entrants_p1             = EXCLUDED.new_entrants_p1,
        repeaters                   = EXCLUDED.repeaters,
        learners_with_disability    = EXCLUDED.learners_with_disability,
        school_age_population       = EXCLUDED.school_age_population,
        ger                         = EXCLUDED.ger,
        ner                         = EXCLUDED.ner,
        gir                         = EXCLUDED.gir,
        repetition_rate             = EXCLUDED.repetition_rate,
        sne_inclusion_rate          = EXCLUDED.sne_inclusion_rate,
        load_time                   = NOW();

-- Compute GPI on TOTAL rows (female GER / male GER)
UPDATE dw.enrolment_indicator_fact tot
SET
    gpi_ger = CASE
        WHEN male_row.ger > 0
        THEN ROUND(COALESCE(fem_row.ger, 0) / male_row.ger, 4)
        ELSE NULL
    END,
    gpi_ner = CASE
        WHEN male_row.ner > 0
        THEN ROUND(COALESCE(fem_row.ner, 0) / male_row.ner, 4)
        ELSE NULL
    END
FROM dw.enrolment_indicator_fact male_row
JOIN dw.enrolment_indicator_fact fem_row
    ON  fem_row.district_id   = male_row.district_id
    AND fem_row.school_level  = male_row.school_level
    AND fem_row.academic_year = male_row.academic_year
    AND UPPER(fem_row.gender) IN ('F','FEMALE')
WHERE tot.district_id    = male_row.district_id
  AND tot.school_level   = male_row.school_level
  AND tot.academic_year  = male_row.academic_year
  AND UPPER(tot.gender)  = 'TOTAL'
  AND UPPER(male_row.gender) IN ('M','MALE');

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.enrolment_indicator_fact;
    RAISE NOTICE 'dw.enrolment_indicator_fact: % rows', v;
END; $$;

DROP TABLE IF EXISTS tmp_term1_dates;


-- ── Step 3c: Load dw.school_indicator_fact ────────────────────────────────────
-- Build school surrogate lookup
CREATE TEMP TABLE tmp_school_surrogate AS
SELECT DISTINCT ON (source_id)
    source_id, id AS school_dim_id
FROM dw.schools_dim
ORDER BY source_id, effective_date ASC;
CREATE INDEX ON tmp_school_surrogate (source_id);

-- Build date surrogate lookup (Term 1 start dates)
CREATE TEMP TABLE tmp_term_dates AS
SELECT DISTINCT
    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM aytp.start_date)::INTEGER
    )                   AS academic_year,
    CASE
        WHEN tp.name IN ('1','2','3') THEN 'TERM ' || tp.name
        WHEN tp.name ILIKE 'TERM%'   THEN UPPER(tp.name)
        ELSE 'TERM 1'
    END                 AS term,
    dd.id               AS date_id
FROM public.academic_year_teaching_periods aytp
JOIN public.setting_academic_years   ay ON ay.id = aytp.academic_year_id
JOIN public.setting_teaching_periods tp ON tp.id = aytp.teaching_period_id
JOIN dw.date_dim dd ON dd.system_date = aytp.start_date
WHERE aytp.status    = 'active'
  AND aytp.school_id IS NULL;

CREATE INDEX ON tmp_term_dates (academic_year, term);

INSERT INTO dw.school_indicator_fact (
    school_id,
    date_id,
    school_source_id,
    academic_year,
    term,
    total_learners,
    male_learners,
    female_learners,
    total_teachers,
    male_teachers,
    female_teachers,
    qualified_teachers,
    trained_teachers,
    ptr,
    qualified_teacher_ratio,
    trained_teacher_ratio,
    pct_female_teachers,
    gpi_ptr
)
SELECT
    ts.school_dim_id                AS school_id,
    COALESCE(td.date_id, dd_fb.id) AS date_id,
    sir.school_source_id,
    sir.academic_year,
    sir.term,
    sir.total_learners,
    sir.male_learners,
    sir.female_learners,
    sir.total_teachers,
    sir.male_teachers,
    sir.female_teachers,
    sir.qualified_teachers,
    sir.trained_teachers,
    sir.ptr,
    sir.qualified_teacher_ratio,
    sir.trained_teacher_ratio,
    sir.pct_female_teachers,
    sir.gpi_ptr
FROM stg.school_indicator_raw sir
JOIN tmp_school_surrogate ts
    ON ts.source_id = sir.school_source_id::INTEGER
LEFT JOIN tmp_term_dates td
    ON  td.academic_year = sir.academic_year
    AND td.term          = sir.term
LEFT JOIN dw.date_dim dd_fb
    ON dd_fb.system_date = MAKE_DATE(sir.academic_year, 2, 1)
WHERE sir.school_source_id IS NOT NULL
ON CONFLICT (school_source_id, academic_year, term) DO UPDATE
    SET
        total_learners          = EXCLUDED.total_learners,
        total_teachers          = EXCLUDED.total_teachers,
        ptr                     = EXCLUDED.ptr,
        qualified_teacher_ratio = EXCLUDED.qualified_teacher_ratio,
        trained_teacher_ratio   = EXCLUDED.trained_teacher_ratio,
        pct_female_teachers     = EXCLUDED.pct_female_teachers,
        gpi_ptr                 = EXCLUDED.gpi_ptr,
        load_time               = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.school_indicator_fact;
    RAISE NOTICE 'dw.school_indicator_fact: % rows', v;
END; $$;

DROP TABLE IF EXISTS tmp_school_surrogate;
DROP TABLE IF EXISTS tmp_term_dates;

COMMIT;
