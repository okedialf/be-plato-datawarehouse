-- =============================================================================
-- EMIS Infrastructure Mart ETL: Step 1 — Load DW Fact Table
--
-- Loads stg.infrastructure_raw → dw.infrastructure_fact.
-- dw.infrastructure_type_dim was already seeded in Step 0.
-- Safe to re-run (ON CONFLICT DO UPDATE).
--
-- FIX: academic_year_teaching_periods has multiple rows per year+term
-- (one per school_type), causing the date JOIN to fan out and produce
-- duplicates even after DISTINCT ON in the staging subquery.
-- Solution: deduplicate tmp_date_sk with DISTINCT ON, and use a CTE
-- with a final DISTINCT ON over the full natural key before inserting.
-- =============================================================================

BEGIN;

-- ── Surrogate key lookups ─────────────────────────────────────────────────────

CREATE TEMP TABLE tmp_school_sk AS
SELECT DISTINCT ON (source_id)
    source_id, id AS school_dim_id
FROM dw.schools_dim
WHERE is_current = TRUE
ORDER BY source_id;
CREATE INDEX ON tmp_school_sk (source_id);

CREATE TEMP TABLE tmp_type_sk AS
SELECT DISTINCT ON (source_id)
    source_id, id AS type_dim_id
FROM dw.infrastructure_type_dim
ORDER BY source_id;
CREATE INDEX ON tmp_type_sk (source_id);

-- Deduplicate date lookup — DISTINCT ON (academic_year, term)
-- academic_year_teaching_periods has 4 rows per year+term (one per school_type)
CREATE TEMP TABLE tmp_date_sk AS
SELECT DISTINCT ON (academic_year, term)
    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM aytp.start_date)::INTEGER
    )                   AS academic_year,
    CASE
        WHEN tp.name IN ('1','2','3') THEN 'TERM ' || tp.name
        WHEN tp.name ILIKE 'TERM%'   THEN UPPER(tp.name)
        ELSE 'TERM 1'
    END                 AS term,
    dd.id               AS date_dim_id
FROM public.academic_year_teaching_periods aytp
JOIN public.setting_academic_years   ay ON ay.id  = aytp.academic_year_id
JOIN public.setting_teaching_periods tp ON tp.id  = aytp.teaching_period_id
JOIN dw.date_dim dd                      ON dd.system_date = aytp.start_date
WHERE aytp.school_id IS NULL
  AND aytp.status = 'active'
ORDER BY academic_year, term;
CREATE INDEX ON tmp_date_sk (academic_year, term);


-- ── Load dw.infrastructure_fact ───────────────────────────────────────────────
-- Use a CTE to fully resolve all surrogates first, then DISTINCT ON the
-- natural key to guarantee no duplicate rows reach the INSERT.
INSERT INTO dw.infrastructure_fact (
    school_id, infra_type_id, date_id,
    school_source_id, building_source_id, academic_year, term,
    total_number, area,
    completion_status, usage_mode, structure_condition,
    gender_usage, user_category
)
SELECT DISTINCT ON (ir.school_id, ir.building_id, ir.academic_year, ir.term)
    ts.school_dim_id                                AS school_id,
    tt.type_dim_id                                  AS infra_type_id,
    COALESCE(
        dt.date_dim_id,
        (SELECT id FROM dw.date_dim
         WHERE system_date = MAKE_DATE(ir.academic_year, 2, 1)
         LIMIT 1)
    )                                               AS date_id,
    ir.school_id                                    AS school_source_id,
    ir.building_id                                  AS building_source_id,
    ir.academic_year,
    ir.term,
    ir.total_number,
    ir.area,
    ir.completion_status,
    ir.usage_mode,
    ir.structure_condition,
    ir.gender_usage,
    ir.user_category

FROM stg.infrastructure_raw ir
JOIN tmp_school_sk ts ON ts.source_id = ir.school_id::INTEGER
JOIN tmp_type_sk tt   ON tt.source_id = ir.building_id
LEFT JOIN tmp_date_sk dt
    ON  dt.academic_year = ir.academic_year
    AND dt.term          = ir.term

WHERE ir.school_id    IS NOT NULL
  AND ir.building_id  IS NOT NULL
  AND ir.academic_year IS NOT NULL

ORDER BY ir.school_id, ir.building_id, ir.academic_year, ir.term

ON CONFLICT (school_source_id, building_source_id, academic_year, term) DO UPDATE
    SET total_number        = EXCLUDED.total_number,
        area                = EXCLUDED.area,
        completion_status   = EXCLUDED.completion_status,
        usage_mode          = EXCLUDED.usage_mode,
        structure_condition = EXCLUDED.structure_condition,
        gender_usage        = EXCLUDED.gender_usage,
        user_category       = EXCLUDED.user_category,
        load_time           = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.infrastructure_fact;
    RAISE NOTICE 'dw.infrastructure_fact: % rows loaded', v;
END; $$;

DROP TABLE IF EXISTS tmp_school_sk;
DROP TABLE IF EXISTS tmp_type_sk;
DROP TABLE IF EXISTS tmp_date_sk;

COMMIT;
