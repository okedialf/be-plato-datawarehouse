-- =============================================================================
-- EMIS Teachers Mart ETL: Step 3 — Load dw.hr_fact
--
-- Reads from stg.teachers_flat.
-- Resolves all surrogate keys from DW dimensions.
-- Inserts new rows only (ON CONFLICT DO NOTHING on natural key).
-- Safe to re-run.
--
-- Surrogate key resolution:
--   person_id + term_start_date → dw.teacher_dim  (SCD2: version valid on term date)
--   school_source_id            → dw.schools_dim  (SCD2: version valid on term date)
--   term_start_date             → dw.date_dim
-- =============================================================================

BEGIN;

INSERT INTO dw.hr_fact (
    teacher_id,
    school_id,
    date_id,
    person_id,
    school_source_id,
    academic_year,
    term,
    load_time
)
SELECT
    -- Teacher SCD2 surrogate: version valid on term start date
    td.id                   AS teacher_id,

    -- School SCD2 surrogate: version valid on term start date
    sd.id                   AS school_id,

    -- Date surrogate
    dd.id                   AS date_id,

    -- Natural keys
    tf.person_id,
    tf.school_id            AS school_source_id,
    tf.academic_year,
    tf.term,

    NOW()                   AS load_time

FROM stg.teachers_flat tf

-- Resolve teacher SCD2 surrogate (version valid on term start date)
JOIN dw.teacher_dim td
    ON  td.source_id       = tf.person_id
    AND tf.term_start_date >= td.effective_date
    AND tf.term_start_date <  td.expiration_date

-- Resolve school SCD2 surrogate (version valid on term start date)
JOIN dw.schools_dim sd
    ON  sd.source_id       = tf.school_id::INTEGER
    AND tf.term_start_date >= sd.effective_date
    AND tf.term_start_date <  sd.expiration_date

-- Resolve date surrogate
JOIN dw.date_dim dd
    ON dd.system_date = tf.term_start_date

WHERE tf.term_start_date IS NOT NULL

ON CONFLICT (person_id, school_source_id, academic_year, term)
DO NOTHING;


-- ── Fallback: historical rows where no SCD2 version existed at term date ──────
-- Uses earliest available teacher_dim version for that teacher
INSERT INTO dw.hr_fact (
    teacher_id, school_id, date_id,
    person_id, school_source_id, academic_year, term, load_time
)
SELECT
    td_fallback.id          AS teacher_id,
    sd_fallback.id          AS school_id,
    dd.id                   AS date_id,
    tf.person_id,
    tf.school_id            AS school_source_id,
    tf.academic_year,
    tf.term,
    NOW()
FROM stg.teachers_flat tf

-- Earliest teacher_dim version
JOIN (
    SELECT DISTINCT ON (source_id)
        id, source_id
    FROM dw.teacher_dim
    ORDER BY source_id, effective_date ASC
) td_fallback ON td_fallback.source_id = tf.person_id

-- Earliest school_dim version
JOIN (
    SELECT DISTINCT ON (source_id)
        id, source_id
    FROM dw.schools_dim
    ORDER BY source_id, effective_date ASC
) sd_fallback ON sd_fallback.source_id = tf.school_id::INTEGER

JOIN dw.date_dim dd
    ON dd.system_date = tf.term_start_date

WHERE tf.term_start_date IS NOT NULL

ON CONFLICT (person_id, school_source_id, academic_year, term)
DO NOTHING;


-- ── Verification ──────────────────────────────────────────────────────────────
DO $$
DECLARE
    v_fact_total    BIGINT;
    v_flat_total    BIGINT;
    v_unresolved    BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_fact_total FROM dw.hr_fact;
    SELECT COUNT(*) INTO v_flat_total FROM stg.teachers_flat;

    SELECT COUNT(*) INTO v_unresolved
    FROM stg.teachers_flat tf
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.hr_fact f
        WHERE f.person_id        = tf.person_id
          AND f.school_source_id = tf.school_id
          AND f.academic_year    = tf.academic_year
          AND f.term             = tf.term
    );

    RAISE NOTICE 'dw.hr_fact total=% | stg_flat=% | unresolved=%',
        v_fact_total, v_flat_total, v_unresolved;

    IF v_unresolved > 0 THEN
        RAISE WARNING '% teacher rows could not resolve all dimension keys.', v_unresolved;
    END IF;
END;
$$;

COMMIT;
