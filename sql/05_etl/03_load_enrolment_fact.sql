-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 3 — Load dw.enrolment_fact
--
-- Reads from stg.enrolments_flat.
-- Resolves all surrogate keys from their respective DW dimensions.
-- Inserts new rows only (ON CONFLICT DO NOTHING on the natural key).
-- Safe to re-run — duplicate enrolments are silently skipped.
--
-- Key joins:
--   learner_source_id  → dw.learner_dim   (SCD2: pick version valid on term date)
--   school_source_id   → dw.schools_dim   (SCD2: pick version valid on term date)
--   grade              → dw.grade_dim
--   academic_year+term → dw.date_dim      (first day of term)
--   enrolment_type     → dw.enrolment_type_dim
-- =============================================================================

BEGIN;

-- ── Step 3a: Resolve the date_id for each academic_year+term combination ──────
-- Map academic_year + term → the first calendar date of that term.
-- Uganda term dates (approximate):
--   Term 1: February 1
--   Term 2: June 1
--   Term 3: September 1
-- Adjust these dates to match actual MoES gazetted term opening dates.
CREATE TEMP TABLE tmp_term_dates AS
SELECT DISTINCT
    academic_year,
    term,
    CASE term
        WHEN 'TERM 1' THEN MAKE_DATE(academic_year, 2, 1)
        WHEN 'TERM 2' THEN MAKE_DATE(academic_year, 6, 1)
        WHEN 'TERM 3' THEN MAKE_DATE(academic_year, 9, 1)
        ELSE               MAKE_DATE(academic_year, 2, 1)
    END AS term_start_date
FROM stg.enrolments_flat;

CREATE INDEX ON tmp_term_dates (academic_year, term);

-- ── Step 3b: Insert into enrolment_fact ───────────────────────────────────────
INSERT INTO dw.enrolment_fact (
    learner_id,
    school_id,
    grade_id,
    date_id,
    enrolment_type_id,
    learner_source_id,
    school_source_id,
    enrolment_source_id,
    academic_year,
    term,
    load_time
)
SELECT
    -- ── Learner SCD2 surrogate: pick version current on term start date ────
    ld.id                   AS learner_id,

    -- ── School SCD2 surrogate: pick version current on term start date ────
    sd.id                   AS school_id,

    -- ── Grade surrogate ───────────────────────────────────────────────────
    gd.id                   AS grade_id,

    -- ── Date surrogate: first day of the term ────────────────────────────
    dd.id                   AS date_id,

    -- ── Enrolment type surrogate ──────────────────────────────────────────
    etd.id                  AS enrolment_type_id,

    -- ── Natural keys (retained for deduplication) ─────────────────────────
    ef.learner_source_id,
    ef.school_source_id,
    ef.enrolment_source_id,
    ef.academic_year,
    ef.term,

    NOW()                   AS load_time

FROM stg.enrolments_flat ef

-- ── Resolve term start date ────────────────────────────────────────────────
JOIN tmp_term_dates td
    ON  td.academic_year = ef.academic_year
    AND td.term          = ef.term

-- ── Resolve learner SCD2 surrogate ───────────────────────────────────────
-- Pick the learner_dim row whose SCD2 window covers the term start date.
-- If no historical version exists for that date, fall back to is_current.
JOIN dw.learner_dim ld
    ON  ld.source_id       = ef.learner_source_id
    AND td.term_start_date >= ld.effective_date
    AND td.term_start_date <  ld.expiration_date

-- ── Resolve school SCD2 surrogate ────────────────────────────────────────
JOIN dw.schools_dim sd
    ON  sd.source_id       = ef.school_source_id::TEXT::INTEGER
    AND td.term_start_date >= sd.effective_date
    AND td.term_start_date <  sd.expiration_date

-- ── Resolve grade surrogate ───────────────────────────────────────────────
LEFT JOIN dw.grade_dim gd
    ON gd.grade_code = ef.grade

-- ── Resolve date surrogate from date_dim ──────────────────────────────────
JOIN dw.date_dim dd
    ON dd.system_date = td.term_start_date

-- ── Resolve enrolment type surrogate ─────────────────────────────────────
LEFT JOIN dw.enrolment_type_dim etd
    ON etd.enrolment_type = ef.enrolment_type

-- ── Only active enrolments ───────────────────────────────────────────────
WHERE ef.is_active = TRUE

-- ── Skip rows where we cannot resolve required dimension keys ────────────
  AND ld.id  IS NOT NULL
  AND sd.id  IS NOT NULL
  AND dd.id  IS NOT NULL

ON CONFLICT (learner_source_id, school_source_id, academic_year, term)
DO NOTHING;   -- Safe re-run: skip already-loaded rows

-- ── Step 3c: Handle learners with no historical dim version on term date ──
-- These are learners enrolled in past terms before their dim row was loaded.
-- Fall back: use the earliest available dim version for that learner.
INSERT INTO dw.enrolment_fact (
    learner_id, school_id, grade_id, date_id, enrolment_type_id,
    learner_source_id, school_source_id, enrolment_source_id,
    academic_year, term, load_time
)
SELECT
    ld_fallback.id          AS learner_id,
    sd_fallback.id          AS school_id,
    gd.id                   AS grade_id,
    dd.id                   AS date_id,
    etd.id                  AS enrolment_type_id,
    ef.learner_source_id,
    ef.school_source_id,
    ef.enrolment_source_id,
    ef.academic_year,
    ef.term,
    NOW()
FROM stg.enrolments_flat ef

JOIN tmp_term_dates td
    ON  td.academic_year = ef.academic_year
    AND td.term          = ef.term

-- Earliest learner dim version (for historical loads before SCD2 existed)
JOIN (
    SELECT DISTINCT ON (source_id)
        id, source_id
    FROM dw.learner_dim
    ORDER BY source_id, effective_date ASC
) ld_fallback
    ON ld_fallback.source_id = ef.learner_source_id

-- Earliest school dim version
JOIN (
    SELECT DISTINCT ON (source_id)
        id, source_id
    FROM dw.schools_dim
    ORDER BY source_id, effective_date ASC
) sd_fallback
    ON sd_fallback.source_id = ef.school_source_id::TEXT::INTEGER

LEFT JOIN dw.grade_dim gd
    ON gd.grade_code = ef.grade

JOIN dw.date_dim dd
    ON dd.system_date = td.term_start_date

LEFT JOIN dw.enrolment_type_dim etd
    ON etd.enrolment_type = ef.enrolment_type

WHERE ef.is_active = TRUE
  AND dd.id IS NOT NULL

ON CONFLICT (learner_source_id, school_source_id, academic_year, term)
DO NOTHING;   -- Already loaded by step 3b or previous run

-- ── Step 3d: Verification counts ─────────────────────────────────────────────
DO $$
DECLARE
    v_fact_total    BIGINT;
    v_flat_active   BIGINT;
    v_unresolved    BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_fact_total  FROM dw.enrolment_fact;
    SELECT COUNT(*) INTO v_flat_active FROM stg.enrolments_flat WHERE is_active = TRUE;

    -- Rows in flat that did not make it into the fact (unresolved dimension)
    SELECT COUNT(*) INTO v_unresolved
    FROM stg.enrolments_flat ef
    WHERE ef.is_active = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM dw.enrolment_fact f
          WHERE f.learner_source_id   = ef.learner_source_id
            AND f.school_source_id    = ef.school_source_id
            AND f.academic_year       = ef.academic_year
            AND f.term                = ef.term
      );

    RAISE NOTICE 'dw.enrolment_fact total=% | stg_active=% | unresolved=%',
        v_fact_total, v_flat_active, v_unresolved;

    IF v_unresolved > 0 THEN
        RAISE WARNING '% enrolment rows could not resolve all dimension keys. Check dw.grade_dim and dw.date_dim coverage.', v_unresolved;
    END IF;
END;
$$;

DROP TABLE IF EXISTS tmp_term_dates;

COMMIT;
