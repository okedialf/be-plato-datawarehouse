-- =============================================================================
-- EMIS Non-Teaching Staff Mart ETL: Step 3 — Load dw.non_teaching_staff_fact
-- Uses temp table JOIN approach (no correlated subqueries) for performance.
-- Safe to re-run (ON CONFLICT DO NOTHING).
-- =============================================================================

BEGIN;

-- ── Build surrogate lookup temp tables ────────────────────────────────────────
CREATE TEMP TABLE tmp_nts_surrogate AS
SELECT DISTINCT ON (source_id)
    source_id,
    id AS nts_dim_id
FROM dw.non_teaching_staff_dim
ORDER BY source_id, effective_date ASC;

CREATE INDEX ON tmp_nts_surrogate (source_id);

CREATE TEMP TABLE tmp_school_surrogate AS
SELECT DISTINCT ON (source_id)
    source_id,
    id AS school_dim_id
FROM dw.schools_dim
ORDER BY source_id, effective_date ASC;

CREATE INDEX ON tmp_school_surrogate (source_id);

DO $$ BEGIN RAISE NOTICE 'Surrogate lookup tables built'; END; $$;


-- ── Load non_teaching_staff_fact ──────────────────────────────────────────────
INSERT INTO dw.non_teaching_staff_fact (
    staff_id,
    school_id,
    date_id,
    person_id,
    school_source_id,
    academic_year,
    term,
    is_primary_school,
    is_on_government_payroll,
    load_time
)
SELECT
    ns.nts_dim_id               AS staff_id,
    ss.school_dim_id            AS school_id,
    dd.id                       AS date_id,
    tf.person_id,
    tf.school_id                AS school_source_id,
    tf.academic_year,
    tf.term,
    tf.is_primary_school,
    tf.is_on_government_payroll,
    NOW()                       AS load_time
FROM stg.non_teaching_staff_flat tf
JOIN tmp_nts_surrogate ns
    ON ns.source_id = tf.person_id
JOIN tmp_school_surrogate ss
    ON ss.source_id = tf.school_id::INTEGER
JOIN dw.date_dim dd
    ON dd.system_date = tf.term_start_date
WHERE tf.term_start_date IS NOT NULL
ON CONFLICT (person_id, school_source_id, academic_year, term)
DO NOTHING;


-- ── Verification ──────────────────────────────────────────────────────────────
DO $$
DECLARE
    v_fact_total  BIGINT;
    v_flat_total  BIGINT;
    v_unresolved  BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_fact_total FROM dw.non_teaching_staff_fact;
    SELECT COUNT(*) INTO v_flat_total FROM stg.non_teaching_staff_flat
        WHERE term_start_date IS NOT NULL;

    SELECT COUNT(*) INTO v_unresolved
    FROM stg.non_teaching_staff_flat tf
    WHERE tf.term_start_date IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM dw.non_teaching_staff_fact f
          WHERE f.person_id        = tf.person_id
            AND f.school_source_id = tf.school_id
            AND f.academic_year    = tf.academic_year
            AND f.term             = tf.term
      );

    RAISE NOTICE 'dw.non_teaching_staff_fact total=% | stg_flat=% | unresolved=%',
        v_fact_total, v_flat_total, v_unresolved;

    IF v_unresolved > 0 THEN
        RAISE WARNING '% rows could not resolve dimension keys.', v_unresolved;
    END IF;
END;
$$;

DROP TABLE IF EXISTS tmp_nts_surrogate;
DROP TABLE IF EXISTS tmp_school_surrogate;

COMMIT;
