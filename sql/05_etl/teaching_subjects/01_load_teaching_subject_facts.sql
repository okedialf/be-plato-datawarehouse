-- =============================================================================
-- EMIS Teaching Subjects Mart ETL: Step 1 — Load DW Tables
--
-- Loads from staging into:
--   dw.subject_dim            ← stg.subject_dim_raw
--   dw.teaching_subject_fact  ← stg.teaching_subject_flat
--
-- Safe to re-run (ON CONFLICT DO UPDATE / DO NOTHING).
-- =============================================================================

BEGIN;

-- ── Step 1a: Load dw.subject_dim ─────────────────────────────────────────────
-- Full upsert — subject attributes can change (e.g. subject activated/archived)
INSERT INTO dw.subject_dim (
    source_id, subject_level, subject_name,
    is_language_subject, is_science_subject, is_principal_subject,
    is_olevel_subject, is_alevel_subject, is_mandatory, is_examinable,
    has_lab_yn, is_active_yn
)
SELECT
    source_id, subject_level, subject_name,
    is_language_subject, is_science_subject, is_principal_subject,
    is_olevel_subject, is_alevel_subject, is_mandatory, is_examinable,
    has_lab_yn, is_active_yn
FROM stg.subject_dim_raw
ON CONFLICT (source_id, subject_level) DO UPDATE
    SET subject_name          = EXCLUDED.subject_name,
        is_language_subject   = EXCLUDED.is_language_subject,
        is_science_subject    = EXCLUDED.is_science_subject,
        is_principal_subject  = EXCLUDED.is_principal_subject,
        is_olevel_subject     = EXCLUDED.is_olevel_subject,
        is_alevel_subject     = EXCLUDED.is_alevel_subject,
        is_mandatory          = EXCLUDED.is_mandatory,
        is_examinable         = EXCLUDED.is_examinable,
        has_lab_yn            = EXCLUDED.has_lab_yn,
        is_active_yn          = EXCLUDED.is_active_yn,
        load_time             = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.subject_dim;
    RAISE NOTICE 'dw.subject_dim: % rows', v;
END; $$;


-- ── Step 1b: Build surrogate key lookup tables ────────────────────────────────
-- Teacher surrogate: use current version of teacher_dim
CREATE TEMP TABLE tmp_teacher_surrogate AS
SELECT DISTINCT ON (source_id)
    source_id,
    id AS teacher_dim_id
FROM dw.teacher_dim
WHERE is_current = TRUE
ORDER BY source_id;
CREATE INDEX ON tmp_teacher_surrogate (source_id);

-- School surrogate: use current version of schools_dim
CREATE TEMP TABLE tmp_school_surrogate AS
SELECT DISTINCT ON (source_id)
    source_id,
    id AS school_dim_id
FROM dw.schools_dim
WHERE is_current = TRUE
ORDER BY source_id;
CREATE INDEX ON tmp_school_surrogate (source_id);

-- Subject surrogate
CREATE TEMP TABLE tmp_subject_surrogate AS
SELECT source_id, subject_level, id AS subject_dim_id
FROM dw.subject_dim;
CREATE INDEX ON tmp_subject_surrogate (source_id, subject_level);

-- Date surrogate: use today's date as load date
CREATE TEMP TABLE tmp_load_date AS
SELECT id AS date_dim_id
FROM dw.date_dim
WHERE system_date = CURRENT_DATE
LIMIT 1;


-- ── Step 1c: Load dw.teaching_subject_fact ────────────────────────────────────
INSERT INTO dw.teaching_subject_fact (
    teacher_id,
    school_id,
    subject_id,
    date_id,
    person_id,
    school_source_id,
    subject_source_id,
    subject_level
)
SELECT
    tt.teacher_dim_id                           AS teacher_id,
    ts.school_dim_id                            AS school_id,
    subj.subject_dim_id                         AS subject_id,
    COALESCE(ld.date_dim_id,
        (SELECT id FROM dw.date_dim ORDER BY system_date DESC LIMIT 1))
                                                AS date_id,
    tsf.person_id,
    tsf.school_id                               AS school_source_id,
    tsf.subject_id                              AS subject_source_id,
    tsf.subject_level
FROM stg.teaching_subject_flat tsf
JOIN tmp_teacher_surrogate tt
    ON tt.source_id = tsf.person_id
JOIN tmp_school_surrogate ts
    ON ts.source_id = tsf.school_id::INTEGER
JOIN tmp_subject_surrogate subj
    ON subj.source_id     = tsf.subject_id
    AND subj.subject_level = tsf.subject_level
CROSS JOIN tmp_load_date ld
WHERE tsf.person_id   IS NOT NULL
  AND tsf.school_id   IS NOT NULL
  AND tsf.subject_id  IS NOT NULL

ON CONFLICT (person_id, subject_source_id, subject_level) DO UPDATE
    SET teacher_id   = EXCLUDED.teacher_id,
        school_id    = EXCLUDED.school_id,
        date_id      = EXCLUDED.date_id,
        load_time    = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.teaching_subject_fact;
    RAISE NOTICE 'dw.teaching_subject_fact: % rows', v;
END; $$;

DROP TABLE IF EXISTS tmp_teacher_surrogate;
DROP TABLE IF EXISTS tmp_school_surrogate;
DROP TABLE IF EXISTS tmp_subject_surrogate;
DROP TABLE IF EXISTS tmp_load_date;

COMMIT;
