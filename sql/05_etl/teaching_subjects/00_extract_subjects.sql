-- =============================================================================
-- EMIS Teaching Subjects Mart ETL: Step 0 — Extract Subjects
--
-- Populates:
--   stg.subject_dim_raw    — combined primary + secondary subject list
--   stg.teaching_subject_flat — teacher × subject with context
--
-- NOTE: stg.teacher_subjects_raw was already populated by the Teachers Mart
--   ETL (00_extract_teachers_raw.sql step 0b). We reuse it here.
--   It contains: id, employee_id, person_id, subject_id, subject_name,
--                is_science_subject, is_language_subject
--   These are secondary school subjects only.
--
-- Primary school subjects are on teachers.trained_subject_1_id and
--   trained_subject_2_id which reference setting_primary_school_subjects.
-- =============================================================================

BEGIN;

-- ── Step 0a: Build stg.subject_dim_raw ──────────────────────────────────────
-- Combines secondary subjects + primary subjects into one table
TRUNCATE TABLE stg.subject_dim_raw;

-- Secondary subjects
INSERT INTO stg.subject_dim_raw (
    source_id, subject_level, subject_name,
    is_language_subject, is_science_subject, is_principal_subject,
    is_olevel_subject, is_alevel_subject, is_mandatory, is_examinable,
    has_lab_yn, is_active_yn, is_archived_yn, date_created, date_updated
)
SELECT
    id, 'SECONDARY', name,
    is_language_subject, is_science_subject, is_principal_subject,
    is_olevel_subject, is_alevel_subject, is_mandatory, is_examinable,
    has_lab_yn, is_active_yn, is_archived_yn, date_created, date_updated
FROM public.setting_secondary_school_subjects
WHERE deleted_at IS NULL
ON CONFLICT (source_id, subject_level) DO UPDATE
    SET subject_name         = EXCLUDED.subject_name,
        is_active_yn         = EXCLUDED.is_active_yn,
        date_updated         = EXCLUDED.date_updated;

-- Primary subjects
INSERT INTO stg.subject_dim_raw (
    source_id, subject_level, subject_name,
    is_language_subject, is_science_subject,
    is_active_yn, is_archived_yn, date_created, date_updated
)
SELECT
    id, 'PRIMARY', name,
    COALESCE(is_language_subject, FALSE),
    FALSE,   -- primary subjects have no is_science_subject flag
    is_active_yn, is_archived_yn, date_created, date_updated
FROM public.setting_primary_school_subjects
WHERE deleted_at IS NULL
ON CONFLICT (source_id, subject_level) DO UPDATE
    SET subject_name     = EXCLUDED.subject_name,
        is_active_yn     = EXCLUDED.is_active_yn,
        date_updated     = EXCLUDED.date_updated;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.subject_dim_raw;
    RAISE NOTICE 'stg.subject_dim_raw: % rows (primary + secondary combined)', v;
END; $$;


-- ── Step 0b: Build stg.teaching_subject_flat ─────────────────────────────────
-- One row per teacher per subject, with teacher + school context.
-- Sources:
--   stg.teacher_subjects_raw  — secondary subjects per teacher
--   public.teachers           — primary subjects (trained_subject_1_id, _2_id)
--   stg.teachers_raw          — teacher attributes
--   stg.teacher_school_raw    — current school deployment

TRUNCATE TABLE stg.teaching_subject_flat;

-- Build all teacher-subject combinations in a CTE first to deduplicate,
-- then insert. ON CONFLICT within a single INSERT fails when the same row
-- appears twice in the source (PostgreSQL limitation).

-- Secondary subjects (from stg.teacher_subjects_raw)
INSERT INTO stg.teaching_subject_flat (
    person_id, employee_id, subject_id, subject_name, subject_level,
    is_science_subject, is_language_subject, is_principal_subject,
    is_olevel_subject, is_alevel_subject,
    school_id, school_name, school_type,
    teacher_type, gender, qualification,
    admin_unit_source_id,
    date_created, date_updated
)
SELECT DISTINCT ON (tsr.person_id, tsr.subject_id)
    tsr.person_id,
    tsr.employee_id,
    tsr.subject_id,
    tsr.subject_name,
    'SECONDARY'                                     AS subject_level,
    tsr.is_science_subject,
    tsr.is_language_subject,
    sub.is_principal_subject,
    sub.is_olevel_subject,
    sub.is_alevel_subject,
    tschool.school_id,
    s.name                                          AS school_name,
    sst.name                                        AS school_type,
    tr.teacher_type,
    tr.gender,
    tr.qualification,
    COALESCE(s.parish_id, s.sub_county_id,
             s.county_id, s.district_id, s.region_id) AS admin_unit_source_id,
    tsr.date_created,
    tsr.date_updated
FROM stg.teacher_subjects_raw tsr
JOIN stg.teachers_raw tr
    ON tr.person_id = tsr.person_id
JOIN stg.teacher_school_raw tschool
    ON tschool.person_id = tsr.person_id
LEFT JOIN public.schools s
    ON s.id = tschool.school_id
LEFT JOIN public.setting_school_types sst
    ON sst.id = s.school_type_id
LEFT JOIN public.setting_secondary_school_subjects sub
    ON sub.id = tsr.subject_id
ORDER BY tsr.person_id, tsr.subject_id
ON CONFLICT (person_id, subject_id) DO UPDATE
    SET subject_name     = EXCLUDED.subject_name,
        school_id        = EXCLUDED.school_id,
        date_updated     = EXCLUDED.date_updated;


-- Primary subjects: deduplicate with UNION (not UNION ALL) then insert
-- UNION removes duplicates where trained_subject_1_id = trained_subject_2_id
INSERT INTO stg.teaching_subject_flat (
    person_id, employee_id, subject_id, subject_name, subject_level,
    is_science_subject, is_language_subject,
    school_id, school_name, school_type,
    teacher_type, gender, qualification,
    admin_unit_source_id,
    date_created, date_updated
)
SELECT DISTINCT ON (p.person_id, p.subject_id)
    p.person_id, p.employee_id, p.subject_id, p.subject_name, p.subject_level,
    p.is_science_subject, p.is_language_subject,
    p.school_id, p.school_name, p.school_type,
    p.teacher_type, p.gender, p.qualification,
    p.admin_unit_source_id, p.date_created, p.date_updated
FROM (
    -- Subject 1
    SELECT
        t.person_id, tr.employee_id,
        psub.id AS subject_id, psub.name AS subject_name, 'PRIMARY' AS subject_level,
        FALSE AS is_science_subject,
        COALESCE(psub.is_language_subject, FALSE) AS is_language_subject,
        tschool.school_id, s.name AS school_name, sst.name AS school_type,
        tr.teacher_type, tr.gender, tr.qualification,
        COALESCE(s.parish_id, s.sub_county_id,
                 s.county_id, s.district_id, s.region_id) AS admin_unit_source_id,
        t.date_created, t.date_updated
    FROM public.teachers t
    JOIN public.setting_primary_school_subjects psub
        ON psub.id = t.trained_subject_1_id AND psub.deleted_at IS NULL
    JOIN stg.teachers_raw tr ON tr.person_id = t.person_id
    JOIN stg.teacher_school_raw tschool ON tschool.person_id = t.person_id
    LEFT JOIN public.schools s ON s.id = tschool.school_id
    LEFT JOIN public.setting_school_types sst ON sst.id = s.school_type_id
    WHERE t.trained_subject_1_id IS NOT NULL

    UNION

    -- Subject 2 (UNION deduplicates where sub1 = sub2)
    SELECT
        t.person_id, tr.employee_id,
        psub.id, psub.name, 'PRIMARY',
        FALSE, COALESCE(psub.is_language_subject, FALSE),
        tschool.school_id, s.name, sst.name,
        tr.teacher_type, tr.gender, tr.qualification,
        COALESCE(s.parish_id, s.sub_county_id,
                 s.county_id, s.district_id, s.region_id),
        t.date_created, t.date_updated
    FROM public.teachers t
    JOIN public.setting_primary_school_subjects psub
        ON psub.id = t.trained_subject_2_id AND psub.deleted_at IS NULL
    JOIN stg.teachers_raw tr ON tr.person_id = t.person_id
    JOIN stg.teacher_school_raw tschool ON tschool.person_id = t.person_id
    LEFT JOIN public.schools s ON s.id = tschool.school_id
    LEFT JOIN public.setting_school_types sst ON sst.id = s.school_type_id
    WHERE t.trained_subject_2_id IS NOT NULL
) p
ORDER BY p.person_id, p.subject_id
ON CONFLICT (person_id, subject_id) DO NOTHING;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.teaching_subject_flat;
    RAISE NOTICE 'stg.teaching_subject_flat: % rows (teacher × subject)', v;
END; $$;

COMMIT;
