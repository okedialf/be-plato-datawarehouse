-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 0 — Extract from Source (GCP Testing Version)
-- All tables reference public schema directly (no FDW / src schema).
--
-- KEY CORRECTIONS from data dictionary review:
--   src.enrolments            → public.learner_enrolments
--   src.learner_disabilities  → public.learner_disabilities
--   src.lkup_academic_years   → public.setting_academic_years
--   src.lkup_teaching_periods → public.setting_teaching_periods
--   src.lkup_disability_types → public.setting_disability_types
--   No separate pre/primary/secondary enrolment subtype tables —
--   grade is directly on learner_enrolments.education_grade_id
--   Orphan status: learners.is_father_dead, learners.is_mother_dead
--   Identity docs: identity_documents table joined via setting_identity_document_types
-- =============================================================================

BEGIN;

-- ── 0a: learners_raw ─────────────────────────────────────────────────────────
TRUNCATE TABLE stg.learners_raw;

INSERT INTO stg.learners_raw (
    person_id, lin, surname, given_name, date_of_birth, gender,
    nationality, district_of_birth_id,
    nin, passport_no, student_pass_no, work_permit_no, refugee_id,
    is_flagged_yn, deleted_at, date_created, date_updated
)
SELECT
    l.person_id,
    l.lin,
    p.surname,
    TRIM(COALESCE(p.first_name,'') || ' ' || COALESCE(p.other_names,'')) AS given_name,
    p.birth_date                                            AS date_of_birth,
    p.gender,
    COALESCE(ac.name, 'UGANDA')                             AS nationality,
    l.district_of_birth_id,
    MAX(CASE WHEN UPPER(idt.name) LIKE '%NATIONAL%'
              OR UPPER(idt.name) LIKE '%NIN%'
             THEN id_docs.document_id END)                  AS nin,
    MAX(CASE WHEN UPPER(idt.name) LIKE '%PASSPORT%'
             THEN id_docs.document_id END)                  AS passport_no,
    MAX(CASE WHEN UPPER(idt.name) LIKE '%STUDENT%PASS%'
             THEN id_docs.document_id END)                  AS student_pass_no,
    MAX(CASE WHEN UPPER(idt.name) LIKE '%WORK%PERMIT%'
             THEN id_docs.document_id END)                  AS work_permit_no,
    MAX(CASE WHEN UPPER(idt.name) LIKE '%REFUGEE%'
             THEN id_docs.document_id END)                  AS refugee_id,
    l.is_flagged_yn,
    l.deleted_at,
    l.date_created,
    l.date_updated
FROM public.learners l
JOIN public.persons p
    ON p.id = l.person_id
LEFT JOIN public.admin_unit_countries ac
    ON ac.id = p.country_id
LEFT JOIN public.identity_documents id_docs
    ON id_docs.person_id = p.id
LEFT JOIN public.setting_identity_document_types idt
    ON idt.id = id_docs.identity_type_id
WHERE l.deleted_at IS NULL
GROUP BY
    l.person_id, l.lin, p.surname, p.first_name, p.other_names,
    p.birth_date, p.gender, ac.name, l.district_of_birth_id,
    l.is_flagged_yn, l.deleted_at, l.date_created, l.date_updated;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.learners_raw;
    RAISE NOTICE 'stg.learners_raw: % rows', v;
END; $$;


-- ── 0b: learner_disabilities_raw ─────────────────────────────────────────────
TRUNCATE TABLE stg.learner_disabilities_raw;

INSERT INTO stg.learner_disabilities_raw (
    id, learner_id, disability_type, severity, date_created, date_updated
)
SELECT
    ld.id,
    ld.learner_id,
    sdt.name    AS disability_type,
    NULL        AS severity,
    ld.date_created,
    ld.date_updated
FROM public.learner_disabilities ld
JOIN public.setting_disability_types sdt
    ON sdt.id = ld.disability_type_id;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.learner_disabilities_raw;
    RAISE NOTICE 'stg.learner_disabilities_raw: % rows', v;
END; $$;


-- ── 0c: enrolments_raw ───────────────────────────────────────────────────────
-- Source: public.learner_enrolments (not "enrolments")
TRUNCATE TABLE stg.enrolments_raw;

INSERT INTO stg.enrolments_raw (
    id, learner_id, school_id,
    academic_year_id, teaching_period_id, reporting_status_id,
    learner_promotion_status_id, learner_transfer_status_id,
    learner_transition_status_id, learner_registration_number,
    is_enrolment_active_yn, date_created, date_updated
)
SELECT
    le.id,
    le.learner_id,
    le.school_id,
    le.academic_year_id,
    le.teaching_period_id,
    le.reporting_status_id,
    le.promotion_status_id  AS learner_promotion_status_id,
    NULL                    AS learner_transfer_status_id,
    NULL                    AS learner_transition_status_id,
    NULL                    AS learner_registration_number,
    le.is_enrolment_active_yn,
    le.date_created,
    le.date_updated
FROM public.learner_enrolments le
WHERE le.is_enrolment_active_yn = TRUE;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.enrolments_raw;
    RAISE NOTICE 'stg.enrolments_raw: % rows', v;
END; $$;


-- ── 0d: enrolments_subtype_raw ───────────────────────────────────────────────
-- No separate subtype tables. Grade decoded directly from learner_enrolments.
TRUNCATE TABLE stg.enrolments_subtype_raw;

INSERT INTO stg.enrolments_subtype_raw (
    enrolment_id, school_level, education_class_id,
    education_class_name, familiar_language_id, familiar_language_name,
    date_created
)
SELECT
    le.id                                           AS enrolment_id,
    UPPER(COALESCE(st.name, el.name, 'UNKNOWN'))    AS school_level,
    le.education_grade_id                           AS education_class_id,
    seg.name                                        AS education_class_name,
    le.familiar_language_id,
    sfl.name                                        AS familiar_language_name,
    le.date_created
FROM public.learner_enrolments le
LEFT JOIN public.setting_education_grades seg
    ON seg.id = le.education_grade_id
LEFT JOIN public.setting_education_levels el
    ON el.id = seg.education_level_id
LEFT JOIN public.setting_school_types st
    ON st.id = el.school_type_id
LEFT JOIN public.setting_familiar_languages sfl
    ON sfl.id = le.familiar_language_id
WHERE le.is_enrolment_active_yn = TRUE
ON CONFLICT (enrolment_id) DO UPDATE
    SET education_class_name = EXCLUDED.education_class_name,
        school_level         = EXCLUDED.school_level;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.enrolments_subtype_raw;
    RAISE NOTICE 'stg.enrolments_subtype_raw: % rows', v;
END; $$;


-- ── 0e: learner_promotions_raw ───────────────────────────────────────────────
TRUNCATE TABLE stg.learner_promotions_raw;

INSERT INTO stg.learner_promotions_raw (
    id, learner_id, school_id, academic_year_id,
    from_class_id, to_class_id,
    promotion_status_id, promotion_status_name,
    date_created, date_updated
)
SELECT
    lp.enrolment_id         AS id,
    lp.learner_id,
    lp.school_id,
    lp.academic_year_id,
    lp.education_grade_id   AS from_class_id,
    NULL                    AS to_class_id,
    lp.promotion_status_id,
    sps.name                AS promotion_status_name,
    lp.date_created,
    lp.date_updated
FROM public.learner_promotions lp
LEFT JOIN public.setting_promotion_statuses sps
    ON sps.id = lp.promotion_status_id
ON CONFLICT DO NOTHING;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.learner_promotions_raw;
    RAISE NOTICE 'stg.learner_promotions_raw: % rows', v;
END; $$;


-- ── 0f: learner_transitions_raw ──────────────────────────────────────────────
TRUNCATE TABLE stg.learner_transitions_raw;

INSERT INTO stg.learner_transitions_raw (
    id, learner_id, from_school_id, to_school_id,
    academic_year_id, transition_status_id, transition_status_name,
    date_created, date_updated
)
SELECT
    lt.enrolment_id         AS id,
    lt.learner_id,
    NULL                    AS from_school_id,
    lt.school_id            AS to_school_id,
    lt.academic_year_id,
    lt.promotion_status_id  AS transition_status_id,
    sps.name                AS transition_status_name,
    lt.date_created,
    lt.date_updated
FROM public.learner_transitions lt
LEFT JOIN public.setting_promotion_statuses sps
    ON sps.id = lt.promotion_status_id
ON CONFLICT DO NOTHING;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.learner_transitions_raw;
    RAISE NOTICE 'stg.learner_transitions_raw: % rows', v;
END; $$;

COMMIT;
