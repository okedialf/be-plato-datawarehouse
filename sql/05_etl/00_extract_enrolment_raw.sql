-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 0 — Extract from Read-Only Replica
-- Extracts all enrolment-related source tables into stg schema via postgres_fdw.
-- Run nightly (or termly for full loads).
-- =============================================================================

BEGIN;

-- ── 0a: Extract learners_raw ──────────────────────────────────────────────────
-- Joins src.learners with src.persons to flatten all learner attributes.
-- Full refresh: all learners (not just changed ones) for simplicity.
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
    p.given_name,
    p.date_of_birth,
    p.gender,
    COALESCE(cn.country_name, 'UGANDA')         AS nationality,
    l.district_of_birth_id,
    p.nin,
    p.passport_no,
    p.student_pass_no,
    p.work_permit_no,
    p.refugee_id,
    l.is_flagged_yn,
    l.deleted_at,
    l.date_created,
    l.date_updated
FROM src.learners l
JOIN src.persons p
    ON p.id = l.person_id
LEFT JOIN src.lkup_countries cn
    ON cn.id = p.nationality_id
WHERE l.deleted_at IS NULL;   -- Exclude soft-deleted learners


-- ── 0b: Extract learner_disabilities_raw ─────────────────────────────────────
TRUNCATE TABLE stg.learner_disabilities_raw;

INSERT INTO stg.learner_disabilities_raw (
    id, learner_id, disability_type, severity, date_created, date_updated
)
SELECT
    ld.id,
    ld.learner_id,
    dt.name          AS disability_type,   -- VISUAL, HEARING, WALKING etc.
    ld.severity,
    ld.date_created,
    ld.date_updated
FROM src.learner_disabilities ld
JOIN src.lkup_disability_types dt
    ON dt.id = ld.disability_type_id;


-- ── 0c: Extract enrolments_raw ───────────────────────────────────────────────
-- Full refresh of current academic year + 4 historical years on first load.
-- For nightly delta: filter by date_updated >= last_success_ts from watermark.
TRUNCATE TABLE stg.enrolments_raw;

INSERT INTO stg.enrolments_raw (
    id, learner_id, school_id,
    academic_year_id, teaching_period_id, reporting_status_id,
    learner_promotion_status_id, learner_transfer_status_id,
    learner_transition_status_id,
    learner_registration_number, is_enrolment_active_yn,
    date_created, date_updated
)
SELECT
    e.id,
    e.learner_id,
    e.school_id,
    e.academic_year_id,
    e.teaching_period_id,
    e.reporting_status_id,
    e.learner_promotion_status_id,
    e.learner_transfer_status_id,
    e.learner_transition_status_id,
    e.learner_registration_number,
    e.is_enrolment_active_yn,
    e.date_created,
    e.date_updated
FROM src.enrolments e
WHERE e.is_enrolment_active_yn = TRUE;


-- ── 0d: Extract enrolments_subtype_raw ───────────────────────────────────────
-- Consolidate from all school-level subtype tables.
TRUNCATE TABLE stg.enrolments_subtype_raw;

-- Pre-primary
INSERT INTO stg.enrolments_subtype_raw (
    enrolment_id, school_level, education_class_id,
    education_class_name, familiar_language_id, familiar_language_name, date_created
)
SELECT
    ppe.enrolment_id,
    'PRE_PRIMARY'               AS school_level,
    ppe.education_class_id,
    ec.name                     AS education_class_name,
    ppe.familiar_language_id,
    fl.name                     AS familiar_language_name,
    ppe.date_created
FROM src.pre_primary_enrolments ppe
LEFT JOIN src.lkup_education_classes ec ON ec.id = ppe.education_class_id
LEFT JOIN src.lkup_familiar_languages fl ON fl.id = ppe.familiar_language_id;

-- Primary
INSERT INTO stg.enrolments_subtype_raw (
    enrolment_id, school_level, education_class_id,
    education_class_name, familiar_language_id, familiar_language_name, date_created
)
SELECT
    pe.enrolment_id,
    'PRIMARY'                   AS school_level,
    pe.education_class_id,
    ec.name,
    pe.familiar_language_id,
    fl.name,
    pe.date_created
FROM src.primary_enrolments pe
LEFT JOIN src.lkup_education_classes ec ON ec.id = pe.education_class_id
LEFT JOIN src.lkup_familiar_languages fl ON fl.id = pe.familiar_language_id;

-- Secondary
INSERT INTO stg.enrolments_subtype_raw (
    enrolment_id, school_level, education_class_id,
    education_class_name, familiar_language_id, familiar_language_name, date_created
)
SELECT
    se.enrolment_id,
    'SECONDARY'                 AS school_level,
    se.education_class_id,
    ec.name,
    se.familiar_language_id,
    fl.name,
    se.date_created
FROM src.secondary_enrolments se
LEFT JOIN src.lkup_education_classes ec ON ec.id = se.education_class_id
LEFT JOIN src.lkup_familiar_languages fl ON fl.id = se.familiar_language_id
ON CONFLICT (enrolment_id) DO NOTHING;

-- Certificate
INSERT INTO stg.enrolments_subtype_raw (
    enrolment_id, school_level, education_class_id, education_class_name, date_created
)
SELECT
    ce.enrolment_id,
    'CERTIFICATE'               AS school_level,
    ce.education_class_id,
    ec.name,
    ce.date_created
FROM src.certificate_enrolments ce
LEFT JOIN src.lkup_education_classes ec ON ec.id = ce.education_class_id
ON CONFLICT (enrolment_id) DO NOTHING;

-- Diploma
INSERT INTO stg.enrolments_subtype_raw (
    enrolment_id, school_level, education_class_id, education_class_name, date_created
)
SELECT
    de.enrolment_id,
    'DIPLOMA'                   AS school_level,
    de.education_class_id,
    ec.name,
    de.date_created
FROM src.diploma_enrolments de
LEFT JOIN src.lkup_education_classes ec ON ec.id = de.education_class_id
ON CONFLICT (enrolment_id) DO NOTHING;


-- ── 0e: Extract learner_promotions_raw ───────────────────────────────────────
TRUNCATE TABLE stg.learner_promotions_raw;

INSERT INTO stg.learner_promotions_raw (
    id, learner_id, school_id, academic_year_id,
    from_class_id, to_class_id,
    promotion_status_id, promotion_status_name,
    date_created, date_updated
)
SELECT
    lp.id,
    lp.learner_id,
    lp.school_id,
    lp.academic_year_id,
    lp.from_class_id,
    lp.to_class_id,
    lp.promotion_status_id,
    ps.name         AS promotion_status_name,
    lp.date_created,
    lp.date_updated
FROM src.learner_promotions lp
LEFT JOIN src.lkup_promotion_statuses ps
    ON ps.id = lp.promotion_status_id;


-- ── 0f: Extract learner_transitions_raw ──────────────────────────────────────
TRUNCATE TABLE stg.learner_transitions_raw;

INSERT INTO stg.learner_transitions_raw (
    id, learner_id, from_school_id, to_school_id,
    academic_year_id, transition_status_id, transition_status_name,
    date_created, date_updated
)
SELECT
    lt.id,
    lt.learner_id,
    lt.from_school_id,
    lt.to_school_id,
    lt.academic_year_id,
    lt.transition_status_id,
    ts.name         AS transition_status_name,
    lt.date_created,
    lt.date_updated
FROM src.learner_transitions lt
LEFT JOIN src.lkup_transition_statuses ts
    ON ts.id = lt.transition_status_id;

COMMIT;
