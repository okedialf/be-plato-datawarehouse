-- =============================================================================
-- EMIS Teachers Mart ETL: Step 0 — Extract from Source
-- All tables reference public schema directly.
--
-- Source table chain:
--   public.persons         → personal identity
--   public.teachers        → teacher professional data (person_id BIGINT)
--   public.school_employees → employment record (id BIGINT)
--   public.teaching_staff  → school deployment (employee_id BIGINT)
--   public.teacher_subjects → subjects taught (employee_id BIGINT)
--
-- Key lesson from enrolment mart:
--   ALL IDs are BIGINT — no UUID columns used here
--   teaching_staff has NO teaching_period_id — terms generated from
--   academic_year_teaching_periods in the flatten step
-- =============================================================================

BEGIN;

-- ── 0a: Extract teachers_raw ──────────────────────────────────────────────────
-- Joins: persons + teachers + school_employees
-- One row per teacher (not per school — that happens in teachers_flat)
TRUNCATE TABLE stg.teachers_raw;

INSERT INTO stg.teachers_raw (
    person_id,
    employee_id,
    surname,
    given_name,
    gender,
    date_of_birth,
    nationality,
    nin,
    passport_no,
    teacher_type,
    qualification,
    qualification_category,
    designation,
    ipps_number,
    tmis_number,
    moes_license_number,
    is_on_government_payroll,
    first_appointment_date,
    posting_date,
    subject_category,
    highest_education_level,
    employment_status,
    hrin,
    hcm_employee_number,
    date_started,
    date_ended,
    weekly_teaching_periods,
    is_undergoing_training,
    current_school_id,
    date_created,
    date_updated
)
SELECT
    t.person_id,
    se.id                                               AS employee_id,
    p.surname,
    TRIM(COALESCE(p.first_name,'') || ' '
         || COALESCE(p.other_names,''))                 AS given_name,
    p.gender,
    p.birth_date                                        AS date_of_birth,
    COALESCE(ac.name, 'UGANDA')                         AS nationality,

    -- NIN from identity_documents
    MAX(CASE WHEN UPPER(idt.name) LIKE '%NATIONAL%'
              OR UPPER(idt.name) LIKE '%NIN%'
             THEN id_docs.document_id END)              AS nin,
    MAX(CASE WHEN UPPER(idt.name) LIKE '%PASSPORT%'
             THEN id_docs.document_id END)              AS passport_no,

    -- Teacher professional attributes
    stt.name                                            AS teacher_type,
    stpq.name                                           AS qualification,
    stpq.qualification_category,
    std.name                                            AS designation,
    COALESCE(t.ipps_number, se.ipps_number)             AS ipps_number,
    t.tmis_number,
    t.moes_license_number,
    COALESCE(t.is_on_government_payroll,
             se.is_on_government_payroll)               AS is_on_government_payroll,
    t.first_appointment_date,
    t.posting_date,
    t.subject_category,

    -- Employment attributes from school_employees
    sel.name                                            AS highest_education_level,
    ses.name                                            AS employment_status,
    se.hrin,
    se.hcm_employee_number,
    se.date_started,
    se.date_ended,
    se.weekly_teaching_periods,
    se.is_undergoing_training,

    -- Current school
    t.current_school_id,

    GREATEST(t.date_updated, se.date_updated)           AS date_created,
    GREATEST(t.date_updated, se.date_updated)           AS date_updated

FROM public.teachers t
JOIN public.persons p
    ON p.id = t.person_id
LEFT JOIN public.admin_unit_countries ac
    ON ac.id = p.country_id
LEFT JOIN public.identity_documents id_docs
    ON id_docs.person_id = p.id
LEFT JOIN public.setting_identity_document_types idt
    ON idt.id = id_docs.identity_type_id
LEFT JOIN public.school_employees se
    ON se.person_id = t.person_id
    AND se.is_teaching_staff = TRUE
LEFT JOIN public.setting_teacher_types stt
    ON stt.id = t.teacher_type_id
LEFT JOIN public.setting_teacher_professional_qualifications stpq
    ON stpq.id = t.qualification_id
LEFT JOIN public.setting_teaching_staff_designations std
    ON std.id = t.designation_id
LEFT JOIN public.setting_education_levels sel
    ON sel.id = se.highest_education_level_id
LEFT JOIN public.setting_employment_statuses ses
    ON ses.id = se.employment_status_id
WHERE p.deleted_at IS NULL

GROUP BY
    t.person_id, se.id, p.surname, p.first_name, p.other_names,
    p.birth_date, p.gender, ac.name,
    stt.name, stpq.name, stpq.qualification_category, std.name,
    t.ipps_number, se.ipps_number, t.tmis_number, t.moes_license_number,
    t.is_on_government_payroll, se.is_on_government_payroll,
    t.first_appointment_date, t.posting_date, t.subject_category,
    sel.name, ses.name, se.hrin, se.hcm_employee_number,
    se.date_started, se.date_ended, se.weekly_teaching_periods,
    se.is_undergoing_training, t.current_school_id,
    t.date_updated, se.date_updated;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.teachers_raw;
    RAISE NOTICE 'stg.teachers_raw: % rows', v;
END; $$;


-- ── 0b: Extract teacher_subjects_raw ─────────────────────────────────────────
-- One row per teacher per subject.
-- Used to derive discipline (ARTS/SCIENCE) in flatten step.
-- teacher_subjects.employee_id → school_employees.id → teachers.person_id
TRUNCATE TABLE stg.teacher_subjects_raw;

INSERT INTO stg.teacher_subjects_raw (
    id,
    employee_id,
    person_id,
    subject_id,
    subject_name,
    is_science_subject,
    is_language_subject,
    date_created,
    date_updated
)
SELECT
    ts.id,
    ts.employee_id,
    se.person_id,
    ts.subject_id,
    sub.name                AS subject_name,
    sub.is_science_subject,
    sub.is_language_subject,
    ts.date_created,
    ts.date_updated
FROM public.teacher_subjects ts
JOIN public.school_employees se
    ON se.id = ts.employee_id
JOIN public.setting_secondary_school_subjects sub
    ON sub.id = ts.subject_id
WHERE sub.deleted_at IS NULL
  AND sub.is_active_yn = TRUE;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.teacher_subjects_raw;
    RAISE NOTICE 'stg.teacher_subjects_raw: % rows', v;
END; $$;


-- ── 0c: Extract teacher_school_raw ───────────────────────────────────────────
-- Current teacher-to-school deployment from teaching_staff.
-- One row per employee (current deployment only).
TRUNCATE TABLE stg.teacher_school_raw;

INSERT INTO stg.teacher_school_raw (
    employee_id,
    person_id,
    school_id,
    is_transfer_appointment,
    appointment_date,
    staff_category,
    date_created,
    date_updated
)
SELECT DISTINCT ON (ts.employee_id)
    ts.employee_id,
    ts.person_id,
    ts.school_id,
    ts.is_transfer_appointment,
    ts.appointment_date,
    ts.staff_category,
    ts.date_created,
    ts.date_updated
FROM public.teaching_staff ts
WHERE ts.school_id IS NOT NULL
ORDER BY ts.employee_id, ts.date_updated DESC NULLS LAST;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.teacher_school_raw;
    RAISE NOTICE 'stg.teacher_school_raw: % rows', v;
END; $$;

COMMIT;
