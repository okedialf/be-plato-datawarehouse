-- =============================================================================
-- EMIS Non-Teaching Staff Mart ETL: Step 0 — Extract from Source
--
-- School assignment: non_teaching_staff_postings is empty.
-- School_id comes from school_employees.school_id (55,769 records)
-- with non_teaching_staff.current_school_id as fallback (55,810 records).
-- Both are used via COALESCE in the extract.
-- =============================================================================

BEGIN;

-- ── Extract non_teaching_staff_raw ───────────────────────────────────────────
-- One row per staff member using DISTINCT ON person_id.
TRUNCATE TABLE stg.non_teaching_staff_raw;

INSERT INTO stg.non_teaching_staff_raw (
    person_id,
    employee_id,
    nts_employee_id,
    surname,
    given_name,
    gender,
    date_of_birth,
    nationality,
    nin,
    category,
    role,
    is_transfer_appointment,
    appointment_date,
    posting_date,
    current_school_id,
    highest_education_level,
    employment_status,
    is_on_government_payroll,
    ipps_number,
    hrin,
    hcm_employee_number,
    date_started,
    date_ended,
    is_undergoing_training,
    date_created,
    date_updated
)
SELECT DISTINCT ON (nts.person_id)
    nts.person_id,
    se.id                                               AS employee_id,
    nts.employee_id                                     AS nts_employee_id,
    p.surname,
    TRIM(COALESCE(p.first_name,'') || ' '
         || COALESCE(p.other_names,''))                 AS given_name,
    p.gender,
    p.birth_date                                        AS date_of_birth,
    COALESCE(ac.name, 'UGANDA')                         AS nationality,
    p.id_number                                         AS nin,
    cat.name                                            AS category,
    rol.name                                            AS role,
    nts.is_transfer_appointment,
    nts.appointment_date,
    nts.posting_date,
    -- School: prefer school_employees.school_id, fall back to current_school_id
    COALESCE(se.school_id, nts.current_school_id)       AS current_school_id,
    sel.name                                            AS highest_education_level,
    ses.name                                            AS employment_status,
    COALESCE(se.is_on_government_payroll, FALSE)        AS is_on_government_payroll,
    se.ipps_number,
    se.hrin,
    se.hcm_employee_number,
    se.date_started,
    se.date_ended,
    se.is_undergoing_training,
    GREATEST(COALESCE(nts.date_updated, nts.date_created),
             COALESCE(se.date_updated, se.date_created))  AS date_created,
    GREATEST(COALESCE(nts.date_updated, nts.date_created),
             COALESCE(se.date_updated, se.date_created))  AS date_updated
FROM public.non_teaching_staff nts
JOIN public.persons p
    ON p.id = nts.person_id
LEFT JOIN public.admin_unit_countries ac
    ON ac.id = p.country_id
LEFT JOIN public.school_employees se
    ON se.id = nts.employee_id
LEFT JOIN public.setting_non_teaching_staff_categories cat
    ON cat.id = nts.non_teaching_staff_category_id
LEFT JOIN public.setting_non_teaching_staff_roles rol
    ON rol.id = nts.non_teaching_staff_role_id
LEFT JOIN public.setting_education_levels sel
    ON sel.id = se.highest_education_level_id
LEFT JOIN public.setting_employment_statuses ses
    ON ses.id = se.employment_status_id
WHERE p.deleted_at IS NULL
ORDER BY nts.person_id, se.date_updated DESC NULLS LAST;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.non_teaching_staff_raw;
    RAISE NOTICE 'stg.non_teaching_staff_raw: % rows', v;
END; $$;

-- NOTE: non_teaching_staff_school_raw is not used —
-- non_teaching_staff_postings table is empty in this system.
-- School assignment comes directly from school_employees.school_id.

COMMIT;
