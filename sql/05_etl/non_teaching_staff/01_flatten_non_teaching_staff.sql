-- =============================================================================
-- EMIS Non-Teaching Staff Mart ETL: Step 1 — Flatten Non-Teaching Staff
-- One row per staff member per school per term.
--
-- School source: non_teaching_staff_raw.current_school_id
-- (populated from COALESCE(school_employees.school_id, non_teaching_staff.current_school_id)
-- in the extract step — non_teaching_staff_postings is empty in this system)
--
-- DISTINCT ON academic_year_teaching_periods to prevent 4x row duplication.
-- =============================================================================

BEGIN;

TRUNCATE TABLE stg.non_teaching_staff_flat RESTART IDENTITY;

INSERT INTO stg.non_teaching_staff_flat (
    person_id,
    employee_id,
    school_id,
    academic_year,
    term,
    term_start_date,
    surname,
    given_name,
    gender,
    age,
    nationality,
    nin,
    category,
    role,
    posting_date,
    highest_education_level,
    employment_status,
    is_on_government_payroll,
    ipps_number,
    hrin,
    hcm_employee_number,
    is_undergoing_training,
    school_emis_number,
    school_name,
    school_type,
    school_ownership,
    admin_unit_source_id,
    is_primary_school
)
SELECT
    ntr.person_id,
    ntr.employee_id,
    ntr.current_school_id                       AS school_id,

    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM aytp.start_date)::INTEGER
    )                                           AS academic_year,

    CASE
        WHEN tp.name IN ('1','2','3') THEN 'TERM ' || tp.name
        WHEN tp.name ILIKE 'TERM%'   THEN UPPER(tp.name)
        ELSE 'TERM 1'
    END                                         AS term,

    aytp.start_date                             AS term_start_date,

    ntr.surname,
    ntr.given_name,
    ntr.gender,
    CASE
        WHEN ntr.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER
             - EXTRACT(YEAR FROM ntr.date_of_birth)::INTEGER
        ELSE NULL
    END                                         AS age,
    ntr.nationality,
    ntr.nin,
    ntr.category,
    ntr.role,
    ntr.posting_date,
    ntr.highest_education_level,
    ntr.employment_status,
    ntr.is_on_government_payroll,
    ntr.ipps_number,
    ntr.hrin,
    ntr.hcm_employee_number,
    ntr.is_undergoing_training,

    s.emis_number                               AS school_emis_number,
    s.name                                      AS school_name,
    sst.name                                    AS school_type,
    COALESCE(sos.name,
        CASE WHEN s.is_government_owned_yn
             THEN 'GOVT AIDED' ELSE 'PRIVATE' END) AS school_ownership,

    COALESCE(
        s.parish_id, s.sub_county_id,
        s.county_id, s.district_id, s.region_id
    )                                           AS admin_unit_source_id,

    -- is_primary_school always TRUE since each staff has one school
    TRUE                                        AS is_primary_school

FROM stg.non_teaching_staff_raw ntr

-- Cross with active terms — DISTINCT ON prevents 4x duplication
JOIN (
    SELECT DISTINCT ON (academic_year_id, teaching_period_id)
        academic_year_id,
        teaching_period_id,
        start_date,
        end_date
    FROM public.academic_year_teaching_periods
    WHERE status    = 'active'
      AND school_id IS NULL
    ORDER BY academic_year_id, teaching_period_id, school_type_id ASC
) aytp ON TRUE

JOIN public.setting_academic_years   ay ON ay.id = aytp.academic_year_id
JOIN public.setting_teaching_periods tp ON tp.id = aytp.teaching_period_id

-- School lookups
JOIN public.schools s
    ON s.id = ntr.current_school_id
LEFT JOIN public.setting_school_types       sst ON sst.id = s.school_type_id
LEFT JOIN public.setting_ownership_statuses sos ON sos.id = s.school_ownership_status_id

WHERE ntr.current_school_id IS NOT NULL;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.non_teaching_staff_flat;
    RAISE NOTICE 'stg.non_teaching_staff_flat: % rows', v;
END; $$;

COMMIT;
