-- =============================================================================
-- EMIS Teachers Mart ETL: Step 1 — Flatten Teachers
-- Produces stg.teachers_flat: one row per teacher per school per term.
--
-- KEY DESIGN NOTES:
-- 1. Term rows are generated from academic_year_teaching_periods (lesson from
--    enrolment mart — teaching_staff has NO teaching_period_id column).
--    We cross-join each teacher-school pair with all active terms.
--
-- 2. Discipline derived here by aggregating teacher_subjects_raw:
--    ALL sciences → SCIENCE
--    ALL arts     → ARTS
--    Mixed        → BOTH
--    No subjects  → NULL
--
-- 3. Location resolved via schools.parish_id / sub_county_id / district_id
--    pointing to dw.admin_units_dim.source_id (DW admin unit structure).
--
-- 4. academic_year_teaching_periods.teaching_period_id → setting_teaching_periods.name
--    Same pattern used in enrolment mart.
-- =============================================================================

BEGIN;

TRUNCATE TABLE stg.teachers_flat RESTART IDENTITY;

INSERT INTO stg.teachers_flat (
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
    passport_no,
    teacher_type,
    qualification,
    qualification_category,
    designation,
    discipline,
    ipps_number,
    tmis_number,
    moes_license_number,
    is_on_government_payroll,
    first_appointment_date,
    posting_date,
    highest_education_level,
    employment_status,
    hrin,
    hcm_employee_number,
    weekly_teaching_periods,
    is_undergoing_training,
    school_emis_number,
    school_name,
    school_type,
    school_ownership,
    school_level,
    admin_unit_source_id
)
SELECT
    tr.person_id,
    tr.employee_id,
    tsr.school_id,

    -- Academic year from academic_year_teaching_periods
    -- Name is like "ACADEMIC YEAR 2026" — extract year digits
    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM aytp.start_date)::INTEGER
    )                                           AS academic_year,

    -- Term name — setting_teaching_periods.name is "1","2","3"
    CASE
        WHEN tp.name IN ('1','2','3') THEN 'TERM ' || tp.name
        WHEN tp.name ILIKE 'TERM%'   THEN UPPER(tp.name)
        ELSE 'TERM 1'
    END                                         AS term,

    aytp.start_date                             AS term_start_date,

    -- Personal
    tr.surname,
    tr.given_name,
    tr.gender,
    CASE
        WHEN tr.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER
             - EXTRACT(YEAR FROM tr.date_of_birth)::INTEGER
        ELSE NULL
    END                                         AS age,
    tr.nationality,
    tr.nin,
    tr.passport_no,

    -- Professional
    tr.teacher_type,
    tr.qualification,
    tr.qualification_category,
    tr.designation,

    -- Discipline: derived from teacher_subjects_raw
    -- SCIENCE if all subjects are science
    -- ARTS if all subjects are arts
    -- BOTH if mixed
    CASE
        WHEN disc.total_subjects = 0      THEN NULL
        WHEN disc.science_count = disc.total_subjects THEN 'SCIENCE'
        WHEN disc.science_count = 0       THEN 'ARTS'
        ELSE 'BOTH'
    END                                         AS discipline,

    tr.ipps_number,
    tr.tmis_number,
    tr.moes_license_number,
    tr.is_on_government_payroll,
    tr.first_appointment_date,
    tr.posting_date,
    tr.highest_education_level,
    tr.employment_status,
    tr.hrin,
    tr.hcm_employee_number,
    tr.weekly_teaching_periods,
    tr.is_undergoing_training,

    -- School attributes (denormalized)
    s.emis_number                               AS school_emis_number,
    s.name                                      AS school_name,
    sst.name                                    AS school_type,
    COALESCE(sos.name,
        CASE WHEN s.is_government_owned_yn
             THEN 'GOVT AIDED' ELSE 'PRIVATE' END) AS school_ownership,
    sel.name                                    AS school_level,

    -- Admin unit: best available level (same COALESCE pattern as schools mart)
    COALESCE(
        s.parish_id,
        s.sub_county_id,
        s.county_id,
        s.district_id,
        s.region_id
    )                                           AS admin_unit_source_id

FROM stg.teachers_raw tr

-- Teacher current school deployment
JOIN stg.teacher_school_raw tsr
    ON tsr.person_id = tr.person_id

-- Cross with all active academic year+term combinations
-- This generates one row per teacher per school per term
JOIN public.academic_year_teaching_periods aytp
    ON (aytp.school_id IS NULL             -- global term (not school-specific)
    OR  aytp.school_id = tsr.school_id)    -- or school-specific term
    AND aytp.status = 'active'

JOIN public.setting_academic_years  ay ON ay.id = aytp.academic_year_id
JOIN public.setting_teaching_periods tp ON tp.id = aytp.teaching_period_id

-- Discipline aggregation per teacher
LEFT JOIN (
    SELECT
        person_id,
        COUNT(*)                                AS total_subjects,
        COUNT(*) FILTER (WHERE is_science_subject = TRUE) AS science_count
    FROM stg.teacher_subjects_raw
    GROUP BY person_id
) disc ON disc.person_id = tr.person_id

-- School lookups
JOIN public.schools s
    ON s.id = tsr.school_id
LEFT JOIN public.setting_school_types        sst ON sst.id = s.school_type_id
LEFT JOIN public.setting_ownership_statuses  sos ON sos.id = s.school_ownership_status_id
LEFT JOIN public.setting_education_levels    sel ON sel.id = s.school_type_id

WHERE tsr.school_id IS NOT NULL;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.teachers_flat;
    RAISE NOTICE 'stg.teachers_flat: % rows', v;
END; $$;

COMMIT;
