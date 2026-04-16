-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 1 — Flatten Enrolments (GCP Testing Version)
-- Joins all raw staging tables into stg.enrolments_flat.
--
-- KEY CORRECTIONS from data dictionary review:
--   Academic year decoded from setting_academic_years.name (e.g. "2025")
--   Teaching period from setting_teaching_periods.name
--   Orphan status from learners.is_father_dead + learners.is_mother_dead
--   Enrolment type from learner_enrolments.enrolment_type_id → setting_enrolment_types
--   Repeater from learner_enrolments.is_repeating_yn
--   Returnee from learner_enrolments.is_returning_yn
--   Grade and school level from stg.enrolments_subtype_raw (decoded in step 0)
--   School attributes from public.schools + setting_* lookup tables
--   Admin unit: COALESCE(parish_id, sub_county_id, county_id, district_id, region_id)
-- =============================================================================

BEGIN;

TRUNCATE TABLE stg.enrolments_flat RESTART IDENTITY;

INSERT INTO stg.enrolments_flat (
    enrolment_source_id,
    learner_source_id,
    school_source_id,
    academic_year,
    term,
    lin,
    gender,
    age,
    nationality,
    nin,
    passport_no,
    student_pass_no,
    work_permit_no,
    refugee_id,
    orphan_status,
    is_visual_yn,
    is_hearing_yn,
    is_walking_yn,
    is_self_care_yn,
    is_remembering_yn,
    is_communication_yn,
    has_multiple_disabilities_yn,
    grade,
    school_level,
    enrolment_type,
    is_active,
    school_emis_number,
    school_name,
    school_ownership,
    school_funding_type,
    school_sex_composition,
    school_boarding_status,
    school_type,
    admin_unit_source_id
)
SELECT
    e.id                                            AS enrolment_source_id,
    e.learner_id                                    AS learner_source_id,
    e.school_id                                     AS school_source_id,

    -- Academic year: extract 4-digit year from setting_academic_years.name
    -- e.g. "ACADEMIC YEAR 2026" → 2026
    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM aytp.start_date)::INTEGER
    )                                               AS academic_year,

    -- Teaching period: name is "1", "2", "3" — prefix with TERM
    CASE
        WHEN tp.name IN ('1','2','3') THEN 'TERM ' || tp.name
        WHEN tp.name ILIKE 'TERM%'   THEN UPPER(tp.name)
        ELSE 'TERM 1'
    END                                             AS term,

    -- Learner attributes from stg.learners_raw
    lr.lin,
    lr.gender,
    CASE
        WHEN lr.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER
             - EXTRACT(YEAR FROM lr.date_of_birth)::INTEGER
        ELSE NULL
    END                                             AS age,
    lr.nationality,
    lr.nin,
    lr.passport_no,
    lr.student_pass_no,
    lr.work_permit_no,
    lr.refugee_id,

    -- Orphan status: from learners table directly
    -- learners.is_father_dead and learners.is_mother_dead
    CASE
        WHEN l_src.is_father_dead = TRUE
         AND l_src.is_mother_dead = TRUE THEN 'BOTH DECEASED'
        WHEN l_src.is_mother_dead = TRUE  THEN 'MOTHER DECEASED'
        WHEN l_src.is_father_dead = TRUE  THEN 'FATHER DECEASED'
        ELSE 'NOT APPLICABLE'
    END                                             AS orphan_status,

    -- Disability flags aggregated from learner_disabilities_raw
    COALESCE(dis.is_visual_yn,        FALSE)        AS is_visual_yn,
    COALESCE(dis.is_hearing_yn,       FALSE)        AS is_hearing_yn,
    COALESCE(dis.is_walking_yn,       FALSE)        AS is_walking_yn,
    COALESCE(dis.is_self_care_yn,     FALSE)        AS is_self_care_yn,
    COALESCE(dis.is_remembering_yn,   FALSE)        AS is_remembering_yn,
    COALESCE(dis.is_communication_yn, FALSE)        AS is_communication_yn,
    CASE WHEN (
        COALESCE(dis.is_visual_yn,        FALSE)::INT +
        COALESCE(dis.is_hearing_yn,       FALSE)::INT +
        COALESCE(dis.is_walking_yn,       FALSE)::INT +
        COALESCE(dis.is_self_care_yn,     FALSE)::INT +
        COALESCE(dis.is_remembering_yn,   FALSE)::INT +
        COALESCE(dis.is_communication_yn, FALSE)::INT
    ) > 1 THEN TRUE ELSE FALSE END                  AS has_multiple_disabilities_yn,

    -- Grade and school level from decoded subtype staging
    sub.education_class_name                        AS grade,
    sub.school_level,

    -- Enrolment type: sourced from setting_enrolment_types (BASELINE, NEW, CONTINUING, TRANSFER)
    -- is_repeating_yn and is_returning_yn are additional flags stored on learner_dim,
    -- not separate enrolment types in the source system.
    CASE
        WHEN set_et.name IS NOT NULL THEN UPPER(set_et.name)
        WHEN le.is_returning_yn = TRUE THEN 'CONTINUING'   -- returnees map to CONTINUING
        ELSE 'CONTINUING'
    END                                             AS enrolment_type,

    e.is_enrolment_active_yn                        AS is_active,

    -- School attributes
    s.emis_number                                   AS school_emis_number,
    s.name                                          AS school_name,
    COALESCE(sos.name,
        CASE WHEN s.is_government_owned_yn THEN 'GOVT AIDED'
             ELSE 'PRIVATE' END)                    AS school_ownership,
    COALESCE(sfs.name, '')                          AS school_funding_type,
    CASE
        WHEN s.has_female_students AND s.has_male_students THEN 'MIXED'
        WHEN s.has_female_students                         THEN 'FEMALES ONLY'
        WHEN s.has_male_students                           THEN 'MALES ONLY'
        ELSE NULL
    END                                             AS school_sex_composition,

    -- Boarding status from subtype tables
    CASE
        WHEN COALESCE(pps.admits_day_scholars_yn, prs.admits_day_scholars_yn,
                      sec.admits_day_scholars_yn) = TRUE
         AND COALESCE(pps.admits_boarders_yn, prs.admits_boarders_yn,
                      sec.admits_boarders_yn) = FALSE
            THEN 'DAY SCHOOL'
        WHEN COALESCE(pps.admits_day_scholars_yn, prs.admits_day_scholars_yn,
                      sec.admits_day_scholars_yn) = FALSE
         AND COALESCE(pps.admits_boarders_yn, prs.admits_boarders_yn,
                      sec.admits_boarders_yn) = TRUE
            THEN 'FULLY BOARDING'
        WHEN COALESCE(pps.admits_boarders_yn, prs.admits_boarders_yn,
                      sec.admits_boarders_yn) = TRUE
         AND COALESCE(pps.admits_day_scholars_yn, prs.admits_day_scholars_yn,
                      sec.admits_day_scholars_yn) = TRUE
            THEN 'DAY AND BOARDING'
        ELSE NULL
    END                                             AS school_boarding_status,

    COALESCE(sst.name, '')                          AS school_type,

    -- Admin unit: best available level
    COALESCE(
        s.parish_id,
        s.sub_county_id,
        s.county_id,
        s.district_id,
        s.region_id
    )::INTEGER                                      AS admin_unit_source_id

FROM stg.enrolments_raw e

-- Decode academic year and teaching period
-- learner_enrolments.teaching_period_id is a FK to academic_year_teaching_periods.id
-- which links to both setting_academic_years and setting_teaching_periods
JOIN public.academic_year_teaching_periods aytp ON aytp.id = e.teaching_period_id
JOIN public.setting_academic_years  ay ON ay.id = aytp.academic_year_id
JOIN public.setting_teaching_periods tp ON tp.id = aytp.teaching_period_id

-- Join back to learner_enrolments for enrolment-specific flags
JOIN public.learner_enrolments le
    ON le.id = e.id

-- Learner from staging
JOIN stg.learners_raw lr
    ON lr.person_id = e.learner_id

-- Source learners table for orphan status flags
JOIN public.learners l_src
    ON l_src.person_id = e.learner_id

-- Disability flags aggregated per learner
LEFT JOIN (
    SELECT
        learner_id,
        BOOL_OR(UPPER(disability_type) LIKE '%VISUAL%')        AS is_visual_yn,
        BOOL_OR(UPPER(disability_type) LIKE '%HEARING%')       AS is_hearing_yn,
        BOOL_OR(UPPER(disability_type) LIKE '%WALK%'
             OR UPPER(disability_type) LIKE '%MOBIL%')         AS is_walking_yn,
        BOOL_OR(UPPER(disability_type) LIKE '%SELF%CARE%'
             OR UPPER(disability_type) LIKE '%SELF_CARE%')     AS is_self_care_yn,
        BOOL_OR(UPPER(disability_type) LIKE '%REMEMBER%'
             OR UPPER(disability_type) LIKE '%CONCENTRAT%'
             OR UPPER(disability_type) LIKE '%COGNIT%')        AS is_remembering_yn,
        BOOL_OR(UPPER(disability_type) LIKE '%COMMUNIC%')      AS is_communication_yn
    FROM stg.learner_disabilities_raw
    GROUP BY learner_id
) dis ON dis.learner_id = e.learner_id

-- Grade and school level from subtype staging
LEFT JOIN stg.enrolments_subtype_raw sub
    ON sub.enrolment_id = e.id

-- Enrolment type lookup
LEFT JOIN public.setting_enrolment_types set_et
    ON set_et.id = le.enrolment_type_id

-- School
JOIN public.schools s
    ON s.id = e.school_id
LEFT JOIN public.setting_school_types      sst ON sst.id = s.school_type_id
LEFT JOIN public.setting_ownership_statuses sos ON sos.id = s.school_ownership_status_id
LEFT JOIN public.setting_funding_sources    sfs ON sfs.id = s.funding_source_id

-- Boarding status from school subtype tables
LEFT JOIN public.pre_primary_schools pps ON pps.school_id = s.id
LEFT JOIN public.primary_schools     prs ON prs.school_id = s.id
LEFT JOIN public.secondary_schools   sec ON sec.school_id = s.id;

DO $$ DECLARE v BIGINT; BEGIN
    SELECT COUNT(*) INTO v FROM stg.enrolments_flat;
    RAISE NOTICE 'stg.enrolments_flat: % rows loaded', v;
END; $$;

COMMIT;
