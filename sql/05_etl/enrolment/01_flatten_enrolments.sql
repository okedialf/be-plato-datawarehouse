-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 1 — Flatten Enrolments
-- Joins all raw staging tables into stg.enrolments_flat.
-- Decodes all lookup IDs to business terms.
-- Derives disability flags, orphan status, enrolment type, age.
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
    e.id                                                        AS enrolment_source_id,
    e.learner_id                                                AS learner_source_id,
    e.school_id                                                 AS school_source_id,

    -- Decode academic year and term
    ay.year::INTEGER                                            AS academic_year,
    CASE tp.name
        WHEN '1' THEN 'TERM 1'
        WHEN '2' THEN 'TERM 2'
        WHEN '3' THEN 'TERM 3'
        ELSE COALESCE('TERM '||tp.name, 'TERM 1')
    END                                                         AS term,

    -- Learner attributes
    lr.lin,
    lr.gender,
    EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER
        - EXTRACT(YEAR FROM lr.date_of_birth)::INTEGER          AS age,
    lr.nationality,
    lr.nin,
    lr.passport_no,
    lr.student_pass_no,
    lr.work_permit_no,
    lr.refugee_id,

    -- Orphan status: derived from learner_health or direct flag
    -- NOTE: orphan_status is stored in src.learner_health_issues or
    -- src.learner_parents; adapt to exact EMIS 2.0 source column.
    -- For now mapped from promotion_status as a proxy pending source clarification.
    CASE
        WHEN lh.is_double_orphan_yn IS TRUE             THEN 'BOTH DECEASED'
        WHEN lh.is_father_deceased_yn IS TRUE
         AND lh.is_mother_deceased_yn IS TRUE            THEN 'BOTH DECEASED'
        WHEN lh.is_mother_deceased_yn IS TRUE            THEN 'MOTHER DECEASED'
        WHEN lh.is_father_deceased_yn IS TRUE            THEN 'FATHER DECEASED'
        ELSE 'NOT APPLICABLE'
    END                                                         AS orphan_status,

    -- Disability flags (aggregated from learner_disabilities_raw)
    COALESCE(dis.is_visual_yn,         FALSE)                   AS is_visual_yn,
    COALESCE(dis.is_hearing_yn,        FALSE)                   AS is_hearing_yn,
    COALESCE(dis.is_walking_yn,        FALSE)                   AS is_walking_yn,
    COALESCE(dis.is_self_care_yn,      FALSE)                   AS is_self_care_yn,
    COALESCE(dis.is_remembering_yn,    FALSE)                   AS is_remembering_yn,
    COALESCE(dis.is_communication_yn,  FALSE)                   AS is_communication_yn,
    -- Multiple disabilities = more than one flag is TRUE
    CASE WHEN (
        COALESCE(dis.is_visual_yn,        FALSE)::INT +
        COALESCE(dis.is_hearing_yn,       FALSE)::INT +
        COALESCE(dis.is_walking_yn,       FALSE)::INT +
        COALESCE(dis.is_self_care_yn,     FALSE)::INT +
        COALESCE(dis.is_remembering_yn,   FALSE)::INT +
        COALESCE(dis.is_communication_yn, FALSE)::INT
    ) > 1 THEN TRUE ELSE FALSE END                              AS has_multiple_disabilities_yn,

    -- Grade and school level (from subtype enrolment)
    sub.education_class_name                                    AS grade,
    sub.school_level,

    -- Enrolment type — derived from promotion/transfer/transition status
    CASE
        WHEN e.learner_transfer_status_id = 1               THEN 'TRANSFER'
        WHEN e.learner_transition_status_id = 1             THEN 'TRANSFER'
        WHEN e.learner_promotion_status_id = 2              THEN 'REPEATER'
        WHEN e.learner_promotion_status_id = 3              THEN 'RETURNEE'
        WHEN et.name IS NOT NULL                            THEN UPPER(et.name)
        ELSE 'CONTINUING'
    END                                                         AS enrolment_type,

    e.is_enrolment_active_yn                                    AS is_active,

    -- School attributes (denormalized from src)
    s.emis_number                                               AS school_emis_number,
    s.name                                                      AS school_name,
    COALESCE(own.name, CASE WHEN s.is_government_owned_yn THEN 'GOVT AIDED' ELSE 'PRIVATE' END)
                                                                AS school_ownership,
    fs.name                                                     AS school_funding_type,
    CASE
        WHEN s.has_female_students AND s.has_male_students   THEN 'MIXED'
        WHEN s.has_female_students                           THEN 'FEMALES ONLY'
        WHEN s.has_male_students                             THEN 'MALES ONLY'
        ELSE NULL
    END                                                         AS school_sex_composition,
    -- Boarding status decoded by the existing flatten logic (reuse from schools ETL)
    brd.boarding_status                                         AS school_boarding_status,
    st.name                                                     AS school_type,

    -- Admin unit (best available — same priority as schools flatten)
    COALESCE(
        s.parish_id,
        s.sub_county_id,
        s.county_id,
        s.district_id,
        s.region_id
    )::INTEGER                                                  AS admin_unit_source_id

FROM stg.enrolments_raw e

-- Academic year and term decode
JOIN src.lkup_academic_years  ay ON ay.id = e.academic_year_id
JOIN src.lkup_teaching_periods tp ON tp.id = e.teaching_period_id

-- Learner
JOIN stg.learners_raw lr
    ON lr.person_id = e.learner_id

-- Learner health (for orphan status)
LEFT JOIN src.learner_health_issues lh
    ON lh.learner_id = e.learner_id

-- Disability flags aggregated per learner
LEFT JOIN (
    SELECT
        learner_id,
        BOOL_OR(disability_type = 'VISUAL')        AS is_visual_yn,
        BOOL_OR(disability_type = 'HEARING')       AS is_hearing_yn,
        BOOL_OR(disability_type = 'WALKING')       AS is_walking_yn,
        BOOL_OR(disability_type = 'SELF_CARE')     AS is_self_care_yn,
        BOOL_OR(disability_type = 'REMEMBERING')   AS is_remembering_yn,
        BOOL_OR(disability_type = 'COMMUNICATION') AS is_communication_yn
    FROM stg.learner_disabilities_raw
    GROUP BY learner_id
) dis ON dis.learner_id = e.learner_id

-- Subtype enrolment (grade/class)
LEFT JOIN stg.enrolments_subtype_raw sub
    ON sub.enrolment_id = e.id

-- Enrolment type from lookup
LEFT JOIN src.lkup_enrolment_types et
    ON et.id = e.reporting_status_id

-- School attributes
JOIN src.schools s
    ON s.id = e.school_id
LEFT JOIN src.setting_school_types        st  ON st.id  = s.school_type_id
LEFT JOIN src.setting_ownership_statuses  own ON own.id = s.school_ownership_status_id
LEFT JOIN src.setting_funding_sources     fs  ON fs.id  = s.funding_source_id

-- Boarding status derived subquery (same logic as schools flatten)
LEFT JOIN (
    SELECT
        sr.id AS school_id,
        CASE
            WHEN pp.school_id IS NOT NULL OR pr.school_id IS NOT NULL
              OR se2.school_id IS NOT NULL THEN
                CASE
                    WHEN COALESCE(pp.admits_day_scholars_yn, pr.admits_day_scholars_yn, se2.admits_day_scholars_yn) IS TRUE
                     AND COALESCE(pp.admits_boarders_yn,     pr.admits_boarders_yn,     se2.admits_boarders_yn)     IS FALSE
                        THEN 'DAY SCHOOL'
                    WHEN COALESCE(pp.admits_day_scholars_yn, pr.admits_day_scholars_yn, se2.admits_day_scholars_yn) IS FALSE
                     AND COALESCE(pp.admits_boarders_yn,     pr.admits_boarders_yn,     se2.admits_boarders_yn)     IS TRUE
                        THEN 'FULLY BOARDING'
                    WHEN COALESCE(pp.admits_day_scholars_yn, pr.admits_day_scholars_yn, se2.admits_day_scholars_yn) IS TRUE
                     AND COALESCE(pp.admits_boarders_yn,     pr.admits_boarders_yn,     se2.admits_boarders_yn)     IS TRUE
                        THEN 'DAY AND BOARDING'
                    ELSE NULL
                END
            ELSE NULL
        END AS boarding_status
    FROM src.schools sr
    LEFT JOIN src.pre_primary_schools  pp  ON pp.school_id  = sr.id
    LEFT JOIN src.primary_schools      pr  ON pr.school_id  = sr.id
    LEFT JOIN src.secondary_schools    se2 ON se2.school_id = sr.id
) brd ON brd.school_id = e.school_id;

COMMIT;
