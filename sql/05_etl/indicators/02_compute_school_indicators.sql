-- =============================================================================
-- EMIS Indicators Mart ETL: Step 2 — Compute School Indicators (PTR)
--
-- Aggregates from dw.enrolment_fact + dw.hr_fact → stg.school_indicator_raw
-- Then computes PTR, qualified teacher ratio, % female teachers.
--
-- GRAIN: school × academic_year × term
--
-- FIXES:
-- 1. ef.term is NULL in enrolment_fact — group by school + academic_year only,
--    using hr_fact term (which IS populated) as the term value.
-- 2. district_id resolved via admin_units_dim hierarchy (child_id = parent source_id)
--    same pattern as Step 1.
-- =============================================================================

BEGIN;

TRUNCATE TABLE stg.school_indicator_raw;

-- ── Build unit → district lookup (same as Step 1) ────────────────────────────
CREATE TEMP TABLE tmp_school_district AS
SELECT parish.id AS unit_dim_id, district.id AS district_dim_id
FROM dw.admin_units_dim parish
JOIN dw.admin_units_dim sub_county ON sub_county.source_id = parish.child_id   AND sub_county.current_status = TRUE
JOIN dw.admin_units_dim county     ON county.source_id     = sub_county.child_id AND county.current_status   = TRUE
JOIN dw.admin_units_dim district   ON district.source_id   = county.child_id   AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE parish.current_status = TRUE AND parish.admin_unit_type = 'Parish'
UNION ALL
SELECT ward.id, district.id
FROM dw.admin_units_dim ward
JOIN dw.admin_units_dim sub_county ON sub_county.source_id = ward.child_id     AND sub_county.current_status = TRUE
JOIN dw.admin_units_dim county     ON county.source_id     = sub_county.child_id AND county.current_status   = TRUE
JOIN dw.admin_units_dim district   ON district.source_id   = county.child_id   AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE ward.current_status = TRUE AND ward.admin_unit_type = 'Ward'
UNION ALL
SELECT sc.id, district.id
FROM dw.admin_units_dim sc
JOIN dw.admin_units_dim county   ON county.source_id   = sc.child_id     AND county.current_status   = TRUE
JOIN dw.admin_units_dim district ON district.source_id = county.child_id AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE sc.current_status = TRUE AND sc.admin_unit_type = 'Sub County'
UNION ALL
SELECT tc.id, district.id
FROM dw.admin_units_dim tc
JOIN dw.admin_units_dim county   ON county.source_id   = tc.child_id     AND county.current_status   = TRUE
JOIN dw.admin_units_dim district ON district.source_id = county.child_id AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE tc.current_status = TRUE AND tc.admin_unit_type = 'Town Council'
UNION ALL
SELECT county.id, district.id
FROM dw.admin_units_dim county
JOIN dw.admin_units_dim district ON district.source_id = county.child_id AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE county.current_status = TRUE AND county.admin_unit_type = 'County'
UNION ALL
SELECT muni.id, district.id
FROM dw.admin_units_dim muni
JOIN dw.admin_units_dim district ON district.source_id = muni.child_id   AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE muni.current_status = TRUE AND muni.admin_unit_type = 'Municipality'
UNION ALL
SELECT city.id, district.id
FROM dw.admin_units_dim city
JOIN dw.admin_units_dim district ON district.source_id = city.child_id   AND district.admin_unit_type = 'District' AND district.current_status = TRUE
WHERE city.current_status = TRUE AND city.admin_unit_type = 'City'
UNION ALL
SELECT id, id FROM dw.admin_units_dim
WHERE admin_unit_type = 'District' AND current_status = TRUE;

CREATE INDEX ON tmp_school_district (unit_dim_id);

-- ── Build enrolment counts per school per academic_year ───────────────────────
-- Note: ef.term is NULL so we group by school + academic_year only
CREATE TEMP TABLE tmp_enrolment_counts AS
SELECT
    ef.school_source_id,
    ef.academic_year,
    COUNT(*)                                                              AS total_learners,
    COUNT(*) FILTER (WHERE UPPER(ld.gender) IN ('M','MALE'))             AS male_learners,
    COUNT(*) FILTER (WHERE UPPER(ld.gender) IN ('F','FEMALE'))           AS female_learners
FROM dw.enrolment_fact ef
JOIN dw.learner_dim ld ON ld.id = ef.learner_id AND ld.is_current = TRUE
WHERE ef.academic_year >= 2022
GROUP BY ef.school_source_id, ef.academic_year;

CREATE INDEX ON tmp_enrolment_counts (school_source_id, academic_year);


-- ── Build teacher counts per school per academic_year × term ─────────────────
CREATE TEMP TABLE tmp_teacher_counts AS
SELECT
    f.school_source_id,
    f.academic_year,
    f.term,
    COUNT(*)                                                              AS total_teachers,
    COUNT(*) FILTER (WHERE UPPER(td.gender) IN ('M','MALE'))             AS male_teachers,
    COUNT(*) FILTER (WHERE UPPER(td.gender) IN ('F','FEMALE'))           AS female_teachers,
    COUNT(*) FILTER (WHERE UPPER(td.teacher_type) = 'QUALIFIED')         AS qualified_teachers,
    COUNT(*) FILTER (WHERE UPPER(td.teacher_type) IN ('QUALIFIED','TRAINED')) AS trained_teachers
FROM dw.hr_fact f
JOIN dw.teacher_dim td ON td.id = f.teacher_id AND td.is_current = TRUE
WHERE f.academic_year >= 2022
GROUP BY f.school_source_id, f.academic_year, f.term;

CREATE INDEX ON tmp_teacher_counts (school_source_id, academic_year, term);


-- ── Insert into school_indicator_raw ─────────────────────────────────────────
INSERT INTO stg.school_indicator_raw (
    school_source_id, school_name, school_type, district_id,
    academic_year, term,
    total_learners, male_learners, female_learners,
    total_teachers, male_teachers, female_teachers,
    qualified_teachers, trained_teachers,
    ptr, qualified_teacher_ratio, trained_teacher_ratio,
    pct_female_teachers, gpi_ptr
)
SELECT
    tc.school_source_id,
    sd.name                                                               AS school_name,
    sd.school_type,
    COALESCE(pd.district_dim_id, sd.admin_unit_id)                        AS district_id,
    tc.academic_year,
    tc.term,

    -- Enrolment (joined from annual counts — same value repeated per term)
    COALESCE(ec.total_learners,  0)                                       AS total_learners,
    COALESCE(ec.male_learners,   0)                                       AS male_learners,
    COALESCE(ec.female_learners, 0)                                       AS female_learners,

    -- Teachers
    COALESCE(tc.total_teachers,     0)                                    AS total_teachers,
    COALESCE(tc.male_teachers,      0)                                    AS male_teachers,
    COALESCE(tc.female_teachers,    0)                                    AS female_teachers,
    COALESCE(tc.qualified_teachers, 0)                                    AS qualified_teachers,
    COALESCE(tc.trained_teachers,   0)                                    AS trained_teachers,

    -- PTR
    CASE WHEN COALESCE(tc.total_teachers, 0) > 0
         THEN ROUND(COALESCE(ec.total_learners, 0)::DECIMAL / tc.total_teachers, 2)
         ELSE NULL END                                                     AS ptr,

    -- Qualified teacher ratio
    CASE WHEN COALESCE(tc.total_teachers, 0) > 0
         THEN ROUND(COALESCE(tc.qualified_teachers, 0)::DECIMAL / tc.total_teachers * 100, 2)
         ELSE NULL END                                                     AS qualified_teacher_ratio,

    -- Trained teacher ratio
    CASE WHEN COALESCE(tc.total_teachers, 0) > 0
         THEN ROUND(COALESCE(tc.trained_teachers, 0)::DECIMAL / tc.total_teachers * 100, 2)
         ELSE NULL END                                                     AS trained_teacher_ratio,

    -- % female teachers
    CASE WHEN COALESCE(tc.total_teachers, 0) > 0
         THEN ROUND(COALESCE(tc.female_teachers, 0)::DECIMAL / tc.total_teachers * 100, 2)
         ELSE NULL END                                                     AS pct_female_teachers,

    -- GPI on PTR
    CASE WHEN COALESCE(tc.male_teachers, 0) > 0 AND COALESCE(tc.female_teachers, 0) > 0
         THEN ROUND(
             (COALESCE(ec.female_learners, 0)::DECIMAL / tc.female_teachers)
             / NULLIF(COALESCE(ec.male_learners, 0)::DECIMAL / tc.male_teachers, 0)
         , 4)
         ELSE NULL END                                                     AS gpi_ptr

FROM tmp_teacher_counts tc

-- Join enrolment counts (annual, no term)
LEFT JOIN tmp_enrolment_counts ec
    ON  ec.school_source_id = tc.school_source_id
    AND ec.academic_year    = tc.academic_year

-- Join schools_dim for name, type, admin_unit_id
LEFT JOIN dw.schools_dim sd
    ON  sd.source_id  = tc.school_source_id::INTEGER
    AND sd.is_current = TRUE

-- Resolve to district
LEFT JOIN tmp_school_district pd
    ON pd.unit_dim_id = sd.admin_unit_id

ON CONFLICT (school_source_id, academic_year, term) DO UPDATE
    SET
        total_learners          = EXCLUDED.total_learners,
        total_teachers          = EXCLUDED.total_teachers,
        ptr                     = EXCLUDED.ptr,
        qualified_teacher_ratio = EXCLUDED.qualified_teacher_ratio,
        trained_teacher_ratio   = EXCLUDED.trained_teacher_ratio,
        pct_female_teachers     = EXCLUDED.pct_female_teachers,
        gpi_ptr                 = EXCLUDED.gpi_ptr,
        date_computed           = NOW();

DROP TABLE IF EXISTS tmp_enrolment_counts;
DROP TABLE IF EXISTS tmp_teacher_counts;
DROP TABLE IF EXISTS tmp_school_district;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.school_indicator_raw;
    RAISE NOTICE 'stg.school_indicator_raw: % rows computed', v;
END; $$;

COMMIT;
