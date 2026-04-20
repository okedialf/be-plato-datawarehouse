-- =============================================================================
-- EMIS Infrastructure Mart ETL: Step 0 — Extract from Source
--
-- Populates stg.infrastructure_raw from public.school_building_status_updates,
-- decoding all smallint status fields to readable VARCHAR values.
--
-- SMALLINT DECODINGS:
--   completion_status: 1=COMPLETE, 2=INCOMPLETE, 3=UNDER CONSTRUCTION, 4=PLANNED
--   usage_mode:        1=PERMANENT, 2=TEMPORARY, 3=SEMI-PERMANENT
--   condition:         1=GOOD, 2=FAIR, 3=POOR, 4=DILAPIDATED
--   gender_usage:      1=MALE, 2=FEMALE, 3=BOTH, 4=NONE
--   user_category:     1=TEACHERS ONLY, 2=LEARNERS ONLY, 3=BOTH
--
-- TERM RESOLUTION:
--   academic_year_id → setting_academic_years.name (extract year digits)
--   teaching_period_id → setting_teaching_periods.name ('1','2','3' → 'TERM 1' etc.)
--   Uses DISTINCT ON to avoid 4x duplication from academic_year_teaching_periods
--   (same pattern as enrolment and teachers marts).
-- =============================================================================

BEGIN;

TRUNCATE TABLE stg.infrastructure_raw;

-- ── Seed dw.infrastructure_type_dim before fact extract ──────────────────────
-- Done here so Step 1 can use surrogates immediately.
INSERT INTO dw.infrastructure_type_dim (
    source_id, type_name, item_category,
    is_lab_yn, is_sanitation_yn, is_area_captured_yn,
    gender_usage_yn, user_category_yn,
    is_for_pre_primary, is_for_primary, is_for_secondary,
    is_active_yn
)
SELECT
    sbt.id,
    sbt.name,
    sic.name                        AS item_category,
    COALESCE(sit.is_lab_yn, FALSE),
    COALESCE(sit.is_sanitation_yn, FALSE),
    COALESCE(sit.is_area_captured_yn, FALSE),
    COALESCE(sit.gender_usage_yn, FALSE),
    COALESCE(sit.user_category_yn, FALSE),
    COALESCE(sit.is_for_pre_primary, FALSE),
    COALESCE(sit.is_for_primary, FALSE),
    COALESCE(sit.is_for_secondary, FALSE),
    COALESCE(sbt.is_active_yn, TRUE)
FROM public.setting_school_building_types sbt
LEFT JOIN public.setting_school_item_categories sic
    ON sic.id = sbt.item_category_id
LEFT JOIN public.setting_school_infrastructure_types sit
    ON sit.id = sbt.id   -- building type ID matches infrastructure type ID
WHERE sbt.deleted_at IS NULL
  AND sbt.is_archived_yn = FALSE
ON CONFLICT (source_id) DO UPDATE
    SET type_name           = EXCLUDED.type_name,
        item_category       = EXCLUDED.item_category,
        is_lab_yn           = EXCLUDED.is_lab_yn,
        is_sanitation_yn    = EXCLUDED.is_sanitation_yn,
        is_active_yn        = EXCLUDED.is_active_yn,
        load_time           = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.infrastructure_type_dim;
    RAISE NOTICE 'dw.infrastructure_type_dim: % rows seeded', v;
END; $$;


-- ── Extract infrastructure_raw ────────────────────────────────────────────────
INSERT INTO stg.infrastructure_raw (
    building_status_id,
    school_id,
    school_name,
    school_type,
    admin_unit_source_id,
    building_id,
    building_name,
    item_category,
    infrastructure_type_id,
    infrastructure_type_name,
    is_lab_yn,
    is_sanitation_yn,
    is_area_captured_yn,
    gender_usage_yn,
    user_category_yn,
    academic_year,
    term,
    total_number,
    area,
    completion_status,
    usage_mode,
    structure_condition,
    gender_usage,
    user_category,
    date_created,
    date_updated
)
SELECT
    bsu.id                                              AS building_status_id,
    bsu.school_id,
    s.name                                              AS school_name,
    sst.name                                            AS school_type,

    -- Admin unit (best available from school)
    COALESCE(s.parish_id, s.sub_county_id,
             s.county_id, s.district_id, s.region_id)  AS admin_unit_source_id,

    bsu.building_id,
    sbt.name                                            AS building_name,
    sic.name                                            AS item_category,
    sit.id                                              AS infrastructure_type_id,
    sit.name                                            AS infrastructure_type_name,
    COALESCE(sit.is_lab_yn, FALSE),
    COALESCE(sit.is_sanitation_yn, FALSE),
    COALESCE(sit.is_area_captured_yn, FALSE),
    COALESCE(sit.gender_usage_yn, FALSE),
    COALESCE(sit.user_category_yn, FALSE),

    -- Academic year (extract digits from setting_academic_years.name)
    COALESCE(
        NULLIF(REGEXP_REPLACE(ay.name, '[^0-9]', '', 'g'), '')::INTEGER,
        EXTRACT(YEAR FROM bsu.date_created)::INTEGER
    )                                                   AS academic_year,

    -- Term (decode setting_teaching_periods.name)
    CASE
        WHEN tp.name IN ('1','2','3') THEN 'TERM ' || tp.name
        WHEN tp.name ILIKE 'TERM%'   THEN UPPER(tp.name)
        ELSE 'TERM 1'
    END                                                 AS term,

    bsu.total_number,
    CASE WHEN bsu.area IS NULL OR bsu.area > 9999999999999.99 OR bsu.area < 0
         THEN NULL
         ELSE ROUND(bsu.area::NUMERIC, 2)
    END                                                 AS area,

    -- Decode completion_status smallint
    CASE bsu.completion_status
        WHEN 1 THEN 'COMPLETE'
        WHEN 2 THEN 'INCOMPLETE'
        WHEN 3 THEN 'UNDER CONSTRUCTION'
        WHEN 4 THEN 'PLANNED'
        ELSE NULL
    END                                                 AS completion_status,

    -- Decode usage_mode smallint
    CASE bsu.usage_mode
        WHEN 1 THEN 'PERMANENT'
        WHEN 2 THEN 'TEMPORARY'
        WHEN 3 THEN 'SEMI-PERMANENT'
        ELSE NULL
    END                                                 AS usage_mode,

    -- Decode condition smallint
    CASE bsu.condition
        WHEN 1 THEN 'GOOD'
        WHEN 2 THEN 'FAIR'
        WHEN 3 THEN 'POOR'
        WHEN 4 THEN 'DILAPIDATED'
        ELSE NULL
    END                                                 AS structure_condition,

    -- Decode gender_usage smallint
    CASE bsu.gender_usage
        WHEN 1 THEN 'MALE'
        WHEN 2 THEN 'FEMALE'
        WHEN 3 THEN 'BOTH'
        WHEN 4 THEN 'NONE'
        ELSE NULL
    END                                                 AS gender_usage,

    -- Decode user_category smallint
    CASE bsu.user_category
        WHEN 1 THEN 'TEACHERS ONLY'
        WHEN 2 THEN 'LEARNERS ONLY'
        WHEN 3 THEN 'BOTH'
        ELSE NULL
    END                                                 AS user_category,

    bsu.date_created,
    bsu.date_updated

FROM public.school_building_status_updates bsu

-- School
JOIN public.schools s
    ON s.id = bsu.school_id
LEFT JOIN public.setting_school_types sst
    ON sst.id = s.school_type_id

-- Building type
LEFT JOIN public.setting_school_building_types sbt
    ON sbt.id = bsu.building_id
LEFT JOIN public.setting_school_item_categories sic
    ON sic.id = sbt.item_category_id
LEFT JOIN public.setting_school_infrastructure_types sit
    ON sit.id = bsu.building_id

-- Academic year and term
-- Use DISTINCT ON to avoid duplication from academic_year_teaching_periods
-- (same pattern as all other marts — school_id IS NULL = global term)
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (aytp.academic_year_id, aytp.teaching_period_id)
        aytp.academic_year_id, aytp.teaching_period_id
    FROM public.academic_year_teaching_periods aytp
    WHERE aytp.academic_year_id    = bsu.academic_year_id
      AND aytp.teaching_period_id  = bsu.teaching_period_id
      AND aytp.school_id IS NULL
      AND aytp.status = 'active'
    LIMIT 1
) aytp_dedup ON TRUE

LEFT JOIN public.setting_academic_years ay
    ON ay.id = bsu.academic_year_id
LEFT JOIN public.setting_teaching_periods tp
    ON tp.id = bsu.teaching_period_id

ON CONFLICT (building_status_id) DO UPDATE
    SET total_number        = EXCLUDED.total_number,
        area                = EXCLUDED.area,
        completion_status   = EXCLUDED.completion_status,
        structure_condition = EXCLUDED.structure_condition,
        date_updated        = EXCLUDED.date_updated;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.infrastructure_raw;
    RAISE NOTICE 'stg.infrastructure_raw: % rows extracted', v;
END; $$;

COMMIT;
