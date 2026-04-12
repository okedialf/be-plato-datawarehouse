-- =============================================================================
-- EMIS Data Warehouse: ETL Step 1 — Flatten Schools
-- Called nightly AFTER schools_raw has been loaded.
-- Produces stg.schools_flat — clean, decoded staging source for SCD2.
--
-- Key fix (2026-04 testing):
--   Admin unit resolution uses a smart COALESCE that checks each level
--   against dw.admin_units_dim. This handles cases where parish_id in
--   public.schools uses a legacy numbering system that doesn't exist in
--   the new administrative_units schema. Falls back through:
--   parish → sub_county → county → district → region → admin_unit_id
-- =============================================================================

BEGIN;

-- 1) Wipe the flat table (fresh rebuild every run)
TRUNCATE TABLE stg.schools_flat RESTART IDENTITY;

-- 2) Reload from raw with lookups + smart admin unit resolution
INSERT INTO stg.schools_flat (
    source_id,
    name,
    admin_unit_id,
    emis_number,
    school_type,
    operational_status,
    ownership_status,
    funding_type,
    sex_composition,
    boarding_status,
    founding_body_type,
    effective_date,
    expiration_date,
    is_current,
    change_hash,
    change_reason,
    changed_fields
)
SELECT
    sr.id::INT                                          AS source_id,
    sr.name,

    -- Smart COALESCE: use the most granular admin unit level that actually
    -- exists in dw.admin_units_dim. Handles legacy parish IDs that don't
    -- exist in the new administrative_units schema.
    COALESCE(
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.parish_id           AND a.current_status = TRUE) THEN sr.parish_id           END,
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.sub_county_id       AND a.current_status = TRUE) THEN sr.sub_county_id       END,
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.county_id           AND a.current_status = TRUE) THEN sr.county_id           END,
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.district_id         AND a.current_status = TRUE) THEN sr.district_id         END,
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.region_id           AND a.current_status = TRUE) THEN sr.region_id           END,
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.admin_unit_id       AND a.current_status = TRUE) THEN sr.admin_unit_id       END,
        CASE WHEN EXISTS (SELECT 1 FROM dw.admin_units_dim a WHERE a.source_id = sr.local_government_id AND a.current_status = TRUE) THEN sr.local_government_id END
    )::INT                                              AS admin_unit_id,

    sr.emis_number,

    -- School type: prefer display_name from lookup (VARCHAR(50))
    COALESCE(st.display_name, st.name)::VARCHAR(50)    AS school_type,

    -- Operational status from lookup
    os.name::VARCHAR(20)                               AS operational_status,

    -- Ownership: lookup first, then derive from boolean flag
    COALESCE(
        own.name,
        CASE
            WHEN sr.is_government_owned_yn IS TRUE  THEN 'GOVT AIDED'
            WHEN sr.is_government_owned_yn IS FALSE THEN 'PRIVATE'
            ELSE NULL
        END
    )::VARCHAR(60)                                     AS ownership_status,

    sr.funding_source_id::TEXT::VARCHAR(20)            AS funding_type,

    -- Sex composition from boolean flags
    CASE
        WHEN sr.has_female_students IS TRUE  AND sr.has_male_students IS TRUE  THEN 'MIXED'
        WHEN sr.has_female_students IS TRUE  AND (sr.has_male_students IS FALSE OR sr.has_male_students IS NULL)   THEN 'FEMALES ONLY'
        WHEN sr.has_male_students   IS TRUE  AND (sr.has_female_students IS FALSE OR sr.has_female_students IS NULL) THEN 'MALES ONLY'
        ELSE NULL
    END::VARCHAR(20)                                   AS sex_composition,

    -- Boarding status derived from school-type subtype tables
    CASE
        WHEN (pp.school_id IS NOT NULL OR pr.school_id IS NOT NULL
           OR se.school_id IS NOT NULL OR intl.school_id IS NOT NULL) THEN
            CASE
                WHEN COALESCE(pp.admits_day_scholars_yn, pr.admits_day_scholars_yn,
                              se.admits_day_scholars_yn, intl.admits_day_scholars_yn) IS TRUE
                 AND COALESCE(pp.admits_boarders_yn, pr.admits_boarders_yn,
                              se.admits_boarders_yn, intl.admits_boarders_yn) IS FALSE
                    THEN 'DAY SCHOOL'
                WHEN COALESCE(pp.admits_day_scholars_yn, pr.admits_day_scholars_yn,
                              se.admits_day_scholars_yn, intl.admits_day_scholars_yn) IS FALSE
                 AND COALESCE(pp.admits_boarders_yn, pr.admits_boarders_yn,
                              se.admits_boarders_yn, intl.admits_boarders_yn) IS TRUE
                    THEN 'FULLY BOARDING'
                WHEN COALESCE(pp.admits_day_scholars_yn, pr.admits_day_scholars_yn,
                              se.admits_day_scholars_yn, intl.admits_day_scholars_yn) IS TRUE
                 AND COALESCE(pp.admits_boarders_yn, pr.admits_boarders_yn,
                              se.admits_boarders_yn, intl.admits_boarders_yn) IS TRUE
                    THEN 'DAY AND BOARDING'
                ELSE NULL
            END
        WHEN (cert.school_id IS NOT NULL OR dip.school_id IS NOT NULL) THEN
            CASE
                WHEN COALESCE(cert.admits_day_scholars_yn, dip.admits_day_scholars_yn) IS TRUE
                 AND COALESCE(cert.admits_boarders_yn,     dip.admits_boarders_yn)     IS FALSE
                    THEN 'NON RESIDENTIAL'
                WHEN COALESCE(cert.admits_day_scholars_yn, dip.admits_day_scholars_yn) IS FALSE
                 AND COALESCE(cert.admits_boarders_yn,     dip.admits_boarders_yn)     IS TRUE
                    THEN 'RESIDENTIAL'
                WHEN COALESCE(cert.admits_day_scholars_yn, dip.admits_day_scholars_yn) IS TRUE
                 AND COALESCE(cert.admits_boarders_yn,     dip.admits_boarders_yn)     IS TRUE
                    THEN 'BOTH RES/NON'
                ELSE NULL
            END
        ELSE NULL
    END::VARCHAR(20)                                   AS boarding_status,

    fb.name::VARCHAR(60)                               AS founding_body_type,
    CURRENT_DATE                                       AS effective_date,
    NULL::DATE                                         AS expiration_date,
    TRUE                                               AS is_current,
    NULL::TEXT                                         AS change_hash,
    'PENDING_SCD2'                                     AS change_reason,
    NULL::TEXT                                         AS changed_fields

FROM stg.schools_raw sr
LEFT JOIN public.pre_primary_schools          pp   ON pp.school_id   = sr.id
LEFT JOIN public.primary_schools              pr   ON pr.school_id   = sr.id
LEFT JOIN public.secondary_schools            se   ON se.school_id   = sr.id
LEFT JOIN public.international_schools        intl ON intl.school_id = sr.id
LEFT JOIN public.diploma_awarding_schools     dip  ON dip.school_id  = sr.id
LEFT JOIN public.certificate_awarding_schools cert ON cert.school_id = sr.id
LEFT JOIN public.setting_school_types         st   ON st.id  = sr.school_type_id
LEFT JOIN public.setting_operational_statuses os   ON os.id  = sr.operational_status_id
LEFT JOIN public.setting_ownership_statuses   own  ON own.id = sr.school_ownership_status_id
LEFT JOIN public.setting_founding_bodies      fb   ON fb.id  = sr.founding_body_id
WHERE sr.emis_number IS NULL
   OR sr.emis_number IN (
       SELECT emis_number FROM stg.schools_raw
       WHERE emis_number IS NOT NULL
       GROUP BY emis_number HAVING COUNT(*) = 1
   );

COMMIT;
