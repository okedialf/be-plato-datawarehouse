-- =============================================================================
-- EMIS Data Warehouse: ETL Step — Flatten Schools
-- Called nightly AFTER schools_raw has been loaded.
-- Produces stg.schools_flat — the clean, decoded staging source for SCD2.
-- =============================================================================

BEGIN;

-- 1) Wipe the flat table (fresh rebuild every run)
TRUNCATE TABLE stg.schools_flat RESTART IDENTITY;

-- 2) Reload from raw with lookups + transformations
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
    sr.id::INT                                  AS source_id,
    sr.name                                     AS name,

    -- Best available admin unit id (parish preferred, falls back up hierarchy)
    COALESCE(
        sr.parish_id,
        sr.admin_unit_id,
        sr.sub_county_id,
        sr.county_id,
        sr.district_id,
        sr.region_id,
        sr.local_government_id,
        sr.village_id
    )::INT                                      AS admin_unit_id,

    sr.emis_number,

    -- School type: prefer display_name from lookup
    COALESCE(st.display_name, st.name)::VARCHAR(30)   AS school_type,

    -- Operational status from lookup
    os.name::VARCHAR(20)                              AS operational_status,

    -- Ownership: lookup first, then derive from boolean flag
    COALESCE(
        own.name,
        CASE
            WHEN sr.is_government_owned_yn IS TRUE  THEN 'GOVT AIDED'
            WHEN sr.is_government_owned_yn IS FALSE THEN 'PRIVATE'
            ELSE NULL
        END
    )::VARCHAR(60)                                    AS ownership_status,

    -- Funding type from lookup id (to be refined once lookup table is confirmed)
    sr.funding_source_id::TEXT::VARCHAR(20)           AS funding_type,

    -- Sex composition from boolean flags
    CASE
        WHEN sr.has_female_students IS TRUE  AND sr.has_male_students IS TRUE  THEN 'MIXED'
        WHEN sr.has_female_students IS TRUE  AND (sr.has_male_students IS FALSE OR sr.has_male_students IS NULL)  THEN 'FEMALES ONLY'
        WHEN sr.has_male_students   IS TRUE  AND (sr.has_female_students IS FALSE OR sr.has_female_students IS NULL) THEN 'MALES ONLY'
        ELSE NULL
    END::VARCHAR(20)                                  AS sex_composition,

    -- Boarding status derived from school-type subtype tables
    CASE
        -- Pre-primary, Primary, Secondary, International: DAY/BOARDING/BOTH
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

        -- Certificate / Diploma: RESIDENTIAL/NON-RESIDENTIAL/BOTH
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

        -- Degree awarding: no boarding tracking per business rules
        ELSE NULL
    END::VARCHAR(20)                                  AS boarding_status,

    fb.name::VARCHAR(60)                              AS founding_body_type,

    -- SCD2 fields are computed/populated during the dw load step (not here)
    CURRENT_DATE                                      AS effective_date,
    NULL::DATE                                        AS expiration_date,
    TRUE                                              AS is_current,
    NULL::TEXT                                        AS change_hash,
    'PENDING_SCD2'                                    AS change_reason,
    NULL::TEXT                                        AS changed_fields

FROM stg.schools_raw sr

-- School type subtables (boarding status source)
LEFT JOIN public.pre_primary_schools          pp   ON pp.school_id   = sr.id
LEFT JOIN public.primary_schools              pr   ON pr.school_id   = sr.id
LEFT JOIN public.secondary_schools            se   ON se.school_id   = sr.id
LEFT JOIN public.international_schools        intl ON intl.school_id = sr.id
LEFT JOIN public.diploma_awarding_schools     dip  ON dip.school_id  = sr.id
LEFT JOIN public.certificate_awarding_schools cert ON cert.school_id = sr.id
-- degree_awarding_schools deliberately excluded (no boarding tracking)

-- Lookup tables
LEFT JOIN public.setting_school_types         st   ON st.id  = sr.school_type_id
LEFT JOIN public.setting_operational_statuses os   ON os.id  = sr.operational_status_id
LEFT JOIN public.setting_ownership_statuses   own  ON own.id = sr.school_ownership_status_id
LEFT JOIN public.setting_founding_bodies      fb   ON fb.id  = sr.founding_body_id

-- Deduplicate on emis_number: only include rows with unique emis_number (NULLs allowed through)
WHERE sr.emis_number IS NULL
   OR sr.emis_number IN (
       SELECT emis_number
       FROM   stg.schools_raw
       WHERE  emis_number IS NOT NULL
       GROUP  BY emis_number
       HAVING COUNT(*) = 1
   );

COMMIT;
