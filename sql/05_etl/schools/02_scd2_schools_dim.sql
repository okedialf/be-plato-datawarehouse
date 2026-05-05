-- =============================================================================
-- EMIS Data Warehouse: ETL Step — SCD2 Load → dw.schools_dim
-- Called nightly AFTER 01_flatten_schools.sql.
-- Implements full SCD Type 2 with change_hash, changed_fields, change_reason.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------
-- STEP A: Expire rows where tracked attributes have changed
-- -----------------------------------------------------------------------
UPDATE dw.schools_dim AS dw_s
SET
    expiration_date = CURRENT_DATE - 1,
    is_current      = FALSE
FROM stg.schools_flat stg_s
-- Resolve the DW admin_unit surrogate from the raw admin_unit_id
JOIN dw.admin_units_dim aud
    ON aud.source_id      = stg_s.admin_unit_id
   AND aud.current_status = TRUE
WHERE
    dw_s.source_id  = stg_s.source_id
    AND dw_s.is_current = TRUE
    -- Compare the incoming change_hash to the stored one
    AND dw_s.change_hash <>
        MD5(CONCAT_WS('|',
            COALESCE(stg_s.name,               ''),
            COALESCE(aud.id::TEXT,             ''),   -- resolved admin unit surrogate
            COALESCE(stg_s.emis_number,        ''),
            COALESCE(stg_s.school_type,        ''),
            COALESCE(stg_s.operational_status, ''),
            COALESCE(stg_s.ownership_status,   ''),
            COALESCE(stg_s.funding_type,       ''),
            COALESCE(stg_s.sex_composition,    ''),
            COALESCE(stg_s.boarding_status,    ''),
            COALESCE(stg_s.founding_body_type, '')
        ));


-- -----------------------------------------------------------------------
-- STEP B: Insert new rows (first version OR changed version)
-- -----------------------------------------------------------------------
INSERT INTO dw.schools_dim (
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
    stg_s.source_id,
    stg_s.name,
    aud.id                                              AS admin_unit_id,
    stg_s.emis_number,
    stg_s.school_type,
    stg_s.operational_status,
    stg_s.ownership_status,
    stg_s.funding_type,
    stg_s.sex_composition,
    stg_s.boarding_status,
    stg_s.founding_body_type,
    CURRENT_DATE                                        AS effective_date,
    '9999-12-31'::DATE                                  AS expiration_date,
    TRUE                                                AS is_current,

    -- Compute change hash over all tracked SCD2 columns
    MD5(CONCAT_WS('|',
        COALESCE(stg_s.name,               ''),
        COALESCE(aud.id::TEXT,             ''),
        COALESCE(stg_s.emis_number,        ''),
        COALESCE(stg_s.school_type,        ''),
        COALESCE(stg_s.operational_status, ''),
        COALESCE(stg_s.ownership_status,   ''),
        COALESCE(stg_s.funding_type,       ''),
        COALESCE(stg_s.sex_composition,    ''),
        COALESCE(stg_s.boarding_status,    ''),
        COALESCE(stg_s.founding_body_type, '')
    ))                                                  AS change_hash,

    -- change_reason: determine the most meaningful reason (priority order)
    CASE
        WHEN dw_old.id IS NULL               THEN 'INITIAL_LOAD'
        WHEN dw_old.name <> stg_s.name      THEN 'School renamed'
        WHEN dw_old.admin_unit_id <> aud.id THEN 'Admin unit re-assignment or boundary update'
        WHEN dw_old.operational_status <> stg_s.operational_status THEN 'Operational status updated'
        WHEN dw_old.boarding_status    <> stg_s.boarding_status    THEN 'Boarding status changed'
        WHEN dw_old.sex_composition    <> stg_s.sex_composition    THEN 'Sex composition changed'
        WHEN dw_old.ownership_status   <> stg_s.ownership_status   THEN 'Ownership status changed'
        WHEN dw_old.funding_type       <> stg_s.funding_type       THEN 'Funding type changed'
        WHEN dw_old.founding_body_type <> stg_s.founding_body_type THEN 'Founding body changed'
        ELSE 'Attribute update'
    END                                                 AS change_reason,

    -- changed_fields: comma-separated list of which columns actually changed
    TRIM(BOTH ',' FROM CONCAT_WS(',',
        CASE WHEN dw_old.id IS NULL                                        THEN 'ALL'           ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.name <> stg_s.name     THEN 'name'          ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.admin_unit_id <> aud.id THEN 'admin_unit_id' ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.operational_status <> stg_s.operational_status THEN 'operational_status' ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.boarding_status    <> stg_s.boarding_status    THEN 'boarding_status'    ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.sex_composition    <> stg_s.sex_composition    THEN 'sex_composition'    ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.ownership_status   <> stg_s.ownership_status   THEN 'ownership_status'   ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.funding_type       <> stg_s.funding_type       THEN 'funding_type'       ELSE NULL END,
        CASE WHEN dw_old.id IS NOT NULL AND dw_old.founding_body_type <> stg_s.founding_body_type THEN 'founding_body_type' ELSE NULL END
    ))                                                  AS changed_fields

FROM stg.schools_flat stg_s

-- Resolve the DW admin_unit surrogate
JOIN dw.admin_units_dim aud
    ON aud.source_id      = stg_s.admin_unit_id
   AND aud.current_status = TRUE

-- LEFT JOIN to the OLD (now-expired or never-existed) DW row
LEFT JOIN dw.schools_dim dw_old
    ON  dw_old.source_id  = stg_s.source_id
    AND dw_old.is_current = FALSE          -- just expired in STEP A
    AND dw_old.expiration_date = CURRENT_DATE - 1

-- Only insert where:
--   (a) No current row exists (new school) OR
--   (b) The old row was just expired (changed school)
WHERE NOT EXISTS (
    SELECT 1
    FROM   dw.schools_dim existing
    WHERE  existing.source_id  = stg_s.source_id
      AND  existing.is_current = TRUE
);

COMMIT;
