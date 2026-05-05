-- =============================================================================
-- EMIS Data Warehouse: ETL Step — SCD2 Load → dw.admin_units_dim
-- Run manually (or when admin boundary changes occur).
-- NOT part of the nightly automated pipeline.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------
-- STEP A: Expire rows where admin unit attributes have changed
-- -----------------------------------------------------------------------
UPDATE dw.admin_units_dim dw_a
SET
    expiration_date = CURRENT_DATE - 1,
    current_status  = FALSE
FROM stg.admin_units_raw stg_a
WHERE
    dw_a.source_id        = stg_a.source_id
    AND dw_a.current_status   = TRUE
    AND dw_a.admin_unit_type  = stg_a.admin_unit_type
    AND MD5(CONCAT_WS('|',
            COALESCE(stg_a.name,             ''),
            COALESCE(stg_a.admin_unit_type,  ''),
            COALESCE(stg_a.admin_unit_code,  ''),
            COALESCE(stg_a.voting_code,      ''),
            COALESCE(stg_a.parent_id::TEXT,  '')
        )) <>
        MD5(CONCAT_WS('|',
            COALESCE(dw_a.name,             ''),
            COALESCE(dw_a.admin_unit_type,  ''),
            COALESCE(dw_a.admin_unit_code,  ''),
            COALESCE(dw_a.voting_code,      ''),
            COALESCE(dw_a.child_id::TEXT,   '')
        ));


-- -----------------------------------------------------------------------
-- STEP B: Insert new or changed admin unit rows
-- -----------------------------------------------------------------------
INSERT INTO dw.admin_units_dim (
    child_id,
    admin_unit_type,
    name,
    admin_unit_code,
    voting_code,
    effective_date,
    expiration_date,
    current_status,
    source_id
)
SELECT
    stg_a.parent_id         AS child_id,      -- parent_id in stg maps to child_id in dw (hierarchical)
    stg_a.admin_unit_type,
    stg_a.name,
    stg_a.admin_unit_code,
    stg_a.voting_code,
    CURRENT_DATE            AS effective_date,
    '9999-12-31'::DATE      AS expiration_date,
    TRUE                    AS current_status,
    stg_a.source_id
FROM stg.admin_units_raw stg_a
WHERE NOT EXISTS (
    SELECT 1
    FROM   dw.admin_units_dim existing
    WHERE  existing.source_id       = stg_a.source_id
      AND  existing.admin_unit_type = stg_a.admin_unit_type
      AND  existing.current_status  = TRUE
);

COMMIT;
