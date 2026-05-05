-- =============================================================================
-- EMIS Data Warehouse: ETL Step — Load School Fact (Daily Snapshot)
-- Called nightly AFTER 02_scd2_schools_dim.sql.
-- Inserts one row per school per day into dw.school_fact.
-- Re-runnable: uses ON CONFLICT DO NOTHING to prevent duplicates.
-- =============================================================================

INSERT INTO dw.school_fact (school_id, date_id, location_time)
SELECT
    sd.id                           AS school_id,
    dd.id                           AS date_id,
    CURRENT_TIMESTAMP               AS location_time
FROM
    dw.schools_dim  sd
    JOIN dw.date_dim dd
        ON dd.system_date = CURRENT_DATE
WHERE
    sd.is_current = TRUE
ON CONFLICT (school_id, date_id) DO NOTHING;
