-- =============================================================================
-- EMIS Teacher Transfers Mart ETL: Step 1 — Load DW Fact Table
--
-- Loads APPROVED transfers from stg.teacher_transfers_raw into
-- dw.teacher_transfer_fact, resolving all surrogate keys.
--
-- Only APPROVED transfers (transfer_status = 'APPROVED') are loaded.
-- PENDING and REJECTED remain in staging for audit purposes only.
--
-- SURROGATE KEY RESOLUTION:
--   teacher_id   ← dw.teacher_dim WHERE source_id = person_id AND is_current = TRUE
--   out_school_id ← dw.schools_dim WHERE source_id = outgoing_school_id AND is_current = TRUE
--   in_school_id  ← dw.schools_dim WHERE source_id = incoming_school_id AND is_current = TRUE
--   out_district_id ← dw.admin_units_dim WHERE source_id = outgoing_district_id
--                     AND admin_unit_type = 'District' AND current_status = TRUE
--   in_district_id  ← dw.admin_units_dim WHERE source_id = incoming_district_id
--                     AND admin_unit_type = 'District' AND current_status = TRUE
--   date_id      ← dw.date_dim WHERE system_date = posting_date
-- =============================================================================

BEGIN;

-- ── Build surrogate key lookup tables ─────────────────────────────────────────

-- Teacher surrogate
CREATE TEMP TABLE tmp_teacher_sk AS
SELECT DISTINCT ON (source_id)
    source_id, id AS teacher_dim_id
FROM dw.teacher_dim
WHERE is_current = TRUE
ORDER BY source_id;
CREATE INDEX ON tmp_teacher_sk (source_id);

-- School surrogate
CREATE TEMP TABLE tmp_school_sk AS
SELECT DISTINCT ON (source_id)
    source_id, id AS school_dim_id
FROM dw.schools_dim
WHERE is_current = TRUE
ORDER BY source_id;
CREATE INDEX ON tmp_school_sk (source_id);

-- District surrogate (from admin_units_dim)
CREATE TEMP TABLE tmp_district_sk AS
SELECT source_id, id AS district_dim_id
FROM dw.admin_units_dim
WHERE admin_unit_type = 'District'
  AND current_status  = TRUE;
CREATE INDEX ON tmp_district_sk (source_id);

-- Date surrogate (posting_date → date_dim)
CREATE TEMP TABLE tmp_date_sk AS
SELECT system_date, id AS date_dim_id
FROM dw.date_dim;
CREATE INDEX ON tmp_date_sk (system_date);


-- ── Load dw.teacher_transfer_fact ─────────────────────────────────────────────
INSERT INTO dw.teacher_transfer_fact (
    teacher_id,
    out_school_id,
    in_school_id,
    out_district_id,
    in_district_id,
    date_id,
    person_id,
    transfer_source_id,
    outgoing_school_source_id,
    incoming_school_source_id,
    transfer_status,
    appointment_minute_number,
    is_inter_district,
    days_at_outgoing_station,
    days_to_report,
    posting_date,
    reporting_date
)
SELECT
    tt.teacher_dim_id                                   AS teacher_id,
    out_s.school_dim_id                                 AS out_school_id,
    in_s.school_dim_id                                  AS in_school_id,
    out_d.district_dim_id                               AS out_district_id,
    in_d.district_dim_id                                AS in_district_id,

    -- Date: posting_date; fallback to load date if not in date_dim
    COALESCE(
        dt.date_dim_id,
        (SELECT id FROM dw.date_dim ORDER BY system_date DESC LIMIT 1)
    )                                                   AS date_id,

    r.person_id,
    r.transfer_id                                       AS transfer_source_id,
    r.outgoing_school_id                                AS outgoing_school_source_id,
    r.incoming_school_id                                AS incoming_school_source_id,
    r.transfer_status,
    r.appointment_minute_number,
    r.is_inter_district,
    r.days_at_outgoing_station,

    -- days_to_report: reporting_date - posting_date
    CASE
        WHEN r.reporting_date IS NOT NULL AND r.posting_date IS NOT NULL
        THEN (r.reporting_date - r.posting_date)::INTEGER
        ELSE NULL
    END                                                 AS days_to_report,

    r.posting_date,
    r.reporting_date

FROM stg.teacher_transfers_raw r

-- Teacher surrogate
JOIN tmp_teacher_sk tt
    ON tt.source_id = r.person_id

-- School surrogates (LEFT JOIN — school may not be in DW)
LEFT JOIN tmp_school_sk out_s
    ON out_s.source_id = r.outgoing_school_id::INTEGER
LEFT JOIN tmp_school_sk in_s
    ON in_s.source_id  = r.incoming_school_id::INTEGER

-- District surrogates (LEFT JOIN — local_government may not resolve)
LEFT JOIN tmp_district_sk out_d
    ON out_d.source_id = r.outgoing_district_id::INTEGER
LEFT JOIN tmp_district_sk in_d
    ON in_d.source_id  = r.incoming_district_id::INTEGER

-- Date surrogate
LEFT JOIN tmp_date_sk dt
    ON dt.system_date = r.posting_date

-- Only approved transfers
WHERE r.transfer_status = 'APPROVED'
  AND r.person_id IS NOT NULL

ON CONFLICT (person_id, transfer_source_id) DO UPDATE
    SET teacher_id               = EXCLUDED.teacher_id,
        out_school_id            = EXCLUDED.out_school_id,
        in_school_id             = EXCLUDED.in_school_id,
        out_district_id          = EXCLUDED.out_district_id,
        in_district_id           = EXCLUDED.in_district_id,
        days_at_outgoing_station = EXCLUDED.days_at_outgoing_station,
        days_to_report           = EXCLUDED.days_to_report,
        is_inter_district        = EXCLUDED.is_inter_district,
        load_time                = NOW();

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM dw.teacher_transfer_fact;
    RAISE NOTICE 'dw.teacher_transfer_fact: % rows loaded', v;
END; $$;

DROP TABLE IF EXISTS tmp_teacher_sk;
DROP TABLE IF EXISTS tmp_school_sk;
DROP TABLE IF EXISTS tmp_district_sk;
DROP TABLE IF EXISTS tmp_date_sk;

COMMIT;
