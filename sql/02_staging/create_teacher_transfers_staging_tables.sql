-- =============================================================================
-- EMIS Data Warehouse: Teacher Transfers Mart — Staging Tables
-- Schema: stg
--
-- DESIGN NOTES:
-- The teacher transfers mart answers:
--   - How many teachers were transferred by district / year?
--   - Which teachers have been at their current station beyond the deployment limit?
--   - What is the flow of teachers between districts?
--   - How long did teachers serve at each station before transfer?
--
-- PRIMARY SOURCE: public.teacher_transfers
--   All IDs are BIGINT. Status is smallint (1=pending, 2=approved, 3=rejected).
--   Has outgoing_school_id, incoming_school_id, posting_date, reporting_date.
--   Location resolved via incoming/outgoing_local_government_id →
--   admin_unit_upper_local_governments.district_id → admin_unit_districts.
--
-- SECONDARY SOURCE: public.teacher_postings
--   Used to calculate time-at-station (days from posting_date to transfer date).
--   Grain: 1 row per teacher per posting. Most recent posting before the
--   transfer gives the deployment start date.
--
-- AVOID: staff_transfers + staff_transfer_entries (UUID PKs, denormalized strings).
--
-- FACT GRAIN:
--   dw.teacher_transfer_fact: 1 row per approved transfer event.
--   Natural key: (person_id, posting_date, outgoing_school_id, incoming_school_id)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.teacher_transfers_raw
--    One row per transfer record from public.teacher_transfers.
--    Resolved with school names, district names, teacher attributes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.teacher_transfers_raw (
    -- Source key
    transfer_id                 BIGINT          NOT NULL,   -- teacher_transfers.id

    -- Teacher keys
    person_id                   BIGINT,                     -- teacher_transfers.person_id
    employee_id                 BIGINT,                     -- teacher_transfers.employee_id
    outgoing_employee_id        BIGINT,                     -- teacher_transfers.outgoing_employee_id

    -- Transfer details
    outgoing_school_id          BIGINT,
    incoming_school_id          BIGINT,
    outgoing_school_name        VARCHAR(200),
    incoming_school_name        VARCHAR(200),
    outgoing_school_type        VARCHAR(200),
    incoming_school_type        VARCHAR(200),

    -- Location (district level — resolved from local_government_id)
    outgoing_district_id        BIGINT,                     -- admin_unit_districts.id
    incoming_district_id        BIGINT,
    outgoing_district_name      VARCHAR(200),
    incoming_district_name      VARCHAR(200),
    is_inter_district           BOOLEAN,                    -- TRUE if districts differ

    -- Transfer dates
    posting_date                DATE,
    reporting_date              DATE,

    -- Status (from teacher_transfers.status smallint)
    -- 1=PENDING, 2=APPROVED, 3=REJECTED
    transfer_status             VARCHAR(50),
    appointment_minute_number   VARCHAR(200),
    category_id                 BIGINT,

    -- Time at outgoing station (computed in flatten step)
    days_at_outgoing_station    INTEGER,                    -- posting_date minus last_posting_date

    -- Teacher attributes (from teachers_raw)
    teacher_type                VARCHAR(200),
    gender                      VARCHAR(100),
    qualification               VARCHAR(200),
    discipline                  VARCHAR(100),
    ipps_number                 VARCHAR(200),

    -- Dates
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,

    CONSTRAINT teacher_transfers_raw_pkey PRIMARY KEY (transfer_id)
);

CREATE INDEX IF NOT EXISTS idx_tt_raw_person
    ON stg.teacher_transfers_raw (person_id);
CREATE INDEX IF NOT EXISTS idx_tt_raw_out_school
    ON stg.teacher_transfers_raw (outgoing_school_id);
CREATE INDEX IF NOT EXISTS idx_tt_raw_in_school
    ON stg.teacher_transfers_raw (incoming_school_id);
CREATE INDEX IF NOT EXISTS idx_tt_raw_posting_date
    ON stg.teacher_transfers_raw (posting_date);

COMMENT ON TABLE stg.teacher_transfers_raw IS
    'Raw extract of teacher_transfers joined with schools, districts and teacher attributes. One row per transfer event.';
COMMENT ON COLUMN stg.teacher_transfers_raw.transfer_status IS
    'Decoded from smallint: 1=PENDING, 2=APPROVED, 3=REJECTED.';
COMMENT ON COLUMN stg.teacher_transfers_raw.is_inter_district IS
    'TRUE when outgoing and incoming districts differ — cross-district transfer.';
COMMENT ON COLUMN stg.teacher_transfers_raw.days_at_outgoing_station IS
    'Days from last posting date at outgoing school to this transfer posting_date. NULL if no prior posting found.';
