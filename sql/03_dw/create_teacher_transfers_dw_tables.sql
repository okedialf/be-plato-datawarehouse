-- =============================================================================
-- EMIS Data Warehouse: Teacher Transfers Mart — DW Tables
-- Schema: dw
--
-- STAR SCHEMA:
--   dw.teacher_transfer_fact   — 1 row per approved transfer event
--
-- DIMENSION REFERENCES (all existing — no new dims needed):
--   teacher_id          → dw.teacher_dim     (Teachers mart)
--   out_school_id       → dw.schools_dim     (Schools mart)
--   in_school_id        → dw.schools_dim     (Schools mart)
--   out_district_id     → dw.admin_units_dim (Schools mart)
--   in_district_id      → dw.admin_units_dim (Schools mart)
--   date_id             → dw.date_dim        (Schools mart)
--
-- GRAIN: 1 row per approved transfer event.
-- Natural key: (person_id, posting_date, outgoing_school_source_id, incoming_school_source_id)
--
-- MEASURES:
--   days_at_outgoing_station — days served before transfer (from teacher_postings)
--   days_to_report           — days between posting_date and reporting_date
--
-- NOTE ON STATUS FILTER:
--   Only APPROVED transfers (status=2) are loaded into the fact table.
--   PENDING and REJECTED transfers are kept in staging for audit/analysis
--   but do not represent completed movements.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dw.teacher_transfer_fact (
    id                              SERIAL          NOT NULL,

    -- Dimension foreign keys
    teacher_id                      INTEGER         NOT NULL,   -- FK → dw.teacher_dim(id)
    out_school_id                   INTEGER,                    -- FK → dw.schools_dim(id) — NULL if school not in DW
    in_school_id                    INTEGER,                    -- FK → dw.schools_dim(id)
    out_district_id                 INTEGER,                    -- FK → dw.admin_units_dim(id)
    in_district_id                  INTEGER,                    -- FK → dw.admin_units_dim(id)
    date_id                         INTEGER         NOT NULL,   -- FK → dw.date_dim(id) — posting_date

    -- Natural keys (retained for deduplication and debugging)
    person_id                       BIGINT,                     -- teachers.person_id
    transfer_source_id              BIGINT,                     -- teacher_transfers.id
    outgoing_school_source_id       BIGINT,                     -- schools.id (outgoing)
    incoming_school_source_id       BIGINT,                     -- schools.id (incoming)

    -- Degenerate dimensions
    transfer_status                 VARCHAR(50),                -- APPROVED (all rows) / PENDING / REJECTED
    appointment_minute_number       VARCHAR(200),
    is_inter_district               BOOLEAN,                    -- TRUE = cross-district transfer

    -- Measures
    days_at_outgoing_station        INTEGER,                    -- days served before transfer
    days_to_report                  INTEGER,                    -- posting_date → reporting_date

    -- Dates (denormalized for convenience)
    posting_date                    DATE,
    reporting_date                  DATE,

    -- ETL metadata
    load_time                       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT teacher_transfer_fact_pkey PRIMARY KEY (id),
    CONSTRAINT teacher_transfer_fact_natural_key
        UNIQUE (person_id, transfer_source_id)
);

COMMENT ON TABLE  dw.teacher_transfer_fact IS
    'Teacher transfer fact. Grain: 1 row per approved transfer event. Measures: days served at outgoing station, days to report. Approved transfers only (status=2).';
COMMENT ON COLUMN dw.teacher_transfer_fact.teacher_id IS
    'FK → dw.teacher_dim(id). Current SCD2 version of the teacher at transfer date.';
COMMENT ON COLUMN dw.teacher_transfer_fact.out_school_id IS
    'FK → dw.schools_dim(id). School the teacher is transferring FROM. NULL if school not in DW.';
COMMENT ON COLUMN dw.teacher_transfer_fact.in_school_id IS
    'FK → dw.schools_dim(id). School the teacher is transferring TO.';
COMMENT ON COLUMN dw.teacher_transfer_fact.out_district_id IS
    'FK → dw.admin_units_dim(id) at District level. District of outgoing school.';
COMMENT ON COLUMN dw.teacher_transfer_fact.in_district_id IS
    'FK → dw.admin_units_dim(id) at District level. District of incoming school.';
COMMENT ON COLUMN dw.teacher_transfer_fact.date_id IS
    'FK → dw.date_dim(id). Set to posting_date of the transfer.';
COMMENT ON COLUMN dw.teacher_transfer_fact.days_at_outgoing_station IS
    'Days from last posting date at outgoing school to this transfer posting date. Proxy for deployment duration. NULL if no prior posting found.';
COMMENT ON COLUMN dw.teacher_transfer_fact.days_to_report IS
    'Days between posting_date and reporting_date. Measures how long before the teacher actually reported to the new school.';
COMMENT ON COLUMN dw.teacher_transfer_fact.is_inter_district IS
    'TRUE when outgoing and incoming districts differ — a cross-district transfer. Used to separate inter-district from intra-district transfers in reporting.';

-- FK constraints
ALTER TABLE dw.teacher_transfer_fact
    ADD CONSTRAINT ttf_teacher_fk
    FOREIGN KEY (teacher_id) REFERENCES dw.teacher_dim (id);

ALTER TABLE dw.teacher_transfer_fact
    ADD CONSTRAINT ttf_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

-- out_school_id and in_school_id are nullable FKs
ALTER TABLE dw.teacher_transfer_fact
    ADD CONSTRAINT ttf_out_school_fk
    FOREIGN KEY (out_school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.teacher_transfer_fact
    ADD CONSTRAINT ttf_in_school_fk
    FOREIGN KEY (in_school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.teacher_transfer_fact
    ADD CONSTRAINT ttf_out_district_fk
    FOREIGN KEY (out_district_id) REFERENCES dw.admin_units_dim (id);

ALTER TABLE dw.teacher_transfer_fact
    ADD CONSTRAINT ttf_in_district_fk
    FOREIGN KEY (in_district_id) REFERENCES dw.admin_units_dim (id);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_ttf_teacher
    ON dw.teacher_transfer_fact (teacher_id);
CREATE INDEX IF NOT EXISTS idx_ttf_out_school
    ON dw.teacher_transfer_fact (out_school_id);
CREATE INDEX IF NOT EXISTS idx_ttf_in_school
    ON dw.teacher_transfer_fact (in_school_id);
CREATE INDEX IF NOT EXISTS idx_ttf_out_district
    ON dw.teacher_transfer_fact (out_district_id);
CREATE INDEX IF NOT EXISTS idx_ttf_in_district
    ON dw.teacher_transfer_fact (in_district_id);
CREATE INDEX IF NOT EXISTS idx_ttf_date
    ON dw.teacher_transfer_fact (date_id);
CREATE INDEX IF NOT EXISTS idx_ttf_posting_date
    ON dw.teacher_transfer_fact (posting_date);
CREATE INDEX IF NOT EXISTS idx_ttf_inter_district
    ON dw.teacher_transfer_fact (is_inter_district);
CREATE INDEX IF NOT EXISTS idx_ttf_person
    ON dw.teacher_transfer_fact (person_id);
