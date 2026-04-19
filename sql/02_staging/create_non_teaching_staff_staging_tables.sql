-- =============================================================================
-- EMIS Data Warehouse: Non-Teaching Staff Mart — Staging Tables
-- Schema: stg
-- All IDs are BIGINT. VARCHAR sizes generous (VARCHAR(200) minimum).
-- No UUID columns — non_teaching_staff_postings uses BIGINT throughout.
--
-- Source table chain:
--   persons → school_employees (is_teaching_staff=FALSE) → non_teaching_staff
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.non_teaching_staff_raw
--    One row per non-teaching staff member.
--    Joins persons + school_employees + non_teaching_staff.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.non_teaching_staff_raw (
    -- Keys
    person_id                   BIGINT        NOT NULL,
    employee_id                 BIGINT,                   -- school_employees.id
    nts_employee_id             BIGINT,                   -- non_teaching_staff.employee_id

    -- Personal attributes (from persons)
    surname                     VARCHAR(200),
    given_name                  VARCHAR(200),
    gender                      VARCHAR(200),
    date_of_birth               DATE,
    nationality                 VARCHAR(200),
    nin                         VARCHAR(200),

    -- Role attributes (from non_teaching_staff)
    category                    VARCHAR(200),             -- ADMINISTRATIVE / SUPPORT
    role                        VARCHAR(200),             -- BURSAR, COOK, NURSE etc.
    is_transfer_appointment     BOOLEAN,
    appointment_date            DATE,
    posting_date                DATE,
    current_school_id           BIGINT,

    -- Employment attributes (from school_employees)
    highest_education_level     VARCHAR(200),
    employment_status           VARCHAR(200),
    is_on_government_payroll    BOOLEAN,
    ipps_number                 VARCHAR(200),
    hrin                        VARCHAR(200),
    hcm_employee_number         VARCHAR(200),
    date_started                DATE,
    date_ended                  DATE,
    is_undergoing_training      BOOLEAN,

    -- Audit
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,

    CONSTRAINT non_teaching_staff_raw_pkey PRIMARY KEY (person_id)
);
COMMENT ON TABLE stg.non_teaching_staff_raw IS
    'Raw extract of non_teaching_staff joined with persons and school_employees. One row per staff member.';


-- -----------------------------------------------------------------------------
-- 2. stg.non_teaching_staff_school_raw
--    Current school deployment from non_teaching_staff_postings.
--    One row per employee (most recent active posting).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.non_teaching_staff_school_raw (
    id                          BIGINT        NOT NULL,
    employee_id                 BIGINT        NOT NULL,
    person_id                   BIGINT,
    school_id                   BIGINT,
    posting_date                DATE,
    reporting_date              DATE,
    status                      SMALLINT,
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,
    CONSTRAINT non_teaching_staff_school_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_nts_school_raw_person
    ON stg.non_teaching_staff_school_raw (person_id);
CREATE INDEX IF NOT EXISTS idx_nts_school_raw_school
    ON stg.non_teaching_staff_school_raw (school_id);
COMMENT ON TABLE stg.non_teaching_staff_school_raw IS
    'Raw extract of non_teaching_staff_postings. Current staff-to-school deployments.';


-- -----------------------------------------------------------------------------
-- 3. stg.non_teaching_staff_flat
--    Fully decoded flat staging table.
--    One row per staff member per school per term.
--    Source for dw.non_teaching_staff_dim (SCD2) and dw.non_teaching_staff_fact.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.non_teaching_staff_flat (
    id                          SERIAL        NOT NULL,

    -- Natural keys
    person_id                   BIGINT        NOT NULL,
    employee_id                 BIGINT,
    school_id                   BIGINT,

    -- Academic period
    academic_year               INTEGER       NOT NULL,
    term                        VARCHAR(200)  NOT NULL,
    term_start_date             DATE,

    -- Personal attributes
    surname                     VARCHAR(200),
    given_name                  VARCHAR(200),
    gender                      VARCHAR(200),
    age                         INTEGER,
    nationality                 VARCHAR(200),
    nin                         VARCHAR(200),

    -- Role attributes
    category                    VARCHAR(200),
    role                        VARCHAR(200),
    posting_date                DATE,

    -- Employment attributes
    highest_education_level     VARCHAR(200),
    employment_status           VARCHAR(200),
    is_on_government_payroll    BOOLEAN,
    ipps_number                 VARCHAR(200),
    hrin                        VARCHAR(200),
    hcm_employee_number         VARCHAR(200),
    is_undergoing_training      BOOLEAN,

    -- School attributes (denormalized)
    school_emis_number          VARCHAR(200),
    school_name                 VARCHAR(200),
    school_type                 VARCHAR(200),
    school_ownership            VARCHAR(200),

    -- Admin unit
    admin_unit_source_id        BIGINT,

    -- Flags
    is_primary_school           BOOLEAN       DEFAULT FALSE,

    -- SCD2 change tracking
    change_hash                 TEXT,
    change_reason               TEXT,
    changed_fields              TEXT,

    CONSTRAINT non_teaching_staff_flat_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_nts_flat_person
    ON stg.non_teaching_staff_flat (person_id);
CREATE INDEX IF NOT EXISTS idx_nts_flat_school
    ON stg.non_teaching_staff_flat (school_id);
CREATE INDEX IF NOT EXISTS idx_nts_flat_yr_term
    ON stg.non_teaching_staff_flat (academic_year, term);
COMMENT ON TABLE stg.non_teaching_staff_flat IS
    'Fully decoded flat staging table. One row per staff member per school per term.';
