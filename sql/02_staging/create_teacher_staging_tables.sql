-- =============================================================================
-- EMIS Data Warehouse: Teachers Mart — Staging Tables
-- Schema: stg
-- All IDs are BIGINT to match the actual source system.
-- No UUID columns (staff_postings is UUID but we do not use it here).
-- VARCHAR sizes are generous based on actual source column sizes.
--
-- Source table chain:
--   persons → teachers → school_employees → teaching_staff
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.teachers_raw
--    One row per teacher. Joins persons + teachers + school_employees.
--    This is the master teacher extract — all personal and professional attrs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.teachers_raw (
    -- Keys
    person_id                   BIGINT        NOT NULL,   -- teachers.person_id (PK)
    employee_id                 BIGINT,                   -- school_employees.id

    -- Personal attributes (from persons)
    surname                     VARCHAR(200),
    given_name                  VARCHAR(200),
    gender                      VARCHAR(100),
    date_of_birth               DATE,
    nationality                 VARCHAR(200),

    -- Identity documents (from identity_documents)
    nin                         VARCHAR(200),
    passport_no                 VARCHAR(200),

    -- Teacher professional attributes (from teachers)
    teacher_type                VARCHAR(200),             -- QUALIFIED / TRAINED
    qualification               VARCHAR(200),             -- from setting_teacher_professional_qualifications
    qualification_category      VARCHAR(200),             -- category on qualification
    designation                 VARCHAR(200),             -- from setting_teaching_staff_designations
    ipps_number                 VARCHAR(200),
    tmis_number                 VARCHAR(200),
    moes_license_number         VARCHAR(200),
    is_on_government_payroll    BOOLEAN,
    first_appointment_date      DATE,
    posting_date                DATE,
    subject_category            VARCHAR(200),             -- subject_category on teachers table

    -- Employment attributes (from school_employees)
    highest_education_level     VARCHAR(200),
    employment_status           VARCHAR(200),
    hrin                        VARCHAR(200),             -- HR ID number
    hcm_employee_number         VARCHAR(200),
    date_started                DATE,
    date_ended                  DATE,
    weekly_teaching_periods     SMALLINT,
    is_undergoing_training      BOOLEAN,

    -- Discipline (derived from teacher_subjects in flatten step)
    discipline                  VARCHAR(100),             -- ARTS / SCIENCE / BOTH

    -- Current school (denormalized for convenience)
    current_school_id           BIGINT,

    -- Audit
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,

    CONSTRAINT teachers_raw_pkey PRIMARY KEY (person_id)
);
COMMENT ON TABLE stg.teachers_raw IS
    'Raw extract of teachers joined with persons and school_employees. One row per teacher.';
COMMENT ON COLUMN stg.teachers_raw.discipline IS
    'Derived in flatten from teacher_subjects → setting_secondary_school_subjects.is_science_subject. ARTS / SCIENCE / BOTH.';


-- -----------------------------------------------------------------------------
-- 2. stg.teacher_subjects_raw
--    One row per teacher per subject taught.
--    Used to derive discipline (ARTS/SCIENCE) in the flatten step.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.teacher_subjects_raw (
    id                          BIGINT        NOT NULL,
    employee_id                 BIGINT        NOT NULL,   -- school_employees.id
    person_id                   BIGINT,                   -- teachers.person_id (resolved in extract)
    subject_id                  BIGINT,
    subject_name                VARCHAR(200),
    is_science_subject          BOOLEAN,
    is_language_subject         BOOLEAN,
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,
    CONSTRAINT teacher_subjects_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_teacher_subjects_raw_employee
    ON stg.teacher_subjects_raw (employee_id);
CREATE INDEX IF NOT EXISTS idx_teacher_subjects_raw_person
    ON stg.teacher_subjects_raw (person_id);
COMMENT ON TABLE stg.teacher_subjects_raw IS
    'Raw extract of teacher_subjects joined with setting_secondary_school_subjects.';


-- -----------------------------------------------------------------------------
-- 3. stg.teacher_school_raw
--    One row per teacher per school (current deployment).
--    Sourced from teaching_staff which links teacher to school per term.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.teacher_school_raw (
    employee_id                 BIGINT        NOT NULL,
    person_id                   BIGINT        NOT NULL,
    school_id                   BIGINT,
    is_transfer_appointment     BOOLEAN,
    appointment_date            DATE,
    staff_category              VARCHAR(200),
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,
    CONSTRAINT teacher_school_raw_pkey PRIMARY KEY (employee_id)
);
CREATE INDEX IF NOT EXISTS idx_teacher_school_raw_person
    ON stg.teacher_school_raw (person_id);
CREATE INDEX IF NOT EXISTS idx_teacher_school_raw_school
    ON stg.teacher_school_raw (school_id);
COMMENT ON TABLE stg.teacher_school_raw IS
    'Raw extract of teaching_staff. Current teacher-to-school deployment.';


-- -----------------------------------------------------------------------------
-- 4. stg.teachers_flat
--    Fully decoded flat staging table.
--    One row per teacher per school per term.
--    Source for dw.teacher_dim (SCD2) and dw.hr_fact.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.teachers_flat (
    id                          SERIAL        NOT NULL,

    -- Natural keys
    person_id                   BIGINT        NOT NULL,   -- teachers.person_id
    employee_id                 BIGINT,                   -- school_employees.id
    school_id                   BIGINT,                   -- teaching_staff.school_id

    -- Academic period
    academic_year               INTEGER       NOT NULL,
    term                        VARCHAR(100)  NOT NULL,
    term_start_date             DATE,

    -- Personal attributes
    surname                     VARCHAR(200),
    given_name                  VARCHAR(200),
    gender                      VARCHAR(100),
    age                         INTEGER,
    nationality                 VARCHAR(200),
    nin                         VARCHAR(200),
    passport_no                 VARCHAR(200),

    -- Professional attributes
    teacher_type                VARCHAR(200),
    qualification               VARCHAR(200),
    qualification_category      VARCHAR(200),
    designation                 VARCHAR(200),
    discipline                  VARCHAR(100),             -- ARTS / SCIENCE / BOTH
    ipps_number                 VARCHAR(200),
    tmis_number                 VARCHAR(200),
    moes_license_number         VARCHAR(200),
    is_on_government_payroll    BOOLEAN,
    first_appointment_date      DATE,
    posting_date                DATE,

    -- Employment
    highest_education_level     VARCHAR(200),
    employment_status           VARCHAR(200),
    hrin                        VARCHAR(200),
    hcm_employee_number         VARCHAR(200),
    weekly_teaching_periods     SMALLINT,
    is_undergoing_training      BOOLEAN,

    -- School attributes (denormalized)
    school_emis_number          VARCHAR(100),
    school_name                 VARCHAR(200),
    school_type                 VARCHAR(200),
    school_ownership            VARCHAR(200),
    school_level                VARCHAR(200),

    -- Admin unit (resolved against dw.admin_units_dim)
    admin_unit_source_id        BIGINT,

    -- SCD2 change tracking fields
    change_hash                 TEXT,
    change_reason               TEXT,
    changed_fields              TEXT,

    CONSTRAINT teachers_flat_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_teachers_flat_person
    ON stg.teachers_flat (person_id);
CREATE INDEX IF NOT EXISTS idx_teachers_flat_school
    ON stg.teachers_flat (school_id);
CREATE INDEX IF NOT EXISTS idx_teachers_flat_yr_term
    ON stg.teachers_flat (academic_year, term);
COMMENT ON TABLE stg.teachers_flat IS
    'Fully decoded flat staging table. One row per teacher per school per term. Source for dw.teacher_dim and dw.hr_fact.';
