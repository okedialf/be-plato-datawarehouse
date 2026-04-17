-- =============================================================================
-- EMIS Data Warehouse: Teachers Mart — DW Tables
-- Schema: dw
-- All IDs are BIGINT. VARCHAR sizes are generous.
--
-- DESIGN DECISIONS:
-- 1. SCD2 on teacher_dim — tracked fields:
--    gender, qualification, qualification_category, teacher_type,
--    designation, highest_education_level, is_on_government_payroll, discipline
--
-- 2. HR fact grain: 1 row per teacher per school per term.
--    Natural key: (person_id, school_source_id, academic_year, term)
--
-- 3. Location: fact references dw.schools_dim → dw.admin_units_dim.
--    No separate location dimension — single version of truth.
--
-- 4. discipline (ARTS/SCIENCE/BOTH) is SCD2 tracked because it can change
--    as a teacher picks up new subjects over their career.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.teacher_dim  (SCD Type 2)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.teacher_dim (
    -- Surrogate key
    id                          SERIAL        NOT NULL,

    -- Natural / business keys
    source_id                   BIGINT,                   -- teachers.person_id
    employee_id                 BIGINT,                   -- school_employees.id
    ipps_number                 VARCHAR(200),
    tmis_number                 VARCHAR(200),
    moes_license_number         VARCHAR(200),
    hrin                        VARCHAR(200),
    hcm_employee_number         VARCHAR(200),

    -- Personal attributes
    surname                     VARCHAR(200),
    given_name                  VARCHAR(200),
    gender                      VARCHAR(100),             -- SCD2 tracked
    date_of_birth               DATE,
    nationality                 VARCHAR(200),
    nin                         VARCHAR(200),
    passport_no                 VARCHAR(200),

    -- Professional attributes (SCD2 tracked)
    teacher_type                VARCHAR(200),             -- QUALIFIED / TRAINED
    qualification               VARCHAR(200),
    qualification_category      VARCHAR(200),
    designation                 VARCHAR(200),
    highest_education_level     VARCHAR(200),
    discipline                  VARCHAR(100),             -- ARTS / SCIENCE / BOTH
    is_on_government_payroll    BOOLEAN,

    -- Employment
    first_appointment_date      DATE,
    posting_date                DATE,

    -- SCD2 control fields
    effective_date              DATE          NOT NULL,
    expiration_date             DATE          NOT NULL DEFAULT '9999-12-31',
    is_current                  BOOLEAN       NOT NULL DEFAULT TRUE,
    change_hash                 TEXT          NOT NULL,
    change_reason               TEXT          NOT NULL,
    changed_fields              TEXT          NOT NULL,

    CONSTRAINT teacher_dim_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE  dw.teacher_dim IS
    'Teacher SCD2 dimension. One row per version of tracked teacher attributes.';
COMMENT ON COLUMN dw.teacher_dim.source_id IS
    'teachers.person_id from OLTP — natural key for this teacher.';
COMMENT ON COLUMN dw.teacher_dim.employee_id IS
    'school_employees.id — the employment record key used in teacher_subjects joins.';
COMMENT ON COLUMN dw.teacher_dim.discipline IS
    'ARTS / SCIENCE / BOTH — derived from subjects taught. SCD2 tracked.';

CREATE INDEX IF NOT EXISTS idx_teacher_dim_source_current
    ON dw.teacher_dim (source_id, is_current);
CREATE INDEX IF NOT EXISTS idx_teacher_dim_employee
    ON dw.teacher_dim (employee_id);
CREATE INDEX IF NOT EXISTS idx_teacher_dim_ipps
    ON dw.teacher_dim (ipps_number);
CREATE INDEX IF NOT EXISTS idx_teacher_dim_effective
    ON dw.teacher_dim (effective_date, expiration_date);


-- -----------------------------------------------------------------------------
-- 2. dw.teacher_type_dim
--    Static lookup. Values from setting_teacher_types.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.teacher_type_dim (
    id              SERIAL        NOT NULL,
    source_id       BIGINT,
    teacher_type    VARCHAR(200)  NOT NULL,
    CONSTRAINT teacher_type_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT teacher_type_dim_type_unique UNIQUE (teacher_type)
);
COMMENT ON TABLE dw.teacher_type_dim IS
    'Teacher type lookup. Seeded from public.setting_teacher_types.';


-- -----------------------------------------------------------------------------
-- 3. dw.qualification_dim
--    Static lookup. Values from setting_teacher_professional_qualifications.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.qualification_dim (
    id                      SERIAL        NOT NULL,
    source_id               BIGINT,
    qualification_name      VARCHAR(200)  NOT NULL,
    qualification_category  VARCHAR(200),
    is_for_pre_primary      BOOLEAN       DEFAULT FALSE,
    is_for_primary          BOOLEAN       DEFAULT FALSE,
    is_for_secondary        BOOLEAN       DEFAULT FALSE,
    is_for_certificate      BOOLEAN       DEFAULT FALSE,
    is_for_diploma          BOOLEAN       DEFAULT FALSE,
    is_for_degree           BOOLEAN       DEFAULT FALSE,
    CONSTRAINT qualification_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT qualification_dim_name_unique UNIQUE (qualification_name)
);
COMMENT ON TABLE dw.qualification_dim IS
    'Teacher professional qualification lookup. Seeded from public.setting_teacher_professional_qualifications.';


-- -----------------------------------------------------------------------------
-- 4. dw.designation_dim
--    Static lookup. Values from setting_teaching_staff_designations.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.designation_dim (
    id              SERIAL        NOT NULL,
    source_id       BIGINT,
    designation     VARCHAR(200)  NOT NULL,
    school_type     VARCHAR(200),
    CONSTRAINT designation_dim_pkey            PRIMARY KEY (id),
    CONSTRAINT designation_dim_desig_unique    UNIQUE (designation)
);
COMMENT ON TABLE dw.designation_dim IS
    'Teacher designation lookup. Seeded from public.setting_teaching_staff_designations.';


-- -----------------------------------------------------------------------------
-- 5. dw.hr_fact
--    Grain: 1 row per teacher per school per term.
--    Natural key: (person_id, school_source_id, academic_year, term)
--
--    Location slicing: hr_fact → schools_dim → admin_units_dim
--    No separate location dimension — single version of truth.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.hr_fact (
    id                      SERIAL      NOT NULL,

    -- Dimension foreign keys (surrogate keys)
    teacher_id              BIGINT      NOT NULL,   -- FK → dw.teacher_dim(id)
    school_id               INTEGER     NOT NULL,   -- FK → dw.schools_dim(id)
    date_id                 INTEGER     NOT NULL,   -- FK → dw.date_dim(id)

    -- Natural keys (retained for deduplication and debugging)
    person_id               BIGINT,                -- teachers.person_id from OLTP
    school_source_id        BIGINT,                -- schools.id from OLTP
    academic_year           INTEGER,
    term                    VARCHAR(100),

    -- ETL metadata
    load_time               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT hr_fact_pkey PRIMARY KEY (id),
    CONSTRAINT hr_fact_natural_key_unique
        UNIQUE (person_id, school_source_id, academic_year, term)
);

COMMENT ON TABLE  dw.hr_fact IS
    'HR teacher fact table. Grain: 1 row per teacher per school per term.';
COMMENT ON COLUMN dw.hr_fact.teacher_id IS
    'FK → dw.teacher_dim(id). Points to the SCD2 version current at the term date.';
COMMENT ON COLUMN dw.hr_fact.school_id IS
    'FK → dw.schools_dim(id). Join to schools_dim → admin_units_dim for location.';
COMMENT ON COLUMN dw.hr_fact.date_id IS
    'FK → dw.date_dim(id). Set to the first day of the term.';

-- FK constraints
ALTER TABLE dw.hr_fact
    ADD CONSTRAINT hr_fact_teacher_fk
    FOREIGN KEY (teacher_id) REFERENCES dw.teacher_dim (id);

ALTER TABLE dw.hr_fact
    ADD CONSTRAINT hr_fact_school_fk
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.hr_fact
    ADD CONSTRAINT hr_fact_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_hr_fact_teacher
    ON dw.hr_fact (teacher_id);
CREATE INDEX IF NOT EXISTS idx_hr_fact_school
    ON dw.hr_fact (school_id);
CREATE INDEX IF NOT EXISTS idx_hr_fact_date
    ON dw.hr_fact (date_id);
CREATE INDEX IF NOT EXISTS idx_hr_fact_yr_term
    ON dw.hr_fact (academic_year, term);
CREATE INDEX IF NOT EXISTS idx_hr_fact_person
    ON dw.hr_fact (person_id);


-- -----------------------------------------------------------------------------
-- Seed static lookup dimensions from source
-- Run queries against public.setting_* after creating tables
-- to get actual values — do not hardcode assumptions
-- -----------------------------------------------------------------------------

-- teacher_type_dim: seeded from public.setting_teacher_types
INSERT INTO dw.teacher_type_dim (source_id, teacher_type)
SELECT id, name FROM public.setting_teacher_types
ORDER BY id
ON CONFLICT (teacher_type) DO UPDATE SET source_id = EXCLUDED.source_id;

-- qualification_dim: seeded from public.setting_teacher_professional_qualifications
INSERT INTO dw.qualification_dim (
    source_id, qualification_name, qualification_category,
    is_for_pre_primary, is_for_primary, is_for_secondary,
    is_for_certificate, is_for_diploma, is_for_degree
)
SELECT
    id, name, qualification_category,
    is_for_pre_primary, is_for_primary, is_for_secondary,
    is_for_certificate, is_for_diploma, is_for_degree
FROM public.setting_teacher_professional_qualifications
WHERE is_archived = FALSE
ORDER BY id
ON CONFLICT (qualification_name) DO UPDATE
    SET source_id            = EXCLUDED.source_id,
        qualification_category = EXCLUDED.qualification_category;

-- designation_dim: seeded from public.setting_teaching_staff_designations
INSERT INTO dw.designation_dim (source_id, designation, school_type)
SELECT
    std.id,
    std.name,
    st.name AS school_type
FROM public.setting_teaching_staff_designations std
LEFT JOIN public.setting_school_types st ON st.id = std.school_type_id
ORDER BY std.id
ON CONFLICT (designation) DO UPDATE SET source_id = EXCLUDED.source_id;

-- Verify seeds
SELECT 'teacher_type_dim'  AS dim, COUNT(*) AS rows FROM dw.teacher_type_dim
UNION ALL
SELECT 'qualification_dim' AS dim, COUNT(*) AS rows FROM dw.qualification_dim
UNION ALL
SELECT 'designation_dim'   AS dim, COUNT(*) AS rows FROM dw.designation_dim;
