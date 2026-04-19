-- =============================================================================
-- EMIS Data Warehouse: Non-Teaching Staff Mart — DW Tables
-- Schema: dw
-- All IDs are BIGINT. VARCHAR sizes generous.
--
-- DESIGN DECISIONS:
-- 1. SCD2 on non_teaching_staff_dim tracked fields:
--    gender, category, role, highest_education_level,
--    employment_status, is_on_government_payroll
--
-- 2. Fact grain: 1 row per staff member per school per term.
--    Natural key: (person_id, school_source_id, academic_year, term)
--
-- 3. Location: fact → schools_dim → admin_units_dim (no separate location dim)
--
-- 4. Separate category_dim and role_dim seeded from source settings tables.
--    Roles are linked to categories for hierarchical filtering in BI.
--
-- IMPROVEMENTS over data dictionary:
--    - is_primary_school flag on fact
--    - ipps_number and hrin on dim for payroll/HR cross-referencing
--    - role_dim includes parent category_id for hierarchical reporting
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.non_teaching_staff_dim  (SCD Type 2)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.non_teaching_staff_dim (
    -- Surrogate key
    id                          SERIAL        NOT NULL,

    -- Natural / business keys
    source_id                   BIGINT,                   -- persons.id
    employee_id                 BIGINT,                   -- school_employees.id
    ipps_number                 VARCHAR(200),
    hrin                        VARCHAR(200),
    hcm_employee_number         VARCHAR(200),

    -- Personal attributes
    surname                     VARCHAR(200),
    given_name                  VARCHAR(200),
    gender                      VARCHAR(200),             -- SCD2 tracked
    date_of_birth               DATE,
    nationality                 VARCHAR(200),
    nin                         VARCHAR(200),

    -- Role attributes (SCD2 tracked)
    category                    VARCHAR(200),             -- ADMINISTRATIVE / SUPPORT
    role                        VARCHAR(200),             -- BURSAR, COOK, NURSE etc.

    -- Employment attributes (SCD2 tracked)
    highest_education_level     VARCHAR(200),
    employment_status           VARCHAR(200),
    is_on_government_payroll    BOOLEAN,

    -- Dates
    posting_date                DATE,

    -- SCD2 control fields
    effective_date              DATE          NOT NULL,
    expiration_date             DATE          NOT NULL DEFAULT '9999-12-31',
    is_current                  BOOLEAN       NOT NULL DEFAULT TRUE,
    change_hash                 TEXT          NOT NULL,
    change_reason               TEXT          NOT NULL,
    changed_fields              TEXT          NOT NULL,

    CONSTRAINT non_teaching_staff_dim_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE  dw.non_teaching_staff_dim IS
    'Non-teaching staff SCD2 dimension. One row per version of tracked staff attributes.';
COMMENT ON COLUMN dw.non_teaching_staff_dim.source_id IS
    'persons.id from OLTP — natural key for this staff member.';
COMMENT ON COLUMN dw.non_teaching_staff_dim.category IS
    'ADMINISTRATIVE or SUPPORT — from setting_non_teaching_staff_categories.';
COMMENT ON COLUMN dw.non_teaching_staff_dim.role IS
    'Specific role e.g. BURSAR, COOK, LIBRARIAN, NURSE — from setting_non_teaching_staff_roles.';

CREATE INDEX IF NOT EXISTS idx_nts_dim_source_current
    ON dw.non_teaching_staff_dim (source_id, is_current);
CREATE INDEX IF NOT EXISTS idx_nts_dim_employee
    ON dw.non_teaching_staff_dim (employee_id);
CREATE INDEX IF NOT EXISTS idx_nts_dim_effective
    ON dw.non_teaching_staff_dim (effective_date, expiration_date);


-- -----------------------------------------------------------------------------
-- 2. dw.staff_category_dim
--    Static lookup. Values from setting_non_teaching_staff_categories.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.staff_category_dim (
    id              SERIAL        NOT NULL,
    source_id       BIGINT,
    category_name   VARCHAR(200)  NOT NULL,
    CONSTRAINT staff_category_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT staff_category_dim_name_unique UNIQUE (category_name)
);
COMMENT ON TABLE dw.staff_category_dim IS
    'Non-teaching staff category lookup. Seeded from public.setting_non_teaching_staff_categories.';


-- -----------------------------------------------------------------------------
-- 3. dw.staff_role_dim
--    Static lookup. Values from setting_non_teaching_staff_roles.
--    Linked to staff_category_dim via category_name for hierarchical reporting.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.staff_role_dim (
    id              SERIAL        NOT NULL,
    source_id       BIGINT,
    role_name       VARCHAR(200)  NOT NULL,
    category_name   VARCHAR(200),                         -- denormalized parent category
    category_id     INTEGER,                              -- FK → dw.staff_category_dim(id)
    CONSTRAINT staff_role_dim_pkey            PRIMARY KEY (id),
    CONSTRAINT staff_role_dim_name_unique     UNIQUE (role_name)
);
COMMENT ON TABLE dw.staff_role_dim IS
    'Non-teaching staff role lookup. Seeded from public.setting_non_teaching_staff_roles. Includes parent category for hierarchical reporting.';


-- -----------------------------------------------------------------------------
-- 4. dw.non_teaching_staff_fact
--    Grain: 1 row per staff member per school per term.
--    Natural key: (person_id, school_source_id, academic_year, term)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.non_teaching_staff_fact (
    id                      SERIAL      NOT NULL,

    -- Dimension foreign keys
    staff_id                BIGINT      NOT NULL,   -- FK → dw.non_teaching_staff_dim(id)
    school_id               INTEGER     NOT NULL,   -- FK → dw.schools_dim(id)
    date_id                 INTEGER     NOT NULL,   -- FK → dw.date_dim(id)

    -- Natural keys
    person_id               BIGINT,
    school_source_id        BIGINT,
    academic_year           INTEGER,
    term                    VARCHAR(200),

    -- Degenerate dimensions (useful flags directly on fact)
    is_primary_school       BOOLEAN     DEFAULT FALSE,
    is_on_government_payroll BOOLEAN,

    -- ETL metadata
    load_time               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT non_teaching_staff_fact_pkey PRIMARY KEY (id),
    CONSTRAINT non_teaching_staff_fact_natural_key_unique
        UNIQUE (person_id, school_source_id, academic_year, term)
);

COMMENT ON TABLE  dw.non_teaching_staff_fact IS
    'Non-teaching staff fact table. Grain: 1 row per staff member per school per term.';
COMMENT ON COLUMN dw.non_teaching_staff_fact.staff_id IS
    'FK → dw.non_teaching_staff_dim(id). SCD2 surrogate.';
COMMENT ON COLUMN dw.non_teaching_staff_fact.school_id IS
    'FK → dw.schools_dim(id). Join to schools_dim → admin_units_dim for location.';
COMMENT ON COLUMN dw.non_teaching_staff_fact.is_on_government_payroll IS
    'Degenerate dimension — copied from dim for fast filtering without a join.';

ALTER TABLE dw.non_teaching_staff_fact
    ADD CONSTRAINT nts_fact_staff_fk
    FOREIGN KEY (staff_id) REFERENCES dw.non_teaching_staff_dim (id);

ALTER TABLE dw.non_teaching_staff_fact
    ADD CONSTRAINT nts_fact_school_fk
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.non_teaching_staff_fact
    ADD CONSTRAINT nts_fact_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

CREATE INDEX IF NOT EXISTS idx_nts_fact_staff
    ON dw.non_teaching_staff_fact (staff_id);
CREATE INDEX IF NOT EXISTS idx_nts_fact_school
    ON dw.non_teaching_staff_fact (school_id);
CREATE INDEX IF NOT EXISTS idx_nts_fact_date
    ON dw.non_teaching_staff_fact (date_id);
CREATE INDEX IF NOT EXISTS idx_nts_fact_yr_term
    ON dw.non_teaching_staff_fact (academic_year, term);
CREATE INDEX IF NOT EXISTS idx_nts_fact_payroll
    ON dw.non_teaching_staff_fact (is_on_government_payroll);


-- -----------------------------------------------------------------------------
-- Seed static lookup dimensions from source
-- DISTINCT ON handles duplicate names in source tables
-- -----------------------------------------------------------------------------

-- staff_category_dim
INSERT INTO dw.staff_category_dim (source_id, category_name)
SELECT DISTINCT ON (name) id, name
FROM public.setting_non_teaching_staff_categories
ORDER BY name, id
ON CONFLICT (category_name) DO NOTHING;

-- staff_role_dim (with parent category denormalized)
INSERT INTO dw.staff_role_dim (source_id, role_name, category_name, category_id)
SELECT DISTINCT ON (r.name)
    r.id,
    r.name,
    c.category_name,
    c.id
FROM public.setting_non_teaching_staff_roles r
LEFT JOIN dw.staff_category_dim c
    ON c.source_id = r.non_teaching_staff_category_id
ORDER BY r.name, r.id
ON CONFLICT (role_name) DO NOTHING;

-- Verify
SELECT 'staff_category_dim' AS dim, COUNT(*) AS rows FROM dw.staff_category_dim
UNION ALL
SELECT 'staff_role_dim',          COUNT(*) FROM dw.staff_role_dim;
