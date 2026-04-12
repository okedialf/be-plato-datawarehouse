-- =============================================================================
-- EMIS Data Warehouse: DW Dimension and Fact Tables
-- Run once on initial setup.
--
-- Fixes applied during testing (2026-04):
--   - school_type changed VARCHAR(30) → VARCHAR(50)
--   - day column added to date_dim
--   - UNIQUE constraint added to date_dim.system_date
--   - child_id made nullable in admin_units_dim (top-level units have no parent)
--   - Self-referencing FK on admin_units_dim dropped (causes insert ordering issues)
--   - UNIQUE constraint added to school_fact(school_id, date_id)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.date_dim
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.date_dim (
    id              SERIAL        NOT NULL,
    system_date     DATE          NOT NULL,
    day             INTEGER       NOT NULL,
    month           INTEGER       NOT NULL,
    month_name      VARCHAR(9)    NOT NULL,
    short_month     VARCHAR(3)    NOT NULL,
    year            INTEGER       NOT NULL,
    "FY"            INTEGER       NOT NULL,
    "Quarter"       VARCHAR(10)   NOT NULL,
    CONSTRAINT date_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT date_dim_date_unique UNIQUE      (system_date)
);
COMMENT ON TABLE  dw.date_dim             IS 'Date dimension. Pre-populated 2023-2037. Never truncated.';
COMMENT ON COLUMN dw.date_dim.system_date IS 'Calendar date (one row per day)';
COMMENT ON COLUMN dw.date_dim.day         IS 'Day of month (1-31)';
COMMENT ON COLUMN dw.date_dim."FY"        IS 'Uganda Fiscal Year (Jul-Jun). Jul 2025-Jun 2026 = FY2026';
COMMENT ON COLUMN dw.date_dim."Quarter"   IS 'Fiscal quarter: Q1=Jul-Sep, Q2=Oct-Dec, Q3=Jan-Mar, Q4=Apr-Jun';
COMMENT ON COLUMN dw.date_dim.short_month IS 'Three-letter month abbreviation: JAN, FEB, MAR etc.';


-- -----------------------------------------------------------------------------
-- 2. dw.admin_units_dim  (SCD Type 2)
--
--    NOTE: child_id is nullable — top-level units (Country) have no parent.
--    NOTE: Self-referencing FK on child_id has been removed. The hierarchy
--          is navigated via admin_unit_type and source_id instead. The FK
--          caused insert ordering failures during bulk loads.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.admin_units_dim (
    id               SERIAL       NOT NULL,
    child_id         INTEGER,
    admin_unit_type  VARCHAR(50)  NOT NULL,
    name             VARCHAR(60)  NOT NULL,
    admin_unit_code  VARCHAR(255) NOT NULL,
    voting_code      VARCHAR(10)  NOT NULL,
    effective_date   DATE         NOT NULL,
    expiration_date  DATE         NOT NULL,
    current_status   BOOLEAN      NOT NULL DEFAULT TRUE,
    source_id        INTEGER      NOT NULL,
    CONSTRAINT admin_units_dim_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE  dw.admin_units_dim                 IS 'Admin units SCD2 dimension. One row per version of each admin unit.';
COMMENT ON COLUMN dw.admin_units_dim.child_id        IS 'Source_id of the parent admin unit. NULL for top-level (Country).';
COMMENT ON COLUMN dw.admin_units_dim.admin_unit_type IS 'REGION, DISTRICT, COUNTY, SUB_COUNTY, PARISH, WARD, VILLAGE etc.';
COMMENT ON COLUMN dw.admin_units_dim.source_id       IS 'PK from the OLTP admin_units table';
COMMENT ON COLUMN dw.admin_units_dim.effective_date  IS 'Date this version became effective';
COMMENT ON COLUMN dw.admin_units_dim.expiration_date IS 'Date this version expired. 9999-12-31 = still current';
COMMENT ON COLUMN dw.admin_units_dim.current_status  IS 'TRUE = this is the current active version';


-- -----------------------------------------------------------------------------
-- 3. dw.schools_dim  (SCD Type 2)
--
--    NOTE: school_type is VARCHAR(50) — full names like
--    "CERTIFICATE AWARDING INSTITUTION" exceed 30 chars.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.schools_dim (
    id                  SERIAL       NOT NULL,
    source_id           INTEGER,
    name                VARCHAR(255),
    admin_unit_id       INTEGER,
    emis_number         VARCHAR(50),
    school_type         VARCHAR(50),
    operational_status  VARCHAR(20),
    ownership_status    VARCHAR(60),
    funding_type        VARCHAR(20),
    sex_composition     VARCHAR(20),
    boarding_status     VARCHAR(20),
    founding_body_type  VARCHAR(60),
    effective_date      DATE,
    expiration_date     DATE         NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN,
    change_hash         TEXT         NOT NULL,
    change_reason       TEXT         NOT NULL,
    changed_fields      TEXT         NOT NULL,
    CONSTRAINT schools_dim_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE  dw.schools_dim                    IS 'Schools SCD2 dimension. One row per school attribute-version.';
COMMENT ON COLUMN dw.schools_dim.source_id          IS 'PK from OLTP schools table — the natural key';
COMMENT ON COLUMN dw.schools_dim.admin_unit_id      IS 'FK → dw.admin_units_dim(id). DW surrogate for the effective admin unit version';
COMMENT ON COLUMN dw.schools_dim.school_type        IS 'PREPRIMARY, PRIMARY, SECONDARY, CERTIFICATE AWARDING INSTITUTION, DIPLOMA AWARDING INSTITUTION, DEGREE AWARDING INSTITUTION, INTERNATIONAL';
COMMENT ON COLUMN dw.schools_dim.operational_status IS 'ACTIVE, SUSPENDED, CLOSED';
COMMENT ON COLUMN dw.schools_dim.ownership_status   IS 'SOLE PROPRIETOR, LEGAL PARTNERSHIP, PUBLIC LIMITED COMPANY etc.';
COMMENT ON COLUMN dw.schools_dim.funding_type       IS 'GOVT AIDED, PRIVATE';
COMMENT ON COLUMN dw.schools_dim.sex_composition    IS 'FEMALES ONLY, MALES ONLY, MIXED';
COMMENT ON COLUMN dw.schools_dim.boarding_status    IS 'DAY SCHOOL, FULLY BOARDING, DAY AND BOARDING, RESIDENTIAL, NON RESIDENTIAL';
COMMENT ON COLUMN dw.schools_dim.founding_body_type IS 'GOVERNMENT, ANGLICAN, ROMAN CATHOLIC, ISLAMIC, SEVENTH DAY ADVENTIST, ORTHODOX, COMMUNITY, ENTREPRENEURS, CBO/NGO';
COMMENT ON COLUMN dw.schools_dim.effective_date     IS 'Date this SCD2 row became effective';
COMMENT ON COLUMN dw.schools_dim.expiration_date    IS 'Date this SCD2 row expired. 9999-12-31 = still current';
COMMENT ON COLUMN dw.schools_dim.is_current         IS 'TRUE = this is the current version of the school record';
COMMENT ON COLUMN dw.schools_dim.change_hash        IS 'MD5 hash of all tracked SCD2 columns. Hash diff triggers new version.';
COMMENT ON COLUMN dw.schools_dim.change_reason      IS 'Human-readable reason e.g. "School renamed"';
COMMENT ON COLUMN dw.schools_dim.changed_fields     IS 'Comma-separated list of column names that changed';

ALTER TABLE dw.schools_dim
    ADD CONSTRAINT schools_dim_admin_unit_id_foreign
    FOREIGN KEY (admin_unit_id) REFERENCES dw.admin_units_dim (id);


-- -----------------------------------------------------------------------------
-- 4. dw.school_location_details_dim  (SCD Type 2)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.school_location_details_dim (
    id               INTEGER      NOT NULL,
    school_id        INTEGER,
    physical_address VARCHAR(200),
    postal_address   VARCHAR(100),
    admin_unit_id    INTEGER,
    latitude         FLOAT,
    longitude        FLOAT,
    effective_date   DATE         NOT NULL DEFAULT CURRENT_DATE,
    expiration_date  DATE         NOT NULL DEFAULT '9999-12-31',
    is_current       BOOLEAN      NOT NULL DEFAULT TRUE,
    date_created     DATE,
    CONSTRAINT school_location_details_dim_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE  dw.school_location_details_dim IS 'School location SCD2 dimension. Tracks address and coordinate history.';

ALTER TABLE dw.school_location_details_dim
    ADD CONSTRAINT school_location_dim_school_id_foreign
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.school_location_details_dim
    ADD CONSTRAINT school_location_dim_admin_unit_id_foreign
    FOREIGN KEY (admin_unit_id) REFERENCES dw.admin_units_dim (id);


-- -----------------------------------------------------------------------------
-- 5. dw.school_fact  (Daily snapshot)
--
--    NOTE: UNIQUE constraint on (school_id, date_id) prevents duplicate
--    snapshots and enables ON CONFLICT DO NOTHING for safe re-runs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.school_fact (
    id            SERIAL    NOT NULL,
    school_id     INTEGER   NOT NULL,
    date_id       INTEGER   NOT NULL,
    location_time TIMESTAMP(0) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(0),
    CONSTRAINT school_fact_pkey               PRIMARY KEY (id),
    CONSTRAINT school_fact_school_date_unique UNIQUE      (school_id, date_id)
);
COMMENT ON TABLE  dw.school_fact               IS 'Daily school snapshot fact. One row per school per day.';
COMMENT ON COLUMN dw.school_fact.school_id     IS 'FK → dw.schools_dim(id). Points to the SCD2 row valid on that date.';
COMMENT ON COLUMN dw.school_fact.date_id       IS 'FK → dw.date_dim(id). Daily granularity.';
COMMENT ON COLUMN dw.school_fact.location_time IS 'ETL load timestamp';

ALTER TABLE dw.school_fact
    ADD CONSTRAINT school_fact_school_id_foreign
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.school_fact
    ADD CONSTRAINT school_fact_date_id_foreign
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);


-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_schools_dim_source_id_current
    ON dw.schools_dim (source_id, is_current);

CREATE INDEX IF NOT EXISTS idx_schools_dim_effective
    ON dw.schools_dim (effective_date, expiration_date);

CREATE INDEX IF NOT EXISTS idx_admin_units_dim_source_type_current
    ON dw.admin_units_dim (source_id, admin_unit_type, current_status);

CREATE INDEX IF NOT EXISTS idx_school_fact_date
    ON dw.school_fact (date_id);

CREATE INDEX IF NOT EXISTS idx_date_dim_system_date
    ON dw.date_dim (system_date);
