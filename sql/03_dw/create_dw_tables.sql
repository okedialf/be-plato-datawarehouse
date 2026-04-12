-- =============================================================================
-- EMIS Data Warehouse: DW Dimension and Fact Tables
-- Run once on initial setup.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.date_dim
--    Pre-populated for a wide date range (see populate_date_dim.sql).
--    Never truncated after initial load.
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
    CONSTRAINT date_dim_pkey       PRIMARY KEY (id),
    CONSTRAINT date_dim_date_unique UNIQUE      (system_date)
);
COMMENT ON TABLE  dw.date_dim             IS 'Date dimension. Pre-populated 2010-2040. Never truncated.';
COMMENT ON COLUMN dw.date_dim.system_date IS 'Calendar date (one row per day)';
COMMENT ON COLUMN dw.date_dim."FY"        IS 'Fiscal year (Uganda FY runs Jul-Jun, so Jul 2024 = FY 2025)';
COMMENT ON COLUMN dw.date_dim."Quarter"   IS 'Fiscal quarter label e.g. Q1, Q2, Q3, Q4';
COMMENT ON COLUMN dw.date_dim.short_month IS 'Three-letter month abbreviation: JAN, FEB, MAR …';


-- -----------------------------------------------------------------------------
-- 2. dw.admin_units_dim  (SCD Type 2)
--    One row per admin unit version. current_status = TRUE marks the
--    live version. Loaded manually / on boundary changes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.admin_units_dim (
    id               SERIAL       NOT NULL,
    child_id         INTEGER      NOT NULL,
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
COMMENT ON TABLE  dw.admin_units_dim                  IS 'Admin units SCD2 dimension. One row per version of each admin unit.';
COMMENT ON COLUMN dw.admin_units_dim.child_id         IS 'Self-referencing FK to child admin unit (for hierarchy traversal)';
COMMENT ON COLUMN dw.admin_units_dim.admin_unit_type  IS 'REGION, DISTRICT, COUNTY, SUB_COUNTY, PARISH, WARD, VILLAGE …';
COMMENT ON COLUMN dw.admin_units_dim.source_id        IS 'PK from the OLTP admin_units table';
COMMENT ON COLUMN dw.admin_units_dim.effective_date   IS 'Date this version became effective';
COMMENT ON COLUMN dw.admin_units_dim.expiration_date  IS 'Date this version expired. 9999-12-31 = still current';
COMMENT ON COLUMN dw.admin_units_dim.current_status   IS 'TRUE = this is the current active version';

ALTER TABLE dw.admin_units_dim
    ADD CONSTRAINT admin_units_dim_child_id_foreign
    FOREIGN KEY (child_id) REFERENCES dw.admin_units_dim (id)
    DEFERRABLE INITIALLY DEFERRED;


-- -----------------------------------------------------------------------------
-- 3. dw.schools_dim  (SCD Type 2)
--    One row per school attribute-version. is_current = TRUE marks the
--    live version. Loaded nightly via SCD2 upsert.
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
    CONSTRAINT schools_dim_pkey               PRIMARY KEY (id)
);
COMMENT ON TABLE  dw.schools_dim                 IS 'Schools SCD2 dimension. One row per school attribute-version.';
COMMENT ON COLUMN dw.schools_dim.source_id       IS 'PK from OLTP schools table — the natural key';
COMMENT ON COLUMN dw.schools_dim.admin_unit_id   IS 'FK → dw.admin_units_dim(id). DW surrogate for the effective admin unit version';
COMMENT ON COLUMN dw.schools_dim.emis_number     IS 'EMIS number: level-code + alpha + 6-digit number. Unique per live school.';
COMMENT ON COLUMN dw.schools_dim.school_type     IS 'PREPRIMARY, PRIMARY, SECONDARY, CERTIFICATE, DIPLOMA, DEGREE, INTERNATIONAL';
COMMENT ON COLUMN dw.schools_dim.operational_status IS 'ACTIVE, SUSPENDED, CLOSED';
COMMENT ON COLUMN dw.schools_dim.ownership_status   IS 'SOLE PROPRIETOR, LEGAL PARTNERSHIP, PUBLIC LIMITED COMPANY etc.';
COMMENT ON COLUMN dw.schools_dim.funding_type       IS 'GOVT AIDED, PRIVATE';
COMMENT ON COLUMN dw.schools_dim.sex_composition    IS 'FEMALES ONLY, MALES ONLY, MIXED';
COMMENT ON COLUMN dw.schools_dim.boarding_status    IS 'DAY SCHOOL, FULLY BOARDING, DAY AND BOARDING, RESIDENTIAL, NON RESIDENTIAL';
COMMENT ON COLUMN dw.schools_dim.founding_body_type IS 'GOVERNMENT, ANGLICAN, ROMAN CATHOLIC, ISLAMIC, SEVENTH DAY ADVENTIST, ORTHODOX, COMMUNITY, ENTREPRENEURS, CBO/NGO';
COMMENT ON COLUMN dw.schools_dim.effective_date     IS 'Date this SCD2 row became effective';
COMMENT ON COLUMN dw.schools_dim.expiration_date    IS 'Date this SCD2 row expired. 9999-12-31 = still current';
COMMENT ON COLUMN dw.schools_dim.is_current         IS 'TRUE = this is the current version of the school record';
COMMENT ON COLUMN dw.schools_dim.change_hash        IS 'MD5 hash of all tracked SCD2 columns. Hash diff triggers new SCD2 version.';
COMMENT ON COLUMN dw.schools_dim.change_reason      IS 'Human-readable reason for SCD2 version creation (e.g. "School renamed")';
COMMENT ON COLUMN dw.schools_dim.changed_fields     IS 'Comma-separated list of column names that changed in this version';

ALTER TABLE dw.schools_dim
    ADD CONSTRAINT schools_dim_admin_unit_id_foreign
    FOREIGN KEY (admin_unit_id) REFERENCES dw.admin_units_dim (id);


-- -----------------------------------------------------------------------------
-- 4. dw.school_location_details_dim  (SCD Type 2)
--    Tracks historical school location changes.
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
COMMENT ON TABLE  dw.school_location_details_dim               IS 'School location SCD2 dimension. Tracks address and coordinate history.';
COMMENT ON COLUMN dw.school_location_details_dim.school_id     IS 'FK → dw.schools_dim(id)';
COMMENT ON COLUMN dw.school_location_details_dim.admin_unit_id IS 'FK → dw.admin_units_dim(id) at parish level';
COMMENT ON COLUMN dw.school_location_details_dim.latitude      IS 'Latitude of school location';
COMMENT ON COLUMN dw.school_location_details_dim.longitude     IS 'Longitude of school location';
COMMENT ON COLUMN dw.school_location_details_dim.effective_date IS 'Date this location record became effective';
COMMENT ON COLUMN dw.school_location_details_dim.expiration_date IS '9999-12-31 = current location';

ALTER TABLE dw.school_location_details_dim
    ADD CONSTRAINT school_location_dim_school_id_foreign
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.school_location_details_dim
    ADD CONSTRAINT school_location_dim_admin_unit_id_foreign
    FOREIGN KEY (admin_unit_id) REFERENCES dw.admin_units_dim (id);


-- -----------------------------------------------------------------------------
-- 5. dw.school_fact  (Daily snapshot)
--    One row per school per day. Joined to current SCD2 dimension rows
--    at load time so slicing by school attributes is via dimension join.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.school_fact (
    id            SERIAL    NOT NULL,
    school_id     INTEGER   NOT NULL,
    date_id       INTEGER   NOT NULL,
    location_time TIMESTAMP(0) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(0),
    CONSTRAINT school_fact_pkey                PRIMARY KEY (id),
    CONSTRAINT school_fact_school_date_unique  UNIQUE      (school_id, date_id)
);
COMMENT ON TABLE  dw.school_fact               IS 'Daily school snapshot fact. One row per school per day. Enables COUNT(*) reports across any dimension.';
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
-- Indexes for common query patterns
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
