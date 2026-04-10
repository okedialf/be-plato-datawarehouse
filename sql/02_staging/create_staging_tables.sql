-- =============================================================================
-- EMIS Data Warehouse: Staging Tables
-- Run once to create. Truncated and reloaded nightly by the ETL.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.schools_raw
--    Direct 1:1 copy of public.schools from the OLTP database.
--    Truncated and fully refreshed every night.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.schools_raw (
    id                                   BIGINT,
    school_type_id                       BIGINT,
    logo                                 VARCHAR(191),
    name                                 VARCHAR(200),
    old_emis_number                      VARCHAR(50),
    emis_number                          VARCHAR(50),
    is_government_owned_yn               BOOLEAN,
    school_ownership_status_id           BIGINT,
    legal_ownership_status_id            BIGINT,
    ownership_category_id                BIGINT,
    license_number                       VARCHAR(50),
    registration_number                  VARCHAR(50),
    registration_certificate_status      SMALLINT,
    registration_status_id               BIGINT,
    licence_certificate_expiry_date      DATE,
    registration_certificate_expiry_date DATE,
    has_license_or_registration_certificate BOOLEAN,
    is_certificate_system_generated      BOOLEAN,
    public_service_school_code           VARCHAR(50),
    is_public_school                     BOOLEAN,
    supply_number                        VARCHAR(50),
    physical_address                     TEXT,
    postal_address                       VARCHAR(100),
    district_id                          BIGINT,
    county_id                            BIGINT,
    sub_county_id                        BIGINT,
    parish_id                            BIGINT,
    village_id                           BIGINT,
    region_id                            BIGINT,
    local_government_id                  BIGINT,
    latitude                             DOUBLE PRECISION,
    longitude                            DOUBLE PRECISION,
    email                                VARCHAR(200),
    phone                                VARCHAR(50),
    website                              VARCHAR(100),
    has_health_facility_yn               BOOLEAN,
    health_facility_distance_range_id    BIGINT,
    self_examines_yn                     BOOLEAN,
    examining_body_id                    BIGINT,
    center_number                        VARCHAR(191),
    has_male_students                    BOOLEAN,
    has_female_students                  BOOLEAN,
    founding_body_id                     BIGINT,
    year_founded                         INTEGER,
    funding_source_id                    BIGINT,
    school_land_area                     VARCHAR(191),
    land_owner_type_id                   BIGINT,
    is_operational_yn                    BOOLEAN,
    capital_for_establishment            VARCHAR(191),
    school_closure_reason                VARCHAR(255),
    is_commissioned                      BOOLEAN,
    date_commissioned                    TIMESTAMP(0) WITHOUT TIME ZONE,
    date_created                         TIMESTAMP(0) WITHOUT TIME ZONE,
    date_updated                         TIMESTAMP(0) WITHOUT TIME ZONE,
    operational_status_id                BIGINT,
    suspension_reason_id                 BIGINT,
    admin_unit_id                        BIGINT
);
COMMENT ON TABLE stg.schools_raw IS 'Raw extract of public.schools from OLTP. Full refresh nightly.';


-- -----------------------------------------------------------------------------
-- 2. stg.schools_flat
--    Flattened, decoded staging table. Built from schools_raw + lookup joins.
--    Truncated and rebuilt each night before SCD2 load into dw.schools_dim.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.schools_flat (
    id                  SERIAL         NOT NULL,
    source_id           INTEGER,
    name                VARCHAR(255),
    admin_unit_id       INTEGER,
    emis_number         VARCHAR(50),
    school_type         VARCHAR(30),
    operational_status  VARCHAR(20),
    ownership_status    VARCHAR(60),
    funding_type        VARCHAR(20),
    sex_composition     VARCHAR(20),
    boarding_status     VARCHAR(20),
    founding_body_type  VARCHAR(60),
    effective_date      DATE           NOT NULL DEFAULT CURRENT_DATE,
    expiration_date     DATE,
    is_current          BOOLEAN        NOT NULL DEFAULT TRUE,
    change_hash         TEXT,
    change_reason       TEXT,
    changed_fields      TEXT,
    CONSTRAINT schools_flat_pkey PRIMARY KEY (id),
    CONSTRAINT schools_flat_emis_number_unique UNIQUE (emis_number)
);
COMMENT ON TABLE  stg.schools_flat                IS 'Flattened and decoded staging table built nightly from schools_raw + lookup joins. Source for dw.schools_dim SCD2 load.';
COMMENT ON COLUMN stg.schools_flat.source_id      IS 'PK from the OLTP schools table';
COMMENT ON COLUMN stg.schools_flat.admin_unit_id  IS 'Best available admin unit id (parish preferred). Raw OLTP id — resolved to DW surrogate during DW load.';
COMMENT ON COLUMN stg.schools_flat.change_hash    IS 'MD5 over tracked SCD2 columns. Set during DW load step.';


-- -----------------------------------------------------------------------------
-- 3. stg.admin_units_raw
--    Raw extract of administrative_units.admin_units from OLTP.
--    Loaded manually or when admin boundaries change (not nightly).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.admin_units_raw (
    id               SERIAL        NOT NULL,
    parent_id        INTEGER       NOT NULL,
    admin_unit_type  VARCHAR(50)   NOT NULL,
    name             VARCHAR(60)   NOT NULL,
    admin_unit_code  VARCHAR(255)  NOT NULL,
    voting_code      VARCHAR(10)   NOT NULL,
    effective_date   DATE          NOT NULL DEFAULT CURRENT_DATE,
    expiration_date  DATE          NOT NULL,
    current_status   BOOLEAN       NOT NULL DEFAULT TRUE,
    source_id        INTEGER       NOT NULL,
    CONSTRAINT admin_units_raw_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE  stg.admin_units_raw              IS 'Raw admin units from OLTP. Loaded manually; only refreshed when boundary changes occur.';
COMMENT ON COLUMN stg.admin_units_raw.parent_id    IS 'FK to parent admin unit (e.g. district → region)';
COMMENT ON COLUMN stg.admin_units_raw.source_id    IS 'PK from the OLTP admin_units table';


-- -----------------------------------------------------------------------------
-- 4. stg.school_location_details_raw
--    Raw extract of school_location_details from OLTP.
--    Refreshed nightly alongside schools_raw.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.school_location_details_raw (
    id               BIGINT        NOT NULL,
    school_id        BIGINT,
    physical_address VARCHAR(200),
    postal_address   VARCHAR(100),
    admin_unit_id    BIGINT        NOT NULL,
    latitude         DECIMAL(10,7),
    longitude        DECIMAL(10,7),
    effective_date   DATE          NOT NULL,
    expiration_date  DATE          NOT NULL,
    is_current       BOOLEAN       NOT NULL DEFAULT TRUE,
    date_created     DATE          NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT school_location_details_raw_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE  stg.school_location_details_raw               IS 'Raw location data from OLTP. Full refresh nightly.';
COMMENT ON COLUMN stg.school_location_details_raw.admin_unit_id IS 'FK from admin_units at parish level';
COMMENT ON COLUMN stg.school_location_details_raw.latitude      IS 'Latitude coordinates of institution location';
COMMENT ON COLUMN stg.school_location_details_raw.longitude     IS 'Longitude coordinates of institution location';
