-- =============================================================================
-- EMIS Data Warehouse: Infrastructure Data Mart — DW Tables
-- Schema: dw
--
-- STAR SCHEMA:
--   dw.infrastructure_type_dim  — building/structure type lookup
--   dw.infrastructure_fact      — 1 row per school per type per term
--
-- DIMENSION REFERENCES (existing + new):
--   school_id       → dw.schools_dim     (Schools mart)
--   date_id         → dw.date_dim        (Schools mart)
--   infra_type_id   → dw.infrastructure_type_dim  (NEW — this file)
--
-- No separate Location, Completion Status, Gender Usage or User Category dims.
-- These are stored as decoded VARCHAR columns on the fact table (degenerate dims).
-- The original design had 4 separate dims for these — removed as they add joins
-- without value since each has only 3-4 possible values.
--
-- GRAIN: 1 row per school per infrastructure type per academic year per term.
-- Natural key: (school_source_id, building_source_id, academic_year, term)
--
-- MEASURES:
--   total_number — count of structures of this type at the school
--   area         — total floor area in square metres
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.infrastructure_type_dim
--    One row per building/infrastructure type.
--    Seeded from setting_school_building_types + setting_school_infrastructure_types.
--    Not SCD2 — full upsert on each ETL run.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.infrastructure_type_dim (
    id                      SERIAL          NOT NULL,

    -- Natural key
    source_id               BIGINT          NOT NULL,   -- setting_school_building_types.id
    type_name               VARCHAR(200)    NOT NULL,
    item_category           VARCHAR(200),               -- from setting_school_item_categories

    -- Flags from setting_school_infrastructure_types
    is_lab_yn               BOOLEAN         DEFAULT FALSE,
    is_sanitation_yn        BOOLEAN         DEFAULT FALSE,
    is_area_captured_yn     BOOLEAN         DEFAULT FALSE,
    gender_usage_yn         BOOLEAN         DEFAULT FALSE,  -- does this type capture gender usage?
    user_category_yn        BOOLEAN         DEFAULT FALSE,  -- does this type capture user category?
    is_for_pre_primary      BOOLEAN         DEFAULT FALSE,
    is_for_primary          BOOLEAN         DEFAULT FALSE,
    is_for_secondary        BOOLEAN         DEFAULT FALSE,
    is_active_yn            BOOLEAN         DEFAULT TRUE,

    load_time               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT infrastructure_type_dim_pkey         PRIMARY KEY (id),
    CONSTRAINT infrastructure_type_dim_natural_key  UNIQUE (source_id)
);

COMMENT ON TABLE  dw.infrastructure_type_dim IS
    'Infrastructure/building type dimension. Seeded from setting_school_building_types joined with setting_school_infrastructure_types. Not SCD2.';
COMMENT ON COLUMN dw.infrastructure_type_dim.is_sanitation_yn IS
    'TRUE for sanitation structures (latrines, hand-washing facilities). Filters apply for gender_usage and user_category.';
COMMENT ON COLUMN dw.infrastructure_type_dim.is_lab_yn IS
    'TRUE for laboratory structures. Used to measure science lab coverage.';

CREATE INDEX IF NOT EXISTS idx_infra_type_dim_name
    ON dw.infrastructure_type_dim (type_name);
CREATE INDEX IF NOT EXISTS idx_infra_type_dim_sanitation
    ON dw.infrastructure_type_dim (is_sanitation_yn);


-- -----------------------------------------------------------------------------
-- 2. dw.infrastructure_fact
--    Grain: 1 row per school per infrastructure type per academic year per term.
--    Measures: total_number (count of structures), area (sq metres).
--    Status fields stored as decoded VARCHAR degenerate dimensions.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.infrastructure_fact (
    id                      SERIAL          NOT NULL,

    -- Dimension foreign keys
    school_id               INTEGER         NOT NULL,   -- FK → dw.schools_dim(id)
    infra_type_id           INTEGER         NOT NULL,   -- FK → dw.infrastructure_type_dim(id)
    date_id                 INTEGER         NOT NULL,   -- FK → dw.date_dim(id)

    -- Natural keys
    school_source_id        BIGINT,                     -- schools.id from OLTP
    building_source_id      BIGINT,                     -- setting_school_building_types.id
    academic_year           INTEGER,
    term                    VARCHAR(100),

    -- Measures
    total_number            INTEGER,                    -- count of this structure type at school
    area                    DECIMAL(15,2),              -- floor area in sq metres (NULL if not captured)

    -- Degenerate dimensions (decoded from smallint in ETL)
    completion_status       VARCHAR(100),               -- COMPLETE / INCOMPLETE / UNDER CONSTRUCTION / PLANNED
    usage_mode              VARCHAR(100),               -- PERMANENT / TEMPORARY / SEMI-PERMANENT
    structure_condition     VARCHAR(100),               -- GOOD / FAIR / POOR / DILAPIDATED
    gender_usage            VARCHAR(100),               -- MALE / FEMALE / BOTH / NONE
    user_category           VARCHAR(100),               -- TEACHERS ONLY / LEARNERS ONLY / BOTH

    load_time               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT infrastructure_fact_pkey         PRIMARY KEY (id),
    CONSTRAINT infrastructure_fact_natural_key
        UNIQUE (school_source_id, building_source_id, academic_year, term)
);

COMMENT ON TABLE  dw.infrastructure_fact IS
    'Infrastructure fact. Grain: 1 row per school per building type per academic year per term. Measures: total_number (count of structures), area (sq metres). Status fields stored as decoded VARCHAR.';
COMMENT ON COLUMN dw.infrastructure_fact.total_number IS
    'Count of structures of this type at the school for this survey period.';
COMMENT ON COLUMN dw.infrastructure_fact.area IS
    'Floor area in square metres. NULL when is_area_captured_yn = FALSE for this infrastructure type.';
COMMENT ON COLUMN dw.infrastructure_fact.completion_status IS
    'COMPLETE, INCOMPLETE, UNDER CONSTRUCTION, or PLANNED. Decoded from school_building_status_updates.completion_status smallint.';
COMMENT ON COLUMN dw.infrastructure_fact.structure_condition IS
    'GOOD, FAIR, POOR, or DILAPIDATED. Decoded from school_building_status_updates.condition smallint.';
COMMENT ON COLUMN dw.infrastructure_fact.gender_usage IS
    'MALE, FEMALE, BOTH, or NONE. Decoded from school_building_status_updates.gender_usage smallint. Applicable only for sanitation structures.';
COMMENT ON COLUMN dw.infrastructure_fact.user_category IS
    'TEACHERS ONLY, LEARNERS ONLY, or BOTH. Decoded from school_building_status_updates.user_category smallint. Applicable only for sanitation structures.';

-- FK constraints
ALTER TABLE dw.infrastructure_fact
    ADD CONSTRAINT infra_fact_school_fk
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.infrastructure_fact
    ADD CONSTRAINT infra_fact_type_fk
    FOREIGN KEY (infra_type_id) REFERENCES dw.infrastructure_type_dim (id);

ALTER TABLE dw.infrastructure_fact
    ADD CONSTRAINT infra_fact_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_infra_fact_school
    ON dw.infrastructure_fact (school_id);
CREATE INDEX IF NOT EXISTS idx_infra_fact_type
    ON dw.infrastructure_fact (infra_type_id);
CREATE INDEX IF NOT EXISTS idx_infra_fact_date
    ON dw.infrastructure_fact (date_id);
CREATE INDEX IF NOT EXISTS idx_infra_fact_yr_term
    ON dw.infrastructure_fact (academic_year, term);
CREATE INDEX IF NOT EXISTS idx_infra_fact_condition
    ON dw.infrastructure_fact (structure_condition);
CREATE INDEX IF NOT EXISTS idx_infra_fact_completion
    ON dw.infrastructure_fact (completion_status);
