-- =============================================================================
-- EMIS Data Warehouse: Infrastructure Data Mart — Staging Tables
-- Schema: stg
--
-- DESIGN NOTES:
-- The infrastructure mart answers:
--   - How many classrooms / latrines / labs does each school have?
--   - What is the condition and completion status of structures by district?
--   - What is the gender usage and user category of sanitation facilities?
--   - How many structures were built under M&E construction projects?
--
-- PRIMARY SOURCE: public.school_building_status_updates
--   One row per school per building/infrastructure type per academic year per term.
--   Smallint columns for status fields (decoded in ETL).
--   Links to setting_school_building_types for structure type attributes.
--   Links to setting_school_infrastructure_types for usage flags.
--
-- SECONDARY SOURCE: public.actual_constructed_structures + me_construction_updates
--   Tracks M&E construction project status per structure.
--
-- FACT GRAIN:
--   dw.infrastructure_fact: 1 row per school per infrastructure type per academic year per term.
--   Natural key: (school_source_id, building_source_id, academic_year, term)
--
-- SMALLINT DECODINGS (from EMIS source):
--   completion_status: 1=COMPLETE, 2=INCOMPLETE, 3=UNDER CONSTRUCTION, 4=PLANNED
--   usage_mode:        1=PERMANENT, 2=TEMPORARY, 3=SEMI-PERMANENT
--   condition:         1=GOOD, 2=FAIR, 3=POOR, 4=DILAPIDATED
--   gender_usage:      1=MALE, 2=FEMALE, 3=BOTH, 4=NONE
--   user_category:     1=TEACHERS ONLY, 2=LEARNERS ONLY, 3=BOTH
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg.infrastructure_raw (
    id                          SERIAL          NOT NULL,

    -- Source key
    building_status_id          BIGINT          NOT NULL,   -- school_building_status_updates.id

    -- School
    school_id                   BIGINT          NOT NULL,
    school_name                 VARCHAR(200),
    school_type                 VARCHAR(200),
    admin_unit_source_id        BIGINT,                     -- for resolving to admin_units_dim

    -- Infrastructure type
    building_id                 BIGINT,                     -- setting_school_building_types.id
    building_name               VARCHAR(200),
    item_category               VARCHAR(200),               -- from setting_school_item_categories
    infrastructure_type_id      BIGINT,                     -- setting_school_infrastructure_types.id
    infrastructure_type_name    VARCHAR(200),
    is_lab_yn                   BOOLEAN,
    is_sanitation_yn            BOOLEAN,
    is_area_captured_yn         BOOLEAN,
    gender_usage_yn             BOOLEAN,                    -- does this type track gender usage?
    user_category_yn            BOOLEAN,                    -- does this type track user category?

    -- Academic period
    academic_year               INTEGER,
    term                        VARCHAR(100),

    -- Measures from school_building_status_updates
    total_number                INTEGER,                    -- count of this structure at the school
    area                        DECIMAL(15,2),              -- floor area (sq metres)

    -- Decoded status fields (from smallint)
    completion_status           VARCHAR(100),               -- COMPLETE / INCOMPLETE / UNDER CONSTRUCTION / PLANNED
    usage_mode                  VARCHAR(100),               -- PERMANENT / TEMPORARY / SEMI-PERMANENT
    structure_condition         VARCHAR(100),               -- GOOD / FAIR / POOR / DILAPIDATED
    gender_usage                VARCHAR(100),               -- MALE / FEMALE / BOTH / NONE
    user_category               VARCHAR(100),               -- TEACHERS ONLY / LEARNERS ONLY / BOTH

    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,

    CONSTRAINT infrastructure_raw_pkey PRIMARY KEY (id),
    CONSTRAINT infrastructure_raw_unique
        UNIQUE (building_status_id)
);

CREATE INDEX IF NOT EXISTS idx_infra_raw_school
    ON stg.infrastructure_raw (school_id, academic_year);
CREATE INDEX IF NOT EXISTS idx_infra_raw_building
    ON stg.infrastructure_raw (building_id);

COMMENT ON TABLE stg.infrastructure_raw IS
    'Raw extract of school_building_status_updates joined with building type and school attributes. Smallint status fields decoded to VARCHAR. One row per school per building type per term.';
COMMENT ON COLUMN stg.infrastructure_raw.completion_status IS
    'Decoded from smallint: 1=COMPLETE, 2=INCOMPLETE, 3=UNDER CONSTRUCTION, 4=PLANNED.';
COMMENT ON COLUMN stg.infrastructure_raw.structure_condition IS
    'Decoded from smallint: 1=GOOD, 2=FAIR, 3=POOR, 4=DILAPIDATED.';
COMMENT ON COLUMN stg.infrastructure_raw.gender_usage IS
    'Decoded from smallint: 1=MALE, 2=FEMALE, 3=BOTH, 4=NONE. Applicable only when gender_usage_yn = TRUE on the infrastructure type.';
COMMENT ON COLUMN stg.infrastructure_raw.user_category IS
    'Decoded from smallint: 1=TEACHERS ONLY, 2=LEARNERS ONLY, 3=BOTH. Applicable only when user_category_yn = TRUE on the infrastructure type.';
