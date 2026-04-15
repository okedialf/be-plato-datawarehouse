-- =============================================================================
-- EMIS Data Warehouse: Enrolment Mart — Staging Tables
-- Schema: stg
-- Run once. Truncated and reloaded by the nightly ETL.
--
-- Source: EMIS 2.0 read-only replica via postgres_fdw (src schema)
--   src.enrolments          → stg.enrolments_raw
--   src.learners            → stg.learners_raw
--   src.learner_disabilities → stg.learner_disabilities_raw
--   src.learner_promotions  → stg.learner_promotions_raw
--   src.learner_transitions → stg.learner_transitions_raw
--   src.pre_primary_enrolments  \
--   src.primary_enrolments       }-→ stg.enrolments_subtype_raw
--   src.secondary_enrolments    /
--   src.persons             → used in learner flatten
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.learners_raw
--    Raw extract from src.learners + src.persons (joined in ETL).
--    Contains all learner attributes needed for learner_dim.
--    Refreshed: termly (or nightly for delta)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.learners_raw (
    -- Keys
    person_id               UUID          NOT NULL,   -- PK from persons supertype
    lin                     VARCHAR(30)   NOT NULL,   -- Learner Identification Number (LIN)

    -- Personal attributes (from persons table)
    surname                 VARCHAR(100),
    given_name              VARCHAR(150),
    date_of_birth           DATE,
    gender                  VARCHAR(10),              -- MALE / FEMALE
    nationality             VARCHAR(100),
    district_of_birth_id    UUID,                     -- FK to admin units

    -- Identity documents (PII — mask in analytics)
    nin                     VARCHAR(14),              -- National ID Number
    passport_no             VARCHAR(20),
    student_pass_no         VARCHAR(20),
    work_permit_no          VARCHAR(20),
    refugee_id              VARCHAR(20),

    -- Learner flags
    is_flagged_yn           BOOLEAN,
    deleted_at              TIMESTAMP,

    -- Audit
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,

    CONSTRAINT learners_raw_pkey PRIMARY KEY (person_id)
);
COMMENT ON TABLE stg.learners_raw IS 'Raw extract of src.learners joined with src.persons. Termly refresh.';
COMMENT ON COLUMN stg.learners_raw.lin IS 'Learner Identification Number — unique natural key for the learner.';
COMMENT ON COLUMN stg.learners_raw.nin IS 'PII — mask in analytics-facing views.';


-- -----------------------------------------------------------------------------
-- 2. stg.learner_disabilities_raw
--    One row per learner per disability type.
--    Used to derive the disability flags on learner_dim.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.learner_disabilities_raw (
    id                  UUID          NOT NULL,
    learner_id          UUID          NOT NULL,
    disability_type     VARCHAR(60)   NOT NULL,   -- VISUAL, HEARING, WALKING, SELF_CARE, REMEMBERING, COMMUNICATION
    severity            VARCHAR(30),              -- MILD, MODERATE, SEVERE (if captured)
    date_created        TIMESTAMP,
    date_updated        TIMESTAMP,
    CONSTRAINT learner_disabilities_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_learner_disabilities_raw_learner
    ON stg.learner_disabilities_raw (learner_id);
COMMENT ON TABLE stg.learner_disabilities_raw IS 'Raw extract of src.learner_disabilities. Used to derive boolean disability flags on learner_dim.';


-- -----------------------------------------------------------------------------
-- 3. stg.enrolments_raw
--    One row per enrolment record — the core enrolment extract.
--    Grain: learner × school × academic_year × teaching_period (term).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.enrolments_raw (
    -- PK
    id                          UUID          NOT NULL,

    -- Learner and school keys
    learner_id                  UUID          NOT NULL,
    school_id                   UUID          NOT NULL,

    -- Academic period
    academic_year_id            UUID          NOT NULL,
    teaching_period_id          UUID          NOT NULL,   -- Term 1 / Term 2 / Term 3

    -- Status flags
    reporting_status_id         UUID,
    learner_promotion_status_id INTEGER,                  -- 1=Promoted, 2=Repeat, 3=Discontinued
    learner_transfer_status_id  INTEGER,                  -- 1=Transfer, 2=Continuing
    learner_transition_status_id INTEGER,                 -- 1=Transition, 2=Continuing

    -- Enrolment identifiers
    learner_registration_number VARCHAR(200),
    is_enrolment_active_yn      BOOLEAN,

    -- Audit
    date_created                TIMESTAMP,
    date_updated                TIMESTAMP,

    CONSTRAINT enrolments_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_enrolments_raw_learner   ON stg.enrolments_raw (learner_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_raw_school    ON stg.enrolments_raw (school_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_raw_yr_term   ON stg.enrolments_raw (academic_year_id, teaching_period_id);
COMMENT ON TABLE stg.enrolments_raw IS 'Raw extract of src.enrolments. Core enrolment fact — one row per learner per school per term.';


-- -----------------------------------------------------------------------------
-- 4. stg.enrolments_subtype_raw
--    Subtype enrolment details — grade/class and language.
--    Consolidated from PRE_PRIMARY_ENROLMENTS, PRIMARY_ENROLMENTS,
--    SECONDARY_ENROLMENTS etc. Each maps enrolment_id → education_class_id.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.enrolments_subtype_raw (
    enrolment_id            UUID          NOT NULL,
    school_level            VARCHAR(50)   NOT NULL,   -- PRE_PRIMARY, PRIMARY, SECONDARY etc.
    education_class_id      UUID,                     -- Grade/class FK
    education_class_name    VARCHAR(50),              -- Decoded grade: P1, P2 ... S1, S2 etc.
    familiar_language_id    UUID,
    familiar_language_name  VARCHAR(100),
    date_created            TIMESTAMP,
    CONSTRAINT enrolments_subtype_raw_pkey PRIMARY KEY (enrolment_id)
);
COMMENT ON TABLE stg.enrolments_subtype_raw IS 'Consolidated subtype enrolment details: grade/class per enrolment. Sourced from PRE_PRIMARY_ENROLMENTS, PRIMARY_ENROLMENTS, SECONDARY_ENROLMENTS etc.';


-- -----------------------------------------------------------------------------
-- 5. stg.learner_promotions_raw
--    One row per learner per promotion/progression event.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.learner_promotions_raw (
    id                      UUID          NOT NULL,
    learner_id              UUID          NOT NULL,
    school_id               UUID          NOT NULL,
    academic_year_id        UUID          NOT NULL,
    from_class_id           UUID,                     -- Grade promoted from
    to_class_id             UUID,                     -- Grade promoted to
    promotion_status_id     INTEGER,                  -- 1=Promoted, 2=Repeated, 3=Discontinued
    promotion_status_name   VARCHAR(30),              -- Decoded status
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,
    CONSTRAINT learner_promotions_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_learner_promotions_raw_learner ON stg.learner_promotions_raw (learner_id);
COMMENT ON TABLE stg.learner_promotions_raw IS 'Raw extract of src.learner_promotions. Used for promotions/progression mart.';


-- -----------------------------------------------------------------------------
-- 6. stg.learner_transitions_raw
--    Transition events (e.g. primary to secondary school).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.learner_transitions_raw (
    id                      UUID          NOT NULL,
    learner_id              UUID          NOT NULL,
    from_school_id          UUID,
    to_school_id            UUID,
    academic_year_id        UUID,
    transition_status_id    INTEGER,                  -- 1=Transition, 2=Continuing
    transition_status_name  VARCHAR(30),
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,
    CONSTRAINT learner_transitions_raw_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE stg.learner_transitions_raw IS 'Raw extract of src.learner_transitions. School transition events (primary→secondary etc.).';


-- -----------------------------------------------------------------------------
-- 7. stg.enrolments_flat
--    The fully decoded flat staging table.
--    Joins enrolments_raw + learners_raw + subtype + disabilities.
--    Source for loading dw.enrolment_fact and dw.learner_dim.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.enrolments_flat (
    id                      SERIAL        NOT NULL,

    -- Natural keys (kept for ETL lookups)
    enrolment_source_id     UUID          NOT NULL,   -- enrolments.id from OLTP
    learner_source_id       UUID          NOT NULL,   -- learners.person_id from OLTP
    school_source_id        UUID          NOT NULL,   -- schools.id from OLTP

    -- Decoded period
    academic_year           INTEGER       NOT NULL,   -- e.g. 2025
    term                    VARCHAR(10)   NOT NULL,   -- TERM 1, TERM 2, TERM 3

    -- Learner attributes
    lin                     VARCHAR(30),
    gender                  VARCHAR(10),
    age                     INTEGER,                  -- Computed from DOB at ETL time
    nationality             VARCHAR(100),
    nin                     VARCHAR(14),
    passport_no             VARCHAR(20),
    student_pass_no         VARCHAR(20),
    work_permit_no          VARCHAR(20),
    refugee_id              VARCHAR(20),
    orphan_status           VARCHAR(30),              -- MOTHER DECEASED, FATHER DECEASED, BOTH DECEASED, NOT APPLICABLE

    -- Disability flags (derived from learner_disabilities_raw)
    is_visual_yn            BOOLEAN       DEFAULT FALSE,
    is_hearing_yn           BOOLEAN       DEFAULT FALSE,
    is_walking_yn           BOOLEAN       DEFAULT FALSE,
    is_self_care_yn         BOOLEAN       DEFAULT FALSE,
    is_remembering_yn       BOOLEAN       DEFAULT FALSE,
    is_communication_yn     BOOLEAN       DEFAULT FALSE,
    has_multiple_disabilities_yn BOOLEAN  DEFAULT FALSE,

    -- Grade / class
    grade                   VARCHAR(10),              -- P1, P2 ... S1, S2, S3 etc.
    school_level            VARCHAR(50),              -- PRE_PRIMARY, PRIMARY, SECONDARY etc.

    -- Enrolment status
    enrolment_type          VARCHAR(30),              -- NEW ENTRANT, REPEATER, CONTINUING, TRANSFER, RETURNEE
    is_active               BOOLEAN,

    -- School attributes (denormalized for performance)
    school_emis_number      VARCHAR(50),
    school_name             VARCHAR(200),
    school_ownership        VARCHAR(60),
    school_funding_type     VARCHAR(20),
    school_sex_composition  VARCHAR(20),
    school_boarding_status  VARCHAR(20),
    school_type             VARCHAR(50),

    -- Admin unit (resolved against dw.admin_units_dim)
    admin_unit_source_id    INTEGER,                  -- Raw source id for DW resolution

    -- SCD2 staging fields (populated during DW load step)
    change_hash             TEXT,
    change_reason           TEXT,
    changed_fields          TEXT,

    CONSTRAINT enrolments_flat_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_enrolments_flat_learner
    ON stg.enrolments_flat (learner_source_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_flat_school
    ON stg.enrolments_flat (school_source_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_flat_yr_term
    ON stg.enrolments_flat (academic_year, term);
COMMENT ON TABLE stg.enrolments_flat IS 'Fully decoded flat staging table. Joins enrolments + learners + disabilities + subtypes. Source for dw.enrolment_fact and dw.learner_dim SCD2 load.';
