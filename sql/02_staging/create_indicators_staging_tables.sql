-- =============================================================================
-- EMIS Data Warehouse: Indicators Mart — Staging Tables
-- Schema: stg
--
-- DESIGN NOTES:
-- The indicators mart computes derived statistics from existing DW tables
-- (dw.enrolment_fact, dw.hr_fact, dw.schools_dim, dw.learner_dim) plus
-- UBOS population projections loaded externally.
--
-- Three staging tables feed three fact tables:
--   stg.population_raw           → dw.population_fact
--   stg.enrolment_indicator_raw  → dw.enrolment_indicator_fact
--   stg.school_indicator_raw     → dw.school_indicator_fact
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.population_raw
--    Holds UBOS district population projections loaded from the
--    Population Projections 2025-2050 document (annual load).
--    One row per district per year per sex.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.population_raw (
    id                  SERIAL          NOT NULL,
    district_name       VARCHAR(200)    NOT NULL,   -- matches admin_unit_districts.name
    district_source_id  BIGINT,                     -- admin_unit_districts.id (resolved in ETL)
    year                INTEGER         NOT NULL,
    sex                 VARCHAR(10)     NOT NULL,   -- M, F, TOTAL
    total_population    BIGINT,                     -- all ages
    pop_age_3_5         BIGINT,                     -- pre-primary school age
    pop_age_6_12        BIGINT,                     -- primary school age
    pop_age_13_16       BIGINT,                     -- secondary O-level age
    pop_age_17_18       BIGINT,                     -- secondary A-level age
    pop_age_6           BIGINT,                     -- P1 entry age (for GIR/NIR)
    data_source         VARCHAR(200)    DEFAULT 'UBOS Population Projections 2025-2050',
    date_loaded         TIMESTAMP       DEFAULT NOW(),
    CONSTRAINT population_raw_pkey PRIMARY KEY (id),
    CONSTRAINT population_raw_unique UNIQUE (district_name, year, sex)
);
CREATE INDEX IF NOT EXISTS idx_pop_raw_district
    ON stg.population_raw (district_source_id, year);
COMMENT ON TABLE stg.population_raw IS
    'UBOS district population projections. Loaded annually from UBOS Population Projections 2025-2050 document.';
COMMENT ON COLUMN stg.population_raw.pop_age_6 IS
    'Population aged exactly 6 years — denominator for GIR and NIR in primary education.';


-- -----------------------------------------------------------------------------
-- 2. stg.enrolment_indicator_raw
--    Aggregated enrolment counts per district per school level per academic
--    year per gender. Computed by ETL from dw.enrolment_fact.
--    One row per district × school_level × academic_year × gender.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.enrolment_indicator_raw (
    id                          SERIAL          NOT NULL,
    district_id                 INTEGER,                    -- FK → dw.admin_units_dim(id) at district level
    district_name               VARCHAR(200),
    school_level                VARCHAR(200)    NOT NULL,   -- PRIMARY, SECONDARY, PRE_PRIMARY etc.
    academic_year               INTEGER         NOT NULL,
    gender                      VARCHAR(10)     NOT NULL,   -- M, F, TOTAL

    -- Enrolment counts (from dw.enrolment_fact + dw.learner_dim)
    total_enrolment             BIGINT          DEFAULT 0,
    official_age_enrolment      BIGINT          DEFAULT 0,  -- within official school-age range (NER)
    new_entrants_p1             BIGINT          DEFAULT 0,  -- BASELINE enrolment type in P1 (GIR)
    new_entrants_p1_official_age BIGINT         DEFAULT 0,  -- P1 new entrants aged exactly 6 (NIR)
    repeaters                   BIGINT          DEFAULT 0,  -- is_repeating_yn = TRUE

    -- Disability counts (for SNE indicators)
    learners_with_disability     BIGINT          DEFAULT 0,

    -- Population denominators (joined from stg.population_raw)
    school_age_population       BIGINT,                     -- relevant age group from UBOS
    population_age_6            BIGINT,                     -- for GIR/NIR denominators

    -- Computed indicators (DECIMAL for precision)
    ger                         DECIMAL(8,2),               -- GER %
    ner                         DECIMAL(8,2),               -- NER %
    gir                         DECIMAL(8,2),               -- Gross Intake Ratio %
    repetition_rate             DECIMAL(8,2),               -- Repetition Rate %
    sne_inclusion_rate          DECIMAL(8,2),               -- SNE Inclusion Rate %

    date_computed               TIMESTAMP       DEFAULT NOW(),

    CONSTRAINT enrolment_indicator_raw_pkey PRIMARY KEY (id),
    CONSTRAINT enrolment_indicator_raw_unique
        UNIQUE (district_id, school_level, academic_year, gender)
);
CREATE INDEX IF NOT EXISTS idx_enrol_ind_raw_district
    ON stg.enrolment_indicator_raw (district_id, academic_year);
COMMENT ON TABLE stg.enrolment_indicator_raw IS
    'Aggregated enrolment indicators per district per school level per year per gender. Computed from dw.enrolment_fact.';


-- -----------------------------------------------------------------------------
-- 3. stg.school_indicator_raw
--    PTR and teacher-related indicators per school per term.
--    Computed from dw.hr_fact and dw.enrolment_fact.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.school_indicator_raw (
    id                          SERIAL          NOT NULL,
    school_source_id            BIGINT,                     -- schools.id from OLTP
    school_name                 VARCHAR(200),
    school_type                 VARCHAR(200),
    district_id                 INTEGER,                    -- FK → dw.admin_units_dim(id)
    academic_year               INTEGER         NOT NULL,
    term                        VARCHAR(200)    NOT NULL,

    -- Enrolment counts
    total_learners              INTEGER         DEFAULT 0,
    male_learners               INTEGER         DEFAULT 0,
    female_learners             INTEGER         DEFAULT 0,

    -- Teacher counts
    total_teachers              INTEGER         DEFAULT 0,
    male_teachers               INTEGER         DEFAULT 0,
    female_teachers             INTEGER         DEFAULT 0,
    qualified_teachers          INTEGER         DEFAULT 0,  -- teacher_type = QUALIFIED
    trained_teachers            INTEGER         DEFAULT 0,  -- teacher_type = TRAINED

    -- Computed indicators
    ptr                         DECIMAL(8,2),               -- Pupil-Teacher Ratio
    qualified_teacher_ratio     DECIMAL(8,2),               -- % qualified teachers
    trained_teacher_ratio       DECIMAL(8,2),               -- % trained teachers
    pct_female_teachers         DECIMAL(8,2),               -- % female teachers
    gpi_ptr                     DECIMAL(8,4),               -- Gender Parity Index on PTR

    date_computed               TIMESTAMP       DEFAULT NOW(),

    CONSTRAINT school_indicator_raw_pkey PRIMARY KEY (id),
    CONSTRAINT school_indicator_raw_unique
        UNIQUE (school_source_id, academic_year, term)
);
CREATE INDEX IF NOT EXISTS idx_school_ind_raw_district
    ON stg.school_indicator_raw (district_id, academic_year);
COMMENT ON TABLE stg.school_indicator_raw IS
    'School-level HR indicators per term. PTR, teacher ratios. Computed from dw.hr_fact and dw.enrolment_fact.';
