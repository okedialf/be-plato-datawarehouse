-- =============================================================================
-- EMIS Data Warehouse: Indicators Mart — DW Tables
-- Schema: dw
--
-- THREE FACT TABLES (replacing the single flawed indicator_fact):
--
-- 1. dw.population_fact
--    Grain: district × year × sex × age_group
--    Source: UBOS Population Projections 2025-2050
--    Refresh: annually when UBOS publishes new projections
--
-- 2. dw.enrolment_indicator_fact
--    Grain: district × school_level × academic_year × gender
--    Source: computed from dw.enrolment_fact + dw.population_fact
--    Refresh: annually (Term 1 data is used as the census point)
--    Indicators: GER, NER, GIR, GPI, Repetition Rate, SNE Inclusion Rate
--
-- 3. dw.school_indicator_fact
--    Grain: school × academic_year × term
--    Source: computed from dw.hr_fact + dw.enrolment_fact
--    Refresh: each term
--    Indicators: PTR, % qualified teachers, % female teachers
--
-- DESIGN PRINCIPLES:
--   - No redundant gender_dim, location_dim or year_dim
--   - Gender is a VARCHAR column on fact tables
--   - Location references dw.admin_units_dim (single version of truth)
--   - Date references dw.date_dim
--   - Indicators stored as typed DECIMAL columns (not EAV pattern)
--   - All indicators follow UNESCO technical guidelines (Nov 2009)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.population_fact
--    Holds UBOS district-level population projections by sex.
--    Used as denominators for GER, NER, GIR, NIR.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.population_fact (
    id                  SERIAL          NOT NULL,

    -- Dimension references
    district_id         INTEGER         NOT NULL,   -- FK → dw.admin_units_dim(id) at district level

    -- Natural keys
    district_name       VARCHAR(200)    NOT NULL,   -- denormalized for readability
    year                INTEGER         NOT NULL,
    sex                 VARCHAR(10)     NOT NULL,   -- M, F, TOTAL

    -- Population measures by age group (all from UBOS projections)
    total_population    BIGINT,
    pop_age_3_5         BIGINT,         -- pre-primary school age
    pop_age_6_12        BIGINT,         -- primary school age
    pop_age_13_16       BIGINT,         -- secondary O-level age
    pop_age_17_18       BIGINT,         -- secondary A-level age
    pop_age_6           BIGINT,         -- P1 entry age (GIR/NIR denominator)

    data_source         VARCHAR(200)    DEFAULT 'UBOS Population Projections 2025-2050',
    load_time           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT population_fact_pkey         PRIMARY KEY (id),
    CONSTRAINT population_fact_natural_key  UNIQUE (district_id, year, sex)
);

COMMENT ON TABLE  dw.population_fact IS
    'UBOS district population projections by sex. Denominators for GER, NER, GIR, NIR indicators.';
COMMENT ON COLUMN dw.population_fact.pop_age_6 IS
    'Population aged exactly 6 — denominator for GIR and NIR.';
COMMENT ON COLUMN dw.population_fact.pop_age_6_12 IS
    'Population aged 6-12 — denominator for Primary GER and NER.';

ALTER TABLE dw.population_fact
    ADD CONSTRAINT pop_fact_district_fk
    FOREIGN KEY (district_id) REFERENCES dw.admin_units_dim (id);

CREATE INDEX IF NOT EXISTS idx_pop_fact_district
    ON dw.population_fact (district_id, year, sex);


-- -----------------------------------------------------------------------------
-- 2. dw.enrolment_indicator_fact
--    Grain: district × school_level × academic_year × gender
--    All rate/ratio indicators computed per this grain.
--    UNESCO formulas applied per technical guidelines Nov 2009.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.enrolment_indicator_fact (
    id                          SERIAL          NOT NULL,

    -- Dimension references
    district_id                 INTEGER         NOT NULL,   -- FK → dw.admin_units_dim(id)
    date_id                     INTEGER         NOT NULL,   -- FK → dw.date_dim(id) — Term 1 start date

    -- Degenerate dimensions (columns on fact)
    school_level                VARCHAR(200)    NOT NULL,   -- PRIMARY, SECONDARY, PRE_PRIMARY
    academic_year               INTEGER         NOT NULL,
    gender                      VARCHAR(10)     NOT NULL,   -- M, F, TOTAL

    -- Raw counts (numerators and components)
    total_enrolment             BIGINT          DEFAULT 0,
    official_age_enrolment      BIGINT          DEFAULT 0,  -- within official age range (NER numerator)
    new_entrants_p1             BIGINT          DEFAULT 0,  -- BASELINE type in P1 (GIR numerator)
    new_entrants_p1_official_age BIGINT         DEFAULT 0,  -- P1 new entrants aged 6 (NIR numerator)
    repeaters                   BIGINT          DEFAULT 0,  -- is_repeating_yn = TRUE
    learners_with_disability    BIGINT          DEFAULT 0,

    -- Population denominators (from dw.population_fact)
    school_age_population       BIGINT,                     -- age group for this school level
    population_age_6            BIGINT,                     -- for GIR/NIR

    -- ── Computed indicators (UNESCO formulas) ─────────────────────────────────

    -- Gross Enrolment Ratio (GER)
    -- Formula: (total_enrolment / school_age_population) * 100
    -- Can exceed 100% due to over/under-aged learners
    ger                         DECIMAL(8,2),

    -- Net Enrolment Rate (NER)
    -- Formula: (official_age_enrolment / school_age_population) * 100
    -- Maximum theoretical value: 100%
    ner                         DECIMAL(8,2),

    -- Gross Intake Ratio (GIR) — Primary only
    -- Formula: (new_entrants_p1 / population_age_6) * 100
    -- Applicable only for school_level = PRIMARY
    gir                         DECIMAL(8,2),

    -- Net Intake Rate (NIR) — Primary only
    -- Formula: (new_entrants_p1_official_age / population_age_6) * 100
    -- More precise than GIR — counts only age-appropriate new entrants
    nir                         DECIMAL(8,2),

    -- Gender Parity Index on GER
    -- Formula: female_ger / male_ger
    -- Value of 1.0 = parity; <1 = disadvantage for girls; >1 = advantage for girls
    -- Populated only for gender = TOTAL rows (computed from M and F rows)
    gpi_ger                     DECIMAL(8,4),

    -- Gender Parity Index on NER
    gpi_ner                     DECIMAL(8,4),

    -- Repetition Rate
    -- Formula: (repeaters / total_enrolment) * 100
    -- Ideally approaches 0%; high values signal internal efficiency problems
    repetition_rate             DECIMAL(8,2),

    -- SNE Inclusion Rate (Special Needs Education)
    -- Formula: (learners_with_disability / total_enrolment) * 100
    -- Uganda-specific indicator for inclusive education monitoring
    sne_inclusion_rate          DECIMAL(8,2),

    load_time                   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT enrolment_indicator_fact_pkey        PRIMARY KEY (id),
    CONSTRAINT enrolment_indicator_fact_natural_key
        UNIQUE (district_id, school_level, academic_year, gender)
);

COMMENT ON TABLE  dw.enrolment_indicator_fact IS
    'Education indicators by district, school level, academic year and gender. GER, NER, GIR, NIR, GPI, Repetition Rate, SNE Inclusion Rate. UNESCO technical guidelines Nov 2009.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.ger IS
    'Gross Enrolment Ratio. Formula: (total_enrolment / school_age_population) * 100. Can exceed 100%.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.ner IS
    'Net Enrolment Rate. Formula: (official_age_enrolment / school_age_population) * 100. Max 100%.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.gir IS
    'Gross Intake Ratio. Formula: (new_entrants_p1 / population_age_6) * 100. Primary level only.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.nir IS
    'Net Intake Rate. Formula: (new_entrants_p1_official_age / population_age_6) * 100. Primary level only.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.gpi_ger IS
    'Gender Parity Index on GER. Formula: female_GER / male_GER. Populated on TOTAL gender rows only.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.repetition_rate IS
    'Repetition Rate. Formula: (repeaters / total_enrolment) * 100.';
COMMENT ON COLUMN dw.enrolment_indicator_fact.sne_inclusion_rate IS
    'SNE Inclusion Rate. Formula: (learners_with_disability / total_enrolment) * 100.';

ALTER TABLE dw.enrolment_indicator_fact
    ADD CONSTRAINT enrol_ind_fact_district_fk
    FOREIGN KEY (district_id) REFERENCES dw.admin_units_dim (id);

ALTER TABLE dw.enrolment_indicator_fact
    ADD CONSTRAINT enrol_ind_fact_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

CREATE INDEX IF NOT EXISTS idx_enrol_ind_fact_district
    ON dw.enrolment_indicator_fact (district_id, academic_year);
CREATE INDEX IF NOT EXISTS idx_enrol_ind_fact_level
    ON dw.enrolment_indicator_fact (school_level, academic_year);
CREATE INDEX IF NOT EXISTS idx_enrol_ind_fact_gender
    ON dw.enrolment_indicator_fact (gender, academic_year);


-- -----------------------------------------------------------------------------
-- 3. dw.school_indicator_fact
--    Grain: school × academic_year × term
--    PTR and teacher composition indicators at school level.
--    Joins to schools_dim → admin_units_dim for location slicing.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.school_indicator_fact (
    id                          SERIAL          NOT NULL,

    -- Dimension references
    school_id                   INTEGER         NOT NULL,   -- FK → dw.schools_dim(id)
    date_id                     INTEGER         NOT NULL,   -- FK → dw.date_dim(id)

    -- Natural keys
    school_source_id            BIGINT,                     -- schools.id from OLTP
    academic_year               INTEGER         NOT NULL,
    term                        VARCHAR(200)    NOT NULL,

    -- Raw counts
    total_learners              INTEGER         DEFAULT 0,
    male_learners               INTEGER         DEFAULT 0,
    female_learners             INTEGER         DEFAULT 0,
    total_teachers              INTEGER         DEFAULT 0,
    male_teachers               INTEGER         DEFAULT 0,
    female_teachers             INTEGER         DEFAULT 0,
    qualified_teachers          INTEGER         DEFAULT 0,  -- teacher_type = QUALIFIED
    trained_teachers            INTEGER         DEFAULT 0,  -- teacher_type = TRAINED

    -- ── Computed indicators ────────────────────────────────────────────────────

    -- Pupil-Teacher Ratio (PTR)
    -- Formula: total_learners / total_teachers
    -- UNESCO standard; high PTR = less teacher attention per learner
    -- Uganda norm: Primary ≤ 53:1, Secondary ≤ 40:1
    ptr                         DECIMAL(8,2),

    -- Qualified Teacher Ratio
    -- Formula: (qualified_teachers / total_teachers) * 100
    -- Measures proportion of teachers meeting professional qualification standards
    qualified_teacher_ratio     DECIMAL(8,2),

    -- Trained Teacher Ratio (includes both QUALIFIED and TRAINED types)
    -- Formula: ((qualified_teachers + trained_teachers) / total_teachers) * 100
    trained_teacher_ratio       DECIMAL(8,2),

    -- Percentage of Female Teachers
    -- Formula: (female_teachers / total_teachers) * 100
    -- GPI benchmark: approaching 50% = gender parity in teaching force
    pct_female_teachers         DECIMAL(8,2),

    -- Gender Parity Index on PTR (female learner PTR / male learner PTR)
    -- Detects gender-based differences in class size
    gpi_ptr                     DECIMAL(8,4),

    load_time                   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT school_indicator_fact_pkey        PRIMARY KEY (id),
    CONSTRAINT school_indicator_fact_natural_key
        UNIQUE (school_source_id, academic_year, term)
);

COMMENT ON TABLE  dw.school_indicator_fact IS
    'School-level HR indicators per term. PTR, qualified teacher ratio, % female teachers. Join to schools_dim → admin_units_dim for location slicing.';
COMMENT ON COLUMN dw.school_indicator_fact.ptr IS
    'Pupil-Teacher Ratio. Formula: total_learners / total_teachers. Uganda norms: Primary ≤ 53, Secondary ≤ 40.';
COMMENT ON COLUMN dw.school_indicator_fact.qualified_teacher_ratio IS
    'Qualified Teacher Ratio. Formula: (qualified_teachers / total_teachers) * 100.';
COMMENT ON COLUMN dw.school_indicator_fact.pct_female_teachers IS
    'Percentage of Female Teachers. Formula: (female_teachers / total_teachers) * 100. GPI benchmark = 50%.';

ALTER TABLE dw.school_indicator_fact
    ADD CONSTRAINT school_ind_fact_school_fk
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.school_indicator_fact
    ADD CONSTRAINT school_ind_fact_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

CREATE INDEX IF NOT EXISTS idx_school_ind_fact_school
    ON dw.school_indicator_fact (school_id, academic_year);
CREATE INDEX IF NOT EXISTS idx_school_ind_fact_date
    ON dw.school_indicator_fact (date_id);
CREATE INDEX IF NOT EXISTS idx_school_ind_fact_yr_term
    ON dw.school_indicator_fact (academic_year, term);
