-- =============================================================================
-- EMIS Data Warehouse: Enrolment Mart — DW Tables
-- Schema: dw
-- Run once after create_dw_tables.sql.
--
-- DESIGN DECISIONS:
--
-- 1. NO separate school location dimension in this mart.
--    Schools are referenced via dw.schools_dim (school_id FK).
--    Location slicing is done by joining schools_dim → admin_units_dim.
--    This avoids duplicating location data and maintains a single version
--    of the truth for school attributes.
--
-- 2. SCD Type 2 on learner_dim:
--    Tracked fields: gender, nationality, nin, orphan_status,
--    all six disability flags, has_multiple_disabilities_yn.
--    Changes in any of these trigger a new SCD2 version row.
--    Age is NOT tracked via SCD2 — it is computed at query time or
--    stored at the time of each enrolment fact row.
--
-- 3. SCD Type 2 on schools_dim (already built):
--    Enrolment fact references the school surrogate key valid at the
--    time of enrolment (effective_date ≤ enrolment_date ≤ expiration_date).
--
-- 4. Enrolment fact grain:
--    1 row per learner per school per term.
--    Natural key: (learner_source_id, school_source_id, academic_year, term).
--
-- 5. Small lookup dimensions (grade, enrolment_type, orphan_type,
--    disability_type) are static — no SCD2 needed.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.learner_dim  (SCD Type 2)
--    One row per version of a learner's tracked attributes.
--    is_current = TRUE marks the live version.
--
--    SCD2 tracked fields:
--      gender, nationality, nin, orphan_status,
--      is_visual_yn, is_hearing_yn, is_walking_yn, is_self_care_yn,
--      is_remembering_yn, is_communication_yn, has_multiple_disabilities_yn
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.learner_dim (
    -- Surrogate key
    id                          SERIAL        NOT NULL,

    -- Natural / business key
    source_id                   UUID,                     -- learners.person_id from OLTP
    lin                         VARCHAR(30),              -- Learner Identification Number

    -- Demographic attributes
    gender                      VARCHAR(10),              -- MALE / FEMALE
    age_at_load                 INTEGER,                  -- Age computed at ETL load time (point-in-time)
    nationality                 VARCHAR(100),
    orphan_status               VARCHAR(30),              -- MOTHER DECEASED / FATHER DECEASED / BOTH DECEASED / NOT APPLICABLE

    -- Identity documents (PII — mask in analytics views)
    nin                         VARCHAR(14),
    passport_no                 VARCHAR(20),
    student_pass_no             VARCHAR(20),
    work_permit_no              VARCHAR(20),
    refugee_id                  VARCHAR(20),

    -- Disability flags (SCD2 tracked)
    is_visual_yn                BOOLEAN       DEFAULT FALSE,
    is_hearing_yn               BOOLEAN       DEFAULT FALSE,
    is_walking_yn               BOOLEAN       DEFAULT FALSE,
    is_self_care_yn             BOOLEAN       DEFAULT FALSE,
    is_remembering_yn           BOOLEAN       DEFAULT FALSE,
    is_communication_yn         BOOLEAN       DEFAULT FALSE,
    has_multiple_disabilities_yn BOOLEAN      DEFAULT FALSE,

    -- SCD2 control fields
    effective_date              DATE          NOT NULL,
    expiration_date             DATE          NOT NULL DEFAULT '9999-12-31',
    is_current                  BOOLEAN       NOT NULL DEFAULT TRUE,
    change_hash                 TEXT          NOT NULL,
    change_reason               TEXT          NOT NULL,
    changed_fields              TEXT          NOT NULL,

    CONSTRAINT learner_dim_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE  dw.learner_dim IS 'Learner SCD2 dimension. One row per version of tracked learner attributes. is_current = TRUE marks the live version.';
COMMENT ON COLUMN dw.learner_dim.source_id  IS 'persons.person_id from OLTP — the natural key for this learner.';
COMMENT ON COLUMN dw.learner_dim.lin        IS 'Learner Identification Number — unique stable identifier across the learner''s education lifecycle.';
COMMENT ON COLUMN dw.learner_dim.nin        IS 'National ID Number — PII. Mask in analytics-facing views.';
COMMENT ON COLUMN dw.learner_dim.orphan_status IS 'MOTHER DECEASED | FATHER DECEASED | BOTH DECEASED | NOT APPLICABLE';
COMMENT ON COLUMN dw.learner_dim.age_at_load IS 'Age computed from DOB at the time this SCD2 row was created. For current age, compute at query time.';
COMMENT ON COLUMN dw.learner_dim.effective_date   IS 'Date this SCD2 version became active.';
COMMENT ON COLUMN dw.learner_dim.expiration_date  IS 'Date this SCD2 version expired. 9999-12-31 = still current.';
COMMENT ON COLUMN dw.learner_dim.is_current       IS 'TRUE = this is the current version of the learner record.';
COMMENT ON COLUMN dw.learner_dim.change_hash      IS 'MD5 of all SCD2-tracked columns. Hash difference triggers new version.';
COMMENT ON COLUMN dw.learner_dim.change_reason    IS 'Human-readable reason for new SCD2 version e.g. "Disability status updated".';
COMMENT ON COLUMN dw.learner_dim.changed_fields   IS 'Comma-separated list of columns that changed in this version.';

CREATE INDEX IF NOT EXISTS idx_learner_dim_source_current
    ON dw.learner_dim (source_id, is_current);
CREATE INDEX IF NOT EXISTS idx_learner_dim_lin
    ON dw.learner_dim (lin);
CREATE INDEX IF NOT EXISTS idx_learner_dim_effective
    ON dw.learner_dim (effective_date, expiration_date);


-- -----------------------------------------------------------------------------
-- 2. dw.grade_dim
--    Static lookup. No SCD2 needed — grades don't change.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.grade_dim (
    id              SERIAL        NOT NULL,
    grade_code      VARCHAR(10)   NOT NULL,   -- P1, P2 ... P7, S1 ... S6, Baby Class etc.
    grade_name      VARCHAR(50),              -- Primary One, Senior One etc.
    school_level    VARCHAR(50),              -- PRE_PRIMARY, PRIMARY, SECONDARY, TERTIARY
    sort_order      INTEGER,                  -- For ordering P1 < P2 < ... < S6
    CONSTRAINT grade_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT grade_dim_code_unique UNIQUE      (grade_code)
);
COMMENT ON TABLE dw.grade_dim IS 'Grade/class lookup. Static — no SCD2. Covers all education levels.';
COMMENT ON COLUMN dw.grade_dim.grade_code IS 'e.g. P1, P7, S1, S6, Baby Class, Diploma Year 1.';
COMMENT ON COLUMN dw.grade_dim.sort_order IS 'Numeric ordering to allow correct grade progression queries.';


-- -----------------------------------------------------------------------------
-- 3. dw.enrolment_type_dim
--    Static lookup. Values from EMIS lkup_enrolment_types.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.enrolment_type_dim (
    id                  SERIAL        NOT NULL,
    source_id           INTEGER,              -- BK from EMIS lookup table
    enrolment_type      VARCHAR(50)   NOT NULL, -- NEW ENTRANT, REPEATER, CONTINUING, TRANSFER, RETURNEE
    description         VARCHAR(200),
    CONSTRAINT enrolment_type_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT enrolment_type_dim_type_unique UNIQUE      (enrolment_type)
);
COMMENT ON TABLE dw.enrolment_type_dim IS 'Enrolment type lookup. NEW ENTRANT, REPEATER, CONTINUING, TRANSFER, RETURNEE.';


-- -----------------------------------------------------------------------------
-- 4. dw.orphan_type_dim
--    Static lookup for orphan status categories.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.orphan_type_dim (
    id              SERIAL        NOT NULL,
    orphan_status   VARCHAR(50)   NOT NULL,   -- MOTHER DECEASED, FATHER DECEASED, BOTH DECEASED, NOT APPLICABLE
    description     VARCHAR(200),
    CONSTRAINT orphan_type_dim_pkey          PRIMARY KEY (id),
    CONSTRAINT orphan_type_dim_status_unique UNIQUE      (orphan_status)
);
COMMENT ON TABLE dw.orphan_type_dim IS 'Orphan status lookup. MOTHER DECEASED | FATHER DECEASED | BOTH DECEASED | NOT APPLICABLE.';


-- -----------------------------------------------------------------------------
-- 5. dw.disability_type_dim
--    Static lookup for the six disability types tracked in EMIS.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.disability_type_dim (
    id               SERIAL        NOT NULL,
    disability_code  VARCHAR(30)   NOT NULL,  -- VISUAL, HEARING, WALKING, SELF_CARE, REMEMBERING, COMMUNICATION
    disability_name  VARCHAR(100),
    description      VARCHAR(300),
    CONSTRAINT disability_type_dim_pkey        PRIMARY KEY (id),
    CONSTRAINT disability_type_dim_code_unique UNIQUE      (disability_code)
);
COMMENT ON TABLE dw.disability_type_dim IS 'Disability type lookup. Six types tracked in EMIS: VISUAL, HEARING, WALKING, SELF_CARE, REMEMBERING, COMMUNICATION.';


-- -----------------------------------------------------------------------------
-- 6. dw.enrolment_fact
--    Grain: 1 row per learner per school per term.
--    Natural key: (learner_source_id, school_source_id, academic_year, term)
--
--    NOTE: The fact references dw.schools_dim (not a separate school location
--    dimension). Location slicing is done by joining:
--      enrolment_fact → schools_dim → admin_units_dim
--    This is the single version of the truth for school location.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.enrolment_fact (
    id                  SERIAL      NOT NULL,

    -- Dimension foreign keys
    learner_id          INTEGER     NOT NULL,   -- FK → dw.learner_dim(id)   SCD2 surrogate
    school_id           INTEGER     NOT NULL,   -- FK → dw.schools_dim(id)   SCD2 surrogate
    grade_id            INTEGER     NOT NULL,   -- FK → dw.grade_dim(id)
    date_id             INTEGER     NOT NULL,   -- FK → dw.date_dim(id)
    enrolment_type_id   INTEGER     NOT NULL,   -- FK → dw.enrolment_type_dim(id)

    -- Natural keys retained for deduplication and debugging
    learner_source_id   UUID,                   -- learners.person_id from OLTP
    school_source_id    UUID,                   -- schools.id from OLTP
    enrolment_source_id UUID,                   -- enrolments.id from OLTP
    academic_year       INTEGER,                -- e.g. 2025
    term                VARCHAR(10),            -- TERM 1, TERM 2, TERM 3

    -- ETL metadata
    load_time           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT enrolment_fact_pkey PRIMARY KEY (id),

    -- Prevent duplicate enrolment rows (safe re-run on ETL failure)
    CONSTRAINT enrolment_fact_natural_key_unique
        UNIQUE (learner_source_id, school_source_id, academic_year, term)
);

COMMENT ON TABLE  dw.enrolment_fact IS 'Enrolment fact table. Grain: 1 row per learner per school per term. References SCD2 surrogate keys valid at the time of enrolment.';
COMMENT ON COLUMN dw.enrolment_fact.learner_id  IS 'FK → dw.learner_dim(id). Points to the SCD2 learner version current at enrolment time.';
COMMENT ON COLUMN dw.enrolment_fact.school_id   IS 'FK → dw.schools_dim(id). Points to the SCD2 school version current at enrolment time. Join to schools_dim → admin_units_dim for location.';
COMMENT ON COLUMN dw.enrolment_fact.grade_id    IS 'FK → dw.grade_dim(id). Grade/class at the time of enrolment.';
COMMENT ON COLUMN dw.enrolment_fact.date_id     IS 'FK → dw.date_dim(id). Set to the first day of the term for the given academic year.';
COMMENT ON COLUMN dw.enrolment_fact.enrolment_type_id IS 'FK → dw.enrolment_type_dim(id). NEW ENTRANT, REPEATER, CONTINUING, TRANSFER, RETURNEE.';
COMMENT ON COLUMN dw.enrolment_fact.learner_source_id IS 'OLTP natural key — kept for deduplication and debugging.';
COMMENT ON COLUMN dw.enrolment_fact.academic_year IS 'Four-digit academic year e.g. 2025.';
COMMENT ON COLUMN dw.enrolment_fact.term IS 'TERM 1, TERM 2, TERM 3.';

-- Foreign key constraints
ALTER TABLE dw.enrolment_fact
    ADD CONSTRAINT enrolment_fact_learner_fk
    FOREIGN KEY (learner_id) REFERENCES dw.learner_dim (id);

ALTER TABLE dw.enrolment_fact
    ADD CONSTRAINT enrolment_fact_school_fk
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.enrolment_fact
    ADD CONSTRAINT enrolment_fact_grade_fk
    FOREIGN KEY (grade_id) REFERENCES dw.grade_dim (id);

ALTER TABLE dw.enrolment_fact
    ADD CONSTRAINT enrolment_fact_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

ALTER TABLE dw.enrolment_fact
    ADD CONSTRAINT enrolment_fact_enrolment_type_fk
    FOREIGN KEY (enrolment_type_id) REFERENCES dw.enrolment_type_dim (id);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_enrolment_fact_learner
    ON dw.enrolment_fact (learner_id);
CREATE INDEX IF NOT EXISTS idx_enrolment_fact_school
    ON dw.enrolment_fact (school_id);
CREATE INDEX IF NOT EXISTS idx_enrolment_fact_date
    ON dw.enrolment_fact (date_id);
CREATE INDEX IF NOT EXISTS idx_enrolment_fact_grade
    ON dw.enrolment_fact (grade_id);
CREATE INDEX IF NOT EXISTS idx_enrolment_fact_yr_term
    ON dw.enrolment_fact (academic_year, term);
CREATE INDEX IF NOT EXISTS idx_enrolment_fact_type
    ON dw.enrolment_fact (enrolment_type_id);


-- -----------------------------------------------------------------------------
-- 7. Seed static lookup dimensions
-- -----------------------------------------------------------------------------

-- Grade dimension seed
INSERT INTO dw.grade_dim (grade_code, grade_name, school_level, sort_order)
VALUES
    ('BC',  'Baby Class',         'PRE_PRIMARY', 1),
    ('MK',  'Middle Class',       'PRE_PRIMARY', 2),
    ('TK',  'Top Class',          'PRE_PRIMARY', 3),
    ('P1',  'Primary One',        'PRIMARY',     4),
    ('P2',  'Primary Two',        'PRIMARY',     5),
    ('P3',  'Primary Three',      'PRIMARY',     6),
    ('P4',  'Primary Four',       'PRIMARY',     7),
    ('P5',  'Primary Five',       'PRIMARY',     8),
    ('P6',  'Primary Six',        'PRIMARY',     9),
    ('P7',  'Primary Seven',      'PRIMARY',     10),
    ('S1',  'Senior One',         'SECONDARY',   11),
    ('S2',  'Senior Two',         'SECONDARY',   12),
    ('S3',  'Senior Three',       'SECONDARY',   13),
    ('S4',  'Senior Four',        'SECONDARY',   14),
    ('S5',  'Senior Five',        'SECONDARY',   15),
    ('S6',  'Senior Six',         'SECONDARY',   16),
    ('Y1',  'Year One',           'TERTIARY',    17),
    ('Y2',  'Year Two',           'TERTIARY',    18),
    ('Y3',  'Year Three',         'TERTIARY',    19),
    ('Y4',  'Year Four',          'TERTIARY',    20)
ON CONFLICT (grade_code) DO NOTHING;

-- Enrolment type dimension seed
INSERT INTO dw.enrolment_type_dim (enrolment_type, description)
VALUES
    ('NEW ENTRANT',  'First-time enrolment in the education system at this level'),
    ('CONTINUING',   'Learner continuing from previous term in the same grade'),
    ('REPEATER',     'Learner repeating the same grade as the previous year'),
    ('TRANSFER',     'Learner who transferred from another school'),
    ('RETURNEE',     'Learner who previously dropped out and has re-enrolled')
ON CONFLICT (enrolment_type) DO NOTHING;

-- Orphan type dimension seed
INSERT INTO dw.orphan_type_dim (orphan_status, description)
VALUES
    ('MOTHER DECEASED',  'Learner has lost their mother'),
    ('FATHER DECEASED',  'Learner has lost their father'),
    ('BOTH DECEASED',    'Learner has lost both parents (double orphan)'),
    ('NOT APPLICABLE',   'Both parents are living')
ON CONFLICT (orphan_status) DO NOTHING;

-- Disability type dimension seed
INSERT INTO dw.disability_type_dim (disability_code, disability_name, description)
VALUES
    ('VISUAL',         'Visual Impairment',      'Difficulty or inability to see'),
    ('HEARING',        'Hearing Impairment',     'Difficulty or inability to hear'),
    ('WALKING',        'Mobility Difficulty',    'Difficulty walking or climbing stairs'),
    ('SELF_CARE',      'Self-care Difficulty',   'Difficulty washing, dressing, or feeding oneself'),
    ('REMEMBERING',    'Cognitive Difficulty',   'Difficulty remembering, concentrating, or communicating'),
    ('COMMUNICATION',  'Communication Difficulty','Difficulty communicating using usual language')
ON CONFLICT (disability_code) DO NOTHING;
