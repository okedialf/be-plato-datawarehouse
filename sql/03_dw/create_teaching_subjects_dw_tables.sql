-- =============================================================================
-- EMIS Data Warehouse: Teaching Subjects Mart — DW Tables
-- Schema: dw
--
-- STAR SCHEMA:
--   dw.subject_dim             — subject dimension (PRIMARY + SECONDARY subjects)
--   dw.teaching_subject_fact   — fact table (teacher × subject)
--
-- GRAIN: 1 row per teacher per subject
--   A teacher can teach multiple subjects.
--   A subject can be taught by many teachers.
--   No term dimension — subject assignments are not term-specific in EMIS.
--
-- DIMENSION REFERENCES on fact:
--   teacher_id  → dw.teacher_dim   (from teachers mart)
--   school_id   → dw.schools_dim   (from schools mart)
--   subject_id  → dw.subject_dim   (new)
--   date_id     → dw.date_dim      (load date)
--
-- ANALYTICS ENABLED:
--   - Count of teachers per subject per district/school
--   - Science vs Arts vs Language teacher distribution
--   - Subject coverage gap analysis per school
--   - O-level vs A-level subject staffing
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dw.subject_dim
--    One row per subject. Covers both primary and secondary subjects.
--    Not SCD2 — subjects rarely change. Full reload on ETL run.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.subject_dim (
    id                      SERIAL          NOT NULL,

    -- Natural key
    source_id               BIGINT          NOT NULL,   -- setting_*_school_subjects.id
    subject_level           VARCHAR(50)     NOT NULL,   -- PRIMARY or SECONDARY

    -- Attributes
    subject_name            VARCHAR(200)    NOT NULL,
    is_language_subject     BOOLEAN         DEFAULT FALSE,
    is_science_subject      BOOLEAN         DEFAULT FALSE,  -- secondary only; FALSE for primary
    is_principal_subject    BOOLEAN         DEFAULT FALSE,  -- secondary only
    is_olevel_subject       BOOLEAN         DEFAULT FALSE,  -- secondary only
    is_alevel_subject       BOOLEAN         DEFAULT FALSE,  -- secondary only
    is_mandatory            BOOLEAN         DEFAULT FALSE,
    is_examinable           BOOLEAN         DEFAULT FALSE,
    has_lab_yn              BOOLEAN         DEFAULT FALSE,
    is_active_yn            BOOLEAN         DEFAULT TRUE,

    -- Derived grouping (for reporting)
    -- SCIENCE | ARTS | LANGUAGE | PRACTICAL
    subject_category        VARCHAR(50)     GENERATED ALWAYS AS (
        CASE
            WHEN is_language_subject = TRUE             THEN 'LANGUAGE'
            WHEN is_science_subject  = TRUE             THEN 'SCIENCE'
            ELSE                                             'ARTS'
        END
    ) STORED,

    load_time               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT subject_dim_pkey         PRIMARY KEY (id),
    CONSTRAINT subject_dim_natural_key  UNIQUE (source_id, subject_level)
);

COMMENT ON TABLE  dw.subject_dim IS
    'Subject dimension covering both primary and secondary school subjects. Grain: 1 row per subject. Not SCD2 — full reload on each ETL run.';
COMMENT ON COLUMN dw.subject_dim.source_id IS
    'setting_secondary_school_subjects.id or setting_primary_school_subjects.id.';
COMMENT ON COLUMN dw.subject_dim.subject_level IS
    'PRIMARY or SECONDARY — needed to disambiguate subject IDs across the two source tables.';
COMMENT ON COLUMN dw.subject_dim.subject_category IS
    'Derived: LANGUAGE if is_language_subject, SCIENCE if is_science_subject, else ARTS.';

CREATE INDEX IF NOT EXISTS idx_subject_dim_name
    ON dw.subject_dim (subject_name);
CREATE INDEX IF NOT EXISTS idx_subject_dim_level
    ON dw.subject_dim (subject_level);
CREATE INDEX IF NOT EXISTS idx_subject_dim_category
    ON dw.subject_dim (subject_category);


-- -----------------------------------------------------------------------------
-- 2. dw.teaching_subject_fact
--    Grain: 1 row per teacher per subject.
--    Joins to teacher_dim, schools_dim, subject_dim, date_dim.
--    Used to count teachers per subject per school/district.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.teaching_subject_fact (
    id                  SERIAL          NOT NULL,

    -- Dimension foreign keys
    teacher_id          INTEGER         NOT NULL,   -- FK → dw.teacher_dim(id)
    school_id           INTEGER         NOT NULL,   -- FK → dw.schools_dim(id)
    subject_id          INTEGER         NOT NULL,   -- FK → dw.subject_dim(id)
    date_id             INTEGER         NOT NULL,   -- FK → dw.date_dim(id)  (load date)

    -- Natural keys (retained for debugging and re-runs)
    person_id           BIGINT,                     -- teachers.person_id from OLTP
    school_source_id    BIGINT,                     -- schools.id from OLTP
    subject_source_id   BIGINT,                     -- setting_*_school_subjects.id

    -- Degenerate dimensions
    subject_level       VARCHAR(50),                -- PRIMARY or SECONDARY

    -- No measures — this is a coverage fact (presence/absence)
    -- Counts are derived by aggregating rows in BI queries

    load_time           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT teaching_subject_fact_pkey        PRIMARY KEY (id),
    CONSTRAINT teaching_subject_fact_natural_key UNIQUE (person_id, subject_source_id, subject_level)
);

COMMENT ON TABLE  dw.teaching_subject_fact IS
    'Teaching subject fact. Grain: 1 row per teacher per subject. No measures — aggregate rows to count teachers per subject. Join to subject_dim for subject attributes, teacher_dim for teacher attributes, schools_dim for location.';
COMMENT ON COLUMN dw.teaching_subject_fact.teacher_id IS
    'FK → dw.teacher_dim(id). Current version of the teacher.';
COMMENT ON COLUMN dw.teaching_subject_fact.school_id IS
    'FK → dw.schools_dim(id). Current school the teacher is deployed to.';
COMMENT ON COLUMN dw.teaching_subject_fact.subject_id IS
    'FK → dw.subject_dim(id).';
COMMENT ON COLUMN dw.teaching_subject_fact.date_id IS
    'FK → dw.date_dim(id). Date this record was loaded — not a teaching period.';

-- FK constraints
ALTER TABLE dw.teaching_subject_fact
    ADD CONSTRAINT tsf_teacher_fk
    FOREIGN KEY (teacher_id) REFERENCES dw.teacher_dim (id);

ALTER TABLE dw.teaching_subject_fact
    ADD CONSTRAINT tsf_school_fk
    FOREIGN KEY (school_id) REFERENCES dw.schools_dim (id);

ALTER TABLE dw.teaching_subject_fact
    ADD CONSTRAINT tsf_subject_fk
    FOREIGN KEY (subject_id) REFERENCES dw.subject_dim (id);

ALTER TABLE dw.teaching_subject_fact
    ADD CONSTRAINT tsf_date_fk
    FOREIGN KEY (date_id) REFERENCES dw.date_dim (id);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_tsf_teacher
    ON dw.teaching_subject_fact (teacher_id);
CREATE INDEX IF NOT EXISTS idx_tsf_school
    ON dw.teaching_subject_fact (school_id);
CREATE INDEX IF NOT EXISTS idx_tsf_subject
    ON dw.teaching_subject_fact (subject_id);
CREATE INDEX IF NOT EXISTS idx_tsf_person
    ON dw.teaching_subject_fact (person_id);
CREATE INDEX IF NOT EXISTS idx_tsf_subject_level
    ON dw.teaching_subject_fact (subject_level);
