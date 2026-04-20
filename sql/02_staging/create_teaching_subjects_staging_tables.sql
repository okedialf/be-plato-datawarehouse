-- =============================================================================
-- EMIS Data Warehouse: Teaching Subjects Mart — Staging Tables
-- Schema: stg
--
-- DESIGN NOTES:
-- The teaching subjects mart answers:
--   - How many teachers teach each subject by school / district / level?
--   - What is the subject coverage per school (subjects offered vs required)?
--   - How many science vs arts vs language teachers per district?
--
-- GRAIN of fact table:
--   dw.teaching_subject_fact: 1 row per teacher per subject
--   (not per term — subject assignments are not term-specific in EMIS)
--
-- SOURCE TABLES:
--   public.teacher_subjects          — teacher ↔ subject link (employee_id, subject_id)
--   public.school_employees          — employee_id → person_id
--   public.setting_secondary_school_subjects — subject attributes (secondary)
--   public.setting_primary_school_subjects   — subject attributes (primary)
--   stg.teacher_subjects_raw         — already populated by teachers mart ETL
--
-- NOTE: teacher_subjects.subject_id references setting_secondary_school_subjects
--   only. Primary subjects are captured via teachers.trained_subject_1_id and
--   trained_subject_2_id on the teachers table, which reference
--   setting_primary_school_subjects. Both are included here.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. stg.subject_dim_raw
--    Combined subject list from both primary and secondary subject tables.
--    Unified before loading into dw.subject_dim.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.subject_dim_raw (
    source_id           BIGINT          NOT NULL,
    subject_level       VARCHAR(50)     NOT NULL,   -- PRIMARY or SECONDARY
    subject_name        VARCHAR(200)    NOT NULL,
    is_language_subject BOOLEAN         DEFAULT FALSE,
    is_science_subject  BOOLEAN         DEFAULT FALSE,  -- secondary only
    is_principal_subject BOOLEAN        DEFAULT FALSE,  -- secondary only
    is_olevel_subject   BOOLEAN         DEFAULT FALSE,  -- secondary only
    is_alevel_subject   BOOLEAN         DEFAULT FALSE,  -- secondary only
    is_mandatory        BOOLEAN         DEFAULT FALSE,
    is_examinable       BOOLEAN         DEFAULT FALSE,
    has_lab_yn          BOOLEAN         DEFAULT FALSE,
    is_active_yn        BOOLEAN         DEFAULT TRUE,
    is_archived_yn      BOOLEAN         DEFAULT FALSE,
    date_created        TIMESTAMP,
    date_updated        TIMESTAMP,
    CONSTRAINT subject_dim_raw_pkey PRIMARY KEY (source_id, subject_level)
);
COMMENT ON TABLE stg.subject_dim_raw IS
    'Combined subject list from setting_secondary_school_subjects and setting_primary_school_subjects. Source for dw.subject_dim.';


-- -----------------------------------------------------------------------------
-- 2. stg.teaching_subject_flat
--    One row per teacher per subject. Links teacher to subject with
--    school and location context. Source for dw.teaching_subject_fact.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.teaching_subject_flat (
    id                      SERIAL          NOT NULL,

    -- Teacher keys
    person_id               BIGINT          NOT NULL,   -- teachers.person_id
    employee_id             BIGINT,                     -- school_employees.id

    -- Subject
    subject_id              BIGINT          NOT NULL,   -- setting_secondary_school_subjects.id
    subject_name            VARCHAR(200),
    subject_level           VARCHAR(50),                -- PRIMARY / SECONDARY
    is_science_subject      BOOLEAN,
    is_language_subject     BOOLEAN,
    is_principal_subject    BOOLEAN,
    is_olevel_subject       BOOLEAN,
    is_alevel_subject       BOOLEAN,

    -- School context (from teacher_school_raw)
    school_id               BIGINT,
    school_name             VARCHAR(200),
    school_type             VARCHAR(200),

    -- Teacher attributes (from teachers_raw)
    teacher_type            VARCHAR(200),
    gender                  VARCHAR(100),
    qualification           VARCHAR(200),

    -- Admin unit (source_id for joining to admin_units_dim)
    admin_unit_source_id    BIGINT,

    -- Dates
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,

    CONSTRAINT teaching_subject_flat_pkey PRIMARY KEY (id),
    CONSTRAINT teaching_subject_flat_unique UNIQUE (person_id, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_teaching_subject_flat_person
    ON stg.teaching_subject_flat (person_id);
CREATE INDEX IF NOT EXISTS idx_teaching_subject_flat_subject
    ON stg.teaching_subject_flat (subject_id);
CREATE INDEX IF NOT EXISTS idx_teaching_subject_flat_school
    ON stg.teaching_subject_flat (school_id);
COMMENT ON TABLE stg.teaching_subject_flat IS
    'One row per teacher per subject with school and teacher context. Source for dw.teaching_subject_fact.';
