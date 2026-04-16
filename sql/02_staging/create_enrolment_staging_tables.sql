-- =============================================================================
-- EMIS Enrolment Mart: Staging Tables — Corrected Version
-- All ID columns use BIGINT to match the actual EMIS source system.
-- The source system uses BIGINT PKs not UUIDs.
-- =============================================================================

-- 1. stg.learners_raw
CREATE TABLE IF NOT EXISTS stg.learners_raw (
    person_id               BIGINT        NOT NULL,
    lin                     VARCHAR(30),
    surname                 VARCHAR(100),
    given_name              VARCHAR(150),
    date_of_birth           DATE,
    gender                  VARCHAR(20),
    nationality             VARCHAR(150),
    district_of_birth_id    BIGINT,
    nin                     VARCHAR(30),
    passport_no             VARCHAR(100),
    student_pass_no         VARCHAR(100),
    work_permit_no          VARCHAR(100),
    refugee_id              VARCHAR(100),
    is_flagged_yn           BOOLEAN,
    deleted_at              TIMESTAMP,
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,
    CONSTRAINT learners_raw_pkey PRIMARY KEY (person_id)
);

-- 2. stg.learner_disabilities_raw
CREATE TABLE IF NOT EXISTS stg.learner_disabilities_raw (
    id                  BIGINT        NOT NULL,
    learner_id          BIGINT        NOT NULL,
    disability_type     VARCHAR(60)   NOT NULL,
    severity            VARCHAR(30),
    date_created        TIMESTAMP,
    date_updated        TIMESTAMP,
    CONSTRAINT learner_disabilities_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_learner_disabilities_raw_learner
    ON stg.learner_disabilities_raw (learner_id);

-- 3. stg.enrolments_raw
CREATE TABLE IF NOT EXISTS stg.enrolments_raw (
    id                           BIGINT        NOT NULL,
    learner_id                   BIGINT        NOT NULL,
    school_id                    BIGINT        NOT NULL,
    academic_year_id             BIGINT        NOT NULL,
    teaching_period_id           BIGINT        NOT NULL,
    reporting_status_id          BIGINT,
    learner_promotion_status_id  BIGINT,
    learner_transfer_status_id   BIGINT,
    learner_transition_status_id BIGINT,
    learner_registration_number  VARCHAR(200),
    is_enrolment_active_yn       BOOLEAN,
    date_created                 TIMESTAMP,
    date_updated                 TIMESTAMP,
    CONSTRAINT enrolments_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_enrolments_raw_learner
    ON stg.enrolments_raw (learner_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_raw_school
    ON stg.enrolments_raw (school_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_raw_yr_term
    ON stg.enrolments_raw (academic_year_id, teaching_period_id);

-- 4. stg.enrolments_subtype_raw
CREATE TABLE IF NOT EXISTS stg.enrolments_subtype_raw (
    enrolment_id            BIGINT        NOT NULL,
    school_level            VARCHAR(50)   NOT NULL,
    education_class_id      BIGINT,
    education_class_name    VARCHAR(50),
    familiar_language_id    BIGINT,
    familiar_language_name  VARCHAR(100),
    date_created            TIMESTAMP,
    CONSTRAINT enrolments_subtype_raw_pkey PRIMARY KEY (enrolment_id)
);

-- 5. stg.learner_promotions_raw
CREATE TABLE IF NOT EXISTS stg.learner_promotions_raw (
    id                      BIGINT        NOT NULL,
    learner_id              BIGINT        NOT NULL,
    school_id               BIGINT        NOT NULL,
    academic_year_id        BIGINT        NOT NULL,
    from_class_id           BIGINT,
    to_class_id             BIGINT,
    promotion_status_id     BIGINT,
    promotion_status_name   VARCHAR(30),
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,
    CONSTRAINT learner_promotions_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_learner_promotions_raw_learner
    ON stg.learner_promotions_raw (learner_id);

-- 6. stg.learner_transitions_raw
CREATE TABLE IF NOT EXISTS stg.learner_transitions_raw (
    id                      BIGINT        NOT NULL,
    learner_id              BIGINT        NOT NULL,
    from_school_id          BIGINT,
    to_school_id            BIGINT,
    academic_year_id        BIGINT,
    transition_status_id    BIGINT,
    transition_status_name  VARCHAR(30),
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,
    CONSTRAINT learner_transitions_raw_pkey PRIMARY KEY (id)
);

-- 7. stg.enrolments_flat
CREATE TABLE IF NOT EXISTS stg.enrolments_flat (
    id                           SERIAL        NOT NULL,
    enrolment_source_id          BIGINT        NOT NULL,
    learner_source_id            BIGINT        NOT NULL,
    school_source_id             BIGINT        NOT NULL,
    academic_year                INTEGER       NOT NULL,
    term                         VARCHAR(10)   NOT NULL,
    lin                          VARCHAR(30),
    gender                       VARCHAR(10),
    age                          INTEGER,
    nationality                  VARCHAR(100),
    nin                          VARCHAR(14),
    passport_no                  VARCHAR(100),
    student_pass_no              VARCHAR(100),
    work_permit_no               VARCHAR(100),
    refugee_id                   VARCHAR(100),
    orphan_status                VARCHAR(50),
    is_visual_yn                 BOOLEAN       DEFAULT FALSE,
    is_hearing_yn                BOOLEAN       DEFAULT FALSE,
    is_walking_yn                BOOLEAN       DEFAULT FALSE,
    is_self_care_yn              BOOLEAN       DEFAULT FALSE,
    is_remembering_yn            BOOLEAN       DEFAULT FALSE,
    is_communication_yn          BOOLEAN       DEFAULT FALSE,
    has_multiple_disabilities_yn BOOLEAN       DEFAULT FALSE,
    grade                        VARCHAR(50),
    school_level                 VARCHAR(100),
    enrolment_type               VARCHAR(50),
    is_active                    BOOLEAN,
    school_emis_number           VARCHAR(50),
    school_name                  VARCHAR(200),
    school_ownership             VARCHAR(100),
    school_funding_type          VARCHAR(100),
    school_sex_composition       VARCHAR(50),
    school_boarding_status       VARCHAR(50),
    school_type                  VARCHAR(100),
    admin_unit_source_id         INTEGER,
    change_hash                  TEXT,
    change_reason                TEXT,
    changed_fields               TEXT,
    CONSTRAINT enrolments_flat_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_enrolments_flat_learner
    ON stg.enrolments_flat (learner_source_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_flat_school
    ON stg.enrolments_flat (school_source_id);
CREATE INDEX IF NOT EXISTS idx_enrolments_flat_yr_term
    ON stg.enrolments_flat (academic_year, term);
