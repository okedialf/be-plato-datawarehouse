-- =============================================================================
-- EMIS Enrolment Mart ETL: Step 2 — SCD2 Load into dw.learner_dim
--
-- Reads from stg.enrolments_flat (one row per enrolment).
-- First deduplicates to one row per distinct learner.
-- Then applies SCD Type 2 logic:
--   NEW learner   → INSERT with effective_date = today, is_current = TRUE
--   CHANGED attrs → expire old row, INSERT new version
--   UNCHANGED     → skip
--
-- SCD2 tracked fields:
--   gender, nationality, nin, orphan_status,
--   is_visual_yn, is_hearing_yn, is_walking_yn,
--   is_self_care_yn, is_remembering_yn, is_communication_yn,
--   has_multiple_disabilities_yn
-- =============================================================================

BEGIN;

-- ── Step 2a: Build a deduplicated snapshot of learners from flat staging ──────
-- One row per learner_source_id — take the most recently created enrolment row
-- as the authoritative source of learner attributes.
CREATE TEMP TABLE tmp_learners_current AS
SELECT DISTINCT ON (learner_source_id)
    learner_source_id,
    lin,
    gender,
    age                         AS age_at_load,
    nationality,
    orphan_status,
    nin,
    passport_no,
    student_pass_no,
    work_permit_no,
    refugee_id,
    is_visual_yn,
    is_hearing_yn,
    is_walking_yn,
    is_self_care_yn,
    is_remembering_yn,
    is_communication_yn,
    has_multiple_disabilities_yn,
    -- Compute the change hash over all SCD2-tracked fields
    MD5(
        COALESCE(gender,            '') ||'|'||
        COALESCE(nationality,       '') ||'|'||
        COALESCE(nin,               '') ||'|'||
        COALESCE(orphan_status,     '') ||'|'||
        COALESCE(is_visual_yn::TEXT,        'false') ||'|'||
        COALESCE(is_hearing_yn::TEXT,       'false') ||'|'||
        COALESCE(is_walking_yn::TEXT,       'false') ||'|'||
        COALESCE(is_self_care_yn::TEXT,     'false') ||'|'||
        COALESCE(is_remembering_yn::TEXT,   'false') ||'|'||
        COALESCE(is_communication_yn::TEXT, 'false') ||'|'||
        COALESCE(has_multiple_disabilities_yn::TEXT, 'false')
    ) AS change_hash
FROM stg.enrolments_flat
ORDER BY learner_source_id, age DESC NULLS LAST;

-- Index for fast join against dw.learner_dim
CREATE INDEX ON tmp_learners_current (learner_source_id);

-- ── Step 2b: Expire changed rows ──────────────────────────────────────────────
-- For every learner that already exists in dw.learner_dim with is_current = TRUE
-- but whose change_hash no longer matches → expire that row.
UPDATE dw.learner_dim AS dim
SET
    expiration_date = CURRENT_DATE - 1,
    is_current      = FALSE
FROM tmp_learners_current stg
WHERE dim.source_id   = stg.learner_source_id
  AND dim.is_current  = TRUE
  AND dim.change_hash <> stg.change_hash;

-- ── Step 2c: Insert new rows (new learners + changed learners) ────────────────
-- Insert where:
--   (a) the learner has no current row in dw.learner_dim  → brand new learner
--   (b) the learner had a current row that was just expired in 2b → new version
INSERT INTO dw.learner_dim (
    source_id,
    lin,
    gender,
    age_at_load,
    nationality,
    orphan_status,
    nin,
    passport_no,
    student_pass_no,
    work_permit_no,
    refugee_id,
    is_visual_yn,
    is_hearing_yn,
    is_walking_yn,
    is_self_care_yn,
    is_remembering_yn,
    is_communication_yn,
    has_multiple_disabilities_yn,
    effective_date,
    expiration_date,
    is_current,
    change_hash,
    change_reason,
    changed_fields
)
SELECT
    stg.learner_source_id,
    stg.lin,
    stg.gender,
    stg.age_at_load,
    stg.nationality,
    stg.orphan_status,
    stg.nin,
    stg.passport_no,
    stg.student_pass_no,
    stg.work_permit_no,
    stg.refugee_id,
    stg.is_visual_yn,
    stg.is_hearing_yn,
    stg.is_walking_yn,
    stg.is_self_care_yn,
    stg.is_remembering_yn,
    stg.is_communication_yn,
    stg.has_multiple_disabilities_yn,
    CURRENT_DATE                AS effective_date,
    '9999-12-31'::DATE          AS expiration_date,
    TRUE                        AS is_current,
    stg.change_hash,

    -- Change reason: INITIAL_LOAD for new learners, attribute description for changes
    CASE
        WHEN dim.id IS NULL THEN 'INITIAL_LOAD'
        ELSE 'ATTRIBUTE_CHANGE'
    END                         AS change_reason,

    -- Changed fields: compare each tracked column against the expired row
    CASE
        WHEN dim.id IS NULL THEN 'INITIAL_LOAD'
        ELSE TRIM(BOTH ',' FROM
            CASE WHEN dim.gender         <> stg.gender         THEN 'gender,'        ELSE '' END ||
            CASE WHEN dim.nationality    <> stg.nationality     THEN 'nationality,'   ELSE '' END ||
            CASE WHEN COALESCE(dim.nin,'') <> COALESCE(stg.nin,'') THEN 'nin,'       ELSE '' END ||
            CASE WHEN dim.orphan_status  <> stg.orphan_status   THEN 'orphan_status,' ELSE '' END ||
            CASE WHEN dim.is_visual_yn         <> stg.is_visual_yn         THEN 'is_visual_yn,'         ELSE '' END ||
            CASE WHEN dim.is_hearing_yn        <> stg.is_hearing_yn        THEN 'is_hearing_yn,'        ELSE '' END ||
            CASE WHEN dim.is_walking_yn        <> stg.is_walking_yn        THEN 'is_walking_yn,'        ELSE '' END ||
            CASE WHEN dim.is_self_care_yn      <> stg.is_self_care_yn      THEN 'is_self_care_yn,'      ELSE '' END ||
            CASE WHEN dim.is_remembering_yn    <> stg.is_remembering_yn    THEN 'is_remembering_yn,'    ELSE '' END ||
            CASE WHEN dim.is_communication_yn  <> stg.is_communication_yn  THEN 'is_communication_yn,'  ELSE '' END ||
            CASE WHEN dim.has_multiple_disabilities_yn <> stg.has_multiple_disabilities_yn
                 THEN 'has_multiple_disabilities_yn,' ELSE '' END
        )
    END                         AS changed_fields

FROM tmp_learners_current stg

-- LEFT JOIN to detect new vs changed (changed = dim row was expired in step 2b)
LEFT JOIN dw.learner_dim dim
    ON  dim.source_id   = stg.learner_source_id
    AND dim.is_current  = TRUE   -- only match still-current rows (not just expired)

-- Only insert where no current row exists (new learner OR just expired = no match)
WHERE dim.id IS NULL;

-- ── Step 2d: Verify ───────────────────────────────────────────────────────────
DO $$
DECLARE
    v_total     BIGINT;
    v_current   BIGINT;
    v_historical BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_total     FROM dw.learner_dim;
    SELECT COUNT(*) INTO v_current   FROM dw.learner_dim WHERE is_current = TRUE;
    SELECT COUNT(*) INTO v_historical FROM dw.learner_dim WHERE is_current = FALSE;

    RAISE NOTICE 'dw.learner_dim  total=% | current=% | historical=%',
        v_total, v_current, v_historical;
END;
$$;

DROP TABLE IF EXISTS tmp_learners_current;

COMMIT;
