-- =============================================================================
-- EMIS Non-Teaching Staff Mart ETL: Step 2 — SCD2 Load into dw.non_teaching_staff_dim
--
-- SCD2 tracked fields:
--   gender, category, role, highest_education_level,
--   employment_status, is_on_government_payroll
-- =============================================================================

BEGIN;

-- ── Step 2a: Deduplicate to one row per staff member ──────────────────────────
CREATE TEMP TABLE tmp_nts_current AS
SELECT DISTINCT ON (person_id)
    person_id,
    employee_id,
    surname,
    given_name,
    gender,
    nationality,
    nin,
    category,
    role,
    highest_education_level,
    employment_status,
    is_on_government_payroll,
    ipps_number,
    hrin,
    hcm_employee_number,
    posting_date,
    MD5(
        COALESCE(gender,                    '') ||'|'||
        COALESCE(category,                  '') ||'|'||
        COALESCE(role,                      '') ||'|'||
        COALESCE(highest_education_level,   '') ||'|'||
        COALESCE(employment_status,         '') ||'|'||
        COALESCE(is_on_government_payroll::TEXT, 'false')
    ) AS change_hash
FROM stg.non_teaching_staff_flat
ORDER BY person_id, term_start_date DESC NULLS LAST;

CREATE INDEX ON tmp_nts_current (person_id);

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM tmp_nts_current;
    RAISE NOTICE 'tmp_nts_current: % distinct staff members', v;
END; $$;


-- ── Step 2b: Expire changed rows ──────────────────────────────────────────────
UPDATE dw.non_teaching_staff_dim AS dim
SET
    expiration_date = CURRENT_DATE - 1,
    is_current      = FALSE
FROM tmp_nts_current stg
WHERE dim.source_id   = stg.person_id
  AND dim.is_current  = TRUE
  AND dim.change_hash <> stg.change_hash;


-- ── Step 2c: Insert new and changed rows ──────────────────────────────────────
INSERT INTO dw.non_teaching_staff_dim (
    source_id,
    employee_id,
    surname,
    given_name,
    gender,
    nationality,
    nin,
    category,
    role,
    highest_education_level,
    employment_status,
    is_on_government_payroll,
    ipps_number,
    hrin,
    hcm_employee_number,
    posting_date,
    effective_date,
    expiration_date,
    is_current,
    change_hash,
    change_reason,
    changed_fields
)
SELECT
    stg.person_id,
    stg.employee_id,
    stg.surname,
    stg.given_name,
    stg.gender,
    stg.nationality,
    stg.nin,
    stg.category,
    stg.role,
    stg.highest_education_level,
    stg.employment_status,
    stg.is_on_government_payroll,
    stg.ipps_number,
    stg.hrin,
    stg.hcm_employee_number,
    stg.posting_date,
    CURRENT_DATE            AS effective_date,
    '9999-12-31'::DATE      AS expiration_date,
    TRUE                    AS is_current,
    stg.change_hash,

    CASE WHEN dim.id IS NULL THEN 'INITIAL_LOAD'
         ELSE 'ATTRIBUTE_CHANGE'
    END                     AS change_reason,

    CASE WHEN dim.id IS NULL THEN 'INITIAL_LOAD'
         ELSE TRIM(BOTH ',' FROM
            CASE WHEN COALESCE(dim.gender,'')                  <> COALESCE(stg.gender,'')                  THEN 'gender,'                  ELSE '' END ||
            CASE WHEN COALESCE(dim.category,'')                <> COALESCE(stg.category,'')                THEN 'category,'                ELSE '' END ||
            CASE WHEN COALESCE(dim.role,'')                    <> COALESCE(stg.role,'')                    THEN 'role,'                    ELSE '' END ||
            CASE WHEN COALESCE(dim.highest_education_level,'') <> COALESCE(stg.highest_education_level,'') THEN 'highest_education_level,'  ELSE '' END ||
            CASE WHEN COALESCE(dim.employment_status,'')       <> COALESCE(stg.employment_status,'')       THEN 'employment_status,'        ELSE '' END ||
            CASE WHEN COALESCE(dim.is_on_government_payroll::TEXT,'false') <> COALESCE(stg.is_on_government_payroll::TEXT,'false')
                 THEN 'is_on_government_payroll,' ELSE '' END
         )
    END                     AS changed_fields

FROM tmp_nts_current stg
LEFT JOIN dw.non_teaching_staff_dim dim
    ON  dim.source_id  = stg.person_id
    AND dim.is_current = TRUE
WHERE dim.id IS NULL;


-- ── Step 2d: Verify ───────────────────────────────────────────────────────────
DO $$
DECLARE
    v_total     BIGINT;
    v_current   BIGINT;
    v_historical BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_total     FROM dw.non_teaching_staff_dim;
    SELECT COUNT(*) INTO v_current   FROM dw.non_teaching_staff_dim WHERE is_current = TRUE;
    SELECT COUNT(*) INTO v_historical FROM dw.non_teaching_staff_dim WHERE is_current = FALSE;
    RAISE NOTICE 'dw.non_teaching_staff_dim  total=% | current=% | historical=%',
        v_total, v_current, v_historical;
END;
$$;

DROP TABLE IF EXISTS tmp_nts_current;

COMMIT;
