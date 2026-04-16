-- =============================================================================
-- EMIS Enrolment Mart: Sync dw.enrolment_type_dim with public.setting_enrolment_types
-- Run this once to correct the seed data.
--
-- Source values (from public.setting_enrolment_types):
--   1 = BASELINE
--   2 = NEW
--   3 = CONTINUING
--   4 = TRANSFER
--
-- Note: The original seed had RETURNEE, REPEATER, NEW ENTRANT which do not
-- exist in the source table. REPEATER is handled via learner_enrolments.is_repeating_yn
-- and stored as a separate flag — not as an enrolment type in EMIS.
-- =============================================================================

-- Clear existing seed data and reload from source
TRUNCATE TABLE dw.enrolment_type_dim RESTART IDENTITY CASCADE;

INSERT INTO dw.enrolment_type_dim (source_id, enrolment_type, description)
VALUES
    (1, 'BASELINE',    'Learner enrolled at the time of the baseline census'),
    (2, 'NEW',         'New learner enrolling for the first time at this school'),
    (3, 'CONTINUING',  'Learner continuing from the previous term or year'),
    (4, 'TRANSFER',    'Learner who transferred from another school')
ON CONFLICT (enrolment_type) DO UPDATE
    SET source_id   = EXCLUDED.source_id,
        description = EXCLUDED.description;

-- Verify
SELECT id, source_id, enrolment_type, description
FROM dw.enrolment_type_dim
ORDER BY source_id;
