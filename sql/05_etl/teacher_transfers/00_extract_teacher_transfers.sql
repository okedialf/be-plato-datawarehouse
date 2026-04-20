-- =============================================================================
-- EMIS Teacher Transfers Mart ETL: Step 0 — Extract from Source
--
-- Populates stg.teacher_transfers_raw from public.teacher_transfers,
-- resolving school names, district names, and teacher attributes.
--
-- KEY SOURCE TABLE FACTS:
--   teacher_transfers.status: 1=PENDING, 2=APPROVED, 3=REJECTED
--   teacher_transfers.incoming_local_government_id / outgoing_local_government_id
--     → admin_unit_upper_local_governments.district_id
--     → admin_unit_districts.name
--   Schools resolved via outgoing_school_id / incoming_school_id → public.schools
--   Days at station computed from teacher_postings (most recent posting before transfer)
--
-- LESSON FROM PREVIOUS MARTS:
--   admin_units_dim uses current_status (not is_current)
--   admin_unit_type is mixed case: 'District', 'Parish' etc.
--   child_id = parent's source_id
-- =============================================================================

BEGIN;

TRUNCATE TABLE stg.teacher_transfers_raw;

INSERT INTO stg.teacher_transfers_raw (
    transfer_id,
    person_id,
    employee_id,
    outgoing_employee_id,
    outgoing_school_id,
    incoming_school_id,
    outgoing_school_name,
    incoming_school_name,
    outgoing_school_type,
    incoming_school_type,
    outgoing_district_id,
    incoming_district_id,
    outgoing_district_name,
    incoming_district_name,
    is_inter_district,
    posting_date,
    reporting_date,
    transfer_status,
    appointment_minute_number,
    category_id,
    days_at_outgoing_station,
    teacher_type,
    gender,
    qualification,
    discipline,
    ipps_number,
    date_created,
    date_updated
)
SELECT
    tt.id                                               AS transfer_id,
    tt.person_id,
    tt.employee_id,
    tt.outgoing_employee_id,

    tt.outgoing_school_id,
    tt.incoming_school_id,

    -- School names
    out_s.name                                          AS outgoing_school_name,
    in_s.name                                           AS incoming_school_name,
    out_sst.name                                        AS outgoing_school_type,
    in_sst.name                                         AS incoming_school_type,

    -- District resolution:
    -- incoming/outgoing_local_government_id →
    -- admin_unit_upper_local_governments.district_id →
    -- admin_unit_districts.id
    out_dist.id                                         AS outgoing_district_id,
    in_dist.id                                          AS incoming_district_id,
    out_dist.name                                       AS outgoing_district_name,
    in_dist.name                                        AS incoming_district_name,

    -- Inter-district flag
    CASE
        WHEN out_dist.id IS NOT NULL
             AND in_dist.id IS NOT NULL
             AND out_dist.id <> in_dist.id
        THEN TRUE
        ELSE FALSE
    END                                                 AS is_inter_district,

    tt.posting_date,
    tt.reporting_date,

    -- Decode status smallint
    CASE tt.status
        WHEN 1 THEN 'PENDING'
        WHEN 2 THEN 'APPROVED'
        WHEN 3 THEN 'REJECTED'
        ELSE 'UNKNOWN'
    END                                                 AS transfer_status,

    tt.appointment_minute_number,
    tt.category_id,

    -- Days at outgoing station:
    -- Find the most recent posting to the outgoing school before this transfer
    -- Use teacher_postings table (posting_date = date teacher was posted to that school)
    CASE
        WHEN tt.posting_date IS NOT NULL AND last_post.posting_date IS NOT NULL
        THEN (tt.posting_date - last_post.posting_date)::INTEGER
        ELSE NULL
    END                                                 AS days_at_outgoing_station,

    -- Teacher attributes from stg.teachers_raw (if available)
    tr.teacher_type,
    tr.gender,
    tr.qualification,
    -- Discipline derived from teacher_subjects
    CASE
        WHEN disc.total_subjects = 0 OR disc.total_subjects IS NULL THEN NULL
        WHEN disc.science_count = disc.total_subjects               THEN 'SCIENCE'
        WHEN disc.science_count = 0                                 THEN 'ARTS'
        ELSE 'BOTH'
    END                                                 AS discipline,
    tr.ipps_number,

    tt.date_created,
    tt.date_updated

FROM public.teacher_transfers tt

-- Outgoing school
LEFT JOIN public.schools out_s
    ON out_s.id = tt.outgoing_school_id
LEFT JOIN public.setting_school_types out_sst
    ON out_sst.id = out_s.school_type_id

-- Incoming school
LEFT JOIN public.schools in_s
    ON in_s.id = tt.incoming_school_id
LEFT JOIN public.setting_school_types in_sst
    ON in_sst.id = in_s.school_type_id

-- Outgoing district via upper local government
LEFT JOIN public.admin_unit_upper_local_governments out_ulg
    ON out_ulg.id = tt.outgoing_local_government_id
LEFT JOIN public.admin_unit_districts out_dist
    ON out_dist.id = out_ulg.district_id
    AND out_dist.is_archived_yn = FALSE

-- Incoming district via upper local government
LEFT JOIN public.admin_unit_upper_local_governments in_ulg
    ON in_ulg.id = tt.incoming_local_government_id
LEFT JOIN public.admin_unit_districts in_dist
    ON in_dist.id = in_ulg.district_id
    AND in_dist.is_archived_yn = FALSE

-- Most recent posting to the outgoing school prior to this transfer
-- Used to compute days_at_outgoing_station
LEFT JOIN LATERAL (
    SELECT tp.posting_date
    FROM public.teacher_postings tp
    WHERE tp.person_id  = tt.person_id
      AND tp.school_id  = tt.outgoing_school_id
      AND tp.posting_date < tt.posting_date
      AND tp.status IN (1, 2)   -- posted or received
    ORDER BY tp.posting_date DESC
    LIMIT 1
) last_post ON TRUE

-- Teacher attributes
LEFT JOIN stg.teachers_raw tr
    ON tr.person_id = tt.person_id

-- Discipline from teacher subjects
LEFT JOIN (
    SELECT
        person_id,
        COUNT(*)                                            AS total_subjects,
        COUNT(*) FILTER (WHERE is_science_subject = TRUE)  AS science_count
    FROM stg.teacher_subjects_raw
    GROUP BY person_id
) disc ON disc.person_id = tt.person_id

ON CONFLICT (transfer_id) DO UPDATE
    SET transfer_status         = EXCLUDED.transfer_status,
        posting_date            = EXCLUDED.posting_date,
        reporting_date          = EXCLUDED.reporting_date,
        days_at_outgoing_station = EXCLUDED.days_at_outgoing_station,
        date_updated            = EXCLUDED.date_updated;

DO $$ DECLARE v BIGINT; v_approved BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.teacher_transfers_raw;
    SELECT COUNT(*) INTO v_approved
    FROM stg.teacher_transfers_raw WHERE transfer_status = 'APPROVED';
    RAISE NOTICE 'stg.teacher_transfers_raw: % total rows, % approved transfers', v, v_approved;
END; $$;

COMMIT;
