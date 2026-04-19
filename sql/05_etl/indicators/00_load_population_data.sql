-- =============================================================================
-- EMIS Indicators Mart ETL: Step 0 — Load UBOS Population Projections
--
-- SOURCE: UBOS Population Projections 2025-2050 (Volume I, July 2025)
-- Data: Table 5 — Midyear District Population Projections by sex
-- Grain: district × year × sex (M/F/TOTAL)
--
-- IMPORTANT NOTES:
-- 1. The UBOS document provides TOTAL district population by sex only
--    (not broken down by age group at district level).
-- 2. Age-specific breakdowns (3-5, 6-12 etc.) are available at NATIONAL
--    level only in the projections document.
-- 3. For district-level age estimates we apply the national age distribution
--    proportions to district totals — a standard demographic estimation method.
-- 4. This script must be run MANUALLY each year when UBOS releases new
--    projections, or when the national age structure proportions are updated.
--
-- HOW TO UPDATE:
-- 1. Open the UBOS PDF (Table 5)
-- 2. Extract district male/female/total populations for the required year
-- 3. Replace the INSERT VALUES below with the new data
-- 4. Run this script in pgAdmin
--
-- NATIONAL AGE PROPORTIONS (from 2024 NPHC / UBOS projections):
--   Age 3-5  (pre-primary): ~5.5% of total population
--   Age 6    (P1 entry):    ~1.8% of total population
--   Age 6-12 (primary):     ~12.5% of total population
--   Age 13-16 (O-level):    ~6.8% of total population
--   Age 17-18 (A-level):    ~3.2% of total population
-- These proportions should be updated when new census data is available.
-- =============================================================================

BEGIN;

-- ── Step 0a: Truncate and reload population staging ──────────────────────────
TRUNCATE TABLE stg.population_raw;

-- ── Step 0b: Insert 2025 district populations from UBOS Table 5 ──────────────
-- Values sourced from: Population Projections 2025-2050 Vol I, Table 5
-- Only 2025 data shown here as example — extend for other years as needed
-- Format: (district_name, year, sex, total_population)
-- Age breakdowns are computed from national proportions in Step 0c

INSERT INTO stg.population_raw
    (district_name, year, sex, total_population)
VALUES
-- Kampala (stored as 'KAMPALA CITY' in admin_unit_districts, id=30)
('KAMPALA CITY', 2025, 'M', 1020940),
('KAMPALA CITY', 2025, 'F', 810220),
('KAMPALA CITY', 2025, 'TOTAL', 1831160),
-- Buganda region
('Kalangala',     2025, 'M', 48260),  ('Kalangala',     2025, 'F', 31180),  ('Kalangala',     2025, 'TOTAL', 79440),
('Kiboga',        2025, 'M', 99760),  ('Kiboga',        2025, 'F', 91780),  ('Kiboga',        2025, 'TOTAL', 191540),
('Luweero',       2025, 'M', 312700), ('Luweero',       2025, 'F', 342310), ('Luweero',       2025, 'TOTAL', 655010),
('Masaka',        2025, 'M', 60530),  ('Masaka',        2025, 'F', 61890),  ('Masaka',        2025, 'TOTAL', 122420),
('Mpigi',         2025, 'M', 159340), ('Mpigi',         2025, 'F', 187050), ('Mpigi',         2025, 'TOTAL', 346390),
('Mubende',       2025, 'M', 265210), ('Mubende',       2025, 'F', 286110), ('Mubende',       2025, 'TOTAL', 551320),
('Mukono',        2025, 'M', 483590), ('Mukono',        2025, 'F', 510920), ('Mukono',        2025, 'TOTAL', 994510),
('Nakasongola',   2025, 'M', 122970), ('Nakasongola',   2025, 'F', 119530), ('Nakasongola',   2025, 'TOTAL', 242500),
('Rakai',         2025, 'M', 197140), ('Rakai',         2025, 'F', 154420), ('Rakai',         2025, 'TOTAL', 351560),
('Ssembabule',    2025, 'M', 165090), ('Ssembabule',    2025, 'F', 156780), ('Ssembabule',    2025, 'TOTAL', 321870),
('Kayunga',       2025, 'M', 219560), ('Kayunga',       2025, 'F', 242140), ('Kayunga',       2025, 'TOTAL', 461700),
('Wakiso',        2025, 'M', 1977220),('Wakiso',        2025, 'F', 1720380),('Wakiso',        2025, 'TOTAL', 3697600),
('Lyantonde',     2025, 'M', 75430),  ('Lyantonde',     2025, 'F', 63440),  ('Lyantonde',     2025, 'TOTAL', 138870),
('Mityana',       2025, 'M', 209010), ('Mityana',       2025, 'F', 223250), ('Mityana',       2025, 'TOTAL', 432260),
('Nakaseke',      2025, 'M', 132260), ('Nakaseke',      2025, 'F', 132390), ('Nakaseke',      2025, 'TOTAL', 264650),
('Buikwe',        2025, 'M', 260820), ('Buikwe',        2025, 'F', 285930), ('Buikwe',        2025, 'TOTAL', 546750),
('Bukomansimbi',  2025, 'M', 94480),  ('Bukomansimbi',  2025, 'F', 112280), ('Bukomansimbi',  2025, 'TOTAL', 206760),
('Butambala',     2025, 'M', 80280),  ('Butambala',     2025, 'F', 68930),  ('Butambala',     2025, 'TOTAL', 149210),
('Buvuma',        2025, 'M', 62810),  ('Buvuma',        2025, 'F', 56710),  ('Buvuma',        2025, 'TOTAL', 119520),
('Gomba',         2025, 'M', 104520), ('Gomba',         2025, 'F', 108300), ('Gomba',         2025, 'TOTAL', 212820),
('Kalungu',       2025, 'M', 108920), ('Kalungu',       2025, 'F', 127520), ('Kalungu',       2025, 'TOTAL', 236440),
('Kyankwanzi',    2025, 'M', 145020), ('Kyankwanzi',    2025, 'F', 154920), ('Kyankwanzi',    2025, 'TOTAL', 299940),
('Lwengo',        2025, 'M', 148940), ('Lwengo',        2025, 'F', 197460), ('Lwengo',        2025, 'TOTAL', 346400),
('Kyotera',       2025, 'M', 136740), ('Kyotera',       2025, 'F', 148920), ('Kyotera',       2025, 'TOTAL', 285660),
('Kassanda',      2025, 'M', 164530), ('Kassanda',      2025, 'F', 172740), ('Kassanda',      2025, 'TOTAL', 337270),
('Masaka City',   2025, 'M', 165380), ('Masaka City',   2025, 'F', 150370), ('Masaka City',   2025, 'TOTAL', 315750)
-- NOTE: Add all remaining districts from UBOS Table 5 following the same pattern
-- Full district list from the PDF: Busoga, Acholi, Bunyoro, Lango, West Nile,
-- Tooro, Karamoja, Kigezi, Ankole, Bugisu, Bukedi, Teso, Sebei, Madi regions
-- Use the values from the UBOS Population Projections 2025-2050 document
ON CONFLICT (district_name, year, sex) DO UPDATE
    SET total_population = EXCLUDED.total_population;


-- ── Step 0c: Compute age-specific breakdowns from national proportions ────────
-- Applies national age distribution proportions to district total populations.
-- Proportions derived from UBOS 2024 NPHC national age structure.
-- Update these proportions when new census data is available.
UPDATE stg.population_raw
SET
    -- Pre-primary age 3-5: ~5.5% of total
    pop_age_3_5   = ROUND(total_population * 0.055),
    -- Primary entry age 6: ~1.8% of total
    pop_age_6     = ROUND(total_population * 0.018),
    -- Primary age 6-12: ~12.5% of total
    pop_age_6_12  = ROUND(total_population * 0.125),
    -- Secondary O-level age 13-16: ~6.8% of total
    pop_age_13_16 = ROUND(total_population * 0.068),
    -- Secondary A-level age 17-18: ~3.2% of total
    pop_age_17_18 = ROUND(total_population * 0.032)
WHERE total_population IS NOT NULL;


-- ── Step 0d: Resolve district_source_id from admin_unit_districts ────────────
-- Links each district name in population_raw to the source system district ID.
-- This enables joining to dw.admin_units_dim in later steps.
UPDATE stg.population_raw pr
SET district_source_id = aud.id
FROM public.admin_unit_districts aud
WHERE UPPER(TRIM(pr.district_name)) = UPPER(TRIM(aud.name))
  AND aud.is_archived_yn = FALSE;

-- Report any unmatched districts
DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v
    FROM stg.population_raw
    WHERE district_source_id IS NULL
      AND year = 2025 AND sex = 'TOTAL';
    IF v > 0 THEN
        RAISE WARNING '% districts in population_raw could not be matched to admin_unit_districts. Check district name spelling.', v;
    END IF;
END; $$;

DO $$ DECLARE v BIGINT;
BEGIN
    SELECT COUNT(*) INTO v FROM stg.population_raw;
    RAISE NOTICE 'stg.population_raw: % rows loaded', v;
END; $$;

COMMIT;
