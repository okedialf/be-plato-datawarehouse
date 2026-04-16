-- =============================================================================
-- EMIS Data Warehouse: Populate Date Dimension
-- Run ONCE after create_dw_tables.sql.
-- Covers 2022-01-01 through 2037-12-31 (16 years — 2022 needed for historical enrolment data).
-- Uganda Fiscal Year runs July-June (e.g., Jul 2025-Jun 2026 = FY2026).
--
-- Prerequisites:
--   - dw.date_dim must exist (run create_dw_tables.sql first)
--   - UNIQUE constraint on system_date must exist
-- =============================================================================

INSERT INTO dw.date_dim (system_date, day, month, month_name, short_month, year, "FY", "Quarter")
SELECT
    d::DATE                                             AS system_date,
    EXTRACT(DAY   FROM d)::INTEGER                      AS day,
    EXTRACT(MONTH FROM d)::INTEGER                      AS month,
    TO_CHAR(d, 'Month')                                 AS month_name,
    TO_CHAR(d, 'MON')                                   AS short_month,
    EXTRACT(YEAR  FROM d)::INTEGER                      AS year,
    -- Uganda FY: Jul-Jun. If month >= 7, FY = year+1, else FY = year.
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 7
        THEN EXTRACT(YEAR FROM d)::INTEGER + 1
        ELSE EXTRACT(YEAR FROM d)::INTEGER
    END                                                 AS "FY",
    -- Fiscal quarters (Q1=Jul-Sep, Q2=Oct-Dec, Q3=Jan-Mar, Q4=Apr-Jun)
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (7,  8,  9)  THEN 'Q1'
        WHEN EXTRACT(MONTH FROM d) IN (10, 11, 12) THEN 'Q2'
        WHEN EXTRACT(MONTH FROM d) IN (1,  2,  3)  THEN 'Q3'
        WHEN EXTRACT(MONTH FROM d) IN (4,  5,  6)  THEN 'Q4'
    END                                                 AS "Quarter"
FROM
    GENERATE_SERIES('2022-01-01'::DATE, '2037-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (system_date) DO NOTHING;

-- Verify
SELECT COUNT(*) AS total_days,
       MIN(system_date) AS first_date,
       MAX(system_date) AS last_date
FROM dw.date_dim;
