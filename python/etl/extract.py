"""
etl/extract.py
==============
Extracts data from the OLTP source database into staging tables on the DW.
Full refresh strategy: TRUNCATE then INSERT via server-side COPY for speed.
"""

import logging
from io import StringIO

import psycopg2

logger = logging.getLogger(__name__)


def _truncate(dw_conn, table: str):
    with dw_conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY")
    dw_conn.commit()
    logger.info("Truncated %s", table)


def extract_schools_raw(dw_conn, oltp_conn, run_id: int) -> int:
    """
    Full refresh of stg.schools_raw from public.schools on OLTP.
    Uses server-side COPY for efficiency (~60k rows).
    Returns row count inserted.
    """
    _truncate(dw_conn, "stg.schools_raw")

    # Stream rows from OLTP via COPY TO STDOUT
    copy_out_sql = """
        COPY (
            SELECT
                id, school_type_id, logo, name, old_emis_number, emis_number,
                is_government_owned_yn, school_ownership_status_id, legal_ownership_status_id,
                ownership_category_id, license_number, registration_number,
                registration_certificate_status, registration_status_id,
                licence_certificate_expiry_date, registration_certificate_expiry_date,
                has_license_or_registration_certificate, is_certificate_system_generated,
                public_service_school_code, is_public_school, supply_number,
                physical_address, postal_address,
                district_id, county_id, sub_county_id, parish_id, village_id,
                region_id, local_government_id,
                latitude, longitude, email, phone, website,
                has_health_facility_yn, health_facility_distance_range_id,
                self_examines_yn, examining_body_id, center_number,
                has_male_students, has_female_students,
                founding_body_id, year_founded, funding_source_id,
                school_land_area, land_owner_type_id, is_operational_yn,
                capital_for_establishment, school_closure_reason,
                is_commissioned, date_commissioned, date_created, date_updated,
                operational_status_id, suspension_reason_id, admin_unit_id
            FROM public.schools
        ) TO STDOUT WITH (FORMAT CSV, HEADER FALSE, NULL '')
    """

    copy_in_sql = """
        COPY stg.schools_raw (
            id, school_type_id, logo, name, old_emis_number, emis_number,
            is_government_owned_yn, school_ownership_status_id, legal_ownership_status_id,
            ownership_category_id, license_number, registration_number,
            registration_certificate_status, registration_status_id,
            licence_certificate_expiry_date, registration_certificate_expiry_date,
            has_license_or_registration_certificate, is_certificate_system_generated,
            public_service_school_code, is_public_school, supply_number,
            physical_address, postal_address,
            district_id, county_id, sub_county_id, parish_id, village_id,
            region_id, local_government_id,
            latitude, longitude, email, phone, website,
            has_health_facility_yn, health_facility_distance_range_id,
            self_examines_yn, examining_body_id, center_number,
            has_male_students, has_female_students,
            founding_body_id, year_founded, funding_source_id,
            school_land_area, land_owner_type_id, is_operational_yn,
            capital_for_establishment, school_closure_reason,
            is_commissioned, date_commissioned, date_created, date_updated,
            operational_status_id, suspension_reason_id, admin_unit_id
        ) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
    """

    buffer = StringIO()

    with oltp_conn.cursor() as oltp_cur:
        oltp_cur.copy_expert(copy_out_sql, buffer)

    buffer.seek(0)
    row_count = buffer.read().count("\n")
    buffer.seek(0)

    with dw_conn.cursor() as dw_cur:
        dw_cur.copy_expert(copy_in_sql, buffer)
    dw_conn.commit()

    logger.info("Extracted %d rows → stg.schools_raw", row_count)
    return row_count


def extract_school_location_details_raw(dw_conn, oltp_conn, run_id: int) -> int:
    """
    Full refresh of stg.school_location_details_raw.
    Returns row count inserted.
    """
    _truncate(dw_conn, "stg.school_location_details_raw")

    copy_out_sql = """
        COPY (
            SELECT
                id, school_id, physical_address, postal_address, admin_unit_id,
                latitude, longitude, effective_date, expiration_date, is_current, date_created
            FROM public.school_location_details
        ) TO STDOUT WITH (FORMAT CSV, HEADER FALSE, NULL '')
    """

    copy_in_sql = """
        COPY stg.school_location_details_raw (
            id, school_id, physical_address, postal_address, admin_unit_id,
            latitude, longitude, effective_date, expiration_date, is_current, date_created
        ) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
    """

    buffer = StringIO()
    with oltp_conn.cursor() as cur:
        cur.copy_expert(copy_out_sql, buffer)

    buffer.seek(0)
    row_count = buffer.read().count("\n")
    buffer.seek(0)

    with dw_conn.cursor() as cur:
        cur.copy_expert(copy_in_sql, buffer)
    dw_conn.commit()

    logger.info("Extracted %d rows → stg.school_location_details_raw", row_count)
    return row_count
