"""
etl/dq_checks.py
================
Data quality checks run after each ETL cycle.
Each check writes a row to dw_audit.dq_check_log and returns its result.
"""

import logging
from datetime import date
from typing import List, Dict

logger = logging.getLogger(__name__)


def _log_check(conn, run_id: int, check_name: str, status: str, detail: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw_audit.dq_check_log (run_id, check_name, status, detail)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, check_name, status, detail),
        )
    conn.commit()


def run_dq_checks(conn, as_of_date: date, run_id: int) -> List[Dict]:
    results = []

    checks = [
        _check_schools_raw_not_empty,
        _check_schools_flat_not_empty,
        _check_null_emis_numbers,
        _check_no_orphan_current_schools,
        _check_fact_rows_today,
        _check_duplicate_fact_rows,
        _check_schools_dim_all_have_current,
    ]

    for check_fn in checks:
        result = check_fn(conn, run_id, as_of_date)
        results.append(result)
        level = logging.WARNING if result["status"] in ("FAIL", "WARN") else logging.INFO
        logger.log(level, "DQ [%s] %s — %s", result["status"], result["check_name"], result["detail"])

    return results


# ── Individual checks ─────────────────────────────────────────────────────────

def _check_schools_raw_not_empty(conn, run_id, as_of_date):
    name = "schools_raw_not_empty"
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg.schools_raw")
        n = cur.fetchone()[0]
    status = "PASS" if n > 0 else "FAIL"
    detail = f"{n} rows in stg.schools_raw"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}


def _check_schools_flat_not_empty(conn, run_id, as_of_date):
    name = "schools_flat_not_empty"
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg.schools_flat")
        n = cur.fetchone()[0]
    status = "PASS" if n > 0 else "FAIL"
    detail = f"{n} rows in stg.schools_flat"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}


def _check_null_emis_numbers(conn, run_id, as_of_date):
    name = "null_emis_numbers_in_dim"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM dw.schools_dim WHERE emis_number IS NULL AND is_current = TRUE"
        )
        n = cur.fetchone()[0]
    status = "WARN" if n > 0 else "PASS"
    detail = f"{n} current schools_dim rows have NULL emis_number"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}


def _check_no_orphan_current_schools(conn, run_id, as_of_date):
    name = "no_orphan_admin_unit_id"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dw.schools_dim sd
            WHERE sd.is_current = TRUE
              AND sd.admin_unit_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM dw.admin_units_dim aud
                  WHERE aud.id = sd.admin_unit_id
              )
            """
        )
        n = cur.fetchone()[0]
    status = "FAIL" if n > 0 else "PASS"
    detail = f"{n} current schools_dim rows reference a missing admin_units_dim id"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}


def _check_fact_rows_today(conn, run_id, as_of_date):
    name = "fact_rows_loaded_today"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dw.school_fact sf
            JOIN dw.date_dim dd ON dd.id = sf.date_id
            WHERE dd.system_date = %s
            """,
            (as_of_date,),
        )
        n = cur.fetchone()[0]
    status = "PASS" if n > 0 else "FAIL"
    detail = f"{n} school_fact rows for {as_of_date}"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}


def _check_duplicate_fact_rows(conn, run_id, as_of_date):
    name = "no_duplicate_fact_rows"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT school_id, date_id, COUNT(*) AS cnt
                FROM dw.school_fact
                GROUP BY school_id, date_id
                HAVING COUNT(*) > 1
            ) dups
            """
        )
        n = cur.fetchone()[0]
    status = "FAIL" if n > 0 else "PASS"
    detail = f"{n} duplicate (school_id, date_id) combinations in school_fact"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}


def _check_schools_dim_all_have_current(conn, run_id, as_of_date):
    name = "all_staged_schools_have_current_dim_row"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM stg.schools_flat sf
            WHERE NOT EXISTS (
                SELECT 1 FROM dw.schools_dim sd
                WHERE sd.source_id = sf.source_id
                  AND sd.is_current = TRUE
            )
            """
        )
        n = cur.fetchone()[0]
    status = "FAIL" if n > 0 else "PASS"
    detail = f"{n} staged schools have no current row in dw.schools_dim"
    _log_check(conn, run_id, name, status, detail)
    return {"check_name": name, "status": status, "detail": detail}
