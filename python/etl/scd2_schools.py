"""
etl/scd2_schools.py  —  Runs the SCD2 SQL for dw.schools_dim.
Returns a dict of row counts for audit.
"""

import logging
import os
from datetime import date

logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "../../sql/05_etl/02_scd2_schools_dim.sql")


def run_scd2_schools_dim(dw_conn, as_of_date: date, run_id: int) -> dict:
    """
    Runs SCD2 upsert for dw.schools_dim.
    Returns counts: expired rows, inserted rows, total current rows.
    """
    # Count before
    with dw_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dw.schools_dim WHERE is_current = TRUE")
        count_before = cur.fetchone()[0]

    # Run SCD2 SQL
    with open(SQL_PATH) as f:
        sql = f.read()

    with dw_conn.cursor() as cur:
        cur.execute(sql)
    dw_conn.commit()

    # Count after
    with dw_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dw.schools_dim WHERE is_current = TRUE")
        count_after = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM dw.schools_dim WHERE expiration_date = %s AND is_current = FALSE",
            (as_of_date,),
        )
        expired = cur.fetchone()[0]

    inserted = count_after - (count_before - expired)

    logger.info(
        "SCD2 schools_dim: expired=%d  inserted=%d  total_current=%d",
        expired, inserted, count_after,
    )
    return {
        "schools_dim_expired":       expired,
        "schools_dim_inserted":      max(inserted, 0),
        "schools_dim_total_current": count_after,
    }
