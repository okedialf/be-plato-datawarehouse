"""
etl/load_fact.py  —  Loads dw.school_fact daily snapshot.
"""

import logging
import os
from datetime import date

logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "../../sql/05_etl/03_load_school_fact.sql")


def load_school_fact(dw_conn, as_of_date: date, run_id: int) -> int:
    """
    Inserts one snapshot row per current school for as_of_date.
    Re-runnable: ON CONFLICT DO NOTHING prevents duplicates.
    Returns number of rows inserted.
    """
    with open(SQL_PATH) as f:
        sql = f.read()

    with dw_conn.cursor() as cur:
        cur.execute(sql)
        rows_inserted = cur.rowcount
    dw_conn.commit()

    logger.info("school_fact: inserted %d snapshot rows for %s", rows_inserted, as_of_date)
    return rows_inserted
