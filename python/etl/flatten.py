"""
etl/flatten.py  —  Runs the SQL flatten script via psycopg2.
"""

import logging
import os

logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "../../sql/05_etl/01_flatten_schools.sql")


def flatten_schools(dw_conn, run_id: int) -> int:
    """Execute the flatten SQL and return the row count in stg.schools_flat."""
    with open(SQL_PATH) as f:
        sql = f.read()

    with dw_conn.cursor() as cur:
        cur.execute(sql)
        # Count rows in the flat table after load
        cur.execute("SELECT COUNT(*) FROM stg.schools_flat")
        count = cur.fetchone()[0]
    dw_conn.commit()

    logger.info("schools_flat populated with %d rows", count)
    return count
