import os
import psycopg2
import logging
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dwsync_teachers.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

REMOTE_DB = {
    "host":     os.getenv("REMOTE_DB_HOST"),
    "port":     os.getenv("REMOTE_DB_PORT"),
    "dbname":   os.getenv("REMOTE_DB_NAME"),
    "user":     os.getenv("REMOTE_DB_USER"),
    "password": os.getenv("REMOTE_DB_PASSWORD")
}

LOCAL_DB = {
    "host":     os.getenv("LOCAL_DB_HOST"),
    "port":     os.getenv("LOCAL_DB_PORT"),
    "dbname":   os.getenv("LOCAL_DB_NAME"),
    "user":     os.getenv("LOCAL_DB_USER"),
    "password": os.getenv("LOCAL_DB_PASSWORD")
}

# No Python extraction — all handled by SQL ETL scripts as tested.
TABLE_MAPPINGS = []

TEACHERS_ETL_SCRIPTS = [
    "sql/05_etl/teachers/00_extract_teachers_raw.sql",
    "sql/05_etl/teachers/01_flatten_teachers.sql",
    "sql/05_etl/teachers/02_scd2_teacher_dim.sql",
    "sql/05_etl/teachers/03_load_hr_fact.sql",
    "sql/05_etl/teachers/04_dq_checks_teachers.sql",
]


def get_connection(config, autocommit=False):
    conn = psycopg2.connect(**config)
    conn.autocommit = autocommit
    return conn


def run_sql_script(conn, script_path):
    abs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"SQL script not found: {abs_path}")
    with open(abs_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
        if conn.notices:
            for notice in conn.notices:
                log.info(f"  DB NOTICE: {notice.strip()}")
            conn.notices.clear()


def run_teachers_etl():
    local_conn = get_connection(LOCAL_DB, autocommit=True)
    try:
        for script_path in TEACHERS_ETL_SCRIPTS:
            script_name = os.path.basename(script_path)
            log.info(f"Running ETL script: {script_name}")
            start = datetime.now()
            run_sql_script(local_conn, script_path)
            elapsed = (datetime.now() - start).total_seconds()
            log.info(f"  {script_name} completed in {elapsed:.1f}s")
        log.info("Teachers Mart ETL completed successfully.")
    except FileNotFoundError as e:
        log.error(str(e))
        raise
    except Exception as e:
        log.error(f"ETL script FAILED: {e}")
        raise
    finally:
        local_conn.close()


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("EMIS DW Monthly Sync — Teachers Mart")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        run_teachers_etl()

        log.info("=" * 60)
        log.info("ALL STEPS COMPLETED SUCCESSFULLY")
        log.info("=" * 60)

    except Exception as e:
        log.error("=" * 60)
        log.error(f"SYNC FAILED: {e}")
        log.error("=" * 60)
        raise SystemExit(1)
