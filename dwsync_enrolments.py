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
        logging.FileHandler("dwsync_enrolments.log", encoding="utf-8")
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

# Only the tables that feed directly into stg.* staging tables.
# Lookup tables (setting_academic_years etc.) are joined inside the
# SQL ETL scripts and do not need to be staged separately.
TABLE_MAPPINGS = [
    # Large
    ("public.learners",           "stg.learners_raw",   100_000),
    # Largest
    ("public.learner_enrolments", "stg.enrolments_raw",  50_000),
]

# ETL SQL scripts — run in order after extract
ENROLMENT_ETL_SCRIPTS = [
    "sql/05_etl/enrolment/00_extract_enrolment_raw.sql",
    "sql/05_etl/enrolment/01_flatten_enrolments.sql",
    "sql/05_etl/enrolment/02_scd2_learner_dim.sql",
    "sql/05_etl/enrolment/03_load_enrolment_fact.sql",
    "sql/05_etl/enrolment/04_dq_checks_enrolments.sql",
]

INSERT_BATCH_SIZE = 10_000


def get_connection(config, autocommit=False):
    conn = psycopg2.connect(**config)
    conn.autocommit = autocommit
    return conn


def get_column_names(remote_conn, table):
    with remote_conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} LIMIT 0")
        return [desc[0] for desc in cur.description]


def get_row_count(remote_conn, table):
    with remote_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def fetch_and_insert(remote_conn, local_conn, remote_table, local_table, batch_size=None):
    fetch_size = batch_size or 100_000
    columns = get_column_names(remote_conn, remote_table)
    total   = get_row_count(remote_conn, remote_table)
    log.info(f"  Remote rows: {total:,}")

    with local_conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {local_table} RESTART IDENTITY CASCADE;")
    local_conn.commit()
    log.info(f"  Truncated {local_table}")

    cols  = ", ".join(columns)
    query = f"INSERT INTO {local_table} ({cols}) VALUES %s"

    inserted    = 0
    cursor_name = f"cur_{remote_table.replace('.','_')}_{datetime.now().strftime('%H%M%S')}"

    with remote_conn.cursor(name=cursor_name) as server_cur:
        server_cur.execute(f"SELECT * FROM {remote_table}")
        server_cur.itersize = fetch_size

        batch = []
        for row in server_cur:
            batch.append(row)
            if len(batch) >= INSERT_BATCH_SIZE:
                with local_conn.cursor() as cur:
                    execute_values(cur, query, batch)
                local_conn.commit()
                inserted += len(batch)
                log.info(f"  Inserted {inserted:,} / {total:,} rows into {local_table}")
                batch = []

        if batch:
            with local_conn.cursor() as cur:
                execute_values(cur, query, batch)
            local_conn.commit()
            inserted += len(batch)

    log.info(f"  Completed: {inserted:,} rows inserted into {local_table}")
    return inserted


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


def sync_tables():
    remote_conn = get_connection(REMOTE_DB)
    local_conn  = get_connection(LOCAL_DB, autocommit=False)

    try:
        for remote_table, local_table, batch_size in TABLE_MAPPINGS:
            log.info(f"Syncing {remote_table} → {local_table}")
            start = datetime.now()
            fetch_and_insert(remote_conn, local_conn, remote_table, local_table, batch_size)
            elapsed = (datetime.now() - start).total_seconds()
            log.info(f"  {remote_table} done in {elapsed:.0f}s")

        log.info("Extract step completed successfully.")

    except Exception as e:
        local_conn.rollback()
        log.error(f"Extract step FAILED: {e}")
        raise

    finally:
        remote_conn.close()
        local_conn.close()


def run_enrolment_etl():
    local_conn = get_connection(LOCAL_DB, autocommit=True)

    try:
        for script_path in ENROLMENT_ETL_SCRIPTS:
            script_name = os.path.basename(script_path)
            log.info(f"Running ETL script: {script_name}")
            start = datetime.now()
            run_sql_script(local_conn, script_path)
            elapsed = (datetime.now() - start).total_seconds()
            log.info(f"  {script_name} completed in {elapsed:.1f}s")

        log.info("Enrolment Mart ETL completed successfully.")

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
    log.info("EMIS DW Monthly Sync — Enrolments Mart")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        sync_tables()
        run_enrolment_etl()

        log.info("=" * 60)
        log.info("ALL STEPS COMPLETED SUCCESSFULLY")
        log.info("=" * 60)

    except Exception as e:
        log.error("=" * 60)
        log.error(f"SYNC FAILED: {e}")
        log.error("=" * 60)
        raise SystemExit(1)
