import os
import psycopg2
import logging
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dwsync.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# --- CONFIGURATION FROM ENV ---

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

# Mapping: remote_table -> local_table (extract step)
TABLE_MAPPINGS = {
    "public.schools": "stg.schools_raw",
}

# Schools Mart ETL steps — run in this exact order after extract.
# Paths are relative to this script's location.
SCHOOLS_ETL_SCRIPTS = [
    "sql/05_etl/schools/01_flatten_schools.sql",
    "sql/05_etl/schools/02_scd2_schools_dim.sql",
    "sql/05_etl/schools/04_scd2_admin_units_dim.sql",
    "sql/05_etl/schools/03_load_school_fact.sql",
    "sql/05_etl/schools/04_dq_checks_schools.sql",
]


# --- FUNCTIONS ---

def get_connection(config, autocommit=False):
    conn = psycopg2.connect(**config)
    conn.autocommit = autocommit
    return conn


def fetch_remote_data(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT row_to_json(t) FROM {table} t")

        colnames = None
        rows     = []
        skipped  = 0

        for (json_row,) in cur:
            try:
                values = list(json_row.values())
                if colnames is None:
                    colnames = list(json_row.keys())
                rows.append(tuple(values))
            except Exception:
                skipped += 1

        if skipped:
            log.warning(f"Skipped {skipped} bad rows from {table}")

    return colnames, rows


def truncate_local_table(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")


def insert_data(conn, table, columns, rows):
    if not rows:
        log.warning(f"No data fetched for {table} — insert skipped.")
        return

    cols  = ", ".join(columns)
    query = f"INSERT INTO {table} ({cols}) VALUES %s"

    with conn.cursor() as cur:
        execute_values(cur, query, rows)


def run_sql_script(conn, script_path):
    """
    Read a .sql file and execute it using autocommit=True so that
    the BEGIN/COMMIT statements inside the SQL file are fully in control.
    This avoids the 'transaction already in progress' warning and ensures
    SCD2 logic works correctly.
    """
    abs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_path)

    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"SQL script not found: {abs_path}")

    with open(abs_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)

        # Print any NOTICE messages raised by the SQL (e.g. row counts)
        if conn.notices:
            for notice in conn.notices:
                log.info(f"  DB NOTICE: {notice.strip()}")
            conn.notices.clear()


def sync_tables():
    """Step 1: Extract — pull data from EMIS read-only replica into staging."""
    remote_conn = get_connection(REMOTE_DB)
    local_conn  = get_connection(LOCAL_DB, autocommit=False)

    try:
        for remote_table, local_table in TABLE_MAPPINGS.items():
            log.info(f"Syncing {remote_table} → {local_table}")

            columns, rows = fetch_remote_data(remote_conn, remote_table)
            log.info(f"  Fetched {len(rows):,} rows from {remote_table}")

            truncate_local_table(local_conn, local_table)
            log.info(f"  Truncated {local_table}")

            insert_data(local_conn, local_table, columns, rows)
            log.info(f"  Inserted {len(rows):,} rows into {local_table}")

        local_conn.commit()
        log.info("Extract step completed successfully.")

    except Exception as e:
        local_conn.rollback()
        log.error(f"Extract step FAILED: {e}")
        raise

    finally:
        remote_conn.close()
        local_conn.close()


def run_schools_etl():
    """
    Step 2: Transform & Load — run the Schools Mart ETL SQL scripts.
    Uses autocommit=True so the SQL scripts' own BEGIN/COMMIT are in control.
    """
    local_conn = get_connection(LOCAL_DB, autocommit=True)

    try:
        for script_path in SCHOOLS_ETL_SCRIPTS:
            script_name = os.path.basename(script_path)
            log.info(f"Running ETL script: {script_name}")
            start = datetime.now()

            run_sql_script(local_conn, script_path)

            elapsed = (datetime.now() - start).total_seconds()
            log.info(f"  {script_name} completed in {elapsed:.1f}s")

        log.info("Schools Mart ETL completed successfully.")

    except FileNotFoundError as e:
        log.error(str(e))
        raise

    except Exception as e:
        log.error(f"ETL script FAILED: {e}")
        raise

    finally:
        local_conn.close()


# --- MAIN ---

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("EMIS DW Nightly Sync — Schools Mart")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        sync_tables()
        run_schools_etl()

        log.info("=" * 60)
        log.info("ALL STEPS COMPLETED SUCCESSFULLY")
        log.info("=" * 60)

    except Exception as e:
        log.error("=" * 60)
        log.error(f"SYNC FAILED: {e}")
        log.error("=" * 60)
        raise SystemExit(1)
