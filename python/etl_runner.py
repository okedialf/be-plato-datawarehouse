#!/usr/bin/env python3
"""
EMIS Data Warehouse — Nightly Schools ETL Pipeline
===================================================
Orchestrates the full nightly extract → stage → transform → SCD2 → fact cycle.

Usage:
    python etl_runner.py
    python etl_runner.py --run-date 2026-02-10   # backfill a specific date

Environment:
    Reads from .env (local dev) or GCP Secret Manager (production).
    See config/settings.py and README.md.
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from config.settings import get_settings
from etl.extract import extract_schools_raw, extract_school_location_details_raw
from etl.flatten import flatten_schools
from etl.scd2_schools import run_scd2_schools_dim
from etl.load_fact import load_school_fact
from etl.dq_checks import run_dq_checks
from etl.notify import send_notification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("emis_etl")

PIPELINE_NAME = "nightly_schools_etl"


def parse_args():
    parser = argparse.ArgumentParser(description="EMIS Schools ETL pipeline")
    parser.add_argument(
        "--run-date",
        type=str,
        default=str(date.today()),
        help="Business date to process (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


def get_connection(settings):
    """Open and return a psycopg2 connection to the DW (Cloud SQL)."""
    return psycopg2.connect(
        host=settings.dw_host,
        port=settings.dw_port,
        dbname=settings.dw_name,
        user=settings.dw_user,
        password=settings.dw_password,
        sslmode=settings.dw_sslmode,
        connect_timeout=30,
    )


def get_oltp_connection(settings):
    """Open and return a psycopg2 connection to the OLTP source database."""
    return psycopg2.connect(
        host=settings.oltp_host,
        port=settings.oltp_port,
        dbname=settings.oltp_name,
        user=settings.oltp_user,
        password=settings.oltp_password,
        sslmode=settings.oltp_sslmode,
        connect_timeout=30,
        options="-c default_transaction_read_only=on",  # read-only safety
    )


def start_run(conn, as_of_date: date) -> int:
    """Insert an etl_run row and return the new run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw_audit.etl_run (pipeline, as_of_date, start_ts, status)
            VALUES (%s, %s, NOW(), 'RUNNING')
            RETURNING run_id
            """,
            (PIPELINE_NAME, as_of_date),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    logger.info("ETL run started — run_id=%d  as_of_date=%s", run_id, as_of_date)
    return run_id


def finish_run(conn, run_id: int, status: str, row_counts: dict, error: str = None):
    """Mark the etl_run row as SUCCESS or FAILED."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dw_audit.etl_run
            SET end_ts          = NOW(),
                status          = %s,
                row_counts_json = %s,
                error_message   = %s
            WHERE run_id = %s
            """,
            (status, json.dumps(row_counts), error, run_id),
        )
    conn.commit()
    logger.info("ETL run finished — run_id=%d  status=%s", run_id, status)


def log_step(conn, run_id: int, step_name: str, status: str,
             rows_affected: int = None, message: str = None):
    """Upsert a step log row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw_audit.etl_step_log
                (run_id, step_name, status, rows_affected, message, end_ts)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (run_id, step_name, status, rows_affected, message),
        )
    conn.commit()


def main():
    args = parse_args()
    as_of_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()

    settings = get_settings()
    row_counts = {}

    # ── Open connections ──────────────────────────────────────────────────────
    try:
        dw_conn   = get_connection(settings)
        oltp_conn = get_oltp_connection(settings)
    except Exception as exc:
        logger.critical("Cannot connect to database: %s", exc)
        sys.exit(1)

    run_id = start_run(dw_conn, as_of_date)

    try:
        # ── Step 1: Extract schools_raw ───────────────────────────────────────
        logger.info("Step 1: Extracting stg.schools_raw …")
        n = extract_schools_raw(dw_conn, oltp_conn, run_id)
        row_counts["schools_raw"] = n
        log_step(dw_conn, run_id, "extract_schools_raw", "SUCCESS", n)

        # ── Step 2: Extract school_location_details_raw ───────────────────────
        logger.info("Step 2: Extracting stg.school_location_details_raw …")
        n = extract_school_location_details_raw(dw_conn, oltp_conn, run_id)
        row_counts["school_location_details_raw"] = n
        log_step(dw_conn, run_id, "extract_location_raw", "SUCCESS", n)

        # ── Step 3: Flatten schools ───────────────────────────────────────────
        logger.info("Step 3: Flattening stg.schools_flat …")
        n = flatten_schools(dw_conn, run_id)
        row_counts["schools_flat"] = n
        log_step(dw_conn, run_id, "flatten_schools", "SUCCESS", n)

        # ── Step 4: SCD2 load → dw.schools_dim ───────────────────────────────
        logger.info("Step 4: SCD2 loading dw.schools_dim …")
        scd2_counts = run_scd2_schools_dim(dw_conn, as_of_date, run_id)
        row_counts.update(scd2_counts)
        log_step(dw_conn, run_id, "scd2_schools_dim", "SUCCESS",
                 scd2_counts.get("schools_dim_inserted", 0))

        # ── Step 5: Load school_fact (daily snapshot) ─────────────────────────
        logger.info("Step 5: Loading dw.school_fact …")
        n = load_school_fact(dw_conn, as_of_date, run_id)
        row_counts["school_fact_inserted"] = n
        log_step(dw_conn, run_id, "load_school_fact", "SUCCESS", n)

        # ── Step 6: Data quality checks ───────────────────────────────────────
        logger.info("Step 6: Running DQ checks …")
        dq_results = run_dq_checks(dw_conn, as_of_date, run_id)
        row_counts["dq_checks"] = len(dq_results)
        dq_failed = [r for r in dq_results if r["status"] == "FAIL"]
        if dq_failed:
            logger.warning("DQ FAILURES: %s", dq_failed)

        # ── Mark run SUCCESS ──────────────────────────────────────────────────
        finish_run(dw_conn, run_id, "SUCCESS", row_counts)
        send_notification(settings, run_id, as_of_date, "SUCCESS", row_counts, dq_results)

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        finish_run(dw_conn, run_id, "FAILED", row_counts, str(exc))
        send_notification(settings, run_id, as_of_date, "FAILED", row_counts, [], error=str(exc))
        dw_conn.close()
        oltp_conn.close()
        sys.exit(1)

    dw_conn.close()
    oltp_conn.close()
    logger.info("Pipeline complete. Row counts: %s", json.dumps(row_counts))


if __name__ == "__main__":
    main()
