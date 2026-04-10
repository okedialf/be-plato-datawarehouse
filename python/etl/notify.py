"""
etl/notify.py
=============
Sends an email notification after each ETL run (success or failure).
Uses SMTP (configurable). Safe to disable by leaving NOTIFY_EMAIL blank.
"""

import logging
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict

logger = logging.getLogger(__name__)


def send_notification(
    settings,
    run_id: int,
    as_of_date: date,
    status: str,
    row_counts: dict,
    dq_results: List[Dict],
    error: str = None,
):
    """Send ETL run summary email. Silently skips if NOTIFY_EMAIL is not set."""
    if not settings.notify_email:
        logger.info("NOTIFY_EMAIL not set — skipping email notification")
        return

    recipients = [r.strip() for r in settings.notify_email.split(",") if r.strip()]
    subject = f"[EMIS DW] {'✅' if status == 'SUCCESS' else '❌'} Nightly ETL {status} — {as_of_date}"

    body_lines = [
        f"<h2>EMIS Data Warehouse — Nightly ETL Report</h2>",
        f"<p><b>Run ID:</b> {run_id} &nbsp; <b>Date:</b> {as_of_date} &nbsp; "
        f"<b>Status:</b> <span style='color:{'green' if status=='SUCCESS' else 'red'}'>{status}</span></p>",
        "<h3>Row Counts</h3><table border='1' cellpadding='4' cellspacing='0'>",
        "<tr><th>Step</th><th>Rows</th></tr>",
    ]
    for key, val in row_counts.items():
        body_lines.append(f"<tr><td>{key}</td><td>{val}</td></tr>")
    body_lines.append("</table>")

    if dq_results:
        body_lines.append("<h3>Data Quality Checks</h3><table border='1' cellpadding='4' cellspacing='0'>")
        body_lines.append("<tr><th>Check</th><th>Status</th><th>Detail</th></tr>")
        for dq in dq_results:
            colour = {"PASS": "green", "WARN": "orange", "FAIL": "red"}.get(dq["status"], "black")
            body_lines.append(
                f"<tr><td>{dq['check_name']}</td>"
                f"<td style='color:{colour}'><b>{dq['status']}</b></td>"
                f"<td>{dq['detail']}</td></tr>"
            )
        body_lines.append("</table>")

    if error:
        body_lines.append(f"<h3>Error</h3><pre style='color:red'>{error}</pre>")

    html_body = "\n".join(body_lines)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = settings.smtp_user
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_password)
            server.sendmail(settings.smtp_user, recipients, msg.as_string())
        logger.info("Notification sent to %s", recipients)
    except Exception as exc:
        logger.warning("Failed to send email notification: %s", exc)
