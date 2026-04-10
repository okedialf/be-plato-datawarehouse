"""
config/settings.py
==================
Loads configuration from:
  1. GCP Secret Manager (when running in production on GCP)
  2. .env file (local development)

All database credentials are kept out of source code.
"""

import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Settings:
    # ── Data Warehouse (Cloud SQL PostgreSQL) ──────────────────────────────
    dw_host:      str
    dw_port:      int
    dw_name:      str
    dw_user:      str
    dw_password:  str
    dw_sslmode:   str

    # ── OLTP Source Database ───────────────────────────────────────────────
    oltp_host:     str
    oltp_port:     int
    oltp_name:     str
    oltp_user:     str
    oltp_password: str
    oltp_sslmode:  str

    # ── GCP Project ────────────────────────────────────────────────────────
    gcp_project_id: str

    # ── Notifications ─────────────────────────────────────────────────────
    notify_email:    str   # comma-separated recipient list
    smtp_host:       str
    smtp_port:       int
    smtp_user:       str
    smtp_password:   str


def _secret(name: str, project_id: str) -> str:
    """Fetch a secret from GCP Secret Manager."""
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        resource = f"projects/{project_id}/secrets/{name}/versions/latest"
        response = client.access_secret_version(request={"name": resource})
        return response.payload.data.decode("utf-8").strip()
    except Exception as exc:
        logger.warning("Could not fetch secret '%s' from Secret Manager: %s", name, exc)
        return ""


def _env_or_secret(env_key: str, secret_name: str, project_id: str,
                   default: str = "", use_gcp: bool = False) -> str:
    """
    Priority:
      1. Environment variable (always wins — useful for Cloud Run, local override)
      2. GCP Secret Manager (production)
      3. default
    """
    val = os.environ.get(env_key, "")
    if val:
        return val
    if use_gcp and project_id:
        val = _secret(secret_name, project_id)
        if val:
            return val
    return default


def get_settings() -> Settings:
    """
    Build and return the Settings object.

    For local development:
        Copy config/.env.example → config/.env and fill in values.
        The project root Makefile (or you manually) runs:
            export $(cat config/.env | xargs)
        before calling python etl_runner.py.

    For GCP production:
        Set ENV=production and GCP_PROJECT_ID.
        All secrets are pulled from Secret Manager automatically.
    """
    # Load .env file if present (local dev convenience)
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        logger.info("Loading .env from %s", env_file)
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    os.environ.setdefault(key.strip(), val.strip())

    use_gcp = os.environ.get("ENV", "local").lower() == "production"
    project_id = os.environ.get("GCP_PROJECT_ID", "")

    def get(env_key, secret_name=None, default=""):
        return _env_or_secret(
            env_key,
            secret_name or env_key.lower().replace("_", "-"),
            project_id,
            default=default,
            use_gcp=use_gcp,
        )

    return Settings(
        # DW
        dw_host      = get("DW_HOST",      "dw-host"),
        dw_port      = int(get("DW_PORT",  "dw-port",     "5432")),
        dw_name      = get("DW_NAME",      "dw-name"),
        dw_user      = get("DW_USER",      "dw-user"),
        dw_password  = get("DW_PASSWORD",  "dw-password"),
        dw_sslmode   = get("DW_SSLMODE",   default="require"),

        # OLTP
        oltp_host     = get("OLTP_HOST",     "oltp-host"),
        oltp_port     = int(get("OLTP_PORT", "oltp-port",   "5432")),
        oltp_name     = get("OLTP_NAME",     "oltp-name"),
        oltp_user     = get("OLTP_USER",     "oltp-user"),
        oltp_password = get("OLTP_PASSWORD", "oltp-password"),
        oltp_sslmode  = get("OLTP_SSLMODE",  default="require"),

        # GCP
        gcp_project_id = project_id,

        # Notifications
        notify_email  = get("NOTIFY_EMAIL",  default=""),
        smtp_host     = get("SMTP_HOST",     default="smtp.gmail.com"),
        smtp_port     = int(get("SMTP_PORT", default="587")),
        smtp_user     = get("SMTP_USER",     "smtp-user"),
        smtp_password = get("SMTP_PASSWORD", "smtp-password"),
    )
