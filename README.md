# EMIS Data Warehouse — Schools Data Mart

> **Platform:** Google Cloud SQL for PostgreSQL  
> **Schemas:** `stg` (staging) · `dw` (data warehouse) · `dw_audit` (audit & control)  
> **ETL:** Python + psycopg2 · nightly cron · GCP Secret Manager

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Repository Structure](#2-repository-structure)
3. [Data Model](#3-data-model)
4. [ETL Process Flow](#4-etl-process-flow)
5. [One-Time Setup](#5-one-time-setup)
6. [Nightly Automation](#6-nightly-automation)
7. [GCP Secret Manager](#7-gcp-secret-manager)
8. [Running Locally](#8-running-locally)
9. [Data Quality Checks](#9-data-quality-checks)
10. [Troubleshooting & Audit Queries](#10-troubleshooting--audit-queries)
11. [Extending the Project](#11-extending-the-project)

---

## 1. Architecture Overview

```
┌─────────────────────┐        nightly COPY        ┌──────────────────────────────────────┐
│   OLTP Database     │ ─────────────────────────► │   GCP Cloud SQL (PostgreSQL)          │
│  (public schema)    │    read-only ETL user        │                                      │
│                     │    port 5432 / TLS           │  ┌────────┐  ┌────┐  ┌───────────┐  │
│  public.schools     │                              │  │  stg   │  │ dw │  │ dw_audit  │  │
│  public.school_     │                              │  └────────┘  └────┘  └───────────┘  │
│    location_details │                              └──────────────────────────────────────┘
│  admin_units.*      │                                           │
└─────────────────────┘                                           │ nightly cron (01:00 UTC)
                                                                  ▼
                                                        python etl_runner.py
```

**Key design decisions:**

| Decision | Choice | Reason |
|---|---|---|
| Extract method | Full refresh via COPY | 60k schools is small; simple and reliable |
| SCD Type | SCD Type 2 | Preserve full history of school attribute changes |
| Staging strategy | Truncate + reload nightly | Predictable, easy to re-run |
| Fact grain | 1 row per school per day | Enables COUNT(*) reports at any date |
| Secrets | GCP Secret Manager | No credentials in source code or config files |
| Orchestration | Python + cron | Fastest go-live; upgradeable to Airflow later |

---

## 2. Repository Structure

```
emis-dw/
├── sql/
│   ├── 01_schemas/
│   │   └── create_schemas.sql              # Creates stg, dw, dw_audit schemas
│   ├── 02_staging/
│   │   └── create_staging_tables.sql       # All 4 staging tables
│   ├── 03_dw/
│   │   ├── create_dw_tables.sql            # All DW dimensions + fact table
│   │   └── populate_date_dim.sql           # Pre-populate date_dim 2010–2040
│   ├── 04_audit/
│   │   └── create_audit_tables.sql         # ETL run/step/error/DQ log tables
│   └── 05_etl/
│       ├── 01_flatten_schools.sql          # Stage: decode raw → flat
│       ├── 02_scd2_schools_dim.sql         # DW: SCD2 upsert for schools_dim
│       ├── 03_load_school_fact.sql         # DW: daily snapshot fact load
│       └── 04_scd2_admin_units_dim.sql     # DW: SCD2 for admin_units_dim (manual)
│
├── python/
│   ├── etl_runner.py                       # Main ETL orchestrator (run this)
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py                     # Loads secrets from env / GCP Secret Manager
│   └── etl/
│       ├── __init__.py
│       ├── extract.py                      # COPY-based OLTP → staging extract
│       ├── flatten.py                      # Runs flatten SQL
│       ├── scd2_schools.py                 # Runs SCD2 SQL + counts
│       ├── load_fact.py                    # Runs fact load SQL
│       ├── dq_checks.py                    # 7 automated DQ checks
│       └── notify.py                       # Email notification on run complete
│
├── config/
│   └── .env.example                        # Template — copy to .env for local dev
│
├── .github/
│   └── workflows/
│       └── ci.yml                          # GitHub Actions: lint + SQL file checks
│
├── .gitignore
├── requirements.txt
└── README.md                               # This file
```

---

## 3. Data Model

### Staging Tables (`stg` schema)

| Table | Source | Refresh |
|---|---|---|
| `stg.schools_raw` | `public.schools` | Nightly (full refresh) |
| `stg.school_location_details_raw` | `public.school_location_details` | Nightly (full refresh) |
| `stg.schools_flat` | Built from `schools_raw` + OLTP lookups | Nightly (truncate + rebuild) |
| `stg.admin_units_raw` | `administrative_units.admin_units` | **Manual** (boundary changes only) |

### DW Tables (`dw` schema)

| Table | Type | Grain | Load |
|---|---|---|---|
| `dw.date_dim` | Dimension | 1 row per calendar day | Pre-populated once (2010–2040) |
| `dw.admin_units_dim` | SCD2 Dimension | 1 row per admin unit version | Manual |
| `dw.schools_dim` | SCD2 Dimension | 1 row per school attribute-version | Nightly |
| `dw.school_location_details_dim` | SCD2 Dimension | 1 row per location version | Nightly |
| `dw.school_fact` | Daily Snapshot Fact | 1 row per school per day | Nightly |

### SCD2 Fields on `schools_dim`

| Field | Purpose |
|---|---|
| `effective_date` | Date this version became active |
| `expiration_date` | Date this version expired (`9999-12-31` = still current) |
| `is_current` | `TRUE` = current live row |
| `change_hash` | MD5 of all tracked columns — diff triggers new version |
| `change_reason` | Human-readable reason e.g. `"School renamed"` |
| `changed_fields` | Comma-separated list of changed columns e.g. `"name,boarding_status"` |

**Tracked SCD2 fields:** `name`, `admin_unit_id`, `emis_number`, `school_type`, `operational_status`, `ownership_status`, `funding_type`, `sex_composition`, `boarding_status`, `founding_body_type`

---

## 4. ETL Process Flow

```
01:00  ┌─────────────────────────────────────────────┐
       │  Job 0 — Pre-checks                         │
       │  • Validate OLTP + DW connectivity          │
       │  • Create etl_run row in dw_audit           │
       └────────────────┬────────────────────────────┘
                        │
01:02  ┌────────────────▼────────────────────────────┐
       │  Job 1 — Extract (OLTP → stg)               │
       │  • TRUNCATE + COPY schools_raw              │
       │  • TRUNCATE + COPY school_location_         │
       │    details_raw                              │
       └────────────────┬────────────────────────────┘
                        │
01:05  ┌────────────────▼────────────────────────────┐
       │  Job 2 — Flatten (stg.schools_flat)         │
       │  • Decode lookup IDs → text values          │
       │  • Derive sex_composition, boarding_status  │
       │  • Choose best admin_unit_id (parish first) │
       └────────────────┬────────────────────────────┘
                        │
01:08  ┌────────────────▼────────────────────────────┐
       │  Job 3 — SCD2 Load → dw.schools_dim         │
       │  • Compute change_hash per school           │
       │  • Expire changed rows (set expiration_date,│
       │    is_current = FALSE)                      │
       │  • Insert new versions with changed_fields  │
       │    and change_reason populated              │
       └────────────────┬────────────────────────────┘
                        │
01:12  ┌────────────────▼────────────────────────────┐
       │  Job 4 — Load dw.school_fact               │
       │  • 1 row per current school for today      │
       │  • ON CONFLICT DO NOTHING (safe to re-run) │
       └────────────────┬────────────────────────────┘
                        │
01:15  ┌────────────────▼────────────────────────────┐
       │  Job 5 — DQ Checks                         │
       │  • 7 automated checks → dw_audit.dq_check_log│
       │  • PASS / WARN / FAIL per check            │
       └────────────────┬────────────────────────────┘
                        │
01:18  ┌────────────────▼────────────────────────────┐
       │  Job 6 — Notify + Audit                    │
       │  • Update etl_run (SUCCESS / FAILED)       │
       │  • Send HTML email with row counts + DQ    │
       └─────────────────────────────────────────────┘
```

---

## 5. One-Time Setup

Run these steps **once** when provisioning the Cloud SQL instance.

### 5.1 Create schemas and tables

Connect to your Cloud SQL instance and run in order:

```bash
psql -h HOST -U postgres -d emis_dw -f sql/01_schemas/create_schemas.sql
psql -h HOST -U postgres -d emis_dw -f sql/02_staging/create_staging_tables.sql
psql -h HOST -U postgres -d emis_dw -f sql/03_dw/create_dw_tables.sql
psql -h HOST -U postgres -d emis_dw -f sql/04_audit/create_audit_tables.sql
```

### 5.2 Populate the date dimension

```bash
psql -h HOST -U postgres -d emis_dw -f sql/03_dw/populate_date_dim.sql
```

This inserts ~11,000 rows covering 2010-01-01 through 2040-12-31. Run once only.

### 5.3 Load admin units (manual)

Admin units change infrequently. Load them manually when needed:

```bash
# 1. Load stg.admin_units_raw from your OLTP
psql -h OLTP_HOST -U readonly_user -d emis_oltp \
  -c "\COPY (SELECT ...) TO STDOUT CSV" | \
psql -h HOST -U etl_user -d emis_dw \
  -c "\COPY stg.admin_units_raw FROM STDIN CSV"

# 2. Run SCD2 upsert into dw.admin_units_dim
psql -h HOST -U etl_user -d emis_dw -f sql/05_etl/04_scd2_admin_units_dim.sql
```

### 5.4 Create the ETL database user

```sql
-- On Cloud SQL, run as postgres superuser
CREATE USER etl_user WITH PASSWORD 'use_secret_manager_in_prod';
GRANT CONNECT ON DATABASE emis_dw TO etl_user;
GRANT USAGE  ON SCHEMA stg, dw, dw_audit TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA stg     TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE          ON ALL TABLES IN SCHEMA dw       TO etl_user;
GRANT SELECT, INSERT, UPDATE                  ON ALL TABLES IN SCHEMA dw_audit TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stg, dw, dw_audit TO etl_user;

-- Read-only OLTP user (run on OLTP database)
CREATE USER etl_readonly WITH PASSWORD 'use_secret_manager_in_prod';
GRANT CONNECT ON DATABASE emis_oltp TO etl_readonly;
GRANT USAGE   ON SCHEMA public TO etl_readonly;
GRANT SELECT  ON public.schools,
               public.school_location_details,
               public.pre_primary_schools,
               public.primary_schools,
               public.secondary_schools,
               public.international_schools,
               public.diploma_awarding_schools,
               public.certificate_awarding_schools,
               public.setting_school_types,
               public.setting_operational_statuses,
               public.setting_ownership_statuses,
               public.setting_founding_bodies
TO etl_readonly;
```

---

## 6. Nightly Automation

### 6.1 Install on the GCP VM / Cloud Run instance

```bash
git clone https://github.com/YOUR_ORG/emis-dw.git
cd emis-dw
pip install -r requirements.txt
```

### 6.2 Configure cron

```bash
crontab -e
```

Add this line (runs at 01:00 UTC every night):

```cron
0 1 * * * cd /opt/emis-dw && ENV=production GCP_PROJECT_ID=your-project-id \
  python python/etl_runner.py >> /var/log/emis_etl.log 2>&1
```

### 6.3 Manual backfill

```bash
python python/etl_runner.py --run-date 2026-01-15
```

---

## 7. GCP Secret Manager

In production, all credentials live in **GCP Secret Manager** — nothing is stored in files or environment variables on disk.

### 7.1 Create secrets

```bash
# Run once per secret
echo -n "your-password-here" | gcloud secrets create dw-password \
    --data-file=- --project=YOUR_PROJECT_ID

# Full list of secrets to create:
# dw-host, dw-port, dw-name, dw-user, dw-password
# oltp-host, oltp-port, oltp-name, oltp-user, oltp-password
# smtp-user, smtp-password
```

### 7.2 Grant access to the service account

```bash
gcloud secrets add-iam-policy-binding dw-password \
    --member="serviceAccount:emis-etl@YOUR_PROJECT.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

Repeat for each secret. The ETL runner will fetch them automatically when `ENV=production`.

### 7.3 How it works in code

`config/settings.py` checks `ENV` at startup:
- `ENV=local` → reads from `config/.env`
- `ENV=production` → fetches from Secret Manager, falls back to env vars

---

## 8. Running Locally

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_ORG/emis-dw.git
cd emis-dw

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Set up local config
cp config/.env.example config/.env
# Edit config/.env with your local database details

# 4. Start Cloud SQL Auth Proxy (for local → Cloud SQL)
./cloud-sql-proxy YOUR_PROJECT:REGION:INSTANCE --port=5432 &

# 5. Run the ETL
cd python
python etl_runner.py

# 6. Run for a specific date (backfill)
python etl_runner.py --run-date 2026-01-15
```

---

## 9. Data Quality Checks

Seven DQ checks run automatically after every ETL cycle. Results are stored in `dw_audit.dq_check_log` and included in the email notification.

| Check | What it tests | Fail action |
|---|---|---|
| `schools_raw_not_empty` | `stg.schools_raw` has rows | FAIL → pipeline alert |
| `schools_flat_not_empty` | `stg.schools_flat` has rows | FAIL → pipeline alert |
| `null_emis_numbers_in_dim` | No current `schools_dim` rows with NULL emis_number | WARN (some schools legitimately lack EMIS numbers) |
| `no_orphan_admin_unit_id` | All `schools_dim.admin_unit_id` exist in `admin_units_dim` | FAIL |
| `fact_rows_loaded_today` | `school_fact` has rows for today's date | FAIL |
| `no_duplicate_fact_rows` | No duplicate `(school_id, date_id)` in `school_fact` | FAIL |
| `all_staged_schools_have_current_dim_row` | Every school in staging has a current row in `schools_dim` | FAIL |

### Query recent DQ results

```sql
SELECT run_id, check_name, status, detail, logged_at
FROM   dw_audit.dq_check_log
WHERE  logged_at >= CURRENT_DATE
ORDER  BY logged_at DESC;
```

---

## 10. Troubleshooting & Audit Queries

### View last 10 ETL runs

```sql
SELECT run_id, as_of_date, status, start_ts, end_ts,
       EXTRACT(EPOCH FROM (end_ts - start_ts)) / 60 AS duration_mins,
       row_counts_json
FROM   dw_audit.etl_run
ORDER  BY run_id DESC
LIMIT  10;
```

### View step-level detail for a failed run

```sql
SELECT step_name, status, rows_affected, message, start_ts, end_ts
FROM   dw_audit.etl_step_log
WHERE  run_id = <your_run_id>
ORDER  BY step_log_id;
```

### See what changed for a specific school

```sql
SELECT id, effective_date, expiration_date, is_current,
       change_reason, changed_fields, name, boarding_status, operational_status
FROM   dw.schools_dim
WHERE  source_id = <oltp_school_id>
ORDER  BY effective_date;
```

### Count schools by operational status today

```sql
SELECT sd.operational_status, COUNT(*) AS school_count
FROM   dw.school_fact sf
JOIN   dw.schools_dim  sd ON sd.id = sf.school_id
JOIN   dw.date_dim     dd ON dd.id = sf.date_id
WHERE  dd.system_date = CURRENT_DATE
GROUP  BY sd.operational_status
ORDER  BY school_count DESC;
```

### Count schools by sex composition and district (today)

```sql
SELECT aud.name AS district, sd.sex_composition, COUNT(*) AS total
FROM   dw.school_fact sf
JOIN   dw.schools_dim    sd  ON sd.id  = sf.school_id
JOIN   dw.admin_units_dim aud ON aud.id = sd.admin_unit_id
JOIN   dw.date_dim        dd  ON dd.id  = sf.date_id
WHERE  dd.system_date    = CURRENT_DATE
  AND  aud.admin_unit_type = 'DISTRICT'
  AND  aud.current_status  = TRUE
GROUP  BY aud.name, sd.sex_composition
ORDER  BY aud.name, sd.sex_composition;
```

### Schools that changed boarding status — when and what changed

```sql
SELECT source_id, name, effective_date, boarding_status,
       change_reason, changed_fields
FROM   dw.schools_dim
WHERE  changed_fields LIKE '%boarding_status%'
ORDER  BY source_id, effective_date;
```

---

## 11. Extending the Project

| Next step | How |
|---|---|
| **Add more data marts** (Enrollment, Teachers) | Add staging + DW tables following the same pattern; create a new ETL step module |
| **Switch to Airflow** | Wrap each `etl/` module in an Airflow task; the Python modules are already self-contained |
| **Incremental loads** | Replace full COPY in `extract.py` with `WHERE date_updated >= last_success_ts` from `dw_audit.etl_watermark` |
| **School Location Details SCD2** | Add `05_etl/05_scd2_school_location_dim.sql` following the same pattern as `02_scd2_schools_dim.sql` |
| **dbt** | The SQL ETL files map directly to dbt models; staging files → `models/staging/`, DW loads → `models/marts/` |
| **Looker / Data Studio** | Point at `dw.*` views/tables directly; the denormalised `schools_dim` makes simple joins trivial |
