# 0) Prereqs
# - Docker Desktop installed and running
# - Internet connection (for API fallback if local CSV missing)
# - Git (optional, for cloning the repo)

# 1) Start everything (DB + pipeline). One command:
docker compose up --build

# The pipeline will:
#  - Start PostgreSQL (chai_pg) and the ETL container (chai_pipeline),
#  - Read CSV from data/raw (by default),
#  - Write processed files to data/processed (as Parquet),
#  - Create and populate tables in Postgres:
#       * public.stg_hss              – staging layer
#       * public.mart_hss_country_year – aggregated analytics mart
#  - Apply indexes and constraints for optimized queries.

# Logs should end with:
# [Model] Rebuilt mart_hss_country_year with constraints and indexes
# Done ✅

# To stop the containers:
docker compose down
# To reset everything (including database volume):
docker compose down -v


# 2) Validate in Postgres (Windows-friendly, uses plain SQL):
# These commands confirm data moved through all pipeline stages.

# List all tables created in the 'public' schema:
docker compose exec db psql -U chai -d chaidb -c \
"SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename;"

# Check record counts in each stage:
docker compose exec db psql -U chai -d chaidb -c "SELECT COUNT(*) AS stg_rows FROM public.stg_hss;"
docker compose exec db psql -U chai -d chaidb -c "SELECT COUNT(*) AS mart_rows FROM public.mart_hss_country_year;"

# Preview sample transformed data:
docker compose exec db psql -U chai -d chaidb -c \
"SELECT country_code, year, dtp3_coverage_pct, dhis2_reporting_pct, hss_readiness_index
 FROM public.mart_hss_country_year
 ORDER BY country_code, year DESC
 LIMIT 10;"

# Expected results:
# - public.stg_hss ≈ 140 rows
# - public.mart_hss_country_year ≈ 35 aggregated rows


# 3) Switch to API ingestion (optional)- I have included this in case you want to connect directly to API
# The pipeline can fetch data from a REST API if the local CSV is missing or you just need to connect to API.
# Edit .env or run with an override:
SOURCE=api docker compose up --build

# By default, the pipeline reads from:
#   data/raw/hss_indicators_snapshot.csv
# If this file is unavailable, it automatically falls back to API ingestion.


# 4) Dependencies and setup (Refer to /src/requirments.txt)
# No manual setup needed — Docker handles everything automatically.
# Python dependencies inside the chai_pipeline container include:
#   pandas==2.2.2
#   requests==2.32.3
#   SQLAlchemy==2.0.32
#   psycopg2-binary==2.9.9
#   python-dateutil==2.9.0.post0
#   pyarrow

# Environment variables (defined in docker-compose.yml):
#   DB_HOST=db
#   DB_PORT=5432
#   DB_NAME=chaidb
#   DB_USER=chai
#   DB_PASSWORD=chai_pw


# 5) Data source used
# The dataset represents Health System Strengthening (HSS) indicators.
#  - File: data/raw/hss_indicators_snapshot.csv
#  - Columns: country_code, country_name, year, indicator, value
#  - Records: 140 (aggregated to ~35 country-year combinations)
# The processed file is saved as:
#   data/processed/hss_processed.parquet


# 6) How to validate that data moved through each stage:
#  - Ingest: Confirm "[1/3] Ingest..." appears in logs.
#  - Transform: Check data/processed/hss_processed.parquet exists.
#  - Load: Verify record count in public.stg_hss (≈140 rows).
#  - Model: Verify public.mart_hss_country_year created (≈35 rows, with indexes).

# You can inspect indexes and constraints:
docker compose exec db psql -U chai -d chaidb -c "\d+ public.mart_hss_country_year"

# Key computed metric:
#   hss_readiness_index =
#     (0.35 * dtp3_coverage_pct)
#   + (0.25 * dhis2_reporting_pct)
#   + (0.25 * lmis_fill_rate_pct)
#   + (0.15 * (100 - stockout_rate_pct))
# This composite score measures overall health system readiness per country-year.

# Troubleshooting:
#  - If containers fail to start, ensure Docker Desktop is running.
#  - To rebuild cleanly:
docker compose build --no-cache && docker compose up


# ALL SCRIPTS ARE STORED IN /src/


# Author:
#  Herrings Mkwara
#  MSc Applied Data Science – Malmö University, Sweden
#  GitHub: https://github.com/mkwarah
