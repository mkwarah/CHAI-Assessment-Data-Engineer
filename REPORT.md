# CHAI Data Engineer Technical Assessment — REPORT

---

## 1. Design and Architecture Choices

### Overview
The pipeline is designed around a **modular, containerized ETL architecture**, ensuring reproducibility, maintainability, and ease of testing.

### Components
- **Docker Compose** orchestrates both services:
  - `chai_pg` — PostgreSQL 16 database
  - `chai_pipeline` — Python container running the ETL process (pandas + SQLAlchemy)
- **Folder structure** clearly separates raw, processed, and modeling layers:
  ```
  data/raw/          → Input CSV or API snapshot
  data/processed/    → Cleaned, transformed Parquet file
  src/               → Modular ETL scripts
  ├── ingest.py
  ├── transform.py
  ├── load.py
  ├── model.py
  └── orchestrate.py
  ```
- **Pipeline flow (ETL)**  
  1. **Ingest:** Reads local CSV (`data/raw/hss_indicators_snapshot.csv`) or API fallback.  
  2. **Transform:** Cleans and standardizes indicator data, saves Parquet (`data/processed/hss_processed.parquet`).  
  3. **Load:** Inserts clean data into staging table `public.stg_hss` in Postgres.  
  4. **Model:** Aggregates data into `public.mart_hss_country_year`, deriving a composite metric `hss_readiness_index`.

### Design Rationale
- **Containerization** ensures consistent environments across machines.
- **Separation of stages** (ingest, transform, load, model) improves readability and debugging.
- **Database-first design**: Postgres chosen for strong relational integrity and SQL-based analytics.
- **Parquet layer**: Provides efficient intermediate storage and interoperability with future analytical tools.
- **Idempotent runs**: Each execution truncates and rebuilds tables, ensuring repeatable results.

---

## 2. Optimization Strategies Applied

### Database-Level Optimizations
- **Indexes** on key columns `(country_code, year)` for both staging and mart tables to accelerate joins and aggregations:
  ```sql
  CREATE INDEX IF NOT EXISTS idx_stg_hss_country_year ON stg_hss(country_code, year);
  CREATE INDEX IF NOT EXISTS idx_mart_hss_country_year ON mart_hss_country_year(country_code, year);
  ```
- **Primary Key and Constraints** on mart table:
  - `PRIMARY KEY (country_code, year)` ensures uniqueness.
  - `CHECK` constraints validate all percentage values between 0 and 100.
- **Typed Columns**: Numeric precision (`NUMERIC(5,2)`) chosen to maintain analytical accuracy and minimize storage.

### ETL-Level Optimizations
- **Vectorized pandas operations**: Efficient transformations without Python loops.
- **Efficient writes**: Parquet format for intermediate files using `pyarrow`.
- **Idempotent truncation**: The load process uses `TRUNCATE` before inserts to prevent duplicates.
- **ANALYZE** command run post-modeling to refresh Postgres statistics for optimal query planning.

### Operational Optimizations
- **Single-command orchestration**: One `docker compose up --build` triggers the entire ETL → database pipeline.
- **Minimal dependencies**: Lightweight Python stack ensures fast image builds and low memory footprint.

---

## 3. Scaling and Extension Strategy

### Data and Source Scaling
- **Source Upgrade**: Replace static CSV with API integrations for live data from WHO, UNICEF, or DHIS2 endpoints.
- **Automated Refresh**: Add scheduling (e.g., via Airflow or Prefect) for periodic re-ingestion.

### Infrastructure Scaling
- **Database Partitioning**: Partition mart tables by `year` for faster queries on larger datasets.
- **Cloud Migration**: Move data layers to cloud storage (e.g., AWS S3 for raw/processed, RDS for Postgres).
- **Container Orchestration**: Deploy containers on Kubernetes for horizontal scaling and resilience.

### Quality & Monitoring
- **Data Validation**: Integrate tools like *Great Expectations* to enforce schema and quality rules.
- **Logging & Alerts**: Extend to Prometheus + Grafana for performance monitoring and anomaly detection.
- **CI/CD Integration**: Add GitHub Actions for automated testing and deployment of new pipeline versions.

### Analytical Extension
- **Visualization Layer**: Connect mart data to BI tools (Metabase, Power BI) for dynamic dashboards.
- **Additional Indicators**: Extend schema to include more health system metrics and external joins.

---

## Summary

| Aspect | Approach |
|---------|-----------|
| **Architecture** | Modular ETL inside Docker (Ingest → Transform → Load → Model) |
| **Optimizations** | Indexes, typed columns, constraints, and idempotent rebuilds |
| **Scalability** | API integration, orchestration tools, cloud migration, data validation |

This design provides a **clean, maintainable, and production-ready foundation** for CHAI's data engineering workflows. It balances reproducibility, analytical performance, and future scalability.

---

**Author:**  
*Herrings Mkwara*  
MSc Applied Data Science - Malmö University, Sweden  
GitHub: https://github.com/mkwarah 
Email: mtg.herrings@gmail.com
