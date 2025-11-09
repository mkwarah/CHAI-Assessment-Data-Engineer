import os
from sqlalchemy import create_engine, text

# --- DB config from env (override in docker-compose.yml) ---
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "chaidb")
DB_USER = os.environ.get("DB_USER", "chai")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "chai_pw")

def db_engine():
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def main():
    eng = db_engine()
    with eng.begin() as conn:
        # 1) Safety: ensure public schema exists (usually does)
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public;"))

        # 2) Helpful indexes on staging for faster aggregation
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_stg_hss_country_year ON public.stg_hss(country_code, year);
            CREATE INDEX IF NOT EXISTS idx_stg_hss_indicator    ON public.stg_hss(indicator);
        """))

        # 3) Create mart table with explicit types + constraints (idempotent)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.mart_hss_country_year (
                country_code           TEXT        NOT NULL,
                country_name           TEXT        NOT NULL,
                year                   INT         NOT NULL,
                dtp3_coverage_pct      NUMERIC(5,2),
                dhis2_reporting_pct    NUMERIC(5,2),
                lmis_fill_rate_pct     NUMERIC(5,2),
                stockout_rate_pct      NUMERIC(5,2),
                hss_readiness_index    NUMERIC(6,2),
                CONSTRAINT pk_mart_hss_country_year PRIMARY KEY (country_code, year),
                CONSTRAINT ck_pct_dt3            CHECK (dtp3_coverage_pct    IS NULL OR (dtp3_coverage_pct    BETWEEN 0 AND 100)),
                CONSTRAINT ck_pct_dhis2          CHECK (dhis2_reporting_pct  IS NULL OR (dhis2_reporting_pct  BETWEEN 0 AND 100)),
                CONSTRAINT ck_pct_lmis           CHECK (lmis_fill_rate_pct   IS NULL OR (lmis_fill_rate_pct   BETWEEN 0 AND 100)),
                CONSTRAINT ck_pct_stockout       CHECK (stockout_rate_pct    IS NULL OR (stockout_rate_pct    BETWEEN 0 AND 100)),
                CONSTRAINT ck_readiness          CHECK (hss_readiness_index  IS NULL OR (hss_readiness_index  BETWEEN 0 AND 100))
            );
            COMMENT ON TABLE  public.mart_hss_country_year IS 'Country-year health system summary (DTP3, DHIS2 reporting, LMIS fill rate, stockout rate, composite index)';
            COMMENT ON COLUMN public.mart_hss_country_year.hss_readiness_index IS 'Weighted composite: 35%DTP3 + 25%DHIS2 + 25%LMIS + 15%(100 - stockout)';
        """))

        # 4) Rebuild mart deterministically (no duplicates on re-runs)
        conn.execute(text("TRUNCATE TABLE public.mart_hss_country_year;"))

        # 5) Populate mart from staging (same logic as before, but INSERT INTO … SELECT …)
        conn.execute(text("""
            WITH base AS (
                SELECT
                    country_code,
                    MIN(country_name) AS country_name,
                    year,
                    MAX(CASE WHEN indicator = 'HSS.DTP3.COVERAGE'   THEN value END) AS dtp3_coverage_pct,
                    MAX(CASE WHEN indicator = 'HSS.DHIS2.REPORTING' THEN value END) AS dhis2_reporting_pct,
                    MAX(CASE WHEN indicator = 'HSS.LMIS.FILL_RATE'  THEN value END) AS lmis_fill_rate_pct,
                    MAX(CASE WHEN indicator = 'HSS.STOCKOUT.RATE'   THEN value END) AS stockout_rate_pct
                FROM public.stg_hss
                GROUP BY country_code, year
            )
            INSERT INTO public.mart_hss_country_year (
                country_code, country_name, year,
                dtp3_coverage_pct, dhis2_reporting_pct, lmis_fill_rate_pct, stockout_rate_pct,
                hss_readiness_index
            )
            SELECT
                country_code,
                country_name,
                year,
                dtp3_coverage_pct,
                dhis2_reporting_pct,
                lmis_fill_rate_pct,
                stockout_rate_pct,
                (
                    COALESCE(dtp3_coverage_pct, 0)                   * 0.35
                  + COALESCE(dhis2_reporting_pct, 0)                 * 0.25
                  + COALESCE(lmis_fill_rate_pct, 0)                  * 0.25
                  + (100 - COALESCE(stockout_rate_pct, 100))         * 0.15
                ) AS hss_readiness_index
            FROM base;
        """))

        # 6) Mart indexes to speed common filters/joins
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_mart_hss_country_year ON public.mart_hss_country_year (country_code, year);
            -- Optional: if you often filter by readiness thresholds, this helps
            CREATE INDEX IF NOT EXISTS idx_mart_hss_readiness ON public.mart_hss_country_year (hss_readiness_index);
        """))

        # 7) Analyze for good query plans
        conn.execute(text("ANALYZE public.mart_hss_country_year;"))

    print("[Model] Rebuilt mart_hss_country_year with constraints and indexes")

if __name__ == "__main__":
    main()
