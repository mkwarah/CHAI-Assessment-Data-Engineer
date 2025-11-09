import pandas as pd
from sqlalchemy import create_engine, text
from .config import SQLALCHEMY_URL

DDL_STG = """
CREATE TABLE IF NOT EXISTS public.stg_hss (
  country_code   TEXT,
  country_name   TEXT,
  year           INT,
  indicator      TEXT,
  indicator_name TEXT,
  unit           TEXT,
  value          DOUBLE PRECISION
);
"""

DDL_MART = """
DROP TABLE IF EXISTS public.mart_hss_country_year;
CREATE TABLE public.mart_hss_country_year AS
SELECT
  country_code,
  country_name,
  year,
  MAX(CASE WHEN indicator = 'HSS.DTP3.COVERAGE'   THEN value END) AS dtp3_coverage_pct,
  MAX(CASE WHEN indicator = 'HSS.DHIS2.REPORTING' THEN value END) AS dhis2_reporting_pct,
  MAX(CASE WHEN indicator = 'HSS.LMIS.FILL_RATE'  THEN value END) AS lmis_fill_rate_pct,
  MAX(CASE WHEN indicator = 'HSS.STOCKOUT.RATE'   THEN value END) AS stockout_rate_pct
FROM public.stg_hss
GROUP BY country_code, country_name, year;
"""

DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_stg_hss_country_year ON public.stg_hss(country_code, year);
CREATE INDEX IF NOT EXISTS idx_mart_country_year ON public.mart_hss_country_year(country_code, year);
"""

def run_load(df: pd.DataFrame):
    engine = create_engine(SQLALCHEMY_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(DDL_STG))
        # Write staging (replace or append; here we'll replace for idempotency)
        df.to_sql("stg_hss", con=conn, schema="public", if_exists="replace", index=False, method="multi", chunksize=500)
        # Build mart
        conn.execute(text(DDL_MART))
        # Indexes
        conn.execute(text(DDL_INDEXES))
