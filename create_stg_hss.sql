CREATE TABLE IF NOT EXISTS public.stg_hss (
  country_code   TEXT,
  country_name   TEXT,
  year           INT,
  indicator      TEXT,
  indicator_name TEXT,
  unit           TEXT,
  value          DOUBLE PRECISION
);
