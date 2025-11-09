import os

SOURCE = os.getenv("SOURCE", "csv").lower()             # 'csv' or 'api'
RAW_DIR = "/app/data/raw"
PROCESSED_DIR = "/app/data/processed"

# API config
WB_COUNTRIES = os.getenv("WB_COUNTRIES", "RWA;UGA;KEN;TZA;BDI")
WB_DATE_RANGE = os.getenv("WB_DATE_RANGE", "2000:2024")

# DB config
DB_USER = os.getenv("POSTGRES_USER", "chai")
DB_PW = os.getenv("POSTGRES_PASSWORD", "chai_pw")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "chaidb")

SQLALCHEMY_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
