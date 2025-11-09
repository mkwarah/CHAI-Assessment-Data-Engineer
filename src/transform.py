import pandas as pd
from pathlib import Path
from .config import PROCESSED_DIR

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Clean types
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # Standardize text
    for col in ["country_code","country_name","indicator","indicator_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    # Add unit
    df["unit"] = "%"
    # Column order
    cols = ["country_code","country_name","year","indicator","indicator_name","unit","value"]
    df = df[cols]
    # Save processed snapshot
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
    df.to_parquet(Path(PROCESSED_DIR)/"hss_processed.parquet", index=False)
    df.to_csv(Path(PROCESSED_DIR)/"hss_processed.csv", index=False)
    return df
