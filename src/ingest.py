import json
import pandas as pd
from pathlib import Path
from .config import SOURCE, RAW_DIR, WB_COUNTRIES, WB_DATE_RANGE
from .utils import fetch_with_retry

def from_csv() -> pd.DataFrame:
    """Load health indicators from local CSV snapshot."""
    csv_path = Path(RAW_DIR) / "hss_indicators_snapshot.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Local CSV not found at {csv_path}")
    print(f"[Ingest] Reading local CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    # Save a safety copy of the raw data
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    df.to_csv(Path(RAW_DIR) / "raw_snapshot_copy.csv", index=False)
    return df


def from_world_bank() -> pd.DataFrame:
    """Fetch indicators from the World Bank API (with retry & backoff)."""
    indicators = [
        ("HSS.DTP3.COVERAGE", "DTP3 immunization coverage (%)"),
        ("HSS.DHIS2.REPORTING", "DHIS2 reporting completeness (%)"),
        ("HSS.LMIS.FILL_RATE", "LMIS order fill rate (%)"),
        ("HSS.STOCKOUT.RATE", "Essential medicine stockout rate (%)"),
    ]
    frames = []

    print(f"[Ingest] Fetching data from World Bank API for {len(indicators)} indicators...")
    for ind, ind_name in indicators:
        url = (
            f"https://api.worldbank.org/v2/country/{WB_COUNTRIES}/indicator/{ind}"
            f"?format=json&per_page=20000&date={WB_DATE_RANGE}"
        )
        print(f"  → Fetching {ind} ...")
        resp = fetch_with_retry(url)
        payload = resp.json()

        # Save raw JSON snapshot
        Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
        with open(Path(RAW_DIR) / f"wb_{ind}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        # Normalize to DataFrame
        data = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
        tmp = pd.json_normalize(data)
        if tmp.empty:
            print(f"    ⚠️ No data for {ind}")
            continue
        tmp = tmp.rename(columns={
            "country.id": "country_code",
            "country.value": "country_name",
            "date": "year",
            "value": "value",
        })
        tmp["indicator"] = ind
        tmp["indicator_name"] = ind_name
        frames.append(tmp[["country_code", "country_name", "year", "indicator", "indicator_name", "value"]])

    if not frames:
        raise RuntimeError("No data returned from World Bank API — check network or indicator codes.")
    df = pd.concat(frames, ignore_index=True)
    df.to_csv(Path(RAW_DIR) / "world_bank_raw.csv", index=False)
    print(f"[Ingest] Saved API data to {RAW_DIR}/world_bank_raw.csv")
    return df


def run_ingest() -> pd.DataFrame:
    """
    Determine source and load data accordingly.
    Automatically falls back to API if local CSV missing.
    """
    csv_path = Path(RAW_DIR) / "hss_indicators_snapshot.csv"

    if SOURCE == "csv":
        if csv_path.exists():
            return from_csv()
        else:
            print(f"[Ingest] Local CSV not found → falling back to World Bank API fetch.")
            return from_world_bank()

    elif SOURCE == "api":
        return from_world_bank()

    else:
        # Unknown source flag — attempt automatic decision
        print(f"[Ingest] Unknown SOURCE={SOURCE}. Defaulting to World Bank API.")
        return from_world_bank()
