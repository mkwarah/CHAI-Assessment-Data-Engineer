from .ingest import run_ingest
from .transform import transform
from .load import run_load

def main():
    print("[1/3] Ingest…")
    df_raw = run_ingest()
    print(f"  rows: {len(df_raw)}")

    print("[2/3] Transform…")
    df_t = transform(df_raw)
    print(f"  rows after transform: {len(df_t)}")

    print("[3/3] Load…")
    run_load(df_t)
    print("Done ✅")

if __name__ == "__main__":
    main()
