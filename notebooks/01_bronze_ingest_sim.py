"""Simulated bronze ingestion using local sample CSVs.

Runs locally without Databricks to exercise the folder structure.
Replace pandas reads/writes with Spark + Delta when moving to Databricks.
"""

from pathlib import Path
import pandas as pd


RAW_DIR = Path("../data/raw")
BRONZE_DIR = Path("../bronze")


def ingest_claims():
    src = RAW_DIR / "claims" / "sample_claims.csv"
    df = pd.read_csv(src)
    df = df.drop_duplicates()
    dest = BRONZE_DIR / "claims_bronze.csv"
    df.to_csv(dest, index=False)
    return dest


def ingest_ehr():
    src = RAW_DIR / "ehr" / "sample_ehr.csv"
    df = pd.read_csv(src)
    df = df.drop_duplicates()
    dest = BRONZE_DIR / "ehr_bronze.csv"
    df.to_csv(dest, index=False)
    return dest


def ingest_wellness():
    src = RAW_DIR / "wellness" / "sample_wellness.csv"
    df = pd.read_csv(src)
    df = df.drop_duplicates()
    dest = BRONZE_DIR / "wellness_bronze.csv"
    df.to_csv(dest, index=False)
    return dest


def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    outputs = {
        "claims": ingest_claims(),
        "ehr": ingest_ehr(),
        "wellness": ingest_wellness(),
    }
    for name, path in outputs.items():
        print(f"Wrote {name} bronze -> {path}")  # noqa: T201


if __name__ == "__main__":
    main()
