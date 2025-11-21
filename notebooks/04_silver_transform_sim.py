"""Simulated silver transformations using bronze CSVs.

Applies light cleaning and joins ICD/CPT lookups. Swap for Spark/Delta in Databricks.
"""

from pathlib import Path
import pandas as pd


BRONZE_DIR = Path("../bronze")
RAW_ICD_DIR = Path("../data/raw/icd")
SILVER_DIR = Path("../silver")


def enrich_claims():
    claims = pd.read_csv(BRONZE_DIR / "claims_bronze.csv", parse_dates=["service_date"])
    icd = pd.read_csv(RAW_ICD_DIR / "icd_codes.csv")
    cpt = pd.read_csv(RAW_ICD_DIR / "cpt_codes.csv")

    claims = claims.drop_duplicates()
    claims = claims.merge(icd, left_on="diagnosis_code", right_on="code", how="left", suffixes=("", "_icd"))
    claims = claims.merge(cpt, left_on="procedure_code", right_on="code", how="left", suffixes=("", "_cpt"))
    claims = claims.rename(
        columns={
            "description": "diagnosis_desc",
            "description_cpt": "procedure_desc",
        }
    )
    claims = claims.drop(columns=[col for col in claims.columns if col.startswith("code") and col not in {"code"}])
    dest = SILVER_DIR / "claims_silver.csv"
    claims.to_csv(dest, index=False)
    return dest


def clean_ehr():
    ehr = pd.read_csv(BRONZE_DIR / "ehr_bronze.csv", parse_dates=["visit_date"])
    ehr = ehr.drop_duplicates()
    ehr["lab_value_num"] = pd.to_numeric(ehr["lab_value"], errors="coerce")
    dest = SILVER_DIR / "ehr_silver.csv"
    ehr.to_csv(dest, index=False)
    return dest


def aggregate_wellness():
    wellness = pd.read_csv(BRONZE_DIR / "wellness_bronze.csv", parse_dates=["activity_date"])
    wellness = wellness.drop_duplicates()
    daily = (
        wellness.groupby(["member_id", "activity_date"])[["steps", "resting_hr", "active_minutes"]]
        .agg({"steps": "sum", "resting_hr": "mean", "active_minutes": "sum"})
        .reset_index()
    )
    dest = SILVER_DIR / "wellness_silver.csv"
    daily.to_csv(dest, index=False)
    return dest


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    outputs = {
        "claims": enrich_claims(),
        "ehr": clean_ehr(),
        "wellness": aggregate_wellness(),
    }
    for name, path in outputs.items():
        print(f"Wrote {name} silver -> {path}")  # noqa: T201


if __name__ == "__main__":
    main()
