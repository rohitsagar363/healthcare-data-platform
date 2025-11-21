"""Simulated gold layer from silver CSVs.

Produces small fact/dim tables to mimic curated outputs.
Replace with Spark + Delta + Snowflake writes in production.
"""

from pathlib import Path
import pandas as pd


SILVER_DIR = Path("../silver")
GOLD_DIR = Path("../gold")


def build_dim_member(claims, wellness):
    members_from_claims = claims[["member_id"]].drop_duplicates()
    members_from_wellness = wellness[["member_id"]].drop_duplicates()
    members = pd.concat([members_from_claims, members_from_wellness], ignore_index=True).drop_duplicates()
    members["member_name"] = members["member_id"].apply(lambda m: f"Member {m[-3:]}")  # demo label
    dest = GOLD_DIR / "dim_member.csv"
    members.to_csv(dest, index=False)
    return dest


def build_dim_provider(claims):
    providers = (
        claims[["provider_id", "procedure_desc"]]
        .drop_duplicates()
        .rename(columns={"procedure_desc": "specialty_hint"})
    )
    providers["provider_name"] = providers["provider_id"].apply(lambda p: f"Provider {p}")
    dest = GOLD_DIR / "dim_provider.csv"
    providers.to_csv(dest, index=False)
    return dest


def build_fact_claims(claims):
    fact = claims.copy()
    fact["service_date"] = pd.to_datetime(fact["service_date"])
    dest = GOLD_DIR / "fact_claims.csv"
    fact.to_csv(dest, index=False)
    return dest


def build_fact_ehr(ehr):
    fact = ehr.copy()
    fact["visit_date"] = pd.to_datetime(fact["visit_date"])
    dest = GOLD_DIR / "fact_ehr_visits.csv"
    fact.to_csv(dest, index=False)
    return dest


def build_fact_wellness(wellness):
    fact = wellness.copy()
    fact["activity_date"] = pd.to_datetime(fact["activity_date"])
    dest = GOLD_DIR / "fact_wellness.csv"
    fact.to_csv(dest, index=False)
    return dest


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    claims = pd.read_csv(SILVER_DIR / "claims_silver.csv")
    ehr = pd.read_csv(SILVER_DIR / "ehr_silver.csv")
    wellness = pd.read_csv(SILVER_DIR / "wellness_silver.csv")

    outputs = {
        "dim_member": build_dim_member(claims, wellness),
        "dim_provider": build_dim_provider(claims),
        "fact_claims": build_fact_claims(claims),
        "fact_ehr_visits": build_fact_ehr(ehr),
        "fact_wellness": build_fact_wellness(wellness),
    }
    for name, path in outputs.items():
        print(f"Wrote {name} gold -> {path}")  # noqa: T201


if __name__ == "__main__":
    main()
