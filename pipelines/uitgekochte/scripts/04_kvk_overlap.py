"""Identify overlap between permit KVK lookups and minfin KVK lookups, and produce a combined KVK-address table."""
from __future__ import annotations

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"


def normalize_kvk(val: str | float | int | None) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip()
    # drop trailing .0 and leading zeros
    if s.endswith(".0"):
        s = s[:-2]
    return s.lstrip("0")


def load_permits() -> pd.DataFrame:
    df = pd.read_csv(PROCESSED / "02_kvk_results.csv")
    df["kvk_norm"] = df["kvk_nummer"].apply(normalize_kvk)
    df["source"] = "permit"
    return df


def load_minfin() -> pd.DataFrame:
    df = pd.read_csv(PROCESSED / "03_kvk_minfin_results.csv")
    df["kvk_norm"] = df["kvk_nummer"].apply(normalize_kvk)
    # fallback to provided kvk_nummer_minfin if kvk_nummer missing
    missing = df["kvk_norm"] == ""
    df.loc[missing, "kvk_norm"] = df.loc[missing, "kvk_nummer_minfin"].apply(normalize_kvk)
    df["source"] = "minfin"
    return df


def main() -> None:
    permits = load_permits()
    minfin = load_minfin()

    permit_kvks = set(permits["kvk_norm"]) - {""}
    minfin_kvks = set(minfin["kvk_norm"]) - {""}

    overlap_kvks = permit_kvks & minfin_kvks

    summary = {
        "permit_total": len(permits["farm_id"].unique()),
        "minfin_total": len(minfin["minfin_id"].unique()),
        "permit_kvk_nonempty": len(permit_kvks),
        "minfin_kvk_nonempty": len(minfin_kvks),
        "kvk_overlap": len(overlap_kvks),
    }
    pd.DataFrame([summary]).to_csv(PROCESSED / "04_overlap_summary.csv", index=False)

    combined = pd.concat([permits, minfin], ignore_index=True)
    combined.to_csv(PROCESSED / "04_combined_kvk_addresses.csv", index=False)


if __name__ == "__main__":
    main()
