"""Quick analysis summary: farm counts, stages, MinFin amounts, and category coverage."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Set

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def load_master(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def summarize_stages(df: pd.DataFrame) -> Dict[str, int]:
    stage_col = "stage_latest_llm"
    if stage_col not in df.columns:
        return {}
    return df.groupby(stage_col)["farm_id"].nunique().to_dict()


def summarize_minfin_amounts(minfin_raw: Path, minfin_join: Path) -> Dict[str, float]:
    if not minfin_raw.exists() or not minfin_join.exists():
        return {}
    raw = pd.read_csv(minfin_raw)
    join = pd.read_csv(minfin_join)

    # Normalize KVK and sum amounts (convert from x1000 to euros)
    raw["KVKnummer_norm"] = raw["KVKnummer"].astype(str).str.strip().str.replace(".0", "", regex=False)
    raw["amount_eur"] = pd.to_numeric(raw["Bedrag (x1000)"], errors="coerce").fillna(0) * 1000
    amount_by_kvk = raw.groupby("KVKnummer_norm")["amount_eur"].sum()

    join["kvk_norm"] = join["kvk_nummer_minfin"].astype(str).str.strip().str.replace(".0", "", regex=False)
    farm_map = join.groupby("farm_id")["kvk_norm"].first()

    amount_by_farm = {}
    for farm_id, kvk in farm_map.items():
        amount_by_farm[farm_id] = float(amount_by_kvk.get(kvk, 0.0))
    return amount_by_farm


def main() -> None:
    parser = argparse.ArgumentParser(description="Print high-level stats for farms, stages, MinFin, and categories.")
    parser.add_argument("--master", type=Path, default=PROCESSED_DIR / "master_permits.csv")
    parser.add_argument("--minfin-raw", type=Path, default=RAW_DIR / "minfin_dataset.csv")
    parser.add_argument("--minfin-join", type=Path, default=PROCESSED_DIR / "04_minfin_animals_join.csv")
    args = parser.parse_args()

    master = load_master(args.master)
    total_farms = master["farm_id"].nunique()
    print(f"Unique farms: {total_farms}")

    # Stages
    stage_counts = summarize_stages(master)
    if stage_counts:
        print("Stage counts (stage_latest_llm):")
        for k, v in stage_counts.items():
            print(f"  {k}: {v}")
    else:
        print("No stage data found.")

    # MinFin amounts
    amounts = summarize_minfin_amounts(args.minfin_raw, args.minfin_join)
    if amounts:
        farms_with_amount = sum(1 for v in amounts.values() if v > 0)
        total_amount = sum(amounts.values())
        print(f"MinFin amounts: farms with amount={farms_with_amount}, total €{total_amount:,.0f}")
    else:
        print("MinFin amounts: data not available.")

    # Category info based on Huisvesting text (strip leading label).
    def normalize_huisvesting(val: str) -> str:
        if not isinstance(val, str):
            return ""
        v = val.lower().strip()
        v = v.replace("huisvesting", "").strip()
        return v

    if "Huisvesting" in master.columns and "jaar" in master.columns:
        h = master[["farm_id", "Huisvesting", "gem_aantal_dieren", "jaar"]].copy()
        h["huisvesting_norm"] = h["Huisvesting"].apply(normalize_huisvesting)
        h_2022 = h[h["jaar"] == 2022]
        cats_by_farm = h_2022.groupby("farm_id")["huisvesting_norm"].nunique()
        single = cats_by_farm[cats_by_farm == 1].index
        multi = cats_by_farm[cats_by_farm > 1].index
        print(f"Farms with single Huisvesting category: {len(single)}")
        if single.size:
            total_animals_single = (
                h_2022[h_2022["farm_id"].isin(single)]
                .groupby("farm_id")["gem_aantal_dieren"]
                .sum()
                .sum()
            )
            print(f"Total animals (2022) for single-category farms: {int(total_animals_single):,}")
        print(f"Farms with multiple Huisvesting categories: {len(multi)}")
        if multi.size:
            total_animals_multi = (
                h_2022[h_2022["farm_id"].isin(multi)]
                .groupby("farm_id")["gem_aantal_dieren"]
                .sum()
                .sum()
            )
            print(f"Total animals (2022) for multi-category farms: {int(total_animals_multi):,}")
    else:
        print("Category breakdown: skipped (Huisvesting/jaar not available).")

if __name__ == "__main__":
    main()
