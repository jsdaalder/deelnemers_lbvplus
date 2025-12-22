"""
Generate summary metrics for the LBV-plus peak polluter workbook.

Outputs a human-readable Markdown report to metrics_summary.md.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
import pandas as pd


# Project root (pipelines/matching_nrc)
ROOT = Path(__file__).resolve().parent.parent
WORKBOOK = ROOT / "data" / "processed" / "peak_polluters_workbook.xlsx"
OUTPUT = ROOT / "reports" / "metrics_summary.md"


def fmt_int(x: float | int) -> str:
    return f"{int(x):,}".replace(",", " ")


def fmt_float(x: float, decimals: int = 2) -> str:
    return f"{x:,.{decimals}f}".replace(",", " ")


def safe_ratio(num: float, den: float) -> float:
    return 0.0 if den == 0 else num / den


def load_frames() -> dict[str, pd.DataFrame]:
    sheets = [
        "merged",
        "depositie",
        "piekbelasters",
        "dierrechten",
        "ontvangers",
    ]
    return {name: pd.read_excel(WORKBOOK, sheet_name=name) for name in sheets}


def compute_metrics() -> str:
    frames = load_frames()
    merged = frames["merged"]
    depositie = frames["depositie"]
    piek = frames["piekbelasters"]
    dier = frames["dierrechten"]
    ontv = frames["ontvangers"]

    participants = merged[merged["iv_farm_id"].notna()].copy()
    animals_total = dier.groupby("cluster_id")["AANTAL_DIERRECHTEN"].sum()
    participant_animals = animals_total.reindex(participants["cluster_id"]).fillna(0)

    # Totals and means
    subsidy_total_all = merged["subsidie_lbv_plus"].sum()
    subsidy_mean_all = merged["subsidie_lbv_plus"].mean()
    subsidy_total_part = participants["subsidie_lbv_plus"].sum()
    subsidy_mean_part = participants["subsidie_lbv_plus"].mean()

    lbv_dep_total_all = merged["lbv_plus_tot_dep"].sum()
    lbv_dep_total_part = participants["lbv_plus_tot_dep"].sum()

    animal_total = animals_total.sum()
    animal_part = participant_animals.sum()

    cost_per_mole_all = (merged["subsidie_lbv_plus"] / merged["lbv_plus_tot_dep"]).median()
    cost_per_mole_part = (
        participants["subsidie_lbv_plus"] / participants["lbv_plus_tot_dep"]
    ).median()

    province_counts = participants["iv_Instantie_latest"].value_counts()
    stage_counts = participants["iv_stage_latest_llm"].value_counts()

    ontv_sum = ontv["Bedrag (x1000)"].sum()
    ontv_clusters = ontv["Cluster_id"].nunique()

    lines = []
    lines.append(f"# Metrics summary ({dt.date.today().isoformat()})")
    lines.append("")
    lines.append("## Coverage")
    lines.append(
        f"- Depositie table: {fmt_int(len(depositie))} rows (non-recreational locations)"
    )
    lines.append(f"- Piekbelasters: {fmt_int(len(piek))} rows (>2 500 moles N)")
    lines.append(
        f"- Merged peak polluters: {fmt_int(len(merged))} rows / "
        f"{fmt_int(merged['cluster_id'].nunique())} unique cluster_id"
    )
    lines.append(
        f"- Known permit-notice participants (iv_farm_id present): "
        f"{fmt_int(len(participants))} rows / "
        f"{fmt_int(participants['cluster_id'].nunique())} unique cluster_id"
    )
    lines.append("")
    lines.append("## Subsidy estimates (EUR)")
    lines.append(
        f"- All peak polluters: total €{fmt_float(subsidy_total_all)}; "
        f"mean €{fmt_float(subsidy_mean_all)} per location"
    )
    lines.append(
        f"- Known participants: total €{fmt_float(subsidy_total_part)}; "
        f"mean €{fmt_float(subsidy_mean_part)} per location"
    )
    lines.append("")
    lines.append("## Nitrogen deposition (moles, lbv_plus_tot_dep)")
    lines.append(f"- All peak polluters: {fmt_float(lbv_dep_total_all, 2)} moles")
    lines.append(f"- Known participants: {fmt_float(lbv_dep_total_part, 2)} moles")
    lines.append(
        f"- Share of peak-polluter deposition addressed: "
        f"{fmt_float(100 * safe_ratio(lbv_dep_total_part, lbv_dep_total_all), 2)} %"
    )
    lines.append("")
    lines.append("## Animals (AANTAL_DIERRECHTEN as proxy)")
    lines.append(f"- All peak polluters: {fmt_float(animal_total, 2)} rights")
    lines.append(f"- Known participants: {fmt_float(animal_part, 2)} rights")
    lines.append(
        f"- Share of peak-polluter animal rights addressed: "
        f"{fmt_float(100 * safe_ratio(animal_part, animal_total), 2)} %"
    )
    lines.append(
        f"- Mean rights per location: all {fmt_float(animals_total.mean(), 2)}; "
        f"participants {fmt_float(participant_animals.mean(), 2)}"
    )
    lines.append("")
    lines.append("## Cost efficiency")
    lines.append(
        f"- Median cost per mole N (participants): €{fmt_float(cost_per_mole_part, 2)} "
        f"(≈ €{fmt_float(cost_per_mole_part / 14, 2)} per gram N)"
    )
    lines.append(
        f"- Median cost per mole N (all peak polluters): €{fmt_float(cost_per_mole_all, 2)} "
        f"(≈ €{fmt_float(cost_per_mole_all / 14, 2)} per gram N)"
    )
    lines.append("")
    lines.append("## Process stage (known participants)")
    for stage, count in stage_counts.items():
        pct = safe_ratio(count, len(participants)) * 100
        lines.append(f"- {stage}: {fmt_int(count)} ({fmt_float(pct, 2)} %)")
    lines.append("")
    lines.append("## Province distribution (known participants)")
    for prov, count in province_counts.items():
        pct = safe_ratio(count, len(participants)) * 100
        lines.append(f"- {prov}: {fmt_int(count)} ({fmt_float(pct, 2)} %)")
    lines.append("")
    lines.append("## MinFin payment recipients (ontvangers)")
    lines.append(f"- Total amount: €{fmt_float(ontv_sum)}k")
    lines.append(f"- Unique clusters mapped: {fmt_int(ontv_clusters)}")
    lines.append(f"- Rows: {fmt_int(len(ontv))}")
    lines.append("")
    lines.append("## Notes")
    lines.append("- Currency assumed EUR; `Bedrag (x1000)` already in thousands.")
    lines.append("- Province name may appear as the Frisian spelling `Fryslân`.")
    lines.append("- `DIERRECHTEN_PER_STAL` in `merged` is comma-separated; totals use `AANTAL_DIERRECHTEN` from `dierrechten`.")
    lines.append("- `merged` contains one duplicate `cluster_id`; de-duplicate if needed.")

    return "\n".join(lines)


def main() -> None:
    if not WORKBOOK.exists():
        raise SystemExit(f"Workbook not found: {WORKBOOK}")
    text = compute_metrics()
    OUTPUT.write_text(text, encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
