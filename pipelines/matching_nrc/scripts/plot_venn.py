"""
Plot a Venn diagram of known buyout participants:
- Permit/notice participants (iv_farm_id present in merged sheet)
- MinFin payment recipients (ontvangers sheet)

Outputs PNG to figures/venn_permits_minfin.png and echoes counts to stdout.
"""
from __future__ import annotations

import os
from pathlib import Path

# Ensure Matplotlib and fontconfig can write their config/cache
ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".mplconfig"))
os.environ.setdefault("XDG_CACHE_HOME", str(ROOT / ".cache"))

import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib_venn import venn2


WORKBOOK = ROOT / "data" / "processed" / "peak_polluters_workbook.xlsx"
OUT_DIR = ROOT / "figures"
OUT_FILE = OUT_DIR / "venn_permits_minfin.png"


def main() -> None:
    (ROOT / ".mplconfig").mkdir(exist_ok=True)
    (ROOT / ".cache" / "fontconfig").mkdir(parents=True, exist_ok=True)
    merged = pd.read_excel(WORKBOOK, sheet_name="merged", usecols=["cluster_id", "iv_farm_id"])
    ontv = pd.read_excel(WORKBOOK, sheet_name="ontvangers", usecols=["Cluster_id"])

    permit_clusters = set(merged.loc[merged["iv_farm_id"].notna(), "cluster_id"].astype(str))
    minfin_clusters = set(ontv["Cluster_id"].dropna().astype(str))

    only_permit = len(permit_clusters - minfin_clusters)
    only_minfin = len(minfin_clusters - permit_clusters)
    overlap = len(permit_clusters & minfin_clusters)

    print("Permit/notice participants:", len(permit_clusters))
    print("MinFin recipients:", len(minfin_clusters))
    print("Overlap:", overlap)

    OUT_DIR.mkdir(exist_ok=True)
    plt.figure(figsize=(6, 6))
    venn2(
        subsets=(len(permit_clusters), len(minfin_clusters), overlap),
        set_labels=("Permit/notice participants", "MinFin recipients"),
        set_colors=("#4c72b0", "#dd8452"),
        alpha=0.7,
    )
    plt.title("Overlap between permit-notice participants and MinFin recipients")
    plt.tight_layout()
    plt.savefig(OUT_FILE, dpi=300)
    plt.close()
    print(f"Wrote {OUT_FILE}")


if __name__ == "__main__":
    main()
