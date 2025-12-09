"""Generate charts for permits/minfin analysis with aligned filenames."""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def category_from_row(row: pd.Series) -> str:
    if hasattr(row, "get"):
        raw = row.get("Huisvesting")
        rav_val = row.get("rav_code", "")
    else:  # allow direct string value
        raw = row
        rav_val = ""
    if pd.isna(raw):
        raw = ""
    cat = str(raw).lower().strip().replace("huisvesting", "").strip()
    cat = cat.replace("kalkoe", "kalkoenen").replace("rundve", "rundvee")
    rav = str(rav_val).strip()
    if rav.startswith("A4"):
        return "vleeskalveren"
    if cat == "rundvee":
        return "rundvee (excl. kalveren)"
    return cat


def normalize_name(val: str) -> str:
    if not isinstance(val, str):
        return ""
    s = val.lower()
    for ch in ",.;:-/\\|()[]{}'\"":
        s = s.replace(ch, " ")
    words = [w for w in s.split() if w not in {"bv", "b.v.", "vof", "v.o.f.", "stichting", "maatschap", "mts", "vennootschap", "onder", "firma"}]
    return " ".join(words).strip()


def reorder_categories(cats: list[str]) -> list[str]:
    preferred = [
        "kalkoenen",
        "kippen",
        "rundvee (excl. kalveren)",
        "vleeskalveren",
        "varken",
        "geiten",
        "eenden",
        "overig",
        "",
    ]
    seen = []
    for c in preferred:
        if c in cats and c not in seen:
            seen.append(c)
    for c in cats:
        if c not in seen:
            seen.append(c)
    return seen


def stage_counts(df: pd.DataFrame) -> dict:
    return df["stage_latest_llm"].value_counts().to_dict()


def stage_counts_with_animals(df: pd.DataFrame, year: str) -> dict:
    subset = df[(df["jaar"] == int(year)) & df["gem_aantal_dieren"].notna()]
    return subset["stage_latest_llm"].value_counts().to_dict()


def stage_counts_unique(df: pd.DataFrame) -> dict:
    """Stage counts on unique farms (latest stage per farm_id)."""
    return df.drop_duplicates("farm_id")["stage_latest_llm"].value_counts().to_dict()


def stage_counts_with_animals_unique(df: pd.DataFrame, year: str) -> dict:
    """Stage counts on unique farms that have animal numbers in the given year."""
    subset = (
        df[(df["jaar"] == int(year)) & df["gem_aantal_dieren"].notna()]
        .drop_duplicates("farm_id")
    )
    return subset["stage_latest_llm"].value_counts().to_dict()


def plot_charts(master: pd.DataFrame, output_dir: Path, year: str = "2022") -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    chart1_title = "Chart 1: Dataselectie en koppelingen"
    chart2_title = None  # set dynamically
    chart3_title = None
    chart4_title = None
    chart5_title = "Chart 5: Aandeel van nationale dieraantallen dat in de uitkoopregeling zit (%, 2022)"
    chart6_title = None
    chart7_title = None

    # colors
    chart2_purple = "#9B59B6"
    definitive_color = "#4C78A8"
    draft_color = "#F28E2B"
    label_color = "#222222"
    summary_green = "#4CAF50"
    summary_blue = definitive_color
    summary_orange = chart2_purple

    # Data selection numbers
    sources = master[["farm_id", "source"]].drop_duplicates()
    permit_total = sources[sources["source"] == "permit"]["farm_id"].nunique()
    minfin_total = sources[sources["source"] == "minfin"]["farm_id"].nunique()
    overlap = sources.groupby("farm_id")["source"].nunique()
    overlap_total = (overlap > 1).sum()
    permit_only = permit_total - overlap_total
    minfin_only = minfin_total - overlap_total
    unique_farms = sources["farm_id"].nunique()
    linked = master[(master["link_method"] != "niet_gelinkt") & master["rel_anoniem"].notna()]["farm_id"].nunique()
    pct_linked = linked / unique_farms * 100 if unique_farms else 0

    # Chart 1: Venn + pie
    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    ax0, ax1 = axes
    import matplotlib.patches as patches

    r = 1.0
    c1 = (-0.6, 0)
    c2 = (0.6, 0)
    circ1 = patches.Circle(c1, r, edgecolor=summary_orange, facecolor=summary_orange, alpha=0.4, linewidth=2)
    circ2 = patches.Circle(c2, r, edgecolor=summary_blue, facecolor=summary_blue, alpha=0.4, linewidth=2)
    ax0.add_patch(circ1)
    ax0.add_patch(circ2)
    ax0.set_xlim(-2, 2)
    ax0.set_ylim(-1.5, 1.5)
    ax0.axis("off")
    ax0.text(c1[0] - 0.2, 0, f"{permit_only}", ha="center", va="center", fontsize=12, color=label_color)
    ax0.text(c2[0] + 0.2, 0, f"{minfin_only}", ha="center", va="center", fontsize=12, color=label_color)
    ax0.text(0, 0, f"{overlap_total}", ha="center", va="center", fontsize=12, color=label_color, fontweight="bold")
    ax0.text(-1.2, 0.9, "Permit", ha="center", fontsize=11, color=label_color)
    ax0.text(1.2, 0.9, "Minfin", ha="center", fontsize=11, color=label_color)
    ax0.set_title(
        f"Bronnen: We vonden {permit_total + minfin_total} bedrijven waarvan {unique_farms} uniek.",
        fontsize=12,
    )

    methods = master.assign(link_method=master["link_method"].fillna("")).groupby("link_method")["farm_id"].nunique()
    slices = [
        ("Permit adres", methods.get("permit_adres", 0), summary_blue),
        ("Permit KVK-adres", methods.get("permit_kvk_adres", 0), "#6FA8DC"),
        ("Minfin KVK-adres", methods.get("minfin_kvk_adres", 0), summary_orange),
        ("Fosfaat naam (adres)", methods.get("fosfaat_adres", 0), summary_green),
    ]
    not_linked = methods.get("niet_gelinkt", 0)
    if not_linked:
        slices.append(("Niet gelinkt", not_linked, "#BBBBBB"))

    labels = [f"{label} ({count})" for label, count, _ in slices if count > 0]
    sizes = [count for _, count, _ in slices if count > 0]
    colors = [color for _, count, color in slices if count > 0]

    linked_total = unique_farms - not_linked
    pct_linked = linked_total / unique_farms * 100 if unique_farms else 0

    ax1.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=120, textprops={"fontsize": 10})
    ax1.set_title(
        f"Van die {unique_farms} unieke bedrijven konden we er {linked_total} ({pct_linked:.1f}%) linken aan de dataset met dieraantallen",
        fontsize=12,
    )
    ax1.text(
        0.5,
        -0.15,
        "Fosfaat naam match = gekoppeld via dezelfde bedrijfsnaam als in fosfaatbeschikkingen (met plausibiliteitscheck).",
        ha="center",
        va="center",
        fontsize=10,
        transform=ax1.transAxes,
    )
    fig.suptitle(chart1_title, fontsize=14, y=1.02)
    fig.tight_layout()
    fig.savefig(output_dir / "chart1.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Chart 2: Definitieve vergunningen per stage (counts + known animals)
    fig, ax = plt.subplots(figsize=(8, 6))
    stages = stage_counts_unique(master)
    stages_animals = stage_counts_with_animals_unique(master, year)
    total_def = stages.get("definitive_decision", 0)
    known_def = stages_animals.get("definitive_decision", 0)
    names = list(stages.keys())
    vals = [stages.get(k, 0) for k in names]
    known_vals = [stages_animals.get(k, 0) for k in names]
    names_display = [ {"definitive_decision": "Definitief besluit", "draft_decision": "Ontwerpbesluit", "receipt_of_application": "Aanvraag gedaan bij de provincie"}.get(n,n) for n in names ]
    x = range(len(names))
    without_vals = [max(t - k, 0) for t, k in zip(vals, known_vals)]
    ax.bar(x, known_vals, color=summary_green, alpha=0.9, label="Met dieraantallen")
    ax.bar(x, without_vals, bottom=known_vals, color=summary_orange, alpha=0.7, label="Zonder dieraantallen")
    ax.set_title(
        f"Chart 2: Bij {total_def} boeren is de vergunning al definitief ingetrokken,\n"
        f"van {known_def} daarvan weten we hoeveel dieren deze boeren houden.",
        fontsize=14,
    )
    ax.set_ylabel("Aantal bedrijven")
    ax.set_xticks(list(x))
    ax.set_xticklabels(names_display, rotation=20, ha="right")
    for xi, k, w in zip(x, known_vals, without_vals):
        ax.text(xi, k / 2, f"{k}", ha="center", va="center", fontsize=11, color=label_color)
        ax.text(xi, k + w / 2, f"{w}", ha="center", va="center", fontsize=11, color=label_color)
    ax.legend(fontsize=10, loc="upper left", bbox_to_anchor=(1.02, 1), frameon=False)
    fig.tight_layout()
    fig.savefig(output_dir / "chart2.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Chart 3: Animals by category
    cat_totals = (
        master.assign(huisvesting_norm=master.apply(category_from_row, axis=1))
        .loc[lambda d: (d["jaar"] == int(year)) & (d["huisvesting_norm"] != "")]
        .groupby("huisvesting_norm")["gem_aantal_dieren"]
        .sum()
        .reset_index()
    )
    animals_total = cat_totals["gem_aantal_dieren"].sum()
    order = reorder_categories(cat_totals["huisvesting_norm"].tolist())
    cat_totals["huisvesting_norm"] = pd.Categorical(cat_totals["huisvesting_norm"], categories=order, ordered=True)
    cat_totals = cat_totals.sort_values("huisvesting_norm")
    fig, ax = plt.subplots(figsize=(8, 6))
    if not cat_totals.empty:
        max_val = cat_totals["gem_aantal_dieren"].max()
        min_val = cat_totals["gem_aantal_dieren"].min()
        x = range(len(cat_totals))
        ax.bar(x, cat_totals["gem_aantal_dieren"], color=chart2_purple, width=0.7)
        ax.set_title(
            f"Chart 3:\nDe {cat_totals['gem_aantal_dieren'].count()} categorieën dieren\nhielden {int(animals_total):,} dieren",
            fontsize=14,
        )
        ax.set_ylabel("Aantal dieren (log)")
        ax.set_yscale("log")
        ax.set_ylim(bottom=max(min_val * 0.8, min_val * 0.5), top=max_val * 1.2)
        ax.set_xticks(list(x))
        ax.set_xticklabels(cat_totals["huisvesting_norm"], rotation=20, ha="right")
        for xi, v in zip(x, cat_totals["gem_aantal_dieren"]):
            text_y = v * 0.9
            va = "top"
            if v < max_val * 0.05:
                text_y = v * 1.2
                va = "bottom"
            ax.text(xi, text_y, f"{int(v):,}", ha="center", va=va, fontsize=10, color=label_color)
    else:
        ax.text(0.5, 0.5, "Geen diergegevens", ha="center", va="center")
    fig.tight_layout()
    fig.savefig(output_dir / "chart3.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Chart 4: Animals by stage and category
    stage_animals_df = master.assign(huisvesting_norm=master.apply(category_from_row, axis=1))
    stage_animals_count = len(
        stage_animals_df[
            (stage_animals_df["jaar"] == int(year))
            & (stage_animals_df["stage_latest_llm"].isin(["definitive_decision", "draft_decision"]))
            & stage_animals_df["gem_aantal_dieren"].notna()
        ]["farm_id"].unique()
    )
    stage_animals = (
        stage_animals_df
        .loc[
            lambda d: (d["jaar"] == int(year))
            & (d["stage_latest_llm"].isin(["definitive_decision", "draft_decision"]))
            & (d["huisvesting_norm"] != "")
        ]
        .groupby(["huisvesting_norm", "stage_latest_llm"])["gem_aantal_dieren"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )
    order = reorder_categories(stage_animals["huisvesting_norm"].tolist())
    stage_animals["huisvesting_norm"] = pd.Categorical(stage_animals["huisvesting_norm"], categories=order, ordered=True)
    stage_animals = stage_animals.sort_values("huisvesting_norm")
    fig, ax = plt.subplots(figsize=(8, 6))
    if not stage_animals.empty:
        categories = stage_animals["huisvesting_norm"].tolist()
        cols = ["definitive_decision", "draft_decision"]
        x_idx = range(len(categories))
        bottoms = [0] * len(categories)
        total_stage_animals = stage_animals[cols].to_numpy().sum()
        colors = [definitive_color, draft_color]
        for idx, col in enumerate(cols):
            vals = stage_animals[col].tolist()
            label = {"definitive_decision": "Definitief besluit", "draft_decision": "Ontwerpbesluit"}.get(col, col)
            ax.bar(x_idx, vals, bottom=bottoms, color=colors[idx], label=label, width=0.6)
            max_height = max([b + vv for b, vv in zip(bottoms, vals)] or [0])
            for i, v in enumerate(vals):
                if v <= 0:
                    if max_height > 0:
                        text_y = bottoms[i] + max_height * 0.02
                    else:
                        text_y = 0.01
                    ax.text(i, text_y, "0", va="bottom", ha="center", fontsize=9, color=label_color)
                    continue
                bar_total = bottoms[i] + v
                if v < max_height * 0.12:
                    text_y = bar_total + max_height * 0.05
                    va = "bottom"
                else:
                    text_y = bottoms[i] + v * 0.5
                    va = "center"
                ax.text(i, text_y, f"{int(v):,}", va=va, ha="center", fontsize=10, color=label_color)
            bottoms = [b + v for b, v in zip(bottoms, vals)]
        ax.set_title(
            f"Chart 4:\nDe {stage_animals_count} bedrijven waarvan de vergunning definitief of voorlopig is ingetrokken,\n"
            f"hielden in totaal {int(total_stage_animals):,} dieren.",
            fontsize=14,
        )
        ax.set_ylabel("Aantal dieren")
        ax.set_xticks(list(x_idx))
        ax.set_xticklabels(categories, rotation=20, ha="right")
        ax.set_ylim(0, max(bottoms) * 1.2 if bottoms else 1)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x):,}"))
        ax.legend(fontsize=9, loc="upper left", bbox_to_anchor=(1.02, 1))
    else:
        ax.text(0.5, 0.5, "Geen diergegevens", ha="center", va="center")
    fig.tight_layout()
    fig.savefig(output_dir / "chart4.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Chart 5: national vs set
    national = (
        pd.read_csv(PROCESSED_DIR / "01_FTM_animals_with_addresses.csv")
        .loc[lambda d: d["jaar"] == int(year)]
        .assign(bucket=lambda d: d.apply(category_from_row, axis=1))
        .groupby("bucket")["gem_aantal_dieren"]
        .sum()
    )
    permits = master[(master["jaar"] == int(year)) & master["gem_aantal_dieren"].notna()].assign(
        bucket=lambda d: d.apply(category_from_row, axis=1)
    )
    permit_totals = permits.groupby("bucket")["gem_aantal_dieren"].sum()
    def_totals_cmp = (
        permits[permits["stage_latest_llm"] == "definitive_decision"]
        .groupby("bucket")["gem_aantal_dieren"]
        .sum()
    )
    categories_all = reorder_categories(list(set(national.index) | set(permit_totals.index)))
    nat = national.reindex(categories_all, fill_value=0)
    per = permit_totals.reindex(categories_all, fill_value=0)
    per_def = def_totals_cmp.reindex(categories_all, fill_value=0)

    pct_all = (per / nat.replace(0, pd.NA) * 100).fillna(0)
    pct_def = (per_def / nat.replace(0, pd.NA) * 100).fillna(0)

    fig, ax = plt.subplots(figsize=(8, 6))
    x = range(len(categories_all))
    width = 0.35
    total_known = master[master["jaar"] == int(year)]["farm_id"].nunique()
    total_def = master[(master["jaar"] == int(year)) & (master["stage_latest_llm"] == "definitive_decision")]["farm_id"].nunique()
    ax.bar(
        [i - width / 2 for i in x],
        pct_all,
        width=width,
        label=f"{total_known} bedrijven waarvan bekend is dat ze zich laten uitkopen",
        color=summary_green,
    )
    ax.bar(
        [i + width / 2 for i in x],
        pct_def,
        width=width,
        label=f"{total_def} bedrijven waarvan de vergunning definitief is ingetrokken",
        color=summary_orange,
    )
    ax.set_title("Chart 5: Aandeel van nationale dieraantallen dat in de uitkoopregeling zit (%, 2022)")
    ax.set_ylabel("Percentage van nationaal totaal (%)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(categories_all, rotation=20, ha="right")
    for i, v in enumerate(pct_all):
        ax.text(i - width / 2, v + max(pct_all.max(), pct_def.max()) * 0.01, f"{v:.2f}%", ha="center", va="bottom", fontsize=9)
    for i, v in enumerate(pct_def):
        ax.text(i + width / 2, v + max(pct_all.max(), pct_def.max()) * 0.01, f"{v:.2f}%", ha="center", va="bottom", fontsize=9)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), borderaxespad=0)
    fig.tight_layout()
    fig.savefig(output_dir / "chart5.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Chart 6: farm counts by stage/category
    fig, ax = plt.subplots(figsize=(8, 6))
    df_farms_raw = master[
        (master["jaar"] == int(year)) & (master["stage_latest_llm"].isin(["definitive_decision", "draft_decision"]))
    ].copy()
    df_farms_raw["huisvesting_norm"] = df_farms_raw["Huisvesting"].apply(category_from_row)

    df_farms = df_farms_raw[df_farms_raw["huisvesting_norm"] != ""]
    if not df_farms.empty:
        farm_counts = (
            df_farms.groupby(["huisvesting_norm", "stage_latest_llm"])["farm_id"].nunique().unstack(fill_value=0)
        )
        farm_counts = farm_counts.rename(columns={"definitive_decision": "Definitief besluit", "draft_decision": "Ontwerpbesluit"}).reset_index()
        order = reorder_categories(farm_counts["huisvesting_norm"].tolist())
        farm_counts["huisvesting_norm"] = pd.Categorical(farm_counts["huisvesting_norm"], categories=order, ordered=True)
        farm_counts = farm_counts.sort_values("huisvesting_norm")
        categories_fc = farm_counts["huisvesting_norm"].tolist()
        x_fc = range(len(categories_fc))
        bottoms_fc = [0] * len(categories_fc)
        cols = [c for c in farm_counts.columns if c != "huisvesting_norm"]
        colors_fc = [definitive_color, draft_color]
        for idx, col in enumerate(cols):
            vals = farm_counts[col].tolist()
            ax.bar(x_fc, vals, bottom=bottoms_fc, color=colors_fc[idx % len(colors_fc)], width=0.6, label=col)
            max_height_fc = max([b + vv for b, vv in zip(bottoms_fc, vals)] or [0])
            for i, v in enumerate(vals):
                if v <= 0:
                    if max_height_fc > 0:
                        text_y = bottoms_fc[i] + max_height_fc * 0.02
                    else:
                        text_y = 0.01
                    ax.text(i, text_y, "0", va="bottom", ha="center", fontsize=10, color=label_color)
                    continue
                bar_total = bottoms_fc[i] + v
                if v < max_height_fc * 0.05:
                    text_y = bar_total + max_height_fc * 0.03
                    va = "bottom"
                else:
                    text_y = bottoms_fc[i] + v * 0.5
                    va = "center"
                ax.text(i, text_y, f"{int(v):,}", va=va, ha="center", fontsize=10, color=label_color)
            bottoms_fc = [b + v for b, v in zip(bottoms_fc, vals)]
        ax.set_title(
            f"Chart 6: Onder de {df_farms['farm_id'].nunique()} bedrijven die hun vergunning (bijna) ingetrokken hebben,\n"
            "zijn vooral rundvee- en varkensbedrijven.\nGemengde bedrijven hebben voor minstens twee diercategorieën meer dan 100 dieren per bedrijf.",
            fontsize=14,
        )
        ax.set_ylabel("Aantal bedrijven")
        ax.set_xticks(list(x_fc))
        ax.set_xticklabels(categories_fc, rotation=20, ha="right")
        ax.set_ylim(0, max(bottoms_fc) * 1.15 if bottoms_fc else 1)
        ax.legend(fontsize=9, loc="upper left", bbox_to_anchor=(1.02, 1))
    else:
        ax.text(0.5, 0.5, "Geen bedrijven-data", ha="center", va="center")
    fig.tight_layout()
    fig.savefig(output_dir / "chart6.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Chart 7: definitive animals by category
    def_totals = (
        master.assign(huisvesting_norm=master.apply(category_from_row, axis=1))
        .loc[lambda d: (d["jaar"] == int(year)) & (d["stage_latest_llm"] == "definitive_decision")]
        .groupby("huisvesting_norm")["gem_aantal_dieren"]
        .sum()
        .reset_index()
        .sort_values("gem_aantal_dieren", ascending=False)
    )
    fig, ax = plt.subplots(figsize=(8, 6))
    if not def_totals.empty:
        max_def = def_totals["gem_aantal_dieren"].max()
        total_def_animals = def_totals["gem_aantal_dieren"].sum()
        x_def = range(len(def_totals))
        ax.bar(x_def, def_totals["gem_aantal_dieren"], color=definitive_color, width=0.6)
        ax.set_title(
            f"Chart 7:\nDe {def_totals['gem_aantal_dieren'].count()} categorieën waarvan de vergunning definitief is ingetrokken\nhielden {int(total_def_animals):,} dieren",
            fontsize=14,
        )
        ax.set_ylabel("Aantal dieren")
        ax.set_xticks(list(x_def))
        ax.set_xticklabels(def_totals["huisvesting_norm"], rotation=20, ha="right")
        ax.set_ylim(0, max_def * 1.2)
        for i, v in enumerate(def_totals["gem_aantal_dieren"]):
            if v < max_def * 0.05:
                text_y = v + max_def * 0.03
                va = "bottom"
            else:
                text_y = v * 0.5
                va = "center"
            ax.text(i, text_y, f"{int(v):,}", va=va, ha="center", fontsize=10, color=label_color)
    else:
        ax.text(0.5, 0.5, "Geen definitieve diergegevens", ha="center", va="center")
    fig.tight_layout()
    fig.savefig(output_dir / "chart7.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    # Combined grid and PDF
    grid_files = ["chart1.png", "chart2.png", "chart3.png", "chart4.png", "chart5.png", "chart6.png", "chart7.png"]
    ncols = 3
    nrows = (len(grid_files) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(21, 18))
    axes = axes.flatten()
    for idx, fname in enumerate(grid_files):
        ax = axes[idx]
        img = plt.imread(output_dir / fname)
        ax.imshow(img)
        ax.axis("off")
    for j in range(len(grid_files), len(axes)):
        axes[j].axis("off")
    fig.tight_layout()
    fig.savefig(output_dir / "chart_all.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    pdf_path = output_dir / "charts_overview.pdf"
    with PdfPages(pdf_path) as pdf:
        for fname in grid_files:
            img = plt.imread(output_dir / fname)
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.imshow(img)
            ax.axis("off")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate charts for permits/minfin analysis.")
    parser.add_argument(
        "--master",
        type=Path,
        default=PROCESSED_DIR / "master_permits.csv",
        help="Path to master_permits.csv",
    )
    parser.add_argument("--outdir", type=Path, default=PROCESSED_DIR / "charts", help="Output directory for charts")
    parser.add_argument("--year", type=str, default="2022", help="Year to filter animal counts on")
    args = parser.parse_args()

    master = pd.read_csv(args.master)
    plot_charts(master, args.outdir, year=args.year)


if __name__ == "__main__":
    main()
