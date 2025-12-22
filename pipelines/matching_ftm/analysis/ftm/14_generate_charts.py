"""Generate charts for permit/minfin analysis (Venn + link methods pie)."""
from __future__ import annotations

import argparse
import datetime
from pathlib import Path
from typing import Dict, Tuple

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
from PIL import Image
import textwrap

# Repository layout
REPO_ROOT = Path(__file__).resolve().parents[4]
PIPE_ROOT = REPO_ROOT / "pipelines" / "matching_ftm"
PROCESSED_DIR = PIPE_ROOT / "data" / "processed"
CHARTS_DIR = Path(__file__).resolve().parent / "charts"
FTM_RAW_ANIMALS = PIPE_ROOT / "data" / "raw" / "FTM_dieraantallen.csv"
DEFAULT_MASTER = PROCESSED_DIR / "master_participants.csv"
DATA_YEAR = 2021

# Central chart filenames
CHART_FILES = {
    "venn": "1_chart_venn_data_sources.png",
    "link_methods": "2_chart_link_methods.png",
    "animals_by_category": "3_chart_animals_by_category.png",
    "companies_by_category": "4_chart_companies_by_category.png",
    "avg_animals_per_farm": "5_chart_avg_animals_by_category.png",
    "buyout_share": "6_chart_buyout_share.png",
    "permit_stages": "7_chart_permit_stages.png",
    "animals_by_stage": "8_chart_animals_by_stage.png",
    "definitive_progress": "9_chart_definitive_progress.png",
    "overview": "chart_all.png",
}
ALL_CHART_FILENAMES = set(CHART_FILES.values())

# Shared styling so future charts stay consistent
STYLE: Dict[str, object] = {
    "color_permit": "#9EB8D4",
    "color_permit_edge": "#7B94B5",
    "color_minfin": "#F3C19C",
    "color_minfin_edge": "#D89A68",
    "color_fosfaat": "#8ACB88",
    "color_rel": "#B39DDB",
    "color_unlinked": "#B0B0B0",
    "color_permit_kvk": "#6FA8DC",
    "text_color": "#222222",
    "subtitle_color": "#444444",
    "subtitle_fontsize": 9,
    "notes_fontsize": 8,
    "base_fontsize": 10,
    "title_fontsize": 13,
    "label_fontsize": 11,
    "tick_fontsize": 10,
    "legend_fontsize": 10,
    "figsize": (14, 8),
    "pie_figsize": (14, 8),
    "color_definitive": "#4C7B9F",
    "color_draft": "#B4C7DC",
    "radius": 1.2,
    "title_pad": 22,
    "subtitle_pad": 0.02,
    "notes_pad": 0.12,
    "bar_pad_ratio": 0.15,
    "title_wrap_width": 60,
}

SUBTITLE_TEXT = "Bron: master_permits.csv (gekoppelde permit + minfin bedrijven naar FTM dieraantallen)."
CATEGORY_DESCRIPTIONS = (
    "Categorieën: Permit adres = direct adresmatch van vergunning naar FTM; "
    "Permit KVK-adres = via KVK-lookup van vergunning; "
    "Minfin KVK-adres = via KVK-lookup minfin; "
    "Fosfaat naam/adres = fallback via fosfaatbeschikkingen; "
    "Rel-anoniem = via rel_anoniem-crosswalk; "
    "Niet gelinkt = geen dierenkoppeling gevonden."
)

def map_rav_category(code: str) -> str:
    """Map RAV-code to broad animal category."""
    if not code or code == "NAN":
        return ""
    if str(code).upper().startswith("A4"):
        return "vleeskalveren"
    if str(code).upper().startswith("A"):
        return "rundvee (excl. kalveren)"
    if str(code).upper().startswith("D"):
        return "varkens"
    if str(code).upper().startswith("E"):
        return "kippen"
    if str(code).upper().startswith("F"):
        return "kalkoenen"
    if str(code).upper().startswith("C"):
        return "geiten"
    return ""


def wrap_title(text: str, width: int | None = None) -> str:
    """Wrap long titles so they don't stretch plots."""
    wrap_width = width or int(STYLE.get("title_wrap_width", 70))
    return "\n".join(textwrap.wrap(text, width=wrap_width))


def filter_to_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Filter dataframe to the given year if a year column is present."""
    year_cols = [col for col in ("gem_jaar", "jaar") if col in df.columns]
    if not year_cols:
        return df
    col = year_cols[0]
    col_numeric = pd.to_numeric(df[col], errors="coerce")
    filtered = df[(col_numeric == year) | col_numeric.isna()]
    return filtered


def annotate_bar_tops(
    ax: plt.Axes,
    bars,
    values,
    use_log: bool = False,
    last_positions=None,
    labels=None,
    fontsize: int | None = None,
) -> None:
    """Place labels above bars with padding to avoid overlap."""
    if fontsize is None:
        fontsize = int(STYLE.get("tick_fontsize", 10))
    if last_positions is None:
        last_positions = {}

    ylim = ax.get_ylim()
    axis_range = ylim[1] - ylim[0] if ylim[1] > ylim[0] else 1
    min_gap = axis_range * 0.015

    for idx, (bar, value) in enumerate(zip(bars, values)):
        height = bar.get_height()
        if height <= 0:
            continue
        x_center = bar.get_x() + bar.get_width() / 2
        y_top = bar.get_y() + height

        if use_log:
            pad = max(y_top * 0.12, 0.5)
            target_y = y_top + pad
            if x_center in last_positions:
                target_y = max(target_y, last_positions[x_center] * 1.15)
        else:
            pad = max(height * 0.05, min_gap)
            target_y = y_top + pad
            if x_center in last_positions:
                target_y = max(target_y, last_positions[x_center] + min_gap * 1.5)

        last_positions[x_center] = target_y
        if labels:
            text = labels[idx]
        else:
            text = f"{int(value):,}".replace(",", ".")
        ax.text(
            x_center,
            target_y,
            text,
            ha="center",
            va="bottom",
            fontsize=fontsize,
            color=str(STYLE["text_color"]),
        )


def compute_source_counts(df: pd.DataFrame) -> Tuple[int, int, int, int]:
    """Return (permit_total, minfin_total, overlap, unique_total) excluding farms with no animals."""
    sources = df[df.get("has_animals", False)][["farm_id", "source"]].drop_duplicates()
    permit_total = sources[sources["source"] == "permit"]["farm_id"].nunique()
    minfin_total = sources[sources["source"] == "minfin"]["farm_id"].nunique()
    overlap = (sources.groupby("farm_id")["source"].nunique() > 1).sum()
    unique_total = sources["farm_id"].nunique()
    return permit_total, minfin_total, overlap, unique_total


def compute_source_counts_match(df: pd.DataFrame) -> Tuple[int, int, int, int]:
    """Return permit/minfin/overlap counts for matching (farms with animals only)."""
    sources = df[df.get("has_animals", False)][["farm_id", "source"]].drop_duplicates()
    permit_total = sources[sources["source"] == "permit"]["farm_id"].nunique()
    minfin_total = sources[sources["source"] == "minfin"]["farm_id"].nunique()
    overlap = (sources.groupby("farm_id")["source"].nunique() > 1).sum()
    unique_total = sources["farm_id"].nunique()
    return permit_total, minfin_total, overlap, unique_total


def compute_link_methods(df: pd.DataFrame, total_farms: int) -> Tuple[pd.Series, int]:
    """Determine one link_method per farm with animals; fill remainder as niet_gelinkt."""
    usable = df[df.get("has_animals", True)].copy()
    farms_by_method = usable.assign(link_method=usable["link_method"].fillna(""))[
        ["farm_id", "link_method"]
    ].drop_duplicates()
    priority = [
        "permit_adres",
        "permit_kvk_adres",
        "minfin_kvk_adres",
        "fosfaat_adres",
        "linked_via_rel",
        "niet_gelinkt",
    ]

    def pick_method(group: pd.DataFrame) -> str:
        methods = set(group["link_method"])
        for p in priority:
            if p in methods:
                return p
        for val in group["link_method"]:
            if val:
                return val
        return "niet_gelinkt"

    method_per_farm = farms_by_method.groupby("farm_id").apply(pick_method)
    counts = method_per_farm.value_counts()
    linked_farms = len(method_per_farm)
    missing = max(total_farms - linked_farms, 0)
    if missing > 0:
        counts["niet_gelinkt"] = counts.get("niet_gelinkt", 0) + missing
    return counts, total_farms


def compute_animal_counts(df: pd.DataFrame) -> Tuple[pd.Series, int]:
    """Sum animals on linked farms by broad category (uses master_permits source)."""
    linked_farms = set(df.loc[df["link_method"] != "niet_gelinkt", "farm_id"])
    animals = df[df["farm_id"].isin(linked_farms)][["farm_id", "rav_code", "gem_aantal_dieren"]].copy()
    animals["rav_code"] = animals["rav_code"].astype(str).str.upper()
    animals["gem_aantal_dieren"] = pd.to_numeric(animals["gem_aantal_dieren"], errors="coerce")

    category_order = [
        "kalkoenen",
        "kippen",
        "rundvee (excl. kalveren)",
        "vleeskalveren",
        "varkens",
        "geiten",
    ]

    animals["category"] = animals["rav_code"].map(map_rav_category)
    animals = animals.dropna(subset=["gem_aantal_dieren"])
    animals = animals[animals["category"] != ""]

    totals = animals.groupby("category")["gem_aantal_dieren"].sum()
    totals = totals.reindex(category_order, fill_value=0).astype(int)
    return totals, len(linked_farms)


def compute_stage_animal_counts(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Sum animals for farms with draft/definitive decision by category."""
    stage_filter = {"definitive_decision"}
    subset = df[df["stage_latest_llm"].isin(stage_filter)].copy()
    subset["rav_code"] = subset["rav_code"].astype(str).str.upper()
    subset["gem_aantal_dieren"] = pd.to_numeric(subset["gem_aantal_dieren"], errors="coerce")
    subset["category"] = subset["rav_code"].map(map_rav_category)
    subset = subset.dropna(subset=["gem_aantal_dieren"])
    subset = subset[subset["category"] != ""]
    subset = subset[subset["gem_aantal_dieren"] > 0]
    linked_farms = subset["farm_id"].nunique()

    stage_category = subset.groupby(["category", "stage_latest_llm"])["gem_aantal_dieren"].sum().unstack(fill_value=0)
    stage_category = stage_category.reindex(
        ["kalkoenen", "kippen", "rundvee (excl. kalveren)", "vleeskalveren", "varkens", "geiten"],
        fill_value=0,
    )
    stage_category = stage_category.reindex(columns=["definitive_decision", "draft_decision"], fill_value=0).astype(int)
    return stage_category, linked_farms


def build_farm_rel_map(df: pd.DataFrame) -> Dict[str, set]:
    """Map farm_id to set of rel_anoniem (linked only)."""
    rel_map: Dict[str, set] = {}
    linked = df[df["link_method"] != "niet_gelinkt"]
    for _, row in linked.iterrows():
        fid = row.get("farm_id", "")
        rel = row.get("rel_anoniem", "")
        if not fid or not rel:
            continue
        rel_map.setdefault(fid, set()).add(rel)
    return rel_map


def compute_company_categories(master_df: pd.DataFrame, raw_animals_path: Path, year: int) -> Tuple[pd.Series, int]:
    """Count linked farms per animal category; mixed if >1 categories each over threshold."""
    master_df = master_df[master_df.get("has_animals", True)]
    farm_rel_map = build_farm_rel_map(master_df)
    linked_farms = set(farm_rel_map.keys())

    raw = pd.read_csv(raw_animals_path)
    raw = raw[raw["gem_jaar"] == year]
    raw["rav_code"] = raw["rav_code"].astype(str).str.upper()
    raw["category"] = raw["rav_code"].map(map_rav_category)
    raw = raw[raw["category"] != ""]
    raw["gem_aantal_dieren"] = pd.to_numeric(raw["gem_aantal_dieren"], errors="coerce")
    raw = raw[raw["gem_aantal_dieren"] > 0]

    rel_categories: Dict[str, set] = {}
    for _, r in raw.iterrows():
        rel = r.get("rel_anoniem", "")
        if not rel:
            continue
        rel_categories.setdefault(rel, set()).add(r.get("category", ""))

    farm_categories = []
    threshold = 50
    for fid, rels in farm_rel_map.items():
        cat_set = set()
        for rel in rels:
            cat_set.update(rel_categories.get(rel, set()))
        if not cat_set:
            continue
        # sum animals per category for this farm
        farm_subset = raw[raw["rel_anoniem"].isin(rels)]
        cat_sums = farm_subset.groupby("category")["gem_aantal_dieren"].sum()
        over_threshold = {cat: val for cat, val in cat_sums.items() if val > threshold}
        if len(over_threshold) == 0:
            # fallback to max category if nothing passes threshold
            if not cat_sums.empty:
                farm_categories.append(cat_sums.idxmax())
            continue
        if len(over_threshold) == 1:
            farm_categories.append(next(iter(over_threshold)))
        else:
            farm_categories.append("Gemengde bedrijven")

    counts = pd.Series(farm_categories).value_counts()
    order = [
        "kalkoenen",
        "kippen",
        "rundvee (excl. kalveren)",
        "vleeskalveren",
        "varkens",
        "geiten",
        "Gemengde bedrijven",
    ]
    counts = counts.reindex(order, fill_value=0)
    return counts.astype(int), len(farm_categories)


def compute_avg_animals_per_farm(
    master_df: pd.DataFrame, raw_animals_path: Path, year: int
) -> Tuple[pd.Series, int, pd.Series, int]:
    """Average animals per farm by category (linked farms vs full FTM population), given year."""
    farm_rel_map = build_farm_rel_map(master_df)
    raw = pd.read_csv(raw_animals_path)
    raw = raw[raw["gem_jaar"] == year]
    raw["rav_code"] = raw["rav_code"].astype(str).str.upper()
    raw["gem_aantal_dieren"] = pd.to_numeric(raw["gem_aantal_dieren"], errors="coerce")
    raw["category"] = raw["rav_code"].map(map_rav_category)
    raw = raw.dropna(subset=["gem_aantal_dieren"])
    raw = raw[raw["category"] != ""]
    raw = raw[raw["gem_aantal_dieren"] > 0]

    category_order = [
        "kalkoenen",
        "kippen",
        "rundvee (excl. kalveren)",
        "vleeskalveren",
        "varkens",
        "geiten",
    ]

    # Linked farms (via farm_rel_map)
    farm_totals: Dict[str, Dict[str, float]] = {}
    for fid, rels in farm_rel_map.items():
        farm_subset = raw[raw["rel_anoniem"].isin(rels)]
        if farm_subset.empty:
            continue
        sums = farm_subset.groupby("category")["gem_aantal_dieren"].sum()
        farm_totals[fid] = {cat: float(val) for cat, val in sums.items()}

    farm_count = len(farm_totals)
    totals = {cat: 0.0 for cat in category_order}
    for sums in farm_totals.values():
        for cat, val in sums.items():
            totals[cat] = totals.get(cat, 0.0) + val

    if farm_count > 0:
        avg = {cat: totals.get(cat, 0.0) / farm_count for cat in category_order}
    else:
        avg = {cat: 0.0 for cat in category_order}
    linked_avg = pd.Series(avg).reindex(category_order, fill_value=0).round(1)

    # Full FTM population (per rel_anoniem)
    rel_totals: Dict[str, Dict[str, float]] = {}
    for rel, group in raw.groupby("rel_anoniem"):
        sums = group.groupby("category")["gem_aantal_dieren"].sum()
        rel_totals[str(rel)] = {cat: float(val) for cat, val in sums.items()}
    rel_count = len(rel_totals)
    rel_category_totals = {cat: 0.0 for cat in category_order}
    for sums in rel_totals.values():
        for cat, val in sums.items():
            rel_category_totals[cat] = rel_category_totals.get(cat, 0.0) + val
    if rel_count > 0:
        rel_avg = {cat: rel_category_totals.get(cat, 0.0) / rel_count for cat in category_order}
    else:
        rel_avg = {cat: 0.0 for cat in category_order}
    ftm_avg = pd.Series(rel_avg).reindex(category_order, fill_value=0).round(1)

    return linked_avg, farm_count, ftm_avg, rel_count


def compute_permit_stage_links(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-stage totals of unique permit farms and how many have a rel link."""
    permit_df = df[(df["source"] == "permit") & df.get("has_animals", True)].copy()
    year_cols = [col for col in ("gem_jaar", "jaar") if col in permit_df.columns]
    if year_cols:
        col = year_cols[0]
        permit_df = permit_df[pd.to_numeric(permit_df[col], errors="coerce") == DATA_YEAR]
    stages = ["receipt_of_application", "draft_decision", "definitive_decision"]
    rows = []
    for stage in stages:
        stage_farms = permit_df[permit_df["stage_latest_llm"] == stage]
        total = stage_farms["farm_id"].nunique()
        linked = stage_farms.loc[stage_farms["rel_anoniem"].notna(), "farm_id"].nunique()
        rows.append({"stage": stage, "total": total, "linked": linked, "unlinked": total - linked})
    return pd.DataFrame(rows).set_index("stage")


def compute_buyout_share(master_df: pd.DataFrame, raw_animals_path: Path, year: int) -> pd.DataFrame:
    """Return per-category totals and buyout totals with percentages (linked farms only, single year), farm-level.

    Counts all animal categories per farm (summing across rels), excluding zero counts.
    """
    farm_rel_map = build_farm_rel_map(master_df)
    farm_with_animals = 0

    raw = pd.read_csv(raw_animals_path)
    raw = raw[raw["gem_jaar"] == year]
    raw["rav_code"] = raw["rav_code"].astype(str).str.upper()
    raw["gem_aantal_dieren"] = pd.to_numeric(raw["gem_aantal_dieren"], errors="coerce")
    raw["category"] = raw["rav_code"].map(map_rav_category)
    raw = raw.dropna(subset=["gem_aantal_dieren"])
    raw = raw[raw["category"] != ""]
    raw = raw[raw["gem_aantal_dieren"] > 0]

    categories = ["kalkoenen", "kippen", "rundvee (excl. kalveren)", "vleeskalveren", "varkens", "geiten"]
    combined = pd.DataFrame(index=categories)
    combined["totaal"] = raw.groupby("category")["gem_aantal_dieren"].sum().reindex(categories, fill_value=0)

    farm_category_totals: Dict[str, float] = {cat: 0.0 for cat in categories}
    for rels in farm_rel_map.values():
        farm_subset = raw[raw["rel_anoniem"].isin(rels)]
        if farm_subset.empty:
            continue
        farm_with_animals += 1
        sums = farm_subset.groupby("category")["gem_aantal_dieren"].sum()
        for cat, val in sums.items():
            farm_category_totals[cat] = farm_category_totals.get(cat, 0.0) + float(val)

    combined["buyout"] = pd.Series(farm_category_totals).reindex(categories, fill_value=0)
    combined["buyout_pct"] = (combined["buyout"] / combined["totaal"].replace(0, pd.NA) * 100).fillna(0)
    combined["remaining_pct"] = (100 - combined["buyout_pct"]).clip(lower=0)
    combined.attrs["buyout_farms"] = farm_with_animals
    return combined.astype({"totaal": int, "buyout": int})


def compute_ftm_linked_animals(
    master_df: pd.DataFrame, raw_animals_path: Path, year: int
) -> Tuple[pd.Series, int, set]:
    """Sum animals on linked farms using FTM raw dataset for a given year (farm-level, collapsing multiple rels per farm)."""
    farm_rel_map = build_farm_rel_map(master_df)
    raw = pd.read_csv(raw_animals_path)
    raw = raw[raw["gem_jaar"] == year]
    raw["rav_code"] = raw["rav_code"].astype(str).str.upper()
    raw["gem_aantal_dieren"] = pd.to_numeric(raw["gem_aantal_dieren"], errors="coerce")
    raw["category"] = raw["rav_code"].map(map_rav_category)
    raw = raw.dropna(subset=["gem_aantal_dieren"])
    raw = raw[raw["category"] != ""]
    raw = raw[raw["gem_aantal_dieren"] > 0]

    category_order = [
        "kalkoenen",
        "kippen",
        "rundvee (excl. kalveren)",
        "vleeskalveren",
        "varkens",
        "geiten",
    ]

    # Sum animals per farm across all its rels
    farm_category_totals: Dict[str, float] = {cat: 0.0 for cat in category_order}
    farms_with_animals: set = set()
    for fid, rels in farm_rel_map.items():
        farm_subset = raw[raw["rel_anoniem"].isin(rels)]
        if farm_subset.empty:
            continue
        farms_with_animals.add(fid)
        sums = farm_subset.groupby("category")["gem_aantal_dieren"].sum()
        for cat, val in sums.items():
            farm_category_totals[cat] = farm_category_totals.get(cat, 0.0) + float(val)

    totals = pd.Series(farm_category_totals).reindex(category_order, fill_value=0).astype(int)
    return totals, len(farms_with_animals), farms_with_animals


def add_subtitle(fig: plt.Figure, text: str) -> None:
    """Add a centered subtitle/source line below the chart."""
    fig.text(
        0.5,
        -float(STYLE["subtitle_pad"]),
        text,
        ha="center",
        va="top",
        fontsize=float(STYLE["subtitle_fontsize"]),
        color=str(STYLE["subtitle_color"]),
    )


def add_notes(fig: plt.Figure, text: str) -> None:
    """Add a lower note/description block below the subtitle."""
    fig.text(
        0.5,
        -float(STYLE["notes_pad"]),
        text,
        ha="center",
        va="top",
        fontsize=float(STYLE["notes_fontsize"]),
        color=str(STYLE["subtitle_color"]),
        wrap=True,
    )


def plot_chart1_venn_data_sources(
    permit_total: int,
    minfin_total: int,
    overlap: int,
    unique_total: int,
    output_path: Path,
) -> None:
    """Render and save the Venn diagram with the requested Dutch title."""
    permit_only = permit_total - overlap
    minfin_only = minfin_total - overlap
    participants = permit_only + minfin_only + overlap

    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    left_center = (-0.8, 0)
    right_center = (0.8, 0)
    radius = float(STYLE["radius"])

    ax.add_patch(
        patches.Circle(
            left_center,
            radius,
            edgecolor=str(STYLE["color_permit_edge"]),
            facecolor=str(STYLE["color_permit"]),
            alpha=0.65,
            linewidth=2,
        )
    )
    ax.add_patch(
        patches.Circle(
            right_center,
            radius,
            edgecolor=str(STYLE["color_minfin_edge"]),
            facecolor=str(STYLE["color_minfin"]),
            alpha=0.65,
            linewidth=2,
        )
    )

    ax.text(
        left_center[0],
        0,
        f"{permit_only}",
        ha="center",
        va="center",
        fontsize=16,
        color=str(STYLE["text_color"]),
    )
    ax.text(
        right_center[0],
        0,
        f"{minfin_only}",
        ha="center",
        va="center",
        fontsize=16,
        color=str(STYLE["text_color"]),
    )
    ax.text(
        0,
        0,
        f"{overlap}",
        ha="center",
        va="center",
        fontsize=18,
        color=str(STYLE["text_color"]),
        fontweight="bold",
    )
    ax.text(
        left_center[0],
        radius + 0.25,
        "Vergunning ingetrokken",
        ha="center",
        va="center",
        fontsize=12,
        color=str(STYLE["text_color"]),
    )
    ax.text(
        right_center[0],
        radius + 0.25,
        "Voorschot Minfin",
        ha="center",
        va="center",
        fontsize=12,
        color=str(STYLE["text_color"]),
    )

    sentence = (
        f"We vonden {permit_only} bedrijven die bezig zijn met het intrekken van hun vergunning en "
        f"{minfin_only} bedrijven die al een voorschot hebben gekregen. "
        f"{overlap} bedrijven staan in beide datasets. "
        f"Daarmee tellen we {participants} unieke bedrijven die meedoen aan de uitkoopregeling."
    )
    ax.set_title(wrap_title(f"Chart 1: {sentence}"), fontsize=13, pad=float(STYLE["title_pad"]))
    ax.set_aspect("equal", adjustable="box")
    ax.set_xlim(-2.2, 2.2)
    ax.set_ylim(-1.6, 1.6)
    ax.axis("off")

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart2_link_methods(method_counts: pd.Series, total_farms: int, output_path: Path) -> None:
    """Pie chart for link methods with linked total in the title."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    linked_total = total_farms - method_counts.get("niet_gelinkt", 0)

    order = [
        "permit_adres",
        "permit_kvk_adres",
        "minfin_kvk_adres",
        "fosfaat_adres",
        "linked_via_rel",
        "niet_gelinkt",
    ]
    labels_map = {
        "permit_adres": "Permit adres",
        "permit_kvk_adres": "Permit KVK-adres",
        "minfin_kvk_adres": "Minfin KVK-adres",
        "fosfaat_adres": "Fosfaat naam/adres",
        "linked_via_rel": "Rel-anoniem",
        "niet_gelinkt": "Niet gelinkt",
    }
    color_map = {
        "permit_adres": str(STYLE["color_permit"]),
        "permit_kvk_adres": str(STYLE["color_permit_kvk"]),
        "minfin_kvk_adres": str(STYLE["color_minfin"]),
        "fosfaat_adres": str(STYLE["color_fosfaat"]),
        "linked_via_rel": str(STYLE["color_rel"]),
        "niet_gelinkt": str(STYLE["color_unlinked"]),
    }

    slices = []
    for key in order:
        count = int(method_counts.get(key, 0))
        if count > 0:
            slices.append((labels_map.get(key, key), count, color_map.get(key, "#CCCCCC")))

    labels = [f"{label} ({count})" for label, count, _ in slices]
    sizes = [count for _, count, _ in slices]
    colors = [color for _, _, color in slices]

    if not sizes:
        raise SystemExit("No link-method data to plot.")

    def autopct(pct: float) -> str:
        count = int(round(pct / 100.0 * total_farms))
        return f"{pct:.1f}%\n({count})"

    fig, ax = plt.subplots(figsize=STYLE["pie_figsize"])
    ax.pie(
        sizes,
        labels=labels,
        colors=colors,
        autopct=autopct,
        startangle=120,
        textprops={"fontsize": 10},
    )
    ax.set_title(
        wrap_title(
            f"Chart 2: Van de {total_farms} unieke bedrijven hebben we er {linked_total} kunnen linken aan onze dataset met dieraantallen"
        ),
        fontsize=13,
        pad=float(STYLE["title_pad"]),
    )
    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    add_notes(fig, CATEGORY_DESCRIPTIONS)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart3_animals_by_category(counts: pd.Series, linked_farms: int, output_path: Path) -> None:
    """Bar chart showing animal counts per category for linked farms."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = list(counts.index)
    values = [int(v) for v in counts.values]
    bar_heights = [max(v, 1) for v in values]  # avoid log(0) issues

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars = ax.bar(categories, bar_heights, color=str(STYLE["color_permit"]))
    ax.set_yscale("log")
    ax.set_ylabel("Aantal dieren")
    total_animals = sum(values)
    ax.set_title(
        wrap_title(
            f"Chart 3: Op deze {linked_farms} gelinkte bedrijven worden {total_animals:,} dieren gehouden".replace(
                ",", "."
            )
        ),
        fontsize=13,
        pad=float(STYLE["title_pad"]),
    )
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    annotate_bar_tops(ax, bars, values, use_log=True)

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart4_companies_by_category(counts: pd.Series, total_rels: int, output_path: Path) -> None:
    """Bar chart showing company counts per animal category (with mixed)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = list(counts.index)
    values = [int(v) for v in counts.values]

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars = ax.bar(categories, values, color=str(STYLE["color_permit"]))
    ax.set_ylabel("Aantal bedrijven")
    ax.set_title(
        wrap_title(
            f"Chart 4: Deze {sum(values):,}".replace(",", ".")
            + " bedrijven zijn voornamelijk varkenshouderijen en melkveehouderijen"
        ),
        fontsize=13,
        pad=float(STYLE["title_pad"]),
    )
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    annotate_bar_tops(ax, bars, values, use_log=False)

    fig.tight_layout()
    add_subtitle(
        fig,
        "Mixed: >1 diercategorie met elk meer dan 50 dieren; anders neemt het bedrijf de grootste categorie.",
    )
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart5_avg_animals(
    linked_avg: pd.Series, farm_count: int, ftm_avg: pd.Series, ftm_farms: int, output_path: Path
) -> None:
    """Bar chart showing average animals per farm per category (linked vs full FTM)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = list(linked_avg.index)
    linked_vals = [float(v) for v in linked_avg.values]
    ftm_vals = [float(v) for v in ftm_avg.reindex(categories, fill_value=0).values]

    x = list(range(len(categories)))
    width = 0.4

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars1 = ax.bar([i - width / 2 for i in x], linked_vals, width, label=f"Gelinkt ({farm_count} bedrijven)", color=str(STYLE["color_permit"]))
    bars2 = ax.bar([i + width / 2 for i in x], ftm_vals, width, label=f"FTM totaal ({ftm_farms} bedrijven)", color=str(STYLE["color_unlinked"]))

    ax.set_ylabel("Gemiddeld aantal dieren per bedrijf")
    title = (
        "Chart 5: Gemiddeld aantal dieren per categorie "
        f"(gelinkt {farm_count} vs. alle FTM bedrijven {ftm_farms}, jaar {DATA_YEAR})"
    )
    ax.set_title(wrap_title(title), fontsize=STYLE["title_fontsize"], pad=float(STYLE["title_pad"]))
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()
    annotate_bar_tops(
        ax,
        bars1,
        linked_vals,
        use_log=False,
        labels=[f"{v:,.1f}".replace(",", ".") for v in linked_vals],
    )
    annotate_bar_tops(
        ax,
        bars2,
        ftm_vals,
        use_log=False,
        labels=[f"{v:,.1f}".replace(",", ".") for v in ftm_vals],
    )

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart4_permit_stages(stage_df: pd.DataFrame, output_path: Path) -> None:
    """Stacked bar for permit stages with linked vs unlinked rel_anoniem."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    order = [
        ("receipt_of_application", "Ontvangst aanvraag"),
        ("draft_decision", "Ontwerpbesluit"),
        ("definitive_decision", "Definitief besluit"),
    ]
    labels = [label for _, label in order]
    linked = [int(stage_df.at[key, "linked"]) for key, _ in order]
    unlinked = [int(stage_df.at[key, "unlinked"]) for key, _ in order]
    totals = [l + u for l, u in zip(linked, unlinked)]

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars_linked = ax.bar(labels, linked, label="Dieraantallen achterhaald", color=str(STYLE["color_permit"]))
    bars_unlinked = ax.bar(
        labels,
        unlinked,
        bottom=linked,
        label="Dieraantallen niet achterhaald",
        color=str(STYLE["color_unlinked"]),
    )

    ax.set_ylabel("Aantal unieke bedrijven")
    if totals:
        max_height = max(totals)
        pad_ratio = float(STYLE["bar_pad_ratio"])
        ax.set_ylim(0, max_height * (1 + pad_ratio))
    total_farms = sum(totals)
    definitive_total = stage_df.at["definitive_decision", "total"] if "definitive_decision" in stage_df.index else 0
    pct_def = 0 if total_farms == 0 else definitive_total / total_farms * 100
    title = (
        "Chart 6: Van de "
        + f"{total_farms:,}".replace(",", ".")
        + " bedrijven die in het proces zijn de vergunning in te trekken, heeft nog maar "
        + f"{pct_def:.1f}%"
        + " een definitief besluit gekregen"
    )
    ax.set_title(wrap_title(title), fontsize=13, pad=float(STYLE["title_pad"]))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()

    # Show counts above each segment (avoids overlap even for small bars)
    positions = {}
    annotate_bar_tops(ax, bars_linked, linked, use_log=False, last_positions=positions, labels=[str(v) for v in linked])
    annotate_bar_tops(
        ax, bars_unlinked, unlinked, use_log=False, last_positions=positions, labels=[str(v) for v in unlinked]
    )

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart4_stage_animals(stage_counts: pd.DataFrame, stage_farms: int, output_path: Path) -> None:
    """Stacked bar with definitive animals per category."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = stage_counts.index.tolist()
    definitive = stage_counts["definitive_decision"].tolist()
    draft = [0 for _ in definitive]
    total_animals = sum(definitive) + sum(draft)

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bar1 = ax.bar(categories, definitive, label="Definitief besluit", color=str(STYLE["color_definitive"]))
    bar2 = ax.bar(
        categories,
        draft,
        bottom=definitive,
        label="Ontwerpbesluit",
        color=str(STYLE["color_draft"]),
    )

    ax.set_ylabel("Aantal dieren")
    sentence = (
        "Chart 7: De "
        + f"{stage_farms:,}".replace(",", ".")
        + " bedrijven van wie de vergunning definitief is ingetrokken, hielden "
        + f"{total_animals:,}".replace(",", ".")
        + " dieren. Deze stallen staan nu dus leeg."
    )
    ax.set_title(wrap_title(sentence), fontsize=13, pad=float(STYLE["title_pad"]))
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    if definitive:
        max_height = max(definitive)
        pad_ratio = float(STYLE["bar_pad_ratio"])
        ax.set_ylim(0, max_height * (1 + pad_ratio))

    # Ensure labels never overlap, even for tiny segments
    positions = {}
    annotate_bar_tops(ax, bar1, definitive, use_log=False, last_positions=positions)
    annotate_bar_tops(ax, bar2, draft, use_log=False, last_positions=positions)

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart5_buyout_share(buyout_df: pd.DataFrame, output_path: Path) -> None:
    """Per-category pies showing buyout vs remaining animals."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = buyout_df.index.tolist()
    total_buyout = int(buyout_df["buyout"].sum())
    total_farms = int(buyout_df.attrs.get("buyout_farms", 0))

    n = len(categories)
    cols = 3
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=STYLE["figsize"])
    axes = axes.flatten()

    for ax, cat in zip(axes, categories):
        total = float(buyout_df.at[cat, "totaal"])
        buyout = float(buyout_df.at[cat, "buyout"])
        remaining = max(total - buyout, 0.0)
        if total <= 0:
            ax.axis("off")
            continue
        ax.pie(
            [buyout, remaining],
            labels=[
                f"Uitgekocht {int(buyout):,}".replace(",", "."),
                f"Niet uitgekocht {int(remaining):,}".replace(",", "."),
            ],
            colors=[str(STYLE["color_permit"]), str(STYLE["color_unlinked"])],
            autopct=lambda pct: f"{pct:.1f}%",
            startangle=90,
            textprops={"fontsize": 9},
        )
        ax.set_title(f"{cat} ({int(total):,} totaal)".replace(",", "."), fontsize=10)

    # Hide unused subplots
    for ax in axes[len(categories) :]:
        ax.axis("off")

    fig.suptitle(
        wrap_title(
            f"Chart 5: Als alle {total_farms} bedrijven zich laten uitkopen, verdwijnen er "
            + f"{total_buyout:,}".replace(",", ".")
            + " dieren"
        ),
        fontsize=13,
        y=0.98,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart8_definitive_progress(
    pct_participants: float,
    pct_animals: float,
    participants_def: int,
    participants_total: int,
    animals_def: int,
    animals_total: int,
    output_path: Path,
) -> None:
    """Bar chart showing share of participants and animals with definitive decision."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    labels = ["Deelnemers", "Dieren"]
    percents = [pct_participants, pct_animals]
    counts = [
        f"{participants_def}/{participants_total}",
        f"{animals_def:,}/{animals_total:,}".replace(",", "."),
    ]
    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars = ax.bar(labels, percents, color=[str(STYLE["color_permit"]), str(STYLE["color_definitive"])])
    ax.set_ylim(0, 100)
    ax.set_ylabel("Percentage")
    ax.set_title(
        wrap_title(
            "Chart 8: Slechts "
            + f"{pct_participants:.1f}%"
            + " van de bekende deelnemers die bezig zijn met het intrekken van hun vergunning en "
            + f"{pct_animals:.1f}%"
            + " van hun dieren is inmiddels definitief uitgekocht."
        ),
        fontsize=STYLE["title_fontsize"],
        pad=float(STYLE["title_pad"]),
    )
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    annotate_bar_tops(ax, bars, percents, use_log=False, labels=[f"{p:.1f}% ({c})" for p, c in zip(percents, counts)])
    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def combine_charts(charts_dir: Path, output_name: str = "chart_all.png") -> Path:
    """Combine all chart PNGs vertically into one overview image."""
    order_keys = [
        "venn",
        "link_methods",
        "animals_by_category",
        "companies_by_category",
        "avg_animals_per_farm",
        "buyout_share",
        "permit_stages",
        "animals_by_stage",
        "definitive_progress",
    ]
    images = []
    for key in order_keys:
        path = charts_dir / CHART_FILES[key]
        if path.exists():
            images.append(Image.open(path).convert("RGB"))
    if not images:
        return charts_dir / output_name

    max_width = max(img.width for img in images)
    total_height = sum(img.height for img in images)
    combined = Image.new("RGB", (max_width, total_height), "white")
    y_offset = 0
    for img in images:
        combined.paste(img, (0, y_offset))
        y_offset += img.height

    output_path = charts_dir / output_name
    combined.save(output_path)
    return output_path


def generate_charts(master_path: Path, charts_dir: Path) -> None:
    """Wrapper to generate all charts (Venn + linking pie)."""
    # Apply shared matplotlib defaults for consistent sizing
    plt.rcParams.update(
        {
            "font.size": STYLE["base_fontsize"],
            "axes.titlesize": STYLE["title_fontsize"],
            "axes.labelsize": STYLE["label_fontsize"],
            "xtick.labelsize": STYLE["tick_fontsize"],
            "ytick.labelsize": STYLE["tick_fontsize"],
            "legend.fontsize": STYLE["legend_fontsize"],
        }
    )
    # Use dated subfolder to avoid overwriting previous runs
    today_str = datetime.date.today().strftime("%Y_%m_%d")
    charts_dir = charts_dir / today_str
    charts_dir.mkdir(parents=True, exist_ok=True)
    # Clean out old chart files in this run's folder (e.g., after renumbering)
    for png in charts_dir.glob("*.png"):
        if png.name not in ALL_CHART_FILENAMES:
            png.unlink()

    df_raw = pd.read_csv(master_path)
    # mark farms with animals flag (mutated downstream)
    if "has_animals" not in df_raw.columns:
        df_raw["has_animals"] = False
    df_year = filter_to_year(df_raw, DATA_YEAR).copy()
    df_match = df_year.copy()
    match_year_cols = [col for col in ("gem_jaar", "jaar") if col in df_match.columns]
    if match_year_cols:
        col = match_year_cols[0]
        year_numeric = pd.to_numeric(df_match[col], errors="coerce")
        df_match = df_match[year_numeric == DATA_YEAR].copy()

    # Determine farms with animals in FTM for this year (using 2021-only matching set)
    ftm_counts, ftm_linked, farms_with_animals = compute_ftm_linked_animals(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    df_match["has_animals"] = df_match["farm_id"].isin(farms_with_animals)
    df_year["has_animals"] = df_year["farm_id"].isin(farms_with_animals)
    df_raw["has_animals"] = df_raw["farm_id"].isin(farms_with_animals)

    permit_total, minfin_total, overlap, unique_total = compute_source_counts_match(df_match)
    venn_path = charts_dir / CHART_FILES["venn"]
    plot_chart1_venn_data_sources(permit_total, minfin_total, overlap, unique_total, venn_path)
    print(
        f"Saved Venn diagram to {venn_path} "
        f"(permit: {permit_total}, minfin: {minfin_total}, overlap: {overlap}, unique: {unique_total})."
    )

    # Chart 2 should use the same population as chart 1 (matching set), and then show link success
    method_counts, total_farms = compute_link_methods(df_match, unique_total)
    chart2_path = charts_dir / CHART_FILES["link_methods"]
    plot_chart2_link_methods(method_counts, total_farms, chart2_path)
    linked_total = total_farms - method_counts.get("niet_gelinkt", 0)
    print(
        f"Saved link-method pie to {chart2_path} "
        f"(linked: {linked_total}/{total_farms}, unlinked: {method_counts.get('niet_gelinkt', 0)})."
    )

    chart3_path = charts_dir / CHART_FILES["animals_by_category"]
    plot_chart3_animals_by_category(ftm_counts, ftm_linked, chart3_path)
    print(f"Saved animal category bar chart to {chart3_path} (linked farms: {ftm_linked}, source: FTM {DATA_YEAR}).")

    company_counts, company_total = compute_company_categories(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    chart4_path = charts_dir / CHART_FILES["companies_by_category"]
    plot_chart4_companies_by_category(company_counts, company_total, chart4_path)
    print(
        f"Saved company category bar chart to {chart4_path} "
        f"(companies: {company_total}, categories: {company_counts.index.tolist()})."
    )

    linked_avg, avg_farms, ftm_avg, ftm_farms = compute_avg_animals_per_farm(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    chart5a_path = charts_dir / CHART_FILES["avg_animals_per_farm"]
    plot_chart5_avg_animals(linked_avg, avg_farms, ftm_avg, ftm_farms, chart5a_path)
    print(
        f"Saved average animals per farm chart to {chart5a_path} "
        f"(linked farms: {avg_farms}, ftm farms: {ftm_farms}, categories: {linked_avg.index.tolist()})."
    )

    stage_link_df = compute_permit_stage_links(df_year)
    chart4_path = charts_dir / CHART_FILES["permit_stages"]
    plot_chart4_permit_stages(stage_link_df, chart4_path)
    print(
        f"Saved permit stage chart to {chart4_path} "
        f"(stages: {stage_link_df.index.tolist()})."
    )

    stage_counts, stage_farms = compute_stage_animal_counts(df_match)
    chart5_path = charts_dir / CHART_FILES["animals_by_stage"]
    plot_chart4_stage_animals(stage_counts, stage_farms, chart5_path)
    print(
        f"Saved stage stacked bar to {chart5_path} (farms with stage+animals: {stage_farms}, "
        f"categories: {stage_counts.index.tolist()})."
    )

    buyout_df = compute_buyout_share(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    chart6_path = charts_dir / CHART_FILES["buyout_share"]
    plot_chart5_buyout_share(buyout_df, chart6_path)
    print(
        f"Saved buyout share chart to {chart6_path} "
        f"(categories: {buyout_df.index.tolist()}, total_buyout_animals: {int(buyout_df['buyout'].sum())})."
    )

    # Chart 8: definitive progress (participants and animals)
    animals_def = int(stage_counts["definitive_decision"].sum())
    animals_total = int(buyout_df["buyout"].sum())
    participants_def = int(stage_link_df.at["definitive_decision", "total"]) if "definitive_decision" in stage_link_df.index else 0
    participants_total = int(stage_link_df["total"].sum())
    pct_participants = 0 if participants_total == 0 else participants_def / participants_total * 100
    pct_animals = 0 if animals_total == 0 else animals_def / animals_total * 100
    chart8_path = charts_dir / CHART_FILES["definitive_progress"]
    plot_chart8_definitive_progress(
        pct_participants,
        pct_animals,
        participants_def,
        participants_total,
        animals_def,
        animals_total,
        chart8_path,
    )
    print(
        f"Saved definitive progress chart to {chart8_path} "
        f"(participants: {participants_def}/{participants_total}, animals: {animals_def}/{animals_total})."
    )

    overview_path = combine_charts(charts_dir, CHART_FILES["overview"])
    print(f"Combined overview saved to {overview_path}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate charts for permit/minfin analysis.")
    parser.add_argument(
        "--master",
        type=Path,
        default=DEFAULT_MASTER,
        help="Path to master participants CSV (farm_id/source/link_method/etc).",
    )
    parser.add_argument(
        "--charts-dir",
        type=Path,
        default=CHARTS_DIR,
        help="Base directory where charts will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate_charts(args.master, args.charts_dir)


if __name__ == "__main__":
    main()
