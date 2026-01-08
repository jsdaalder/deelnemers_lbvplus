"""Generate charts for permit/minfin analysis (Venn + link methods pie)."""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path
from typing import Dict, Tuple

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.collections import PatchCollection
import pandas as pd
from PIL import Image
import textwrap

# Repository layout
REPO_ROOT = Path(__file__).resolve().parents[4]
PIPE_ROOT = REPO_ROOT / "pipelines" / "matching_ftm"
PROCESSED_DIR = PIPE_ROOT / "data" / "processed"
CHARTS_DIR = Path(__file__).resolve().parent / "charts"
FTM_RAW_ANIMALS = PIPE_ROOT / "data" / "raw" / "FTM_dieraantallen.csv"
WOONPLAATSEN_CSV = PIPE_ROOT / "data" / "raw" / "woonplaatsen.csv"
RVO_OVERVIEW_XLSX = PIPE_ROOT / "data" / "raw" / "rvo_overview_lbv_lbvplus.xlsx"
DEFAULT_MASTER = PROCESSED_DIR / "master_participants.csv"
DATA_YEAR = 2021
RVO_TOTAL_PARTICIPANTS = 988
CBS_ANIMALS = PIPE_ROOT / "data" / "raw" / "cbs_dieraantallen.csv"

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
    "province_known_vs_rvo": "10_chart_known_vs_rvo.png",
    "province_definitive_vs_rvo": "11_chart_definitive_vs_rvo.png",
    "receipt_elapsed": "12_chart_receipt_elapsed.png",
    "stage_vs_voorschot": "13_chart_stage_vs_voorschot.png",
    "buyout_share_known": "14_chart_buyout_share_known.png",
    "companies_by_category_known": "15_chart_companies_by_category_known.png",
    "receipt_vs_draft_def": "16_chart_receipt_vs_draft_def.png",
    "buyout_share_cbs": "17_chart_buyout_share_cbs.png",
    "draft_def_by_province": "18_chart_draft_def_by_province.png",
    "overview": "chart_all.png",
}
ALL_CHART_FILENAMES = set(CHART_FILES.values())

# Utility: normalize province names for filtering and filenames
def normalize_province(value: str) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("provincie ", "")
    collapsed = normalized.replace("-", " ").replace("_", " ").replace(".", " ")
    collapsed = " ".join(collapsed.split())
    aliases = {
        "fryslan": "friesland",
        "fryslân": "friesland",
        "friesland": "friesland",
        "noord brabant": "noord-brabant",
        "noordbrabant": "noord-brabant",
        "brabant": "noord-brabant",
        "n brabant": "noord-brabant",
        "zuid holland": "zuid-holland",
        "zuidholland": "zuid-holland",
        "z holland": "zuid-holland",
        "noord holland": "noord-holland",
        "noordholland": "noord-holland",
        "n holland": "noord-holland",
    }
    return aliases.get(collapsed, aliases.get(normalized, collapsed))


def slugify_label(label: str) -> str:
    return normalize_province(label).replace(" ", "_").replace("/", "_")


def load_woonplaatsen_map(csv_path: Path) -> dict[str, list[str]]:
    """Return mapping from normalized place name to one or more province names."""
    if not csv_path.exists():
        return {}
    try:
        df = pd.read_csv(csv_path, sep=";", skiprows=5, header=None, names=["plaats", "gemeente", "provincie"])
    except Exception:
        return {}
    df = df.dropna(subset=["plaats", "provincie"])
    df["plaats_norm"] = df["plaats"].astype(str).str.strip().str.lower()
    df["prov_clean"] = df["provincie"].astype(str).str.strip()
    place_map: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        place = row["plaats_norm"]
        prov = row["prov_clean"]
        if not place or not prov:
            continue
        provs = place_map.setdefault(place, [])
        if prov not in provs:
            provs.append(prov)
    return place_map


# Shared styling so future charts stay consistent
STYLE: Dict[str, object] = {
    "color_permit": "#9EB8D4",
    "color_permit_edge": "#7B94B5",
    "color_minfin": "#F3C19C",
    "color_minfin_edge": "#D89A68",
    "color_fosfaat": "#8ACB88",
    "color_rel": "#B39DDB",
    "color_unlinked": "#B0B0B0",
    "color_no_process": "#D9D9D9",
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
    year_cols = [col for col in ("jaar", "gem_jaar") if col in df.columns]
    if not year_cols:
        return df
    if len(year_cols) == 1:
        col = year_cols[0]
        col_numeric = pd.to_numeric(df[col], errors="coerce")
        return df[(col_numeric == year) | col_numeric.isna()]

    col_a = pd.to_numeric(df[year_cols[0]], errors="coerce")
    col_b = pd.to_numeric(df[year_cols[1]], errors="coerce")
    mask = (col_a == year) | (col_b == year) | (col_a.isna() & col_b.isna())
    return df[mask]


def parse_day_month_year(value: str) -> datetime.date | None:
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


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
    """Return permit/minfin/overlap counts for all farms (regardless of animals)."""
    sources = df[["farm_id", "source"]].drop_duplicates()
    permit_total = sources[sources["source"] == "permit"]["farm_id"].nunique()
    minfin_total = sources[sources["source"] == "minfin"]["farm_id"].nunique()
    overlap = (sources.groupby("farm_id")["source"].nunique() > 1).sum()
    unique_total = sources["farm_id"].nunique()
    return permit_total, minfin_total, overlap, unique_total


def compute_receipt_elapsed_days(df: pd.DataFrame, ref_date: datetime.date) -> Tuple[pd.Series, dict]:
    """Return days since receipt for permit farms with receipt stage, plus summary stats."""
    receipt = df[(df["source"] == "permit") & (df["stage_latest_llm"] == "receipt_of_application")].copy()
    receipt = receipt.drop_duplicates(subset=["farm_id"])
    receipt["parsed_date"] = receipt["Datum_latest"].apply(parse_day_month_year)
    receipt = receipt[receipt["parsed_date"].notna()].copy()
    receipt["days_elapsed"] = receipt["parsed_date"].apply(lambda d: (ref_date - d).days)
    days = receipt["days_elapsed"].astype(int)
    stats = {
        "farms_total": int(len(receipt)),
        "avg_days": float(days.mean()) if not days.empty else 0.0,
        "min_days": int(days.min()) if not days.empty else 0,
        "max_days": int(days.max()) if not days.empty else 0,
        "ref_date": ref_date.isoformat(),
    }
    return days, stats


def compute_link_methods(df: pd.DataFrame, total_farms: int) -> Tuple[pd.Series, int]:
    """Determine one link_method per farm; tag linked-without-animals separately."""
    usable = df.copy()
    farms_by_method = usable.assign(link_method=usable["link_method"].fillna(""))[["farm_id", "link_method", "has_animals"]].drop_duplicates()
    priority = [
        "permit_adres",
        "permit_kvk_adres",
        "minfin_kvk_adres",
        "fosfaat_adres",
        "linked_via_rel",
        "niet_gelinkt",
    ]

    def pick_method(methods: list[str]) -> str:
        method_set = set(methods)
        for p in priority:
            if p in method_set:
                return p
        for val in methods:
            if val:
                return val
        return "niet_gelinkt"

    method_per_farm = farms_by_method.groupby("farm_id")["link_method"].agg(list).reset_index()
    method_per_farm["link_method"] = method_per_farm["link_method"].apply(pick_method)
    # annotate has_animals per farm
    animals_flag = usable.groupby("farm_id")["has_animals"].max().reset_index().rename(columns={"has_animals": "has_animals_flag"})
    method_per_farm = method_per_farm.merge(animals_flag, on="farm_id", how="left")
    method_per_farm["link_method"] = method_per_farm.apply(
        lambda r: "gelinkt_zonder_dieren"
        if not r.get("has_animals_flag", False) and r["link_method"] != "niet_gelinkt"
        else r["link_method"],
        axis=1,
    )
    counts = method_per_farm["link_method"].value_counts()
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


def compute_stage_animal_counts(
    df: pd.DataFrame, raw_animals_path: Path = FTM_RAW_ANIMALS, year: int = DATA_YEAR
) -> Tuple[pd.DataFrame, int]:
    """Sum animals for definitive decision farms using linked rels (farm-level, raw FTM)."""
    stage_filter = {"definitive_decision"}
    stage_df = df[df["stage_latest_llm"].isin(stage_filter)]
    rel_map = build_farm_rel_map(stage_df)

    if not rel_map:
        empty = pd.DataFrame(
            0,
            index=["kalkoenen", "kippen", "rundvee (excl. kalveren)", "vleeskalveren", "varkens", "geiten"],
            columns=["definitive_decision", "draft_decision"],
        )
        return empty, 0

    raw = pd.read_csv(raw_animals_path)
    raw = raw[raw["gem_jaar"] == year]
    raw["rav_code"] = raw["rav_code"].astype(str).str.upper()
    raw["gem_aantal_dieren"] = pd.to_numeric(raw["gem_aantal_dieren"], errors="coerce")
    raw = raw[raw["gem_aantal_dieren"] > 0]
    raw["category"] = raw["rav_code"].map(map_rav_category)
    raw = raw[raw["category"] != ""]

    rows = []
    farms_with_animals = set()
    for fid, rels in rel_map.items():
        if not rels:
            continue
        subset_raw = raw[raw["rel_anoniem"].astype(str).isin(rels)]
        if subset_raw.empty:
            continue
        farms_with_animals.add(fid)
        sums = subset_raw.groupby("category")["gem_aantal_dieren"].sum()
        for cat, val in sums.items():
            rows.append({"category": cat, "stage_latest_llm": "definitive_decision", "gem_aantal_dieren": val})

    if rows:
        fallback_df = pd.DataFrame(rows)
        stage_category = (
            fallback_df.groupby(["category", "stage_latest_llm"])["gem_aantal_dieren"].sum().unstack(fill_value=0)
        )
    else:
        stage_category = pd.DataFrame()

    stage_category = stage_category.reindex(
        ["kalkoenen", "kippen", "rundvee (excl. kalveren)", "vleeskalveren", "varkens", "geiten"],
        fill_value=0,
    )
    stage_category = stage_category.reindex(columns=["definitive_decision", "draft_decision"], fill_value=0).astype(int)
    # Count farms with known rels, even if animal totals are zero for the year.
    return stage_category, len(rel_map)


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


def filter_by_province(df: pd.DataFrame, province: str) -> pd.DataFrame:
    """Return only rows matching the given province (case-insensitive)."""
    if "Province" not in df.columns:
        return df.iloc[0:0]
    target = normalize_province(province)
    norm = df["Province"].apply(normalize_province)
    return df.loc[norm == target].copy()


def attach_province(df: pd.DataFrame, place_to_province: dict[str, list[str]]) -> pd.DataFrame:
    """Set Province from woonplaatsen map, disambiguating with Instantie_latest when needed."""
    if not place_to_province:
        return df

    place_cols = ["B_PLAATS", "kvk_api_plaats"]

    def resolve(row) -> str:
        for col in place_cols:
            if col not in row:
                continue
            val = row.get(col, "")
            if pd.isna(val) or not str(val).strip():
                continue
            norm = normalize_province(str(val))
            provs = place_to_province.get(norm, [])
            if not provs:
                continue
            if len(provs) == 1:
                return provs[0]
            inst = row.get("Instantie_latest", "")
            inst_norm = normalize_province(str(inst))
            for prov in provs:
                if normalize_province(prov) == inst_norm:
                    return prov
            return provs[0]
        return ""

    df = df.copy()
    df["Province"] = df.apply(resolve, axis=1)
    return df


def plot_province_definitive_bar(counts: dict[str, int], output_path: Path) -> None:
    """Horizontal bar chart of definitive decisions per province."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not counts:
        print("[warn] No definitive counts per province; skipping province chart.")
        return
    # Sort descending
    items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    provinces = [name for name, _ in items]
    values = [val for _, val in items]

    fig, ax = plt.subplots(figsize=(8, 6))
    y_pos = range(len(provinces))
    bars = ax.barh(y_pos, values, color=str(STYLE["color_definitive"]))
    ax.set_yticks(y_pos)
    ax.set_yticklabels(provinces)
    ax.invert_yaxis()
    ax.set_xlabel("Aantal definitieve besluiten")
    ax.set_title(wrap_title("Chart 10: Definitieve besluiten per provincie"), fontsize=STYLE["title_fontsize"], pad=float(STYLE["title_pad"]))
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    annotate_bar_tops(ax, bars, values, use_log=False, labels=[str(v) for v in values])
    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def compute_rvo_comparison(master_df: pd.DataFrame, rvo_path: Path) -> pd.DataFrame:
    """Return dataframe with RVO participants, definitive counts, and known participants per province."""
    if not rvo_path.exists():
        return pd.DataFrame(columns=["province", "rvo_participants", "definitive", "known", "pct_def", "pct_known"])

    # Counts by province from master (Province already attached from woonplaatsen)
    df_def = master_df[master_df["stage_latest_llm"] == "definitive_decision"].copy()
    df_def["prov_norm"] = df_def["Province"].apply(normalize_province)
    def_counts = df_def.groupby("prov_norm")["farm_id"].nunique().to_dict()

    df_known = master_df[
        master_df["stage_latest_llm"].isin(["receipt_of_application", "draft_decision", "definitive_decision"])
    ].copy()
    df_known["prov_norm"] = df_known["Province"].apply(normalize_province)
    known_counts = df_known.groupby("prov_norm")["farm_id"].nunique().to_dict()

    rvo = pd.read_excel(rvo_path)
    if "Actuele_deelnemers" not in rvo.columns:
        return pd.DataFrame(columns=["province", "rvo_participants", "definitive", "known", "pct_def", "pct_known"])

    # First column holds province name (uppercase)
    prov_col = rvo.columns[0]
    rvo["prov_norm"] = rvo[prov_col].apply(normalize_province)
    rvo["rvo_participants"] = pd.to_numeric(rvo["Actuele_deelnemers"], errors="coerce").fillna(0).astype(int)

    rows = []
    for _, row in rvo.iterrows():
        prov_name = str(row[prov_col]).strip().title()
        norm_name = row["prov_norm"]
        total = int(row["rvo_participants"])
        definitive = int(def_counts.get(norm_name, 0))
        known = int(known_counts.get(norm_name, 0))
        pct_def = (definitive / total * 100) if total else 0.0
        pct_known = (known / total * 100) if total else 0.0
        rows.append(
            {
                "province": prov_name,
                "rvo_participants": total,
                "definitive": definitive,
                "known": known,
                "remaining_def": max(total - definitive, 0),
                "remaining_known": max(total - known, 0),
                "pct_def": pct_def,
                "pct_known": pct_known,
            }
        )

    df = pd.DataFrame(rows)
    df = df[df["rvo_participants"] > 0]
    df = df[~df["province"].str.contains("totaal", case=False, na=False)]
    df = df[~df["province"].str.contains("nan", case=False, na=False)]
    df = df.sort_values("rvo_participants", ascending=False)
    return df


def plot_province_definitive_vs_rvo(df: pd.DataFrame, output_path: Path) -> None:
    """Stacked bar chart: definitive vs remaining RVO participants per province."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        print("[warn] RVO comparison data empty; skipping province bar chart.")
        return

    labels = df["province"].tolist()
    definitive = df["definitive"].tolist()
    remaining = df["remaining_def"].tolist()
    totals = df["rvo_participants"].tolist()
    pct = df["pct_def"].tolist()

    y = range(len(labels))
    fig, ax = plt.subplots(figsize=(8, 6))
    bars_remaining = ax.barh(y, remaining, color=str(STYLE["color_unlinked"]), label="Nog niet definitief")
    bars_def = ax.barh(y, definitive, left=remaining, color=str(STYLE["color_definitive"]), label="Definitief")

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Aantal deelnemers (RVO)")
    # Province with highest definitive count (fallback to highest RVO participants if all zero)
    top_prov = ""
    if df["definitive"].max() > 0:
        top_prov = df.loc[df["definitive"].idxmax(), "province"]
    else:
        top_prov = df.loc[df["rvo_participants"].idxmax(), "province"]
    title = (
        f"Chart 11: In {top_prov} zijn de meeste vergunningen al definitief ingetrokken, "
        "terwijl Limburg er nog weinig heeft."
    )
    ax.set_title(wrap_title(title), fontsize=STYLE["title_fontsize"], pad=float(STYLE["title_pad"]))
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.legend(loc="lower right")

    # annotate definitive segment with pct/ratio
    for bar_def, bar_rem, p, d, t in zip(bars_def, bars_remaining, pct, definitive, totals):
        x = bar_rem.get_width() + bar_def.get_width() + 1.0
        y_pos = bar_def.get_y() + bar_def.get_height() / 2
        ax.text(x, y_pos, f"{p:.1f}% ({d}/{t})", va="center", ha="left", fontsize=9, color="#111111")

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT + "\nIn november waren er nog 1023 deelnemers, dat de RVO op verzoek van FTM uitsplitste per provincie. In december daalde het totaal aantal deelnemers verder tot 988. De RVO kon dat getal voor publicatie van dit verhaal niet uitsplitsen.")
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_province_known_vs_rvo(df: pd.DataFrame, output_path: Path) -> None:
    """Stacked bar chart: known (receipt+draft+definitive) vs remaining RVO participants per province."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        print("[warn] RVO comparison data empty; skipping province known chart.")
        return

    labels = df["province"].tolist()
    known = df["known"].tolist()
    remaining = df["remaining_known"].tolist()
    totals = df["rvo_participants"].tolist()
    pct = df["pct_known"].tolist()
    total_participants = sum(totals)
    known_total = sum(known)
    pct_all = (known_total / total_participants * 100) if total_participants else 0.0

    y = range(len(labels))
    fig, ax = plt.subplots(figsize=(8, 6))
    bars_remaining = ax.barh(y, remaining, color=str(STYLE["color_unlinked"]), label="Nog niet gekoppeld")
    bars_known = ax.barh(y, known, left=remaining, color=str(STYLE["color_permit"]), label="Bekend (ontvangst/ontwerp/definitief)")

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Aantal deelnemers (RVO)")
    title = (
        "Chart 10: Van alle deelnemers is "
        + f"{pct_all:.1f}%"
        + " al begonnen met het intrekken van de vergunning. Vooral in Limburg zijn boeren daar al ver mee."
    )
    ax.set_title(wrap_title(title), fontsize=STYLE["title_fontsize"], pad=float(STYLE["title_pad"]))
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.legend(loc="lower right")

    for bar_known, bar_rem, p, k, t in zip(bars_known, bars_remaining, pct, known, totals):
        x = bar_rem.get_width() + bar_known.get_width() + 1.0
        y_pos = bar_known.get_y() + bar_known.get_height() / 2
        ax.text(x, y_pos, f"{p:.1f}% ({k}/{t})", va="center", ha="left", fontsize=9, color="#111111")

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT + "\nIn november waren er nog 1023 deelnemers, dat de RVO op verzoek van FTM uitsplitste per provincie. In december daalde het totaal aantal deelnemers verder tot 988. De RVO kon dat getal voor publicatie van dit verhaal niet uitsplitsen.")
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


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
    master_df: pd.DataFrame, raw_animals_path: Path, year: int, rel_filter: set | None = None
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
    raw_full = raw
    if rel_filter:
        raw_full = raw_full[raw_full["rel_anoniem"].astype(str).isin(rel_filter)]
    for rel, group in raw_full.groupby("rel_anoniem"):
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
    """Return per-stage totals of unique permit farms and how many have a rel link.

    Uses the latest known stage per farm (based on Datum_latest, then stage rank).
    """
    latest_per_farm = select_latest_permit_stage(df, DATA_YEAR)
    stages = ["receipt_of_application", "draft_decision", "definitive_decision"]
    rows = []
    for stage in stages:
        stage_farms = latest_per_farm[latest_per_farm["stage_latest_llm"] == stage]
        total = stage_farms["farm_id"].nunique()
        linked = stage_farms.loc[stage_farms["rel_anoniem"].notna(), "farm_id"].nunique()
        rows.append({"stage": stage, "total": total, "linked": linked, "unlinked": total - linked})
    return pd.DataFrame(rows).set_index("stage")


def select_latest_permit_stage(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return one latest permit row per farm for the given year."""
    permit_df = df[df["source"] == "permit"].copy()
    permit_df = filter_to_year(permit_df, year)
    if permit_df.empty:
        return permit_df
    permit_df["parsed_date"] = permit_df["Datum_latest"].apply(parse_day_month_year)
    stage_rank = {"receipt_of_application": 0, "draft_decision": 1, "definitive_decision": 2}
    permit_df["stage_rank"] = permit_df["stage_latest_llm"].map(stage_rank).fillna(-1).astype(int)
    min_date = pd.Timestamp.min
    permit_df["parsed_date"] = permit_df["parsed_date"].fillna(min_date)
    permit_df = permit_df.sort_values(["farm_id", "parsed_date", "stage_rank"])
    return permit_df.groupby("farm_id", as_index=False).tail(1)


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


def compute_buyout_share_for_farms(
    master_df: pd.DataFrame, raw_animals_path: Path, year: int, farm_ids: set
) -> pd.DataFrame:
    """Return per-category totals and buyout totals for a specific farm subset."""
    farm_rel_map = build_farm_rel_map(master_df)
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

    farm_with_animals = 0
    farm_category_totals: Dict[str, float] = {cat: 0.0 for cat in categories}
    for fid in farm_ids:
        rels = farm_rel_map.get(fid)
        if not rels:
            continue
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


def load_cbs_totals(cbs_path: Path) -> dict[str, int]:
    """Load CBS totals per category (with poultry grouped into 'kippen')."""
    if not cbs_path.exists():
        return {}
    df = pd.read_csv(cbs_path)
    label_col = df.columns[0]
    total_col = df.columns[2]
    totals: dict[str, int] = {}
    for _, row in df.iterrows():
        label = str(row.get(label_col, "")).strip()
        value = row.get(total_col)
        if not label or label == "nan":
            continue
        try:
            total = int(float(str(value).strip()))
        except ValueError:
            continue
        if label == "Graasdieren|Aantal dieren|Rundvee|Rundvee, totaal":
            totals["rundvee_totaal"] = total
        elif label == "Graasdieren|Aantal dieren|Geiten|Geiten, totaal":
            totals["geiten"] = total
        elif label == "Hokdieren|Aantal dieren|Varkens|Varkens, totaal":
            totals["varkens"] = total
        elif label == "Hokdieren|Aantal dieren|Kalkoenen":
            totals["kalkoenen"] = total
        elif label in {
            "Hokdieren|Aantal dieren|Kippen|Kippen, totaal",
            "Hokdieren|Aantal dieren|Overig pluimvee",
        }:
            totals["kippen"] = totals.get("kippen", 0) + total
    return totals


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


def plot_chart2_link_methods(
    method_counts: pd.Series, total_farms: int, output_path: Path, linked_without_animals: int | None = None
) -> None:
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

    sizes_iter = iter(sizes)

    def autopct(pct: float) -> str:
        try:
            count = next(sizes_iter)
        except StopIteration:
            count = round(pct / 100.0 * total_farms)
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
    linked_with_animals = linked_total - (linked_without_animals or 0)
    title = (
        f"Chart 2: Van de {total_farms} unieke bedrijven hebben we er "
        f"{linked_with_animals} kunnen linken aan onze dataset met dieraantallen "
        + (f"(+{linked_without_animals} zonder dieren)" if linked_without_animals else "")
    )
    ax.set_title(wrap_title(title), fontsize=13, pad=float(STYLE["title_pad"]))
    fig.tight_layout()
    extra = ""
    if linked_without_animals is not None and linked_without_animals > 0:
        extra = f"\nVan de {linked_total} gelinkte bedrijven hebben {linked_without_animals} geen dieren in FTM {DATA_YEAR}."
    add_subtitle(fig, SUBTITLE_TEXT + extra)
    add_notes(fig, CATEGORY_DESCRIPTIONS)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart3_animals_by_category(
    counts: pd.Series, linked_farms: int, output_path: Path, linked_without_animals: int | None = None
) -> None:
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
    extra = ""
    if linked_without_animals is not None and linked_without_animals > 0:
        extra = f"\nEr zijn {linked_without_animals} gelinkte bedrijven zonder dieren in FTM {DATA_YEAR}."
    add_subtitle(fig, SUBTITLE_TEXT + extra)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart4_companies_by_category(
    counts: pd.Series,
    total_rels: int,
    output_path: Path,
    title_override: str | None = None,
    subtitle_extra: str | None = None,
    subtitle_base: str | None = None,
) -> None:
    """Bar chart showing company counts per animal category (with mixed)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = list(counts.index)
    values = [int(v) for v in counts.values]

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars = ax.bar(categories, values, color=str(STYLE["color_permit"]))
    ax.set_ylabel("Aantal bedrijven")
    if title_override:
        title = title_override
    else:
        title = (
            f"Chart 4: Deze {sum(values):,}".replace(",", ".")
            + " bedrijven zijn voornamelijk varkenshouderijen en melkveehouderijen"
        )
    ax.set_title(wrap_title(title), fontsize=13, pad=float(STYLE["title_pad"]))
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    annotate_bar_tops(ax, bars, values, use_log=False)

    fig.tight_layout()
    subtitle = subtitle_base or "Mixed: >1 diercategorie met elk meer dan 50 dieren; anders neemt het bedrijf de grootste categorie."
    if subtitle_extra:
        subtitle = subtitle + f"\n{subtitle_extra}"
    add_subtitle(fig, subtitle)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart16_receipt_vs_draft_def(
    counts: dict[str, int],
    output_path: Path,
) -> None:
    """Bar chart comparing receipt-only farms vs draft/definitive farms."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    labels = list(counts.keys())
    values = [int(counts[label]) for label in labels]

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars = ax.bar(labels, values, color=str(STYLE["color_permit"]))
    ax.set_ylabel("Aantal bedrijven")
    ax.set_title(
        wrap_title(
            f"Chart 16: Ontvangst vs ontwerp/definitief ({sum(values):,} bedrijven)".replace(",", ".")
        ),
        fontsize=13,
        pad=float(STYLE["title_pad"]),
    )
    ax.tick_params(axis="x", rotation=10)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    annotate_bar_tops(ax, bars, values, use_log=False)

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart18_draft_def_by_province(counts: pd.DataFrame, output_path: Path) -> None:
    """Stacked bar chart of latest-stage draft vs definitive farms per province."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    provinces = list(counts.index)
    draft = [int(v) for v in counts.get("draft_decision", pd.Series(index=counts.index, data=0)).tolist()]
    definitive = [int(v) for v in counts.get("definitive_decision", pd.Series(index=counts.index, data=0)).tolist()]

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars_draft = ax.bar(provinces, draft, color=str(STYLE["color_permit"]), label="Ontwerpbesluit")
    bars_def = ax.bar(
        provinces,
        definitive,
        bottom=draft,
        color=str(STYLE["color_minfin"]),
        label="Definitief besluit",
    )
    ax.set_ylabel("Aantal bedrijven")
    ax.set_title(
        wrap_title(
            "Chart 18: Ontwerp- of definitief besluit (laatste status) per provincie"
        ),
        fontsize=13,
        pad=float(STYLE["title_pad"]),
    )
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()

    totals = [d + df for d, df in zip(draft, definitive)]
    annotate_bar_tops(ax, bars_def, totals, use_log=False)
    for x, d, df in zip(range(len(provinces)), draft, definitive):
        if d > 0:
            ax.text(x, d / 2, str(d), ha="center", va="center", fontsize=9, color=str(STYLE["text_color"]))
        if df > 0:
            ax.text(
                x,
                d + df / 2,
                str(df),
                ha="center",
                va="center",
                fontsize=9,
                color=str(STYLE["text_color"]),
            )

    fig.tight_layout()
    add_subtitle(fig, "Bron: RVO en officielebekendmakingen.nl.")
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart5_avg_animals(
    linked_avg: pd.Series,
    farm_count: int,
    ftm_avg: pd.Series,
    ftm_farms: int,
    output_path: Path,
    region_label: str | None = None,
) -> None:
    """Bar chart showing average animals per farm per category (linked vs full FTM)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = list(linked_avg.index)
    linked_vals = [float(v) for v in linked_avg.values]
    ftm_vals = [float(v) for v in ftm_avg.reindex(categories, fill_value=0).values]

    x = list(range(len(categories)))
    width = 0.4

    region_suffix = f" ({region_label})" if region_label else ""
    linked_label = f"Gelinkt {region_label}" if region_label else "Gelinkt"
    ftm_label = f"FTM totaal {region_label}" if region_label else "FTM totaal"
    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars1 = ax.bar(
        [i - width / 2 for i in x],
        linked_vals,
        width,
        label=f"{linked_label} ({farm_count} bedrijven)",
        color=str(STYLE["color_permit"]),
    )
    bars2 = ax.bar(
        [i + width / 2 for i in x],
        ftm_vals,
        width,
        label=f"{ftm_label} ({ftm_farms} bedrijven)",
        color=str(STYLE["color_unlinked"]),
    )

    ax.set_ylabel("Gemiddeld aantal dieren per bedrijf")
    title = (
        f"Chart 5{region_suffix}: Gemiddeld aantal dieren per categorie "
        f"(gelinkt {farm_count} vs. FTM {ftm_farms}, jaar {DATA_YEAR})"
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


def plot_chart4_permit_stages(
    stage_df: pd.DataFrame,
    output_path: Path,
    total_participants: int | None = None,
    subtitle_extra: str | None = None,
    subtitle_base: str | None = None,
) -> None:
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
    not_in_process = None
    if total_participants is not None:
        not_in_process = max(int(total_participants) - sum(totals), 0)
        labels = ["Nog geen intrekkingsproces"] + labels

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    positions = list(range(len(labels)))
    stage_positions = positions[1:] if not_in_process is not None else positions
    bars_no_process = None
    if not_in_process is not None:
        bars_no_process = ax.bar(
            [positions[0]],
            [not_in_process],
            label="Nog geen intrekkingsproces",
            color=str(STYLE["color_no_process"]),
            edgecolor=str(STYLE["text_color"]),
        )
    bars_linked = ax.bar(
        stage_positions,
        linked,
        label="Dieraantallen achterhaald",
        color=str(STYLE["color_permit"]),
    )
    bars_unlinked = ax.bar(
        stage_positions,
        unlinked,
        bottom=linked,
        label="Dieraantallen niet achterhaald",
        color=str(STYLE["color_unlinked"]),
    )

    ax.set_ylabel("Aantal unieke bedrijven")
    if totals:
        max_height = max(totals + ([not_in_process] if not_in_process is not None else []))
        pad_ratio = float(STYLE["bar_pad_ratio"])
        ax.set_ylim(0, max_height * (1 + pad_ratio))
    total_farms = sum(totals)
    definitive_total = stage_df.at["definitive_decision", "total"] if "definitive_decision" in stage_df.index else 0
    total_participants = int(total_participants or total_farms)
    pct_def = 0 if total_participants == 0 else definitive_total / total_participants * 100
    title = (
        "Chart 7: Van alle "
        + f"{total_participants:,}".replace(",", ".")
        + " deelnemers, heeft nog maar "
        + f"{pct_def:.1f}%"
        + " de vergunning definitief in laten trekken"
    )
    ax.set_title(wrap_title(title), fontsize=13, pad=float(STYLE["title_pad"]))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.legend()

    # Show counts above each segment (avoids overlap even for small bars)
    positions = {}
    if bars_no_process is not None:
        annotate_bar_tops(
            ax,
            bars_no_process,
            [not_in_process],
            use_log=False,
            last_positions=positions,
            labels=[str(not_in_process)],
        )
    annotate_bar_tops(ax, bars_linked, linked, use_log=False, last_positions=positions, labels=[str(v) for v in linked])
    annotate_bar_tops(
        ax, bars_unlinked, unlinked, use_log=False, last_positions=positions, labels=[str(v) for v in unlinked]
    )

    fig.tight_layout()
    subtitle = subtitle_base or SUBTITLE_TEXT
    if subtitle_extra:
        subtitle = subtitle + f"\n{subtitle_extra}"
    add_subtitle(fig, subtitle)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart13_stage_vs_voorschot(stage_counts: pd.Series, output_path: Path) -> None:
    """Bar chart: total draft+definitive vs those with non-zero animal counts."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    draft_count = int(stage_counts.get("draft_decision", 0))
    definitive_count = int(stage_counts.get("definitive_decision", 0))
    nonzero_count = int(stage_counts.get("nonzero_animals", 0))
    combined = draft_count + definitive_count

    labels = ["Ontwerp/definitief (totaal)", "Ontwerp/definitief met dieren (>0)"]
    values = [combined, nonzero_count]

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bars = ax.bar(labels, values, color=str(STYLE["color_permit"]))
    ax.set_ylabel("Aantal bedrijven")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.tick_params(axis="x", rotation=10)
    annotate_bar_tops(ax, bars, values, use_log=False)

    title = (
        "Chart 13: Van de "
        + f"{combined:,}".replace(",", ".")
        + " bedrijven met een ontwerp- of definitief besluit, zijn er "
        + f"{nonzero_count:,}".replace(",", ".")
        + " met niet-nul dieraantallen"
    )
    ax.set_title(wrap_title(title), fontsize=13, pad=float(STYLE["title_pad"]))
    fig.tight_layout()
    subtitle = (
        SUBTITLE_TEXT
        + "\\nOntwerpbesluit betekent dat het ontwerp ter inzage ligt; definitief besluit betekent dat het besluit is genomen."
        + f" Ontwerpbesluit: {draft_count}, definitief besluit: {definitive_count}."
    )
    add_subtitle(fig, subtitle)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_chart4_stage_animals(
    stage_counts: pd.DataFrame,
    stage_farms: int,
    total_definitive_farms: int,
    output_path: Path,
    region_label: str | None = None,
) -> None:
    """Stacked bar with definitive animals per category."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    categories = stage_counts.index.tolist()
    definitive = stage_counts["definitive_decision"].tolist()
    draft = [0 for _ in definitive]
    total_animals = sum(definitive) + sum(draft)
    region_suffix = f" ({region_label})" if region_label else ""
    region_clause = f" in {region_label}" if region_label else ""

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
        f"Chart 8{region_suffix}: Van de "
        + f"{total_definitive_farms:,}".replace(",", ".")
        + f" bedrijven die gestopt zijn{region_clause}, weten we van "
        + f"{stage_farms:,}".replace(",", ".")
        + " het aantal dieren. Die "
        + f"{stage_farms:,}".replace(",", ".")
        + " bedrijven houden samen "
        + f"{total_animals:,}".replace(",", ".")
        + " dieren."
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


def plot_chart5_buyout_share(
    buyout_df: pd.DataFrame,
    output_path: Path,
    title_override: str | None = None,
    subtitle_extra: str | None = None,
    subtitle_base: str | None = None,
) -> None:
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

    if title_override:
        title = title_override
    else:
        title = (
            f"Chart 6: Als alle {total_farms} bedrijven zich laten uitkopen, verdwijnen er "
            + f"{total_buyout:,}".replace(",", ".")
            + " dieren"
        )
    fig.suptitle(wrap_title(title), fontsize=13, y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    subtitle = subtitle_base or SUBTITLE_TEXT
    if subtitle_extra:
        subtitle = subtitle + f"\n{subtitle_extra}"
    add_subtitle(fig, subtitle)
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
            "Chart 9: Slechts "
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


def plot_chart_receipt_elapsed(days: pd.Series, stats: dict, output_path: Path) -> None:
    """Histogram of days since receipt of application for receipt-only permit farms."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if days.empty:
        print("[warn] No receipt-only farms with valid dates; skipping receipt chart.")
        return

    fig, ax = plt.subplots(figsize=STYLE["figsize"])
    bins = list(range(0, max(days.max(), 1) + 61, 60))
    if bins[-1] < days.max():
        bins.append(days.max() + 1)
    counts, edges, bars = ax.hist(
        days, bins=bins, color=str(STYLE["color_permit"]), edgecolor="#ffffff", alpha=0.8
    )

    avg_days = stats.get("avg_days", 0.0)
    min_days = stats.get("min_days", 0)
    max_days = stats.get("max_days", 0)
    farms_total = stats.get("farms_total", 0)
    ref_date = stats.get("ref_date", "")

    ax.axvline(avg_days, color=str(STYLE["color_definitive"]), linestyle="--", linewidth=2, label="Gemiddelde")
    ax.set_xlabel("Aantal dagen sinds ontvangst")
    ax.set_ylabel("Aantal bedrijven")
    title = (
        "Chart 12: Voor "
        + f"{farms_total:,}".replace(",", ".")
        + " bedrijven met alleen een ontvangstmelding is gemiddeld "
        + f"{avg_days:.1f}"
        + " dagen verstreken (min "
        + f"{min_days}"
        + ", max "
        + f"{max_days}"
        + ")."
    )
    ax.set_title(wrap_title(title), fontsize=STYLE["title_fontsize"], pad=float(STYLE["title_pad"]))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    # Label each bar with its count.
    positions = {}
    annotate_bar_tops(ax, bars, counts, use_log=False, last_positions=positions, labels=[str(int(c)) for c in counts])
    ax.legend(fontsize=STYLE["legend_fontsize"])

    fig.tight_layout()
    add_subtitle(fig, SUBTITLE_TEXT + f"\nPeildatum: {ref_date}.")
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
        "receipt_elapsed",
        "stage_vs_voorschot",
        "buyout_share_known",
        "companies_by_category_known",
        "receipt_vs_draft_def",
        "buyout_share_cbs",
        "draft_def_by_province",
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


def generate_charts(
    master_path: Path,
    charts_dir: Path,
    regions: list[dict[str, object]] | None = None,
) -> None:
    """Wrapper to generate all charts (Venn + linking pie) for national + optional regions."""
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
    if "farm_id_new" in df_raw.columns and df_raw["farm_id_new"].notna().any():
        # Prefer stable IDs for chart aggregation but keep fallback to legacy IDs when missing.
        df_raw["farm_id_legacy"] = df_raw.get("farm_id")
        df_raw["farm_id"] = df_raw["farm_id_new"].where(df_raw["farm_id_new"].notna(), df_raw["farm_id"])
    place_map = load_woonplaatsen_map(WOONPLAATSEN_CSV)
    df_raw = attach_province(df_raw, place_map)
    # mark farms with animals flag (mutated downstream)
    if "has_animals" not in df_raw.columns:
        df_raw["has_animals"] = False
    df_year = filter_to_year(df_raw, DATA_YEAR).copy()
    df_match = df_year.copy()

    # Determine farms with animals in FTM for this year (using 2021-only matching set)
    ftm_counts, ftm_linked, farms_with_animals = compute_ftm_linked_animals(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    df_match["has_animals"] = df_match["farm_id"].isin(farms_with_animals)
    df_year["has_animals"] = df_year["farm_id"].isin(farms_with_animals)
    df_raw["has_animals"] = df_raw["farm_id"].isin(farms_with_animals)

    # --- Compute all metrics first
    permit_total, minfin_total, overlap, unique_total = compute_source_counts_match(df_raw)
    method_counts, total_farms = compute_link_methods(df_match, unique_total)
    linked_total = total_farms - method_counts.get("niet_gelinkt", 0)
    linked_without_animals = max(linked_total - ftm_linked, 0)

    company_counts, company_total = compute_company_categories(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    linked_avg, avg_farms, ftm_avg, ftm_farms = compute_avg_animals_per_farm(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    stage_link_df = compute_permit_stage_links(df_year)
    stage_counts, stage_farms = compute_stage_animal_counts(df_match, FTM_RAW_ANIMALS, DATA_YEAR)
    total_definitive_farms = df_match[df_match["stage_latest_llm"] == "definitive_decision"]["farm_id"].nunique()
    receipt_days, receipt_stats = compute_receipt_elapsed_days(df_year, datetime.date.today())
    buyout_df = compute_buyout_share(df_match, FTM_RAW_ANIMALS, DATA_YEAR)

    # Chart 13: draft/definitive vs voorschot received (exclude receipt-only farms).
    permit_farms = select_latest_permit_stage(df_match, DATA_YEAR)
    permit_farms["Province"] = permit_farms.get("Province").fillna("Onbekend")
    known_with_animals = permit_farms[permit_farms["has_animals"]].copy()
    nonzero_animals_count = int(
        permit_farms[
            permit_farms["stage_latest_llm"].isin({"draft_decision", "definitive_decision"})
            & (permit_farms["has_animals"])
        ]["farm_id"].nunique()
    )
    stage_vs_voorschot_counts = {
        "draft_decision": int(stage_link_df.at["draft_decision", "total"])
        if "draft_decision" in stage_link_df.index
        else 0,
        "definitive_decision": int(stage_link_df.at["definitive_decision", "total"])
        if "definitive_decision" in stage_link_df.index
        else 0,
        "nonzero_animals": nonzero_animals_count,
    }
    chart14_df = known_with_animals[known_with_animals["stage_latest_llm"].isin({"draft_decision", "definitive_decision"})]
    chart14_farm_ids = set(chart14_df["farm_id"].dropna().unique())
    receipt_only_df = permit_farms[permit_farms["stage_latest_llm"] == "receipt_of_application"].copy()
    receipt_only_farm_ids = set(receipt_only_df["farm_id"].dropna().unique())
    chart14_zero_animals = int(
        permit_farms[
            permit_farms["stage_latest_llm"].isin({"draft_decision", "definitive_decision"})
            & (permit_farms["rel_anoniem"].notna())
            & (~permit_farms["has_animals"])
        ]["farm_id"].nunique()
    )
    buyout_known_df = compute_buyout_share_for_farms(df_match, FTM_RAW_ANIMALS, DATA_YEAR, chart14_farm_ids)
    cbs_totals = load_cbs_totals(CBS_ANIMALS)
    buyout_cbs_df = buyout_known_df.copy()
    rundvee_total = int(cbs_totals.get("rundvee_totaal", 0))
    buyout_cbs_combined = pd.DataFrame(
        index=["kalkoenen", "kippen", "rundvee (incl. kalveren)", "varkens", "geiten"]
    )
    buyout_kalkoenen = int(buyout_cbs_df.at["kalkoenen", "buyout"]) if "kalkoenen" in buyout_cbs_df.index else 0
    buyout_kippen = int(buyout_cbs_df.at["kippen", "buyout"]) if "kippen" in buyout_cbs_df.index else 0
    buyout_rundvee = int(buyout_cbs_df.at["rundvee (excl. kalveren)", "buyout"]) if "rundvee (excl. kalveren)" in buyout_cbs_df.index else 0
    buyout_vleeskalveren = int(buyout_cbs_df.at["vleeskalveren", "buyout"]) if "vleeskalveren" in buyout_cbs_df.index else 0
    buyout_varkens = int(buyout_cbs_df.at["varkens", "buyout"]) if "varkens" in buyout_cbs_df.index else 0
    buyout_geiten = int(buyout_cbs_df.at["geiten", "buyout"]) if "geiten" in buyout_cbs_df.index else 0
    buyout_cbs_combined["buyout"] = [
        buyout_kalkoenen,
        buyout_kippen,
        buyout_rundvee + buyout_vleeskalveren,
        buyout_varkens,
        buyout_geiten,
    ]
    buyout_cbs_combined["totaal"] = [
        int(cbs_totals.get("kalkoenen", 0)),
        int(cbs_totals.get("kippen", 0)),
        rundvee_total,
        int(cbs_totals.get("varkens", 0)),
        int(cbs_totals.get("geiten", 0)),
    ]
    buyout_cbs_combined["buyout_pct"] = (
        buyout_cbs_combined["buyout"] / buyout_cbs_combined["totaal"].replace(0, pd.NA) * 100
    ).fillna(0)
    buyout_cbs_combined["remaining_pct"] = (100 - buyout_cbs_combined["buyout_pct"]).clip(lower=0)
    buyout_cbs_combined.attrs["buyout_farms"] = int(buyout_known_df.attrs.get("buyout_farms", 0))
    company_counts_known, company_total_known = compute_company_categories(
        df_match[df_match["farm_id"].isin(chart14_farm_ids)],
        FTM_RAW_ANIMALS,
        DATA_YEAR,
    )
    draft_def_by_province = (
        permit_farms[permit_farms["stage_latest_llm"].isin({"draft_decision", "definitive_decision"})]
        .groupby(["Province", "stage_latest_llm"])["farm_id"]
        .nunique()
        .unstack(fill_value=0)
        .sort_values(by=["definitive_decision", "draft_decision"], ascending=False)
    )

    animals_def = int(stage_counts["definitive_decision"].sum())
    animals_total = int(buyout_df["buyout"].sum())
    participants_def = int(stage_link_df.at["definitive_decision", "total"]) if "definitive_decision" in stage_link_df.index else 0
    participants_total = int(stage_link_df["total"].sum())
    pct_participants = 0 if participants_total == 0 else participants_def / participants_total * 100
    pct_animals = 0 if animals_total == 0 else animals_def / animals_total * 100

    rvo_comp = compute_rvo_comparison(df_year, RVO_OVERVIEW_XLSX)

    # Regional metrics (for later plotting)
    regions = regions or [
        {"name": "Gelderland", "filter": lambda df: filter_by_province(df, "Gelderland"), "subdir": "gelderland", "label": "Gelderland"},
    ]
    region_data: Dict[str, dict] = {}
    for region in regions:
        name = region.get("name")
        filtr = region.get("filter")
        subdir = region.get("subdir")
        label = region.get("label") or name

        df_reg = df_match if filtr is None else filtr(df_match)
        if df_reg.empty:
            continue
        buyout_reg = compute_buyout_share(df_reg, FTM_RAW_ANIMALS, DATA_YEAR)
        stage_counts_p, stage_farms_p = compute_stage_animal_counts(df_reg, FTM_RAW_ANIMALS, DATA_YEAR)
        total_definitive_reg = df_reg[df_reg["stage_latest_llm"] == "definitive_decision"]["farm_id"].nunique()
        region_data[name] = {
            "subdir": subdir,
            "label": label,
            "buyout": buyout_reg.reset_index().rename(columns={"index": "category"}).to_dict(orient="records"),
            "buyout_farms": int(buyout_reg.attrs.get("buyout_farms", 0)),
            "stage_animals": stage_counts_p.reset_index().rename(columns={"index": "category"}).to_dict(orient="records"),
            "stage_farms": int(stage_farms_p),
            "total_definitive_farms": int(total_definitive_reg),
        }

    # Pack metrics into one shareable file
    chart_data = {
        "meta": {
            "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "data_year": DATA_YEAR,
            "master_path": str(master_path),
        },
        "chart1": {
            "permit_total": int(permit_total),
            "minfin_total": int(minfin_total),
            "overlap": int(overlap),
            "unique_total": int(unique_total),
        },
        "chart2": {
            "method_counts": {k: int(v) for k, v in method_counts.items()},
            "total_farms": int(total_farms),
            "linked_total": int(linked_total),
            "linked_without_animals": int(linked_without_animals),
        },
        "chart3": {
            "counts": {k: int(v) for k, v in ftm_counts.items()},
            "linked_farms": int(ftm_linked),
            "linked_without_animals": int(linked_without_animals),
        },
        "chart4": {"counts": {k: int(v) for k, v in company_counts.items()}, "total_companies": int(company_total)},
        "chart5": {
            "linked_avg": {k: float(v) for k, v in linked_avg.items()},
            "ftm_avg": {k: float(v) for k, v in ftm_avg.items()},
            "linked_farms": int(avg_farms),
            "ftm_farms": int(ftm_farms),
        },
        "chart6": {
            "buyout": buyout_df.reset_index().rename(columns={"index": "category"}).to_dict(orient="records"),
            "buyout_farms": int(buyout_df.attrs.get("buyout_farms", 0)),
        },
        "chart7": {
            "stages": stage_link_df.reset_index().rename(columns={"index": "stage"}).to_dict(orient="records"),
            "total_participants": int(RVO_TOTAL_PARTICIPANTS),
        },
        "chart8": {
            "stage_animals": stage_counts.reset_index().rename(columns={"index": "category"}).to_dict(orient="records"),
            "stage_farms": int(stage_farms),
            "total_definitive_farms": int(total_definitive_farms),
        },
        "chart9": {
            "participants_def": int(participants_def),
            "participants_total": int(participants_total),
            "animals_def": int(animals_def),
            "animals_total": int(animals_total),
            "pct_participants": float(pct_participants),
            "pct_animals": float(pct_animals),
        },
        "chart13": {"stage_vs_voorschot": stage_vs_voorschot_counts},
        "chart14": {
            "buyout_known": buyout_known_df.reset_index().rename(columns={"index": "category"}).to_dict(orient="records"),
            "buyout_farms": int(buyout_known_df.attrs.get("buyout_farms", 0)),
            "zero_animals": int(chart14_zero_animals),
        },
        "chart15": {
            "counts": {k: int(v) for k, v in company_counts_known.items()},
            "total_companies": int(company_total_known),
        },
        "chart16": {
            "counts": {
                "Ontvangst (alleen)": int(len(receipt_only_farm_ids)),
                "Ontwerp/definitief": int(len(chart14_farm_ids)),
            }
        },
        "chart17": {
            "buyout_cbs": buyout_cbs_combined.reset_index().rename(columns={"index": "category"}).to_dict(orient="records"),
            "buyout_farms": int(buyout_cbs_combined.attrs.get("buyout_farms", 0)),
        },
        "chart18": {
            "counts": {
                prov: {stage: int(val) for stage, val in row.items()}
                for prov, row in draft_def_by_province.iterrows()
            }
        },
        "chart12": {
            "receipt_days": receipt_days.tolist(),
            "receipt_stats": receipt_stats,
        },
        "rvo_comparison": rvo_comp.to_dict(orient="records"),
        "regions": region_data,
    }
    chart_data_path = charts_dir / "chart_data.json"
    chart_data_path.write_text(json.dumps(chart_data, indent=2, ensure_ascii=False))
    print(f"Saved chart data to {chart_data_path} (all numbers used by the charts).")

    # Reload the shareable data and generate charts from that file
    chart_data = json.loads(chart_data_path.read_text())

    c1 = chart_data["chart1"]
    permit_total = int(c1["permit_total"])
    minfin_total = int(c1["minfin_total"])
    overlap = int(c1["overlap"])
    unique_total = int(c1["unique_total"])

    venn_path = charts_dir / CHART_FILES["venn"]
    plot_chart1_venn_data_sources(permit_total, minfin_total, overlap, unique_total, venn_path)
    print(
        f"Saved Venn diagram to {venn_path} "
        f"(permit: {permit_total}, minfin: {minfin_total}, overlap: {overlap}, unique: {unique_total})."
    )

    c2 = chart_data["chart2"]
    method_counts = pd.Series(c2["method_counts"])
    total_farms = int(c2["total_farms"])
    linked_total = int(c2.get("linked_total", total_farms - method_counts.get("niet_gelinkt", 0)))
    linked_without_animals = int(c2.get("linked_without_animals", 0))
    chart2_path = charts_dir / CHART_FILES["link_methods"]
    plot_chart2_link_methods(method_counts, total_farms, chart2_path, linked_without_animals=linked_without_animals)
    print(
        f"Saved link-method pie to {chart2_path} "
        f"(linked: {linked_total}/{total_farms}, unlinked: {method_counts.get('niet_gelinkt', 0)})."
    )

    c3 = chart_data["chart3"]
    ftm_counts = pd.Series(c3["counts"])
    ftm_linked = int(c3["linked_farms"])
    linked_without_animals = int(c3.get("linked_without_animals", 0))
    chart3_path = charts_dir / CHART_FILES["animals_by_category"]
    plot_chart3_animals_by_category(ftm_counts, ftm_linked, chart3_path, linked_without_animals=linked_without_animals)
    print(f"Saved animal category bar chart to {chart3_path} (linked farms: {ftm_linked}, source: FTM {DATA_YEAR}).")

    c4 = chart_data["chart4"]
    company_counts = pd.Series(c4["counts"])
    company_total = int(c4["total_companies"])
    chart4_path = charts_dir / CHART_FILES["companies_by_category"]
    plot_chart4_companies_by_category(company_counts, company_total, chart4_path)
    print(
        f"Saved company category bar chart to {chart4_path} "
        f"(companies: {company_total}, categories: {company_counts.index.tolist()})."
    )

    c5 = chart_data["chart5"]
    linked_avg = pd.Series(c5["linked_avg"])
    ftm_avg = pd.Series(c5["ftm_avg"])
    avg_farms = int(c5["linked_farms"])
    ftm_farms = int(c5["ftm_farms"])
    chart5a_path = charts_dir / CHART_FILES["avg_animals_per_farm"]
    plot_chart5_avg_animals(linked_avg, avg_farms, ftm_avg, ftm_farms, chart5a_path)
    print(
        f"Saved average animals per farm chart to {chart5a_path} "
        f"(linked farms: {avg_farms}, ftm farms: {ftm_farms}, categories: {linked_avg.index.tolist()})."
    )

    c6 = chart_data["chart6"]
    buyout_df = pd.DataFrame(c6["buyout"]).set_index("category")
    buyout_df.attrs["buyout_farms"] = int(c6.get("buyout_farms", 0))
    chart6_path = charts_dir / CHART_FILES["buyout_share"]
    plot_chart5_buyout_share(buyout_df, chart6_path)
    print(
        f"Saved buyout share chart to {chart6_path} "
        f"(categories: {buyout_df.index.tolist()}, total_buyout_animals: {int(buyout_df['buyout'].sum())})."
    )

    c7 = chart_data["chart7"]
    zero_animals_known = int(chart_data.get("chart14", {}).get("zero_animals", 0))
    stage_link_df = pd.DataFrame(c7["stages"]).set_index("stage")
    chart4_path = charts_dir / CHART_FILES["permit_stages"]
    subtitle_zero = f"{zero_animals_known} gelinkte ontwerp/definitieve bedrijven hebben 0 dieren in FTM {DATA_YEAR}."
    plot_chart4_permit_stages(
        stage_link_df,
        chart4_path,
        total_participants=int(c7.get("total_participants", 0)),
        subtitle_extra=subtitle_zero,
        subtitle_base="Bron: RVO en officielebekendmakingen.nl.",
    )
    print(
        f"Saved permit stage chart to {chart4_path} "
        f"(stages: {stage_link_df.index.tolist()})."
    )

    c8 = chart_data["chart8"]
    stage_counts = pd.DataFrame(c8["stage_animals"]).set_index("category")
    stage_farms = int(c8["stage_farms"])
    chart8_path = charts_dir / CHART_FILES["animals_by_stage"]
    total_definitive_farms = int(c8.get("total_definitive_farms", 0))
    plot_chart4_stage_animals(stage_counts, stage_farms, total_definitive_farms, chart8_path)
    print(
        f"Saved stage stacked bar to {chart8_path} (farms with stage+animals: {stage_farms}, "
        f"categories: {stage_counts.index.tolist()})."
    )

    c9 = chart_data["chart9"]
    chart8_path = charts_dir / CHART_FILES["definitive_progress"]
    plot_chart8_definitive_progress(
        float(c9["pct_participants"]),
        float(c9["pct_animals"]),
        int(c9["participants_def"]),
        int(c9["participants_total"]),
        int(c9["animals_def"]),
        int(c9["animals_total"]),
        chart8_path,
    )
    print(
        f"Saved definitive progress chart to {chart8_path} "
        f"(participants: {c9['participants_def']}/{c9['participants_total']}, animals: {c9['animals_def']}/{c9['animals_total']})."
    )

    c13 = chart_data.get("chart13", {})
    stage_vs_voorschot = pd.Series(c13.get("stage_vs_voorschot", {}))
    chart13_path = charts_dir / CHART_FILES["stage_vs_voorschot"]
    plot_chart13_stage_vs_voorschot(stage_vs_voorschot, chart13_path)
    print(f"Saved stage vs voorschot chart to {chart13_path}.")

    c14 = chart_data.get("chart14", {})
    buyout_known_df = pd.DataFrame(c14.get("buyout_known", [])).set_index("category")
    buyout_known_df.attrs["buyout_farms"] = int(c14.get("buyout_farms", 0))
    zero_animals_known = int(c14.get("zero_animals", 0))
    chart14_path = charts_dir / CHART_FILES["buyout_share_known"]
    total_buyout_known = int(buyout_known_df["buyout"].sum()) if not buyout_known_df.empty else 0
    total_farms_known = int(buyout_known_df.attrs.get("buyout_farms", 0))
    total_buyout_million = total_buyout_known / 1_000_000
    title_known = (
        "Van "
        + f"{total_farms_known} boeren die hun vergunning in hebben laten trekken, konden we de dieraantallen achterhalen. Zij hielden "
        + f"{total_buyout_million:.1f}".replace(".", ",")
        + " miljoen dieren"
    )
    subtitle_known = f"We vonden ook {zero_animals_known} boeren met 0 dieren in de FTM-gegevens."
    plot_chart5_buyout_share(
        buyout_known_df,
        chart14_path,
        title_override=title_known,
        subtitle_extra=subtitle_known,
    )
    print(
        f"Saved buyout share chart (known subset) to {chart14_path} "
        f"(categories: {buyout_known_df.index.tolist()}, total_buyout_animals: {int(buyout_known_df['buyout'].sum())})."
    )

    c15 = chart_data.get("chart15", {})
    company_counts_known = pd.Series(c15.get("counts", {}))
    company_total_known = int(c15.get("total_companies", 0))
    chart15_path = charts_dir / CHART_FILES["companies_by_category_known"]
    title_known_companies = (
        f"Chart 15: Deze {company_total_known:,}".replace(",", ".")
        + " bedrijven zijn voornamelijk varkenshouderijen en melkveehouderijen"
    )
    subtitle_known_companies = (
        "Mixed: >1 diercategorie met elk meer dan 50 dieren; anders neemt het bedrijf de grootste categorie."
        "\nBron: RVO en officielebekendmakingen.nl."
    )
    plot_chart4_companies_by_category(
        company_counts_known,
        company_total_known,
        chart15_path,
        title_override=title_known_companies,
        subtitle_extra=subtitle_zero,
        subtitle_base=subtitle_known_companies,
    )
    print(
        f"Saved company category bar chart (known subset) to {chart15_path} "
        f"(companies: {company_total_known}, categories: {company_counts_known.index.tolist()})."
    )

    c16 = chart_data.get("chart16", {})
    chart16_counts = {k: int(v) for k, v in c16.get("counts", {}).items()}
    chart16_path = charts_dir / CHART_FILES["receipt_vs_draft_def"]
    plot_chart16_receipt_vs_draft_def(chart16_counts, chart16_path)
    print(f"Saved receipt vs draft/definitive chart to {chart16_path}.")

    c17 = chart_data.get("chart17", {})
    buyout_cbs_df = pd.DataFrame(c17.get("buyout_cbs", [])).set_index("category")
    buyout_cbs_df.attrs["buyout_farms"] = int(c17.get("buyout_farms", 0))
    chart17_path = charts_dir / CHART_FILES["buyout_share_cbs"]
    total_buyout_cbs = int(buyout_cbs_df["buyout"].sum()) if not buyout_cbs_df.empty else 0
    total_farms_cbs = int(buyout_cbs_df.attrs.get("buyout_farms", 0))
    total_buyout_cbs_million = total_buyout_cbs / 1_000_000
    title_cbs = (
        "Chart 17: "
        + f"{total_farms_cbs} bedrijven, ".replace(",", ".")
        + f"{total_buyout_cbs_million:.1f}".replace(".", ",")
        + " miljoen dieren (CBS totaal)"
    )
    plot_chart5_buyout_share(
        buyout_cbs_df,
        chart17_path,
        title_override=title_cbs,
        subtitle_extra=subtitle_zero,
        subtitle_base="Bron: RVO, officielebekendmakingen.nl en CBS.",
    )
    print(
        f"Saved buyout share chart (CBS totals) to {chart17_path} "
        f"(categories: {buyout_cbs_df.index.tolist()}, total_buyout_animals: {int(buyout_cbs_df['buyout'].sum())})."
    )

    c18 = chart_data.get("chart18", {})
    draft_def_counts = pd.DataFrame.from_dict(c18.get("counts", {}), orient="index")
    chart18_path = charts_dir / CHART_FILES["draft_def_by_province"]
    plot_chart18_draft_def_by_province(draft_def_counts, chart18_path)
    print(f"Saved draft/def by province chart to {chart18_path}.")

    rvo_comp_loaded = pd.DataFrame(chart_data.get("rvo_comparison", []))
    rvo_def_chart = charts_dir / CHART_FILES["province_definitive_vs_rvo"]
    plot_province_definitive_vs_rvo(rvo_comp_loaded, rvo_def_chart)
    if not rvo_comp_loaded.empty:
        print(f"Saved province definitive vs RVO chart to {rvo_def_chart}.")

    rvo_known_chart = charts_dir / CHART_FILES["province_known_vs_rvo"]
    plot_province_known_vs_rvo(rvo_comp_loaded, rvo_known_chart)
    if not rvo_comp_loaded.empty:
        print(f"Saved province known vs RVO chart to {rvo_known_chart}.")

    c12 = chart_data.get("chart12", {})
    receipt_days = pd.Series(c12.get("receipt_days", []))
    receipt_stats = c12.get("receipt_stats", {})
    receipt_path = charts_dir / CHART_FILES["receipt_elapsed"]
    plot_chart_receipt_elapsed(receipt_days, receipt_stats, receipt_path)
    if not receipt_days.empty:
        print(f"Saved receipt elapsed chart to {receipt_path}.")

    overview_path = combine_charts(charts_dir, CHART_FILES["overview"])
    print(f"Combined overview saved to {overview_path}.")

    # Regional variants (from stored data)
    base_charts_dir = charts_dir
    for name, reg_data in chart_data.get("regions", {}).items():
        subdir = reg_data.get("subdir")
        label = reg_data.get("label") or name
        charts_out = base_charts_dir if subdir is None else base_charts_dir / subdir
        charts_out.mkdir(parents=True, exist_ok=True)

        buyout_reg = pd.DataFrame(reg_data["buyout"]).set_index("category")
        buyout_reg.attrs["buyout_farms"] = int(reg_data.get("buyout_farms", 0))
        buyout_filename = CHART_FILES["buyout_share"] if subdir is None else f"6_chart_buyout_share_{slugify_label(label)}.png"
        buyout_path = charts_out / buyout_filename
        plot_chart5_buyout_share(buyout_reg, buyout_path)
        print(
            f"[region] Saved buyout share chart for {name} to {buyout_path} "
            f"(categories: {buyout_reg.index.tolist()}, total_buyout_animals: {int(buyout_reg['buyout'].sum())})."
        )

        stage_counts_p = pd.DataFrame(reg_data["stage_animals"]).set_index("category")
        stage_farms_p = int(reg_data["stage_farms"])
        total_definitive_reg = int(reg_data.get("total_definitive_farms", 0))
        stage_filename = CHART_FILES["animals_by_stage"] if subdir is None else f"8_chart_animals_by_stage_{slugify_label(label)}.png"
        stage_path = charts_out / stage_filename
        plot_chart4_stage_animals(
            stage_counts_p,
            stage_farms_p,
            total_definitive_reg,
            stage_path,
            region_label=label if subdir else None,
        )
        print(
            f"[region] Saved stage animals chart for {name} to {stage_path} "
            f"(farms with animals: {stage_farms_p})."
        )


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
