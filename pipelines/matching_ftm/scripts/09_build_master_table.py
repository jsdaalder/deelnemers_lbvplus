"""Build a single enriched permits table with all linked data added as columns."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Set
import re
import unicodedata
from collections import defaultdict
import pandas as pd

PIPE_ROOT = Path(__file__).resolve().parents[1]  # pipelines/matching_ftm
RAW_DIR = PIPE_ROOT / "data" / "raw"
PROCESSED_DIR = PIPE_ROOT / "data" / "processed"

MASTER_PATH = PROCESSED_DIR / "master_permits.csv"
EXCLUDE_FOS_FARMS = {"FARM0082"}  # user-specified exclusions for fosfaat fallback
WOONPLAATSEN_CSV = RAW_DIR / "woonplaatsen.csv"

# link_method labels
METHOD_UNLINKED = "niet_gelinkt"
METHOD_PERMIT_ADDRESS = "permit_adres"
METHOD_MINFIN_KVK_ADDRESS = "minfin_kvk_adres"
METHOD_PERMIT_KVK_ADDRESS = "permit_kvk_adres"
METHOD_FOSFAAT_ADDRESS = "fosfaat_adres"


def read_csv(path: Path) -> List[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_crosswalk(path: Path) -> Dict[str, dict]:
    mapping: Dict[str, dict] = {}
    for row in read_csv(path):
        rel = row.get("rel_anoniem", "")
        if rel and rel not in mapping:
            mapping[rel] = {
                "fos_kvk": row.get("fos_kvk", ""),
                "fos_naam": row.get("fos_naam", ""),
            }
    return mapping


def load_name_matches(path: Path) -> Set[str]:
    return {row.get("farm_id", "") for row in read_csv(path)}


def load_kvk_results(path: Path) -> Dict[str, dict]:
    """Pick the first hit per farm_id from KVK results."""
    if not path.exists():
        return {}
    hits = {}
    for row in read_csv(path):
        fid = row.get("farm_id", "")
        if not fid or fid in hits:
            continue
        hits[fid] = {
            "kvk_api_number": row.get("kvk_nummer", ""),
            "kvk_api_name": row.get("company_name", ""),
            "kvk_api_rechtsvorm": row.get("rechtsvorm", ""),
            "kvk_api_actief": row.get("actief", ""),
            "kvk_api_locatie_type": "bezoeklocatie" if row.get("bezoek_straat") not in ("", "Ontbreekt") else (
                "postlocatie" if row.get("post_straat") not in ("", "Ontbreekt") else ""
            ),
            "kvk_api_straat": row.get("bezoek_straat", "") or row.get("post_straat", ""),
            "kvk_api_huisnummer": row.get("bezoek_huisnummer", "") or row.get("post_huisnummer", ""),
            "kvk_api_plaats": row.get("bezoek_plaats", "") or row.get("post_plaats", ""),
            "kvk_api_postcode": row.get("bezoek_postcode", "") or row.get("post_postcode", ""),
        }
    return hits


def aggregate_animals(rows: List[dict]) -> List[dict]:
    """Collapse duplicate animal rows per farm/rel/rav/year, summing counts."""
    if not rows:
        return []
    key_fields = ["farm_id", "rel_anoniem", "rav_code", "jaar", "gem_jaar"]
    sum_fields = ["gem_aantal_dieren"]
    grouped: Dict[tuple, dict] = {}
    for r in rows:
        key = tuple(r.get(f, "") for f in key_fields)
        if key not in grouped:
            grouped[key] = dict(r)
            for sf in sum_fields:
                grouped[key][sf] = float(r.get(sf, "") or 0)
        else:
            for sf in sum_fields:
                grouped[key][sf] = float(grouped[key].get(sf, 0) or 0) + float(r.get(sf, "") or 0)
    # Coerce summed fields back to string/stripped
    for g in grouped.values():
        for sf in sum_fields:
            if sf in g:
                val = g[sf]
                # keep integers without .0
                g[sf] = int(val) if float(val).is_integer() else val
    return list(grouped.values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build master table with animals, permit/minfin metadata, fosfaat KVK/Naam, and name-match flags."
    )
    parser.add_argument(
        "--join",
        type=Path,
        default=PROCESSED_DIR / "04_permit_animals_join.csv",
        help="Permit+animal join (after manual links).",
    )
    parser.add_argument(
        "--minfin-join",
        type=Path,
        default=PROCESSED_DIR / "04_minfin_animals_join.csv",
        help="MinFin+animal join (from KVK-addressed MinFin firms).",
    )
    parser.add_argument(
        "--crosswalk",
        type=Path,
        default=PROCESSED_DIR / "05_fosfaat_rel_crosswalk.csv",
        help="Crosswalk with fos_kvk/fos_naam per rel_anoniem.",
    )
    parser.add_argument(
        "--name-matches",
        type=Path,
        default=PROCESSED_DIR / "07_permit_fosfaat_name_matches.csv",
        help="Name-based matches (for flagging).",
    )
    parser.add_argument(
        "--output-permits",
        type=Path,
        default=MASTER_PATH,
        help="Output master table path (permit rows only).",
    )
    parser.add_argument(
        "--output-minfin",
        type=Path,
        default=PROCESSED_DIR / "master_voorschotten.csv",
        help="Output table path for MinFin-only rows.",
    )
    parser.add_argument(
        "--output-participants",
        type=Path,
        default=PROCESSED_DIR / "master_participants.csv",
        help="Merged participants (permit + minfin, with canonical farm_ids).",
    )
    parser.add_argument(
        "--kvk-results",
        type=Path,
        default=PROCESSED_DIR / "02_kvk_results.csv",
        help="Optional KVK API results to merge (first hit per farm_id).",
    )
    parser.add_argument(
        "--year",
        type=str,
        default="2021",
        help="Animal count year to keep in master.",
    )
    parser.add_argument(
        "--cluster",
        type=Path,
        default=RAW_DIR / "cluster_ids.csv",
        help="Optional cluster lookup with x/y and lbv fields.",
    )
    return parser.parse_args()


def normalize_name(val: str) -> str:
    if not val:
        return ""
    s = val.lower()
    for ch in ",.;:-/\\|()[]{}'\"":
        s = s.replace(ch, " ")
    words = [w for w in s.split() if w not in {"bv", "b.v.", "vof", "v.o.f.", "stichting", "maatschap", "mts", "vennootschap", "onder", "firma"}]
    return " ".join(words).strip()


def normalize_address(straat: str, huisnr: str, postcode: str, plaats: str) -> str:
    def fold(x: str) -> str:
        if not x:
            return ""
        val = str(x).strip().lower()
        val = unicodedata.normalize("NFKD", val)
        return "".join(ch for ch in val if not unicodedata.combining(ch))

    def clean_text(x: str) -> str:
        val = fold(x)
        val = re.sub(r"[^\w\s]", " ", val)
        return " ".join(val.split())

    def clean_code(x: str) -> str:
        val = fold(x)
        val = re.sub(r"\s+", "", val)
        return re.sub(r"[^\w-]", "", val)

    def clean_postcode(x: str) -> str:
        val = fold(x)
        val = re.sub(r"\s+", "", val).upper()
        return re.sub(r"[^\w]", "", val)

    s = clean_text(straat)
    h = clean_code(huisnr)
    pc = clean_postcode(postcode)
    pl = clean_text(plaats)
    return "|".join([s, h, "", pc, pl])


def load_clusters(path: Path) -> Dict[str, dict]:
    """Load cluster info keyed by iv_farm_id."""
    if not path.exists():
        return {}
    mapping: Dict[str, dict] = {}
    for row in read_csv(path):
        fid = row.get("iv_farm_id", "")
        if not fid:
            continue
        mapping[fid] = {
            "cluster_id": row.get("cluster_id", ""),
            "cluster_x_rd": row.get("x", ""),
            "cluster_y_rd": row.get("y", ""),
            "lbv_plus_tot_dep": row.get("lbv_plus_tot_dep", ""),
            "lbv_plus_rank": row.get("lbv_plus_rank", ""),
        }
    return mapping


def load_woonplaatsen(path: Path) -> tuple[Dict[str, str], set]:
    """Map normalized place name to province (proper-cased) and return ambiguous names set."""
    if not path.exists():
        return {}, set()
    df = pd.read_csv(path, sep=";", skiprows=5, header=None, names=["plaats", "gemeente", "provincie"])
    df = df.dropna(subset=["plaats", "provincie"])
    df["plaats_norm"] = df["plaats"].astype(str).str.strip().str.lower()
    df["prov_clean"] = df["provincie"].astype(str).str.strip()
    mapping = dict(zip(df["plaats_norm"], df["prov_clean"]))
    ambiguous = set(df.groupby("plaats_norm")["prov_clean"].nunique().loc[lambda s: s > 1].index)
    # also add dash->space variants for hamlets that may be written with hyphens
    for plaats_norm, prov in list(mapping.items()):
        alt = plaats_norm.replace("-", " ")
        if alt:
            if alt not in mapping:
                mapping[alt] = prov
            if alt in ambiguous:
                ambiguous.add(alt)
    return mapping, ambiguous


def main() -> None:
    args = parse_args()
    permit_rows = read_csv(RAW_DIR / "06_deelnemers_lbv_lbvplus.csv")
    join_rows_all = read_csv(args.join)
    join_rows = [r for r in join_rows_all if r.get("jaar") == args.year]
    join_rows = aggregate_animals(join_rows)
    minfin_join = read_csv(args.minfin_join) if args.minfin_join.exists() else []
    minfin_join = aggregate_animals(minfin_join)
    crosswalk = load_crosswalk(args.crosswalk)
    clusters = load_clusters(args.cluster)
    # map normalized fos naam to crosswalk row for fallback
    fos_name_map = {}
    for rel, row in crosswalk.items():
        fos_raw = row.get("fos_naam", "")
        fos_n = normalize_name(fos_raw)
        # require a non-empty fosfaat company name to use in fallback
        if fos_n and fos_n not in fos_name_map:
            fos_name_map[fos_n] = {"rel_anoniem": rel, **row, "fos_naam_raw": fos_raw}
    name_match_farms = load_name_matches(args.name_matches)
    # load FTM address/animals for KVK-based address fallback
    ftm_rows = read_csv(PROCESSED_DIR / "01_FTM_animals_with_addresses.csv")
    ftm_by_addr: Dict[str, List[dict]] = {}
    for r in ftm_rows:
        key = r.get("normalized_address_key", "")
        if not key:
            continue
        ftm_by_addr.setdefault(key, []).append(r)
    woon_map, woon_ambiguous = load_woonplaatsen(WOONPLAATSEN_CSV)
    kvk_hits = load_kvk_results(args.kvk_results)

    permit_rel_set = {r.get("rel_anoniem", "") for r in join_rows if r.get("rel_anoniem")}

    base_fieldnames = []
    if permit_rows:
        base_fieldnames.extend(permit_rows[0].keys())
    if join_rows:
        for f in join_rows[0].keys():
            if f not in base_fieldnames:
                base_fieldnames.append(f)
    if minfin_join:
        for f in minfin_join[0].keys():
            if f not in base_fieldnames:
                base_fieldnames.append(f)
    for extra in [
        "source",
        "fos_kvk",
        "fos_naam",
        "fos_name_match",
        "kvk_api_number",
        "kvk_api_name",
        "kvk_api_rechtsvorm",
        "kvk_api_actief",
        "kvk_api_locatie_type",
        "kvk_api_straat",
        "kvk_api_huisnummer",
        "kvk_api_plaats",
        "kvk_api_postcode",
        "overlap_with_permit",
        "link_method",
        "cluster_id",
        "FICTIEF_BEDRIJFSNUMMER",
        "cluster_x_rd",
        "cluster_y_rd",
        "lbv_plus_tot_dep",
        "lbv_plus_rank",
        "has_animals",
        "Province",
        "Province_from_woonplaats",
    ]:
        if extra not in base_fieldnames:
            base_fieldnames.append(extra)

    # group join rows by farm
    join_by_farm: Dict[str, List[dict]] = {}
    for r in join_rows:
        fid = r.get("farm_id", "")
        if fid:
            join_by_farm.setdefault(fid, []).append(r)
    # track address-based rels for overlap decisions
    permit_address_rels = {r.get("rel_anoniem", "") for r in join_rows if r.get("rel_anoniem")}

    enriched_permits: List[dict] = []
    enriched_minfin: List[dict] = []

    def ensure_year_fields(row: dict) -> None:
        if not row.get("jaar"):
            row["jaar"] = args.year
        if not row.get("gem_jaar"):
            row["gem_jaar"] = args.year

    def set_has_animals(row: dict) -> None:
        try:
            has = float(row.get("gem_aantal_dieren", "") or 0) > 0
        except Exception:
            has = False
        row["has_animals"] = has

    def set_province(row: dict, source: str) -> None:
        """Use Instantie_latest as province for permit rows; blank for minfin."""
        if source == "permit":
            row["Province"] = row.get("Instantie_latest", "") or ""
        else:
            row["Province"] = ""

    def set_woonplaats_province(row: dict) -> None:
        """Fill Province_from_woonplaats using woonplaatsen.csv lookup on B_PLAATS; leave blank if not found."""
        val = str(row.get("B_PLAATS", "") or "").strip().lower()
        prov = ""
        if val:
            candidates = {val, val.replace("-", " "), val.replace(" ", "-")}
            for cand in candidates:
                if cand and cand in woon_ambiguous:
                    continue
                if cand and cand in woon_map:
                    prov = woon_map[cand]
                    break
        row["Province_from_woonplaats"] = prov

    # Permit-derived rows
    for permit in permit_rows:
        fid = permit.get("farm_id", "")
        matches = join_by_farm.get(fid, [])
        if not matches:
            # emit a placeholder row with permit data only
            row = {k: permit.get(k, "") for k in base_fieldnames}
            row["fos_kvk"] = ""
            row["fos_naam"] = ""
            row["fos_name_match"] = "yes" if fid in name_match_farms else ""
            kvk = kvk_hits.get(fid, {})
            row["kvk_api_number"] = kvk.get("kvk_api_number", "")
            row["kvk_api_name"] = kvk.get("kvk_api_name", "")
            row["kvk_api_rechtsvorm"] = kvk.get("kvk_api_rechtsvorm", "")
            row["kvk_api_actief"] = kvk.get("kvk_api_actief", "")
            row["kvk_api_locatie_type"] = kvk.get("kvk_api_locatie_type", "")
            row["kvk_api_straat"] = kvk.get("kvk_api_straat", "")
            row["kvk_api_huisnummer"] = kvk.get("kvk_api_huisnummer", "")
            row["kvk_api_plaats"] = kvk.get("kvk_api_plaats", "")
            row["kvk_api_postcode"] = kvk.get("kvk_api_postcode", "")
            row["source"] = "permit"
            row["overlap_with_permit"] = "yes"
            row["link_method"] = METHOD_UNLINKED
            # Fosfaat fallback by name if still no rel and have kvk/fos name match
            rel_fallback = ""
            if not row.get("rel_anoniem") and fid not in EXCLUDE_FOS_FARMS:
                nm = normalize_name(kvk.get("kvk_api_name", ""))
                fallback = fos_name_map.get(nm)
                if fallback:
                    # plausibility check using totals if present
                    fos_total = float(fallback.get("fos_total", 0) or 0)
                    ftm_total = float(fallback.get("ftm_total", 0) or 0)
                    plausible = True
                    if fos_total > 0 and ftm_total > 0:
                        ratio = fos_total / ftm_total
                        plausible = 0.5 <= ratio <= 2.0
                    if plausible and fallback.get("fos_naam_raw", ""):
                        rel_fallback = fallback.get("rel_anoniem", "")
                        row["rel_anoniem"] = rel_fallback
                        row["fos_kvk"] = fallback.get("fos_kvk", "")
                        row["fos_naam"] = fallback.get("fos_naam", "")
                        row["link_method"] = METHOD_FOSFAAT_ADDRESS
            if row.get("rel_anoniem"):
                # no join row: if rel present and kvk present, mark as permit_kvk_adres
                if row["link_method"] == METHOD_UNLINKED and kvk.get("kvk_api_number", ""):
                    row["link_method"] = METHOD_PERMIT_KVK_ADDRESS
                ensure_year_fields(row)
                set_has_animals(row)
                set_province(row, "permit")
                set_woonplaats_province(row)
                enriched_permits.append(row)
                continue

            # try KVK-based address match to FTM (may yield multiple rel_anoniem values)
            if kvk.get("kvk_api_straat") and kvk.get("kvk_api_postcode"):
                key = normalize_address(
                    kvk.get("kvk_api_straat", ""),
                    kvk.get("kvk_api_huisnummer", ""),
                    kvk.get("kvk_api_postcode", ""),
                    kvk.get("kvk_api_plaats", ""),
                )
                ftm_matches = ftm_by_addr.get(key, [])
                if ftm_matches:
                    for ftm in ftm_matches:
                        row_copy = row.copy()
                        for f in ftm:
                            if f in row_copy and row_copy.get(f, "") == "":
                                row_copy[f] = ftm[f]
                        row_copy["rel_anoniem"] = ftm.get("rel_anoniem", "")
                        row_copy["link_method"] = METHOD_PERMIT_KVK_ADDRESS
                        ensure_year_fields(row_copy)
                        set_has_animals(row_copy)
                        set_province(row_copy, "permit")
                        set_woonplaats_province(row_copy)
                        enriched_permits.append(row_copy)
                    continue

            ensure_year_fields(row)
            set_has_animals(row)
            set_province(row, "permit")
            set_woonplaats_province(row)
            enriched_permits.append(row)
            continue
        for match in matches:
            row = {k: "" for k in base_fieldnames}
            row.update(permit)
            row.update(match)
            rel = match.get("rel_anoniem", "")
            cw = crosswalk.get(rel, {})
            row["fos_kvk"] = cw.get("fos_kvk", "")
            row["fos_naam"] = cw.get("fos_naam", "")
            row["fos_name_match"] = "yes" if fid in name_match_farms else ""
            kvk = kvk_hits.get(fid, {})
            row["kvk_api_number"] = kvk.get("kvk_api_number", "")
            row["kvk_api_name"] = kvk.get("kvk_api_name", "")
            row["kvk_api_rechtsvorm"] = kvk.get("kvk_api_rechtsvorm", "")
            row["kvk_api_actief"] = kvk.get("kvk_api_actief", "")
            row["kvk_api_locatie_type"] = kvk.get("kvk_api_locatie_type", "")
            row["kvk_api_straat"] = kvk.get("kvk_api_straat", "")
            row["kvk_api_huisnummer"] = kvk.get("kvk_api_huisnummer", "")
            row["kvk_api_plaats"] = kvk.get("kvk_api_plaats", "")
            row["kvk_api_postcode"] = kvk.get("kvk_api_postcode", "")
            row["source"] = "permit"
            row["overlap_with_permit"] = "yes"
            if row.get("rel_anoniem"):
                # direct rel from permit join: treat as permit_adres
                if match.get("normalized_address_key"):
                    row["link_method"] = METHOD_PERMIT_ADDRESS
                elif kvk.get("kvk_api_number", ""):
                    row["link_method"] = METHOD_PERMIT_KVK_ADDRESS
                else:
                    row["link_method"] = METHOD_UNLINKED
            else:
                # if no match rel but kvk present, mark as permit_kvk_adres
                if kvk.get("kvk_api_number", ""):
                    row["link_method"] = METHOD_PERMIT_KVK_ADDRESS
                else:
                    row["link_method"] = METHOD_UNLINKED
            ensure_year_fields(row)
            set_has_animals(row)
            set_province(row, "permit")
            set_woonplaats_province(row)
            enriched_permits.append(row)

    # MinFin-derived rows
    for m in minfin_join:
        row = {k: "" for k in base_fieldnames}
        row.update(m)
        rel = m.get("rel_anoniem", "")
        row["source"] = "minfin"
        row["overlap_with_permit"] = "yes" if rel and rel in permit_rel_set else ""
        # Attach fosfaat info if rel known
        cw = crosswalk.get(rel, {})
        row["fos_kvk"] = cw.get("fos_kvk", "")
        row["fos_naam"] = cw.get("fos_naam", "")
        row["fos_name_match"] = ""
        # KVK API info not applicable to minfin here
        if rel:
            # ensure overlap exclusion: if rel is already in permit_rel_set, treat as permit-owned
            if rel in permit_rel_set:
                row["link_method"] = METHOD_UNLINKED
            else:
                row["link_method"] = METHOD_MINFIN_KVK_ADDRESS
        else:
            # fallback via company name (kvk_api_name not present in this join; use ontvanger)
            nm = normalize_name(m.get("company_name", "") or m.get("ontvanger", ""))
            fallback = fos_name_map.get(nm)
            linked = False
            if fallback and m.get("farm_id", "") not in EXCLUDE_FOS_FARMS and fallback.get("fos_naam_raw", ""):
                fos_total = float(fallback.get("fos_total", 0) or 0)
                ftm_total = float(fallback.get("ftm_total", 0) or 0)
                plausible = True
                if fos_total > 0 and ftm_total > 0:
                    ratio = fos_total / ftm_total
                    plausible = 0.5 <= ratio <= 2.0
                if plausible:
                    rel_fb = fallback.get("rel_anoniem", "")
                    # ensure not overlapping permit rel
                    if rel_fb not in permit_rel_set:
                        row["rel_anoniem"] = rel_fb
                        row["fos_kvk"] = fallback.get("fos_kvk", "")
                        row["fos_naam"] = fallback.get("fos_naam", "")
                        row["link_method"] = METHOD_FOSFAAT_ADDRESS
                        linked = True
            if not linked:
                row["link_method"] = METHOD_UNLINKED
        ensure_year_fields(row)
        set_has_animals(row)
        set_province(row, "minfin")
        set_woonplaats_province(row)
        enriched_minfin.append(row)

    # attach cluster info if available
    for r in enriched_permits + enriched_minfin:
        c = clusters.get(r.get("farm_id", ""))
        if not c:
            continue
        r["cluster_id"] = c.get("cluster_id", r.get("cluster_id", ""))
        fictief = ""
        if r["cluster_id"]:
            fictief = str(r["cluster_id"]).split("-")[0]
        r["FICTIEF_BEDRIJFSNUMMER"] = fictief or r.get("FICTIEF_BEDRIJFSNUMMER", "")
        r["cluster_x_rd"] = c.get("cluster_x_rd", r.get("cluster_x_rd", ""))
        r["cluster_y_rd"] = c.get("cluster_y_rd", r.get("cluster_y_rd", ""))
        r["lbv_plus_tot_dep"] = c.get("lbv_plus_tot_dep", r.get("lbv_plus_tot_dep", ""))
        r["lbv_plus_rank"] = c.get("lbv_plus_rank", r.get("lbv_plus_rank", ""))

    # If rel_anoniem is present but link_method is empty or niet_gelinkt, mark as linked_via_rel for consistency
    for r in enriched_permits + enriched_minfin:
        lm = (r.get("link_method") or "").strip()
        if (not lm or lm == METHOD_UNLINKED) and r.get("rel_anoniem"):
            r["link_method"] = "linked_via_rel"

    # Canonical rel->permit farm_id map
    rel_to_permit_farm = {}
    rel_to_permit_farm_new = {}
    farm_id_to_new = {}
    for r in enriched_permits:
        rel = r.get("rel_anoniem", "")
        fid = r.get("farm_id", "")
        if rel and fid and rel not in rel_to_permit_farm:
            rel_to_permit_farm[rel] = fid
        fid_new = r.get("farm_id_new", "")
        if rel and fid_new and rel not in rel_to_permit_farm_new:
            rel_to_permit_farm_new[rel] = fid_new
        if fid and fid_new and fid not in farm_id_to_new:
            farm_id_to_new[fid] = fid_new

    # Adjust minfin farm_ids when overlapping rel exists in permits
    for r in enriched_minfin:
        rel = r.get("rel_anoniem", "")
        if rel and rel in rel_to_permit_farm:
            r["farm_id"] = rel_to_permit_farm[rel]
            if rel in rel_to_permit_farm_new:
                r["farm_id_new"] = rel_to_permit_farm_new[rel]
        if not r.get("farm_id_new") and r.get("farm_id") in farm_id_to_new:
            r["farm_id_new"] = farm_id_to_new[r["farm_id"]]

    # Participants merged with canonical farm_ids and de-duplication
    participant_rows: List[dict] = []
    seen_keys = set()
    def add_row(row: dict) -> None:
        key = (
            row.get("farm_id", ""),
            row.get("rel_anoniem", ""),
            row.get("source", ""),
            row.get("rav_code", ""),
            row.get("jaar", ""),
            row.get("gem_jaar", ""),
            row.get("normalized_address_key", ""),
        )
        if key in seen_keys:
            return
        seen_keys.add(key)
        participant_rows.append(row)

    for r in enriched_permits:
        add_row(r)
    for r in enriched_minfin:
        add_row(r)

    write_csv(args.output_permits, enriched_permits, base_fieldnames)
    write_csv(args.output_minfin, enriched_minfin, base_fieldnames)
    write_csv(args.output_participants, participant_rows, base_fieldnames)
    print(
        f"Wrote master tables: permits={len(enriched_permits)} -> {args.output_permits}, "
        f"minfin={len(enriched_minfin)} -> {args.output_minfin}, "
        f"participants={len(participant_rows)} -> {args.output_participants}"
    )


if __name__ == "__main__":
    main()
