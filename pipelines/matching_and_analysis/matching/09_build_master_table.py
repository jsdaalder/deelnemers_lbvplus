"""Build a single enriched permits table with all linked data added as columns."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Set
import re

PIPE_ROOT = Path(__file__).resolve().parents[1]  # pipelines/matching_and_analysis
RAW_DIR = PIPE_ROOT / "data" / "raw"
PROCESSED_DIR = PIPE_ROOT / "data" / "processed"

MASTER_PATH = PROCESSED_DIR / "master_permits.csv"
EXCLUDE_FOS_FARMS = {"FARM0082"}  # user-specified exclusions for fosfaat fallback

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
        "--output",
        type=Path,
        default=MASTER_PATH,
        help="Output master table path.",
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
        default="2022",
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
    def clean(x: str) -> str:
        if not x:
            return ""
        x = x.lower().strip()
        x = re.sub(r"\\s+", " ", x)
        return x

    s = clean(straat)
    h = clean(str(huisnr))
    pc = clean(postcode).replace(" ", "")
    pl = clean(plaats)
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


def main() -> None:
    args = parse_args()
    permit_rows = read_csv(RAW_DIR / "06_deelnemers_lbv_lbvplus.csv")
    join_rows_all = read_csv(args.join)
    join_rows = [r for r in join_rows_all if r.get("jaar") == args.year]
    minfin_join_all = read_csv(args.minfin_join) if args.minfin_join.exists() else []
    minfin_join = [r for r in minfin_join_all if r.get("jaar") == args.year]
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
    ftm_by_addr: Dict[str, dict] = {}
    for r in ftm_rows:
        key = r.get("normalized_address_key", "")
        if key and key not in ftm_by_addr:
            ftm_by_addr[key] = r
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

    enriched: List[dict] = []
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
            else:
                # try KVK-based address match to FTM
                if kvk.get("kvk_api_straat") and kvk.get("kvk_api_postcode"):
                    key = normalize_address(
                        kvk.get("kvk_api_straat", ""),
                        kvk.get("kvk_api_huisnummer", ""),
                        kvk.get("kvk_api_postcode", ""),
                        kvk.get("kvk_api_plaats", ""),
                    )
                    ftm = ftm_by_addr.get(key)
                    if ftm:
                        for f in ftm:
                            if f in row and row.get(f, "") == "":
                                row[f] = ftm[f]
                        row["rel_anoniem"] = ftm.get("rel_anoniem", "")
                        row["link_method"] = METHOD_PERMIT_KVK_ADDRESS
            enriched.append(row)
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
            enriched.append(row)

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
        enriched.append(row)

    # attach cluster info if available
    for r in enriched:
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
    for r in enriched:
        lm = (r.get("link_method") or "").strip()
        if (not lm or lm == METHOD_UNLINKED) and r.get("rel_anoniem"):
            r["link_method"] = "linked_via_rel"

    write_csv(args.output, enriched, base_fieldnames)
    print(f"Wrote master table with {len(enriched)} rows to {args.output}")


if __name__ == "__main__":
    main()
