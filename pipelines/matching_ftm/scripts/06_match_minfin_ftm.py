"""Match MinFin firms (with KVK-derived addresses) to FTM animals/addresses to find rel_anoniem links."""
from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def _fold(text: str) -> str:
    if text is None:
        return ""
    value = str(text).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return value


def _clean_text(text: str) -> str:
    value = _fold(text)
    value = re.sub(r"[^\w\s]", " ", value)
    return " ".join(value.split())


def _clean_code(text: str) -> str:
    value = _fold(text)
    value = re.sub(r"\s+", "", value)
    return re.sub(r"[^\w-]", "", value)


def _clean_postcode(text: str) -> str:
    value = _fold(text)
    value = re.sub(r"\s+", "", value).upper()
    return re.sub(r"[^\w]", "", value)

PROVINCE_SUFFIXES = {
    "gld",
    "ov",
    "lb",
    "nb",
    "ut",
    "dr",
    "fr",
    "zh",
    "nh",
    "fl",
    "ze",
    "gr",
}

PLACE_STOPWORDS = {"aan", "de", "den", "der", "het", "in", "op", "te", "ten", "ter", "van"}
PLACE_TAIL_TOKENS = {
    "broek",
    "veen",
    "veld",
    "waard",
    "wijk",
    "dam",
    "berg",
    "bosch",
    "bos",
    "hout",
    "kamp",
    "land",
    "meer",
    "horst",
    "ratum",
}

STREET_SUFFIX_MAP = {
    "straat": "str",
    "str": "straat",
    "weg": "wg",
    "wg": "weg",
    "dijk": "dk",
    "dk": "dijk",
    "laan": "ln",
    "ln": "laan",
    "steeg": "stg",
    "stg": "steeg",
}


def normalize_name(name: str) -> str:
    """Lowercase alnum tokens, drop common punctuation/spaces."""
    cleaned = _clean_text(name)
    return " ".join(cleaned.split())


def _place_variants(place: str) -> List[str]:
    cleaned = _clean_text(place)
    if not cleaned:
        return [""]
    parts = cleaned.split()
    if parts and parts[-1] in PROVINCE_SUFFIXES:
        parts = parts[:-1]
    variants = [" ".join(parts).strip()]
    if len(parts) > 1:
        variants.append(parts[0])
        no_stop = [p for p in parts if p not in PLACE_STOPWORDS]
        if no_stop and no_stop != parts:
            variants.append(" ".join(no_stop))
        if parts[-1] in PLACE_TAIL_TOKENS:
            variants.append(" ".join(parts[:-1]))
    expanded: List[str] = []
    for variant in variants:
        if not variant:
            continue
        expanded.append(variant)
        if len(variant) > 12:
            expanded.append(variant[:12])
    return list(dict.fromkeys(expanded))


def _street_variants(street: str, max_len: int = 20) -> List[str]:
    cleaned = _clean_text(street)
    if not cleaned:
        return [""]
    variants = [cleaned]
    tokens = cleaned.split()
    if tokens:
        last = tokens[-1]
        repl = STREET_SUFFIX_MAP.get(last)
        if repl:
            variants.append(" ".join(tokens[:-1] + [repl]))
    if len(cleaned) > max_len:
        variants.append(cleaned[:max_len])
    if len(cleaned) > 18:
        variants.append(cleaned[:18])
    return list(dict.fromkeys(variants))

def _normalize_number_addition(number: str, addition: str) -> Tuple[str, str]:
    num = _clean_code(number)
    add = _clean_code(addition)
    if add:
        match = re.match(r"^(\d+)", num)
        return (match.group(1) if match else num, add)
    match = re.match(r"^(\d+)([a-z]+)$", num)
    if match:
        return match.group(1), match.group(2)
    return num, ""


def make_key(street: str, number: str, addition: str, postcode: str, city: str) -> str:
    """Normalize address parts to a consistent key."""
    num, add = _normalize_number_addition(number, addition)
    return "|".join(
        [
            _clean_text(street),
            _clean_code(num),
            _clean_code(add),
            _clean_postcode(postcode),
            _clean_text(city),
        ]
    )


def build_keys(street: str, number: str, addition: str, postcode: str, city: str) -> Set[str]:
    keys: Set[str] = set()
    pc = _clean_postcode(postcode)
    num, add = _normalize_number_addition(number, addition)
    num = _clean_code(num)
    add = _clean_code(add)
    for s in _street_variants(street):
        for c in _place_variants(city):
            key = "|".join([s, num, add, pc, c])
            if key.strip("|"):
                keys.add(key)
    return keys


def load_animals_with_addresses(path: Path) -> Dict[str, List[dict]]:
    """Index combined FTM animal+address rows by normalized address key."""
    index: Dict[str, List[dict]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            for key in build_keys(
                row.get("B_STRAATNAAM", ""),
                row.get("B_HUIS_NR", ""),
                row.get("B_HUIS_NR_TOEV", ""),
                row.get("B_POSTCODE", ""),
                row.get("B_PLAATS", ""),
            ):
                index[key].append(row)
    return index


def pick_address(row: dict) -> Tuple[str, str, str, str, str]:
    """Prefer bezoeklocatie; fall back to postlocatie."""
    if any(row.get(f"bezoek_{fld}", "") for fld in ["straat", "huisnummer", "postcode", "plaats"]):
        return (
            row.get("bezoek_straat", ""),
            row.get("bezoek_huisnummer", ""),
            "",
            row.get("bezoek_postcode", ""),
            row.get("bezoek_plaats", ""),
        )
    return (
        row.get("post_straat", ""),
        row.get("post_huisnummer", ""),
        "",
        row.get("post_postcode", ""),
        row.get("post_plaats", ""),
    )


def join_minfin(
    minfin_kvk_path: Path,
    permits_path: Path,
    permit_kvk_results: Path,
    animals_path: Path,
    output_path: Path,
    summary_path: Path,
) -> Tuple[int, int, int]:
    # Build maps from permit KVK results for overlap detection.
    permit_kvk_to_farm: Dict[str, str] = {}
    permit_name_to_farm: Dict[str, str] = {}
    if permit_kvk_results.exists():
        with permit_kvk_results.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                fid = row.get("farm_id", "")
                kvk = _clean_code(row.get("kvk_nummer", ""))
                kvk = kvk.replace(".0", "") if kvk.endswith(".0") else kvk
                name = normalize_name(row.get("company_name", ""))
                if kvk and kvk not in permit_kvk_to_farm:
                    permit_kvk_to_farm[kvk] = fid
                if name and name not in permit_name_to_farm:
                    permit_name_to_farm[name] = fid

    # Build deterministic FARM-style IDs for each unique KVKnummer (or name) from MinFin, reusing permit IDs on overlap.
    permit_max = 0
    if permits_path.exists():
        with permits_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                fid = row.get("farm_id", "")
                if fid.startswith("FARM"):
                    try:
                        permit_max = max(permit_max, int(fid.replace("FARM", "")))
                    except ValueError:
                        pass

    minfin_rows = []
    unique_keys: List[Tuple[str, str]] = []  # (kvk, norm_name)
    with minfin_kvk_path.open(newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        for row in reader:
            kvk_raw = row.get("kvk_nummer_minfin", "") or row.get("kvk_nummer", "")
            kvk = _clean_code(kvk_raw)
            kvk = kvk.replace(".0", "") if kvk.endswith(".0") else kvk
            row["kvk_nummer_minfin"] = kvk
            row["norm_company"] = normalize_name(row.get("company_name", "") or row.get("ontvanger", ""))
            minfin_rows.append(row)
            key = (kvk, row["norm_company"])
            if key not in unique_keys:
                unique_keys.append(key)

    kvk_to_farm: Dict[str, str] = {}
    name_to_farm: Dict[str, str] = {}
    next_id = permit_max + 1
    for kvk, norm_name in sorted(unique_keys):
        target_farm = ""
        if kvk and kvk in permit_kvk_to_farm:
            target_farm = permit_kvk_to_farm[kvk]
        elif norm_name and norm_name in permit_name_to_farm:
            target_farm = permit_name_to_farm[norm_name]
        if not target_farm:
            target_farm = f"FARM{next_id:04d}"
            next_id += 1
        if kvk:
            kvk_to_farm[kvk] = target_farm
        if norm_name:
            name_to_farm[norm_name] = target_farm

    animals_index = load_animals_with_addresses(animals_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "farm_id",
        "minfin_id",
        "ontvanger",
        "kvk_nummer_minfin",
        "query",
        "normalized_address_key",
        "rel_anoniem",
        "jaar",
        "Huisvesting",
        "UBN",
        "rav_code",
        "stal",
        "gem_aantal_dieren",
        "gem_jaar",
        "status",
        "B_STRAATNAAM",
        "B_HUIS_NR",
        "B_HUIS_NR_TOEV",
        "B_POSTCODE",
        "B_PLAATS",
        "company_name",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()

        match_counts = 0
        unmatched = 0
        firm_totals: Dict[str, float] = defaultdict(float)
        firm_keys: Dict[str, Set[str]] = defaultdict(set)

        for row in minfin_rows:
            street, number, addition, postcode, city = pick_address(row)
            key = make_key(street, number, addition, postcode, city)
            kvk = row.get("kvk_nummer_minfin", "")
            norm_name = row.get("norm_company", "")
            farm_id = kvk_to_farm.get(kvk, "") or name_to_farm.get(norm_name, "")
            firm_keys[farm_id].add(key)

            seen: Set[Tuple[str, str, str, str]] = set()
            firm_matched = False
            for animal_row in animals_index.get(key, []):
                sig = (
                    animal_row.get("rel_anoniem", ""),
                    animal_row.get("jaar", ""),
                    animal_row.get("UBN", ""),
                    animal_row.get("stal", ""),
                )
                if sig in seen:
                    continue
                seen.add(sig)
                firm_matched = True
                match_counts += 1
                try:
                    firm_totals[farm_id] += float(animal_row.get("gem_aantal_dieren", 0) or 0)
                except ValueError:
                    pass

                writer.writerow(
                    {
                        "farm_id": farm_id,
                        "minfin_id": row.get("minfin_id", ""),
                        "ontvanger": row.get("ontvanger", ""),
                        "kvk_nummer_minfin": kvk,
                        "query": row.get("query", ""),
                        "normalized_address_key": key,
                        "rel_anoniem": animal_row.get("rel_anoniem", ""),
                        "jaar": animal_row.get("jaar", ""),
                        "Huisvesting": animal_row.get("Huisvesting", ""),
                        "UBN": animal_row.get("UBN", ""),
                        "rav_code": animal_row.get("rav_code", ""),
                        "stal": animal_row.get("stal", ""),
                        "gem_aantal_dieren": animal_row.get("gem_aantal_dieren", ""),
                        "gem_jaar": animal_row.get("gem_jaar", ""),
                        "status": animal_row.get("status", ""),
                        "B_STRAATNAAM": animal_row.get("B_STRAATNAAM", ""),
                        "B_HUIS_NR": animal_row.get("B_HUIS_NR", ""),
                        "B_HUIS_NR_TOEV": animal_row.get("B_HUIS_NR_TOEV", ""),
                        "B_POSTCODE": animal_row.get("B_POSTCODE", ""),
                        "B_PLAATS": animal_row.get("B_PLAATS", ""),
                        "company_name": row.get("company_name", ""),
                    }
                )
            if not firm_matched:
                unmatched += 1

    with summary_path.open("w", newline="", encoding="utf-8") as summary_file:
        fieldnames = ["farm_id", "normalized_keys", "total_gem_aantal_dieren"]
        writer = csv.DictWriter(summary_file, fieldnames=fieldnames)
        writer.writeheader()
        for fid, total in sorted(firm_totals.items()):
            writer.writerow(
                {
                    "farm_id": fid,
                    "normalized_keys": ";".join(sorted(firm_keys.get(fid, []))),
                    "total_gem_aantal_dieren": f"{total:.0f}",
                }
            )

    return len(minfin_rows), match_counts, unmatched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join MinFin firms (with KVK addresses) to FTM animals+addresses to find rel_anoniem links."
    )
    parser.add_argument(
        "--minfin-kvk",
        type=Path,
        default=PROCESSED_DIR / "03_kvk_minfin_results.csv",
        help="KVK lookup results for MinFin dataset.",
    )
    parser.add_argument(
        "--permits",
        type=Path,
        default=RAW_DIR / "06_deelnemers_lbv_lbvplus.csv",
        help="Permit CSV used to set the starting FARM id (we keep numbering consistent).",
    )
    parser.add_argument(
        "--permit-kvk-results",
        type=Path,
        default=PROCESSED_DIR / "02_kvk_results.csv",
        help="Permit KVK API results (for overlap by kvk/name).",
    )
    parser.add_argument(
        "--animals",
        type=Path,
        default=PROCESSED_DIR / "01_FTM_animals_with_addresses.csv",
        help="Path to combined animals+addresses CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROCESSED_DIR / "04_minfin_animals_join.csv",
        help="Path for detailed joined rows.",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=PROCESSED_DIR / "04_minfin_animals_summary.csv",
        help="Path for summary totals per minfin_id.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    firms, matches, unmatched = join_minfin(
        args.minfin_kvk,
        args.permits,
        args.permit_kvk_results,
        args.animals,
        args.output,
        args.summary,
    )
    print(
        f"Processed {firms} MinFin rows; wrote {matches} matched animal rows to {args.output} "
        f"(unmatched firms: {unmatched}). Summary: {args.summary}"
    )


if __name__ == "__main__":
    main()
