"""Join permit farms with combined FTM animal+address data."""
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


def _place_variants(place: str) -> List[str]:
    cleaned = _clean_text(place)
    if not cleaned:
        return [""]
    parts = cleaned.split()
    # strip province suffixes like "gld", "ov", "lb"
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
            _clean_postcode(postcode),  # postcode without spaces to avoid format drift
            _clean_text(city),
        ]
    )


def build_keys(street: str, number: str, addition: str, postcode: str, city: str) -> Set[str]:
    """Generate primary and fallback keys (truncated street/place variants)."""
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


def parse_address_key_all(address_key_all: str) -> Set[str]:
    """Split AddressKeyAll into normalized keys."""
    if not address_key_all:
        return set()
    keys = set()
    for raw in address_key_all.split(","):
        parts = raw.split("|")
        while len(parts) < 5:
            parts.append("")
        for key in build_keys(*parts[:5]):
            keys.add(key)
    return keys


def join_permits(
    permits_path: Path,
    animals_path: Path,
    output_path: Path,
    summary_path: Path,
) -> Tuple[int, int, int]:
    animals_index = load_animals_with_addresses(animals_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with permits_path.open(newline="", encoding="utf-8") as permits_file, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_file:
        permits = list(csv.DictReader(permits_file))
        writer = csv.DictWriter(
            out_file,
            fieldnames=[
                "farm_id",
                "AddressKey",
                "AddressKeyAll",
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
                "doc_id_latest",
                "Titel_latest",
                "Datum_latest",
                "Instantie_latest",
            ],
        )
        writer.writeheader()

        match_counts = 0
        unmatched = 0
        farm_totals: Dict[str, float] = defaultdict(float)
        farm_keys: Dict[str, Set[str]] = defaultdict(set)

        for row in permits:
            normalized_keys = parse_address_key_all(row.get("AddressKeyAll", ""))
            farm_id = row.get("farm_id", "")
            farm_keys[farm_id].update(normalized_keys)

            seen: Set[Tuple[str, str, str, str]] = set()
            farm_matched = False
            for key in normalized_keys:
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
                    farm_matched = True
                    match_counts += 1
                    try:
                        farm_totals[farm_id] += float(animal_row.get("gem_aantal_dieren", 0) or 0)
                    except ValueError:
                        pass

                    writer.writerow(
                        {
                            "farm_id": farm_id,
                            "AddressKey": row.get("AddressKey", ""),
                            "AddressKeyAll": row.get("AddressKeyAll", ""),
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
                            "doc_id_latest": row.get("doc_id_latest", ""),
                            "Titel_latest": row.get("Titel_latest", ""),
                            "Datum_latest": row.get("Datum_latest", ""),
                            "Instantie_latest": row.get("Instantie_latest", ""),
                        }
                    )
            if not farm_matched:
                unmatched += 1

    with summary_path.open("w", newline="", encoding="utf-8") as summary_file:
        fieldnames = ["farm_id", "normalized_keys", "total_gem_aantal_dieren"]
        writer = csv.DictWriter(summary_file, fieldnames=fieldnames)
        writer.writeheader()
        for farm_id, total in sorted(farm_totals.items()):
            writer.writerow(
                {
                    "farm_id": farm_id,
                    "normalized_keys": ";".join(sorted(farm_keys.get(farm_id, []))),
                    "total_gem_aantal_dieren": f"{total:.0f}",
                }
            )

    return len(permits), match_counts, unmatched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join permit dataset with FTM animal+address data using AddressKeyAll."
    )
    parser.add_argument(
        "--permits",
        type=Path,
        default=RAW_DIR / "06_deelnemers_lbv_lbvplus.csv",
        help="Path to permit CSV with AddressKeyAll.",
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
        default=PROCESSED_DIR / "04_permit_animals_join.csv",
        help="Path for detailed joined rows.",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=PROCESSED_DIR / "04_permit_animals_summary.csv",
        help="Path for summary totals per farm.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    permits, matches, unmatched = join_permits(args.permits, args.animals, args.output, args.summary)
    print(
        f"Processed {permits} permit rows; wrote {matches} matched animal rows to {args.output} "
        f"(unmatched permit rows: {unmatched}). Summary: {args.summary}"
    )


if __name__ == "__main__":
    main()
