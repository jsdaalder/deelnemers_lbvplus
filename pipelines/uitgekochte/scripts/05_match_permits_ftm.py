"""Join permit farms with combined FTM animal+address data."""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def _clean(text: str, drop_spaces: bool = False) -> str:
    if text is None:
        return ""
    cleaned = " ".join(text.strip().lower().split())
    return cleaned.replace(" ", "") if drop_spaces else cleaned


def make_key(street: str, number: str, addition: str, postcode: str, city: str) -> str:
    """Normalize address parts to a consistent key."""
    return "|".join(
        [
            _clean(street),
            _clean(number),
            _clean(addition),
            _clean(postcode, drop_spaces=True),  # postcode without spaces to avoid format drift
            _clean(city),
        ]
    )


def load_animals_with_addresses(path: Path) -> Dict[str, List[dict]]:
    """Index combined FTM animal+address rows by normalized address key."""
    index: Dict[str, List[dict]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = make_key(
                row.get("B_STRAATNAAM", ""),
                row.get("B_HUIS_NR", ""),
                row.get("B_HUIS_NR_TOEV", ""),
                row.get("B_POSTCODE", ""),
                row.get("B_PLAATS", ""),
            )
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
        keys.add(make_key(*parts[:5]))
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
