"""Combine FTM animal counts with address data into a single CSV."""
from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def load_addresses(path: Path) -> Tuple[Dict[str, dict], List[str]]:
    """Load address rows keyed by rel_anoniem and return mapping plus field order."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        mapping = {row["rel_anoniem"]: row for row in reader if row.get("rel_anoniem")}
    return mapping, fieldnames


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


def combine(animals_path: Path, addresses_path: Path, output_path: Path, include_missing: bool = False) -> dict:
    """Join animals with address data on rel_anoniem and write to output_path."""
    addresses, address_fields = load_addresses(addresses_path)
    address_suffixes = [field for field in address_fields if field != "rel_anoniem"]
    address_suffixes.append("normalized_address_key")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {"rows_in": 0, "rows_out": 0, "rows_missing_address": 0}

    with animals_path.open(newline="", encoding="utf-8") as animals_file, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as output_file:
        reader = csv.DictReader(animals_file)
        if reader.fieldnames is None:
            raise ValueError("Animals file is missing a header row.")
        fieldnames = reader.fieldnames + address_suffixes
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            stats["rows_in"] += 1
            rel_id = row.get("rel_anoniem")
            address_row = addresses.get(rel_id)

            if not address_row:
                stats["rows_missing_address"] += 1
                if not include_missing:
                    continue
            merged = dict(row)
            for field in address_suffixes:
                if field == "normalized_address_key":
                    if address_row:
                        straat = address_row.get("B_STRAATNAAM", "")
                        huisnr = address_row.get("B_HUIS_NR", "")
                        toevoeg = address_row.get("B_HUIS_NR_TOEV", "")
                        pc = address_row.get("B_POSTCODE", "")
                        plaats = address_row.get("B_PLAATS", "")
                        merged[field] = "|".join(
                            [
                                _clean_text(straat),
                                _clean_code(huisnr),
                                _clean_code(toevoeg),
                                _clean_postcode(pc),
                                _clean_text(plaats),
                            ]
                        )
                    else:
                        merged[field] = ""
                else:
                    merged[field] = address_row.get(field, "") if address_row else ""
            writer.writerow(merged)
            stats["rows_out"] += 1

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine FTM animal counts (FTM_dieraantallen.csv) with addresses (FTM_addresses.csv)."
    )
    parser.add_argument(
        "--animals",
        type=Path,
        default=RAW_DIR / "FTM_dieraantallen.csv",
        help="Path to FTM animal counts CSV.",
    )
    parser.add_argument(
        "--addresses",
        type=Path,
        default=RAW_DIR / "FTM_addresses.csv",
        help="Path to FTM addresses CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROCESSED_DIR / "01_FTM_animals_with_addresses.csv",
        help="Path for the combined CSV.",
    )
    parser.add_argument(
        "--include-missing",
        action="store_true",
        help="Include animal rows even when no address match is found (address columns left blank).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    addresses_path = args.addresses
    v2_path = RAW_DIR / "FTM_addresses_v2.csv"
    if addresses_path == RAW_DIR / "FTM_addresses.csv" and v2_path.exists():
        addresses_path = v2_path
        print(f"[info] Using updated FTM addresses: {addresses_path}")
    stats = combine(args.animals, addresses_path, args.output, include_missing=args.include_missing)
    print(
        f"Wrote {stats['rows_out']} rows to {args.output} "
        f"(source rows: {stats['rows_in']}, missing addresses: {stats['rows_missing_address']})."
    )


if __name__ == "__main__":
    main()
