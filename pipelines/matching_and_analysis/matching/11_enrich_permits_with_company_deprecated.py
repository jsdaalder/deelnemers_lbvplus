"""Enrich permit+animal matches with company KVK/Naam from fosfaat crosswalk."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def load_crosswalk(path: Path) -> Dict[str, dict]:
    mapping: Dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rel = row.get("rel_anoniem", "")
            if not rel or rel in mapping:
                continue
            mapping[rel] = {"fos_kvk": row.get("fos_kvk", ""), "fos_naam": row.get("fos_naam", "")}
    return mapping


def enrich_permit_animals(
    permit_animals_path: Path,
    crosswalk_path: Path,
    output_path: Path,
) -> dict:
    crosswalk = load_crosswalk(crosswalk_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {"rows_in": 0, "rows_out": 0, "rows_enriched": 0}
    with permit_animals_path.open(newline="", encoding="utf-8") as src, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or [])
        for extra in ["fos_kvk", "fos_naam"]:
            if extra not in fieldnames:
                fieldnames.append(extra)
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            stats["rows_in"] += 1
            rel = row.get("rel_anoniem", "")
            cw = crosswalk.get(rel)
            if cw:
                row.update(cw)
                stats["rows_enriched"] += 1
            writer.writerow(row)
            stats["rows_out"] += 1
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach KVK/Naam from fosfaat crosswalk to permit+animal matches (exact address rels only)."
    )
    parser.add_argument(
        "--permit-animals",
        type=Path,
        default=PROCESSED_DIR / "04_permit_animals_join.csv",
        help="Path to permit+animal joined CSV.",
    )
    parser.add_argument(
        "--crosswalk",
        type=Path,
        default=PROCESSED_DIR / "05_fosfaat_rel_crosswalk.csv",
        help="Crosswalk with rel_anoniem, fos_kvk, fos_naam.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROCESSED_DIR / "06_permit_animals_with_company.csv",
        help="Output CSV path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stats = enrich_permit_animals(args.permit_animals, args.crosswalk, args.output)
    print(
        f"Wrote {stats['rows_out']} rows to {args.output} "
        f"(enriched with company info: {stats['rows_enriched']} of {stats['rows_in']})."
    )


if __name__ == "__main__":
    main()
