"""Match permit company names to fosfaat names with light normalization and postcode/street alignment."""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
RAW_DIR = REPO_ROOT / "data" / "raw"

LEGAL_STOPWORDS = {
    "maatschap",
    "mts",
    "vof",
    "v.o.f",
    "vennootschap",
    "firma",
    "onder",
    "vennootschaponderfirma",
    "vof.",
    "bv",
    "b.v",
    "b.v.",
    "holding",
    "melkveebedrijf",
    "melkvee",
    "melkveehouderij",
    "bedrijf",
}


def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, drop common legal/entity stopwords, sort tokens."""
    if not name:
        return ""
    cleaned = re.sub(r"[^a-zA-Z0-9\\s]", " ", name.lower())
    tokens = [t for t in cleaned.split() if t and t not in LEGAL_STOPWORDS]
    return " ".join(sorted(tokens))


def normalize_postcode(pc: str) -> str:
    return "".join(pc.split()).lower()


def normalize_street(street: str) -> str:
    return " ".join(street.lower().split())


def load_fosfaat(path: Path) -> List[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
    return rows


def load_permits(path: Path) -> List[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_minfin_candidates(kvk_results_path: Path, minfin_raw_path: Path) -> List[dict]:
    """Use KVK results for minfin dataset to build addressable candidates."""
    results = []
    # map minfin_id -> kvk row
    kvk_rows = {}
    if kvk_results_path.exists():
        with kvk_results_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                mid = row.get("minfin_id", "")
                if mid and mid not in kvk_rows:
                    kvk_rows[mid] = row

    with minfin_raw_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            mid = row.get("RIS/IBOS-nummer", "")
            ontvanger = row.get("Ontvanger", "")
            kvk = kvk_rows.get(mid, {})
            name = kvk.get("company_name") or ontvanger
            bezoek = {
                "street": kvk.get("bezoek_straat", ""),
                "number": kvk.get("bezoek_huisnummer", ""),
                "toev": "",
                "postcode": kvk.get("bezoek_postcode", ""),
                "plaats": kvk.get("bezoek_plaats", ""),
            }
            post = {
                "street": kvk.get("post_straat", ""),
                "number": kvk.get("post_huisnummer", ""),
                "toev": "",
                "postcode": kvk.get("post_postcode", ""),
                "plaats": kvk.get("post_plaats", ""),
            }
            addr = bezoek if any(bezoek.values()) else post
            if not name or not addr:
                continue
            results.append(
                {
                    "source": "minfin",
                    "id": mid,
                    "company_name": name,
                    "street": addr["street"],
                    "huisnr": addr["number"],
                    "toev": addr["toev"],
                    "postcode": addr["postcode"],
                    "plaats": addr["plaats"],
                }
            )
    return results


def find_matches(candidates: List[dict], fos_rows: List[dict]) -> List[dict]:
    fos_index: Dict[tuple, List[dict]] = {}
    for row in fos_rows:
        norm_name = normalize_name(row.get("NAAM", ""))
        if not norm_name:
            continue
        key = (
            norm_name,
            normalize_street(row.get("STRAATNAAM", "")),
            normalize_postcode(row.get("POSTCODE", "")),
        )
        fos_index.setdefault(key, []).append(row)

    matches: List[dict] = []
    for cand in candidates:
        raw_name = cand.get("company_name", "")
        norm_name = normalize_name(raw_name)
        if not norm_name:
            continue
        street = normalize_street(cand.get("street", ""))
        pc = normalize_postcode(cand.get("postcode", ""))
        key = (norm_name, street, pc)
        for fos in fos_index.get(key, []):
            matches.append(
                {
                    "source": cand.get("source", ""),
                    "farm_id": cand.get("farm_id", ""),
                    "minfin_id": cand.get("id", ""),
                    "candidate_company_name": raw_name,
                    "candidate_company_name_normalized": norm_name,
                    "candidate_street": cand.get("street", ""),
                    "candidate_huisnr": cand.get("huisnr", ""),
                    "candidate_toev": cand.get("toev", ""),
                    "candidate_postcode": cand.get("postcode", ""),
                    "candidate_plaats": cand.get("plaats", ""),
                    "fosfaat_name": fos.get("NAAM", ""),
                    "fosfaat_name_normalized": norm_name,
                    "fosfaat_huisnr": fos.get("HUISNR", ""),
                    "fosfaat_toev": fos.get("TOEV", ""),
                    "fosfaat_postcode": fos.get("POSTCODE", ""),
                    "fosfaat_straat": fos.get("STRAATNAAM", ""),
                    "fosfaat_rel": fos.get("RELATIENUMMER", ""),
                    "match_basis": "name+street+postcode",
                }
            )
    return matches


def write_csv(path: Path, rows: Iterable[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match permit and minfin company names to fosfaat names using normalization plus street+postcode alignment."
    )
    parser.add_argument(
        "--permits",
        type=Path,
        default=RAW_DIR / "06_deelnemers_lbv_lbvplus.csv",
        help="Permit CSV with COMPANY_NAME and address fields.",
    )
    parser.add_argument(
        "--fosfaat",
        type=Path,
        default=PROCESSED_DIR / "05_fosfaat_animals_2015.csv",
        help="Normalized fosfaat output from 03_prepare_2015_linkages.py.",
    )
    parser.add_argument(
        "--kvk-minfin",
        type=Path,
        default=PROCESSED_DIR / "03_kvk_minfin_results.csv",
        help="KVK lookup results for minfin dataset (for addresses).",
    )
    parser.add_argument(
        "--minfin-raw",
        type=Path,
        default=RAW_DIR / "minfin_dataset.csv",
        help="Original minfin dataset (for fallback names).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROCESSED_DIR / "07_permit_fosfaat_name_matches.csv",
        help="Output CSV of candidate name/address matches.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    permits = load_permits(args.permits)
    fos_rows = load_fosfaat(args.fosfaat)

    # Build candidates: permit farms + minfin firms with KVK addresses
    candidates: List[dict] = []
    for p in permits:
        candidates.append(
            {
                "source": "permit",
                "farm_id": p.get("farm_id", ""),
                "id": p.get("farm_id", ""),
                "company_name": p.get("COMPANY_NAME", ""),
                "street": p.get("B_STRAATNAAM", ""),
                "huisnr": p.get("B_HUIS_NR", ""),
                "toev": p.get("B_HUIS_NR_TOEV", ""),
                "postcode": p.get("B_POSTCODE", ""),
                "plaats": p.get("B_PLAATS", ""),
            }
        )
    candidates.extend(load_minfin_candidates(args.kvk_minfin, args.minfin_raw))

    matches = find_matches(candidates, fos_rows)
    write_csv(
        args.output,
        matches,
        fieldnames=[
            "source",
            "farm_id",
            "minfin_id",
            "candidate_company_name",
            "candidate_company_name_normalized",
            "candidate_street",
            "candidate_huisnr",
            "candidate_toev",
            "candidate_postcode",
            "candidate_plaats",
            "fosfaat_name",
            "fosfaat_name_normalized",
            "fosfaat_huisnr",
            "fosfaat_toev",
            "fosfaat_postcode",
            "fosfaat_straat",
            "fosfaat_rel",
            "match_basis",
        ],
    )
    print(f"Wrote {len(matches)} candidate matches to {args.output}")


if __name__ == "__main__":
    main()
