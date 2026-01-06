"""Prepare 2015 dairy animal counts and fosfaat data for address-based matching."""
from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

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


def make_key(street: str, number: str, addition: str, postcode: str, city: str) -> str:
    """Normalize address parts to a consistent key."""
    return "|".join(
        [
            _clean_text(street),
            _clean_code(number),
            _clean_code(addition),
            _clean_postcode(postcode),
            _clean_text(city),
        ]
    )


def load_addresses(path: Path) -> Dict[str, dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["rel_anoniem"]: row for row in csv.DictReader(handle) if row.get("rel_anoniem")}


def aggregate_ftm_2015(ftm_path: Path, addresses: Dict[str, dict]) -> List[dict]:
    """Aggregate 2015 RUNDVEE dairy categories, excluding the total row (VRAAGCODE 230)."""
    code_to_bucket = {
        "211": "cat100_melk_en_kalfkoeien",  # melk- en kalfkoeien
        "201": "cat101_jongvee_lt1",  # fokjongvee <1 jaar (vr)
        "203": "cat101_jongvee_lt1",  # fokjongvee <1 jaar (m)
        "205": "cat102_jongvee_1plus",  # fokjongvee 1-2 jaar (vr)
        "207": "cat102_jongvee_1plus",  # fokjongvee 1-2 jaar (m)
        "209": "cat102_jongvee_1plus",  # fokjongvee 2+ nooit gekalfd
    }

    agg: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    with ftm_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("JAAR") != "2015":
                continue
            if not row.get("CAT_OMS", "").upper().startswith("RUND"):
                continue
            code = row.get("VRAAGCODE")
            if code == "230":  # total line, skip to avoid double counting
                continue
            bucket = code_to_bucket.get(code)
            if not bucket:
                continue
            try:
                count = float(row.get("aantal", 0) or 0)
            except ValueError:
                continue
            agg[row["rel_anoniem"]][bucket] += count

    records = []
    for rel, counts in agg.items():
        addr = addresses.get(rel, {})
        cat100 = counts.get("cat100_melk_en_kalfkoeien", 0.0)
        cat101 = counts.get("cat101_jongvee_lt1", 0.0)
        cat102 = counts.get("cat102_jongvee_1plus", 0.0)
        records.append(
            {
                "rel_anoniem": rel,
                "cat100_melk_en_kalfkoeien": f"{cat100:.0f}",
                "cat101_jongvee_lt1": f"{cat101:.0f}",
                "cat102_jongvee_1plus": f"{cat102:.0f}",
                "cat_total_dairy": f"{cat100 + cat101 + cat102:.0f}",
                "B_STRAATNAAM": addr.get("B_STRAATNAAM", ""),
                "B_HUIS_NR": addr.get("B_HUIS_NR", ""),
                "B_HUIS_NR_TOEV": addr.get("B_HUIS_NR_TOEV", ""),
                "B_POSTCODE": addr.get("B_POSTCODE", ""),
                "B_PLAATS": addr.get("B_PLAATS", ""),
                "normalized_address_key": make_key(
                    addr.get("B_STRAATNAAM", ""),
                    addr.get("B_HUIS_NR", ""),
                    addr.get("B_HUIS_NR_TOEV", ""),
                    addr.get("B_POSTCODE", ""),
                    addr.get("B_PLAATS", ""),
                ),
            }
        )
    return records


def parse_fosfaat(fos_path: Path) -> List[dict]:
    """Extract 2015 average dairy categories (100/101/102) and normalize address keys."""
    with fos_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header_rows = [next(reader) for _ in range(3)]
        col_header = next(reader)  # row with basic column names

        # Find the later occurrence of each category label (block for average 2015 animals).
        label_row = header_rows[2]
        idx_cat100 = max(i for i, v in enumerate(label_row) if "DIERCATEGORIE 100" in v)
        idx_cat101 = max(i for i, v in enumerate(label_row) if "DIERCATEGORIE 101" in v)
        idx_cat102 = max(i for i, v in enumerate(label_row) if "DIERCATEGORIE 102" in v)

        records = []
        for row in reader:
            if not row:
                continue
            def parse_float(idx: int) -> float:
                try:
                    return float(row[idx]) if row[idx].strip() else 0.0
                except (ValueError, IndexError):
                    return 0.0

            cat100 = parse_float(idx_cat100)
            cat101 = parse_float(idx_cat101)
            cat102 = parse_float(idx_cat102)

            street = row[3] if len(row) > 3 else ""
            number = row[4] if len(row) > 4 else ""
            addition = row[5] if len(row) > 5 else ""
            postcode = row[6] if len(row) > 6 else ""
            city = row[7] if len(row) > 7 else ""

            records.append(
                {
                    "RELATIENUMMER": row[0].strip(),
                    "KVK_NR": row[1].strip(),
                    "NAAM": row[2].strip(),
                    "cat100_melk_en_kalfkoeien": f"{cat100:.0f}",
                    "cat101_jongvee_lt1": f"{cat101:.0f}",
                    "cat102_jongvee_1plus": f"{cat102:.0f}",
                    "cat_total_dairy": f"{cat100 + cat101 + cat102:.0f}",
                    "STRAATNAAM": street,
                    "HUISNR": number,
                    "TOEV": addition,
                    "POSTCODE": postcode,
                    "PLAATS": city,
                    "normalized_address_key": make_key(street, number, addition, postcode, city),
                }
            )
    return records


def write_csv(path: Path, rows: Iterable[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_address_crosswalk(ftm_rows: List[dict], fos_rows: List[dict]) -> List[dict]:
    fos_by_key: Dict[str, List[dict]] = defaultdict(list)
    for row in fos_rows:
        fos_by_key[row["normalized_address_key"]].append(row)

    crosswalk = []
    for ftm in ftm_rows:
        key = ftm["normalized_address_key"]
        for fos in fos_by_key.get(key, []):
            crosswalk.append(
                {
                    "normalized_address_key": key,
                    "rel_anoniem": ftm["rel_anoniem"],
                    "ftm_cat100": ftm["cat100_melk_en_kalfkoeien"],
                    "ftm_cat101": ftm["cat101_jongvee_lt1"],
                    "ftm_cat102": ftm["cat102_jongvee_1plus"],
                    "ftm_total": ftm["cat_total_dairy"],
                    "ftm_postcode": ftm["B_POSTCODE"],
                    "ftm_straat": ftm["B_STRAATNAAM"],
                    "ftm_huisnr": ftm["B_HUIS_NR"],
                    "ftm_toev": ftm["B_HUIS_NR_TOEV"],
                    "fos_kvk": fos["KVK_NR"],
                    "fos_naam": fos["NAAM"],
                    "fos_cat100": fos["cat100_melk_en_kalfkoeien"],
                    "fos_cat101": fos["cat101_jongvee_lt1"],
                    "fos_cat102": fos["cat102_jongvee_1plus"],
                    "fos_total": fos["cat_total_dairy"],
                    "fos_postcode": fos["POSTCODE"],
                    "fos_straat": fos["STRAATNAAM"],
                    "fos_huisnr": fos["HUISNR"],
                    "fos_toev": fos["TOEV"],
                }
            )
    return crosswalk


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare 2015 dairy counts from FTM and fosfaatbeschikkingen for matching."
    )
    parser.add_argument(
        "--ftm-animals",
        type=Path,
        default=RAW_DIR / "FTM_dieraantallen_2010-2015.csv",
        help="Path to historical FTM animal counts (2010-2015).",
    )
    parser.add_argument(
        "--ftm-addresses",
        type=Path,
        default=RAW_DIR / "FTM_addresses.csv",
        help="Path to FTM addresses CSV.",
    )
    parser.add_argument(
        "--fosfaat",
        type=Path,
        default=RAW_DIR / "fosfaatbeschikkingen.csv",
        help="Path to fosfaatbeschikkingen CSV.",
    )
    parser.add_argument(
        "--out-ftm",
        type=Path,
        default=PROCESSED_DIR / "05_FTM_2015_rundvee_dairy.csv",
        help="Output path for aggregated 2015 dairy counts with addresses.",
    )
    parser.add_argument(
        "--out-fosfaat",
        type=Path,
        default=PROCESSED_DIR / "05_fosfaat_animals_2015.csv",
        help="Output path for normalized fosfaat records.",
    )
    parser.add_argument(
        "--out-crosswalk",
        type=Path,
        default=PROCESSED_DIR / "05_fosfaat_rel_crosswalk.csv",
        help="Output path for exact address matches between FTM 2015 and fosfaat data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    addresses = load_addresses(args.ftm_addresses)
    ftm_records = aggregate_ftm_2015(args.ftm_animals, addresses)
    fos_records = parse_fosfaat(args.fosfaat)

    write_csv(
        args.out_ftm,
        ftm_records,
        fieldnames=[
            "rel_anoniem",
            "cat100_melk_en_kalfkoeien",
            "cat101_jongvee_lt1",
            "cat102_jongvee_1plus",
            "cat_total_dairy",
            "B_STRAATNAAM",
            "B_HUIS_NR",
            "B_HUIS_NR_TOEV",
            "B_POSTCODE",
            "B_PLAATS",
            "normalized_address_key",
        ],
    )
    write_csv(
        args.out_fosfaat,
        fos_records,
        fieldnames=[
            "RELATIENUMMER",
            "KVK_NR",
            "NAAM",
            "cat100_melk_en_kalfkoeien",
            "cat101_jongvee_lt1",
            "cat102_jongvee_1plus",
            "cat_total_dairy",
            "STRAATNAAM",
            "HUISNR",
            "TOEV",
            "POSTCODE",
            "PLAATS",
            "normalized_address_key",
        ],
    )

    crosswalk = build_address_crosswalk(ftm_records, fos_records)
    write_csv(
        args.out_crosswalk,
        crosswalk,
        fieldnames=[
            "normalized_address_key",
            "rel_anoniem",
            "ftm_cat100",
            "ftm_cat101",
            "ftm_cat102",
            "ftm_total",
            "ftm_straat",
            "ftm_huisnr",
            "ftm_toev",
            "ftm_postcode",
            "fos_kvk",
            "fos_naam",
            "fos_cat100",
            "fos_cat101",
            "fos_cat102",
            "fos_total",
            "fos_straat",
            "fos_huisnr",
            "fos_toev",
            "fos_postcode",
        ],
    )

    print(
        f"Aggregated FTM 2015 dairy records: {len(ftm_records)} -> {args.out_ftm}\\n"
        f"Normalized fosfaat records: {len(fos_records)} -> {args.out_fosfaat}\\n"
        f"Exact address crosswalk entries: {len(crosswalk)} -> {args.out_crosswalk}"
    )


if __name__ == "__main__":
    main()
