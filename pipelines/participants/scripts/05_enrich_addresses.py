#!/usr/bin/env python3
"""
Deterministic address enrichment between the AI annotations (step 04)
and the farm aggregation (step 06).

Responsibilities:
* Duplicate rows when B_HUIS_NR encodes multiple numbers ("3 en 9" or "3-5").
* Fill missing postcodes through the PDOK Locatieserver API.
* Emit a canonical AddressKey column that 06_build_deelnemers.py can use.

Example:
    python scripts/05_enrich_addresses.py \
        --input data/04_lbv_enriched_with_ai_summary.csv \
        --output data/05_lbv_enriched_addresses.csv
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
import requests
import unicodedata

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

DEFAULT_INPUT = DATA_DIR / "04_lbv_enriched_with_ai_summary.csv"
DEFAULT_OUTPUT = DATA_DIR / "05_lbv_enriched_addresses.csv"

COL_STREET = "B_STRAATNAAM"
COL_NUMBER = "B_HUIS_NR"
COL_SUFFIX = "B_HUIS_NR_TOEV"
COL_POSTCODE = "B_POSTCODE"
COL_PLACE = "B_PLAATS"
COL_ADDRESS_KEY = "AddressKey"

PAIR_RE = re.compile(r"^\s*(\d+)\s*(?:en|&)\s*(\d+)\s*$", re.IGNORECASE)
RANGE_RE = re.compile(r"^\s*(\d+)\s*[-–]\s*(\d+)\s*$")
RANGE_SUFFIX_RE = re.compile(r"^\s*(\d+)([A-Za-z]?)\s*[-–]\s*(\d+)([A-Za-z]?)\s*$")
TM_RE = re.compile(r"^\s*(\d+)\s*t/m\s*(\d+)\s*$", re.IGNORECASE)
POSTCODE_RE = re.compile(r"^\s*(\d{4})\s*([A-Za-z]{2})\s*$")
TOKEN_RE = re.compile(r"^(\d+)([A-Za-z]*)$")
LIST_ITEM_RE = re.compile(r"^\s*(\d+)([A-Za-z]{0,4})\s*$")

PDOK_ENDPOINTS = [
    "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free",
]
PDOK_TIMEOUT = 10  # seconds
PDOK_RETRIES = 3
PDOK_RETRY_SLEEP = 0.5  # seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normaliseer adressen en postcodes voor LBV-permits.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input CSV uit stap 04.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV voor stap 06.")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optioneel maximum aantal rijen (handig voor snelle tests).",
    )
    parser.add_argument(
        "--pdok-failures",
        default=str(DATA_DIR / "diagnostics" / "05_pdok_failures.csv"),
        help="CSV output for PDOK lookup failures (default: data/diagnostics/05_pdok_failures.csv).",
    )
    parser.add_argument(
        "--pdok-corrections",
        default=str(DATA_DIR / "diagnostics" / "05_pdok_corrections.csv"),
        help="CSV output for PDOK postcode corrections (default: data/diagnostics/05_pdok_corrections.csv).",
    )
    parser.add_argument(
        "--skip-pdok",
        action="store_true",
        help="Skip PDOK postcode lookups and only build deterministic address keys.",
    )
    return parser.parse_args()


def ensure_address_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in (COL_STREET, COL_NUMBER, COL_SUFFIX, COL_POSTCODE, COL_PLACE):
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str)
    if "Instantie" not in df.columns:
        df["Instantie"] = df.get("Overheidsnaam", "")
    return df


def expand_house_numbers(df: pd.DataFrame) -> pd.DataFrame:
    records: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        base = row.to_dict()
        expansions = list(iter_house_numbers(base.get(COL_NUMBER, "")))
        if not expansions:
            records.append(base)
            continue
        for number, suffix in expansions:
            copy = base.copy()
            copy[COL_NUMBER] = number
            copy[COL_SUFFIX] = suffix
            records.append(copy)
    return pd.DataFrame.from_records(records, columns=df.columns)


def iter_house_numbers(raw: str) -> Iterable[tuple[str, str]]:
    text = (raw or "").strip()
    if not text:
        return []
    if "," in text and " en " in text.lower():
        normalized = re.sub(r"\ben\b", ",", text, flags=re.IGNORECASE)
        parts = [part.strip() for part in normalized.split(",") if part.strip()]
        parsed = []
        for part in parts:
            token_match = LIST_ITEM_RE.match(part)
            if not token_match:
                return []
            num, suf = token_match.groups()
            parsed.append((num, suf.upper()))
        if parsed:
            return parsed
    match = PAIR_RE.match(text)
    if match:
        return [(match.group(1), ""), (match.group(2), "")]

    # Split chains like "7-9-11" or "27-27A-29"
    parts = re.split(r"\s*[-–]\s*", text)
    if len(parts) > 2:
        parsed = []
        for part in parts:
            token_match = TOKEN_RE.match(part)
            if not token_match:
                return []
            num, suf = token_match.groups()
            parsed.append((num, suf.upper()))
        return parsed

    # Handle ranges, including cases like "27-27A" or "32-32a"
    match_suffix_range = RANGE_SUFFIX_RE.match(text)
    if match_suffix_range:
        start_num, start_suf, end_num, end_suf = match_suffix_range.groups()
        # If suffixes are involved, treat as a pair of explicit addresses
        if start_suf or end_suf:
            return [(start_num, start_suf.upper()), (end_num, end_suf.upper())]

    # Plain numeric range like "7-11": treat conservatively as two explicit numbers
    match = RANGE_RE.match(text)
    if match:
        start, end = match.groups()
        return [(start, ""), (end, "")]

    # Explicit range semantics "t/m": expand fully
    match = TM_RE.match(text)
    if match:
        start, end = int(match.group(1)), int(match.group(2))
        if start > end:
            start, end = end, start
        return [(str(num), "") for num in range(start, end + 1)]

    # If it matches the suffix-range shape but without suffix letters, it was handled above
    return []


def normalize_postcode(value: str) -> str:
    match = POSTCODE_RE.match(value or "")
    if not match:
        return ""
    digits, letters = match.groups()
    return f"{digits} {letters.upper()}"


def format_house_for_query(number: str, suffix: str) -> str:
    number = (number or "").strip()
    suffix = (suffix or "").strip()
    return f"{number}{suffix}".strip()


class PdokClient:
    def __init__(self) -> None:
        self.cache: Dict[str, str] = {}
        self.endpoints = PDOK_ENDPOINTS

    def lookup_postcode(self, street: str, number: str, suffix: str, place: str) -> str:
        key = self._cache_key(street, number, suffix, place)
        if key in self.cache:
            return self.cache[key]
        if not street or not number or not place:
            self.cache[key] = ""
            return ""
        query_number = format_house_for_query(number, suffix)
        query = " ".join(part for part in [street, query_number, place] if part)
        params = {"q": query, "fq": "type:adres", "rows": 1}
        headers = {"Accept": "application/json"}
        postcode = ""
        for endpoint in self.endpoints:
            for attempt in range(1, PDOK_RETRIES + 1):
                try:
                    resp = requests.get(endpoint, params=params, timeout=PDOK_TIMEOUT, headers=headers)
                    if resp.status_code == 404:
                        print(
                            f"[warn] PDOK endpoint returned 404 via {endpoint} for '{query}'. "
                            "Skipping retries for this endpoint."
                        )
                        break
                    resp.raise_for_status()
                    data = resp.json()
                    docs = data.get("response", {}).get("docs", [])
                    postcode = docs[0].get("postcode", "") if docs else ""
                    if postcode:
                        break
                except requests.RequestException as exc:  # pragma: no cover - network failures
                    print(
                        f"[warn] PDOK lookup failed via {endpoint} for '{query}' "
                        f"(attempt {attempt}/{PDOK_RETRIES}): {exc}"
                    )
                    if attempt < PDOK_RETRIES:
                        time.sleep(PDOK_RETRY_SLEEP)
                    continue
            if postcode:
                break
        normalized = normalize_postcode(postcode)
        self.cache[key] = normalized
        return normalized

    @staticmethod
    def _cache_key(street: str, number: str, suffix: str, place: str) -> str:
        return "|".join(
            normalize_component(part)
            for part in (street, number, suffix or "", place)
        )


def fill_missing_postcodes(df: pd.DataFrame, client: PdokClient, failures: List[dict]) -> pd.DataFrame:
    for idx, row in df.iterrows():
        existing = normalize_postcode(str(row.get(COL_POSTCODE, "")))
        if existing:
            if existing != row.get(COL_POSTCODE, ""):
                df.at[idx, COL_POSTCODE] = existing
            continue
        street = str(row.get(COL_STREET, "")).strip()
        number = str(row.get(COL_NUMBER, "")).strip()
        suffix = str(row.get(COL_SUFFIX, "")).strip()
        place = str(row.get(COL_PLACE, "")).strip()
        if not (street and number and place):
            failures.append(
                {
                    "doc_id": row.get("doc_id", ""),
                    "street": street,
                    "number": number,
                    "suffix": suffix,
                    "place": place,
                    "reason": "missing_components",
                }
            )
            continue
        looked_up = client.lookup_postcode(street, number, suffix, place)
        if looked_up:
            df.at[idx, COL_POSTCODE] = looked_up
        else:
            failures.append(
                {
                    "doc_id": row.get("doc_id", ""),
                    "street": street,
                    "number": number,
                    "suffix": suffix,
                    "place": place,
                    "reason": "no_match",
                }
            )
    return df


def fill_canonical_postcodes(df: pd.DataFrame, client: PdokClient, corrections: List[dict]) -> pd.DataFrame:
    for idx, row in df.iterrows():
        existing_raw = str(row.get(COL_POSTCODE, ""))
        existing = normalize_postcode(existing_raw)
        if not existing:
            continue
        street = str(row.get(COL_STREET, "")).strip()
        number = str(row.get(COL_NUMBER, "")).strip()
        suffix = str(row.get(COL_SUFFIX, "")).strip()
        place = str(row.get(COL_PLACE, "")).strip()
        if not (street and number and place):
            continue
        looked_up = client.lookup_postcode(street, number, suffix, place)
        if looked_up and looked_up != existing:
            corrections.append(
                {
                    "doc_id": row.get("doc_id", ""),
                    "street": street,
                    "number": number,
                    "suffix": suffix,
                    "place": place,
                    "postcode_old": existing_raw,
                    "postcode_new": looked_up,
                }
            )
            df.at[idx, COL_POSTCODE] = looked_up
    return df


def normalize_component(value: str) -> str:
    text = (value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_address_key(row: pd.Series) -> str:
    street = normalize_component(row.get(COL_STREET, ""))
    number = normalize_component(row.get(COL_NUMBER, ""))
    suffix = normalize_component(row.get(COL_SUFFIX, ""))
    postcode = normalize_component(row.get(COL_POSTCODE, ""))
    place = normalize_component(row.get(COL_PLACE, ""))
    if not (street and number and place):
        return ""
    parts = [street, number, suffix, postcode, place]
    return "|".join(parts)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    failure_path = Path(args.pdok_failures).expanduser().resolve()
    correction_path = Path(args.pdok_corrections).expanduser().resolve()
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    if args.max_rows is not None:
        df = df.head(args.max_rows)
    df = ensure_address_columns(df)
    df = expand_house_numbers(df)
    failures: List[dict] = []
    corrections: List[dict] = []
    if args.skip_pdok:
        print("[info] Skipping PDOK lookups; using only addresses already present in step 04.")
    else:
        pdok_client = PdokClient()
        df = fill_missing_postcodes(df, pdok_client, failures)
        df = fill_canonical_postcodes(df, pdok_client, corrections)
    df[COL_ADDRESS_KEY] = df.apply(build_address_key, axis=1)
    df.to_csv(output_path, index=False)
    if failures:
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(failures).to_csv(failure_path, index=False)
        print(f"[info] Wrote PDOK failures to {failure_path} ({len(failures)} rows).")
    if corrections:
        correction_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(corrections).to_csv(correction_path, index=False)
        print(f"[info] Wrote PDOK corrections to {correction_path} ({len(corrections)} rows).")
    print(f"[info] Wrote {output_path} with {len(df)} rows.")


if __name__ == "__main__":
    main()
