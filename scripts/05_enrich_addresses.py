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
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
import requests

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
TM_RE = re.compile(r"^\s*(\d+)\s*t/m\s*(\d+)\s*$", re.IGNORECASE)
POSTCODE_RE = re.compile(r"^\s*(\d{4})\s*([A-Za-z]{2})\s*$")

PDOK_ENDPOINTS = [
    "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free",
    "https://service.pdok.nl/bzk/locatieserver/search/v3_1/free",
    "https://geodata.nationaalgeoregister.nl/locatieserver/v3/free",
]
PDOK_TIMEOUT = 10  # seconds


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
        for number in expansions:
            copy = base.copy()
            copy[COL_NUMBER] = number
            records.append(copy)
    return pd.DataFrame.from_records(records, columns=df.columns)


def iter_house_numbers(raw: str) -> Iterable[str]:
    text = (raw or "").strip()
    if not text:
        return []
    match = PAIR_RE.match(text)
    if match:
        return [match.group(1), match.group(2)]
    match = RANGE_RE.match(text) or TM_RE.match(text)
    if match:
        start, end = int(match.group(1)), int(match.group(2))
        if start > end:
            start, end = end, start
        return [str(num) for num in range(start, end + 1)]
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
            try:
                resp = requests.get(endpoint, params=params, timeout=PDOK_TIMEOUT, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                docs = data.get("response", {}).get("docs", [])
                postcode = docs[0].get("postcode", "") if docs else ""
                if postcode:
                    break
            except requests.RequestException as exc:  # pragma: no cover - network failures
                print(f"[warn] PDOK lookup failed via {endpoint} for '{query}': {exc}")
                continue
        normalized = normalize_postcode(postcode)
        self.cache[key] = normalized
        return normalized

    @staticmethod
    def _cache_key(street: str, number: str, suffix: str, place: str) -> str:
        return "|".join(
            normalize_component(part)
            for part in (street, number, suffix or "", place)
        )


def fill_missing_postcodes(df: pd.DataFrame, client: PdokClient) -> pd.DataFrame:
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
            continue
        looked_up = client.lookup_postcode(street, number, suffix, place)
        if looked_up:
            df.at[idx, COL_POSTCODE] = looked_up
    return df


def normalize_component(value: str) -> str:
    text = (value or "").strip().lower()
    return re.sub(r"\s+", " ", text)


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
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    if args.max_rows is not None:
        df = df.head(args.max_rows)
    df = ensure_address_columns(df)
    df = expand_house_numbers(df)
    df = fill_missing_postcodes(df, PdokClient())
    df[COL_ADDRESS_KEY] = df.apply(build_address_key, axis=1)
    df.to_csv(output_path, index=False)
    print(f"[info] Wrote {output_path} with {len(df)} rows.")


if __name__ == "__main__":
    main()
