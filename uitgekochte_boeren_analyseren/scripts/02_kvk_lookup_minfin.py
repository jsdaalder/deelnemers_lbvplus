"""KVK lookup for minfin_dataset.csv using the same setup as 02_kvk_permits.py (resumable, incremental)."""
from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"

INPUT_CSV = RAW_DIR / "minfin_dataset.csv"
OUTPUT_CSV = PROCESSED_DIR / "03_kvk_minfin_results.csv"

# Endpoint aligned with browser search; adjust if KVK changes their public search API.
url = "https://web-api.kvk.nl/zoeken/v3/search"

# Update cookie/profileId/User-Agent from a live browser request if you get blocked/empty responses.
headers = {
    # Cookie/profileId/User-Agent copied from the working permit lookup (refresh from browser if requests start failing).
    "cookie": "TS01be00eb=014252a75b1c0c90a1249801f389b98a068b5c69667a6869475efeff5491ec9147a842fe9964fc6b8436c999efc56fe55e4a79bde5",
    "Host": "web-api.kvk.nl",
    "Accept": "application/json, application/hal+json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://www.kvk.nl",
    "profileId": "5C10A89D-635E-49CC-94B8-042DD533B64A",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}


def clean(val: object) -> str:
    if pd.isna(val):
        return ""
    return str(val).strip()


def load_existing(path: Path) -> Dict[str, dict]:
    """Load existing rows so we can retry the ones without addresses."""
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        by_key: Dict[str, dict] = {}
        for row in reader:
            key = row.get("kvk_nummer_minfin") or row.get("minfin_id") or row.get("ontvanger") or ""
            if not key:
                continue
            by_key[key] = row
        return by_key


def write_rows(path: Path, rows: Iterable[dict], fieldnames: List[str]) -> None:
    exists = path.exists()
    mode = "a" if exists else "w"
    with path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_queries(df: pd.DataFrame) -> List[Tuple[str, str, str, str]]:
    queries: List[Tuple[str, str, str, str]] = []
    for _, r in df.iterrows():
        mid = clean(r.get("RIS/IBOS-nummer", ""))
        kvk_num = clean(r.get("KVKnummer", ""))
        ontvanger = clean(r.get("Ontvanger", ""))
        query = kvk_num if kvk_num else ontvanger
        if not query:
            continue
        queries.append((mid, kvk_num, query, ontvanger))
    return queries


def main() -> None:
    df = pd.read_csv(INPUT_CSV)
    queries = build_queries(df)
    existing = load_existing(OUTPUT_CSV)

    total_rows = len(df)
    unique_queries = len(queries)
    found_bezoek = found_post = found_both = 0

    fieldnames = [
        "minfin_id",
        "ontvanger",
        "query",
        "kvk_nummer_minfin",
        "status_code",
        "company_name",
        "kvk_nummer",
        "actief",
        "rechtsvorm",
        "bezoek_straat",
        "bezoek_huisnummer",
        "bezoek_postcode",
        "bezoek_plaats",
        "post_straat",
        "post_huisnummer",
        "post_postcode",
        "post_plaats",
        "sbi_codes",
        "error",
    ]

    out_rows: List[dict] = []

    for idx, (minfin_id, kvk_minfin, query, ontvanger) in enumerate(queries):
        key = kvk_minfin or minfin_id or ontvanger
        prior = existing.get(key)
        has_addr = prior and any(
            prior.get(col)
            for col in ["bezoek_straat", "bezoek_huisnummer", "post_straat", "post_huisnummer"]
        )
        if prior and has_addr:
            out_rows.append(prior)
            continue
        def do_request(q: str):
            params = {
                "q": q,
                "language": "nl",
                "site": "kvk2014",
                "size": "10",
                "start": "0",
                "inschrijvingsstatus": "ingeschreven",
            }
            resp_local = requests.get(url, headers=headers, params=params, timeout=10)
            print("status", resp_local.status_code, "body snippet", resp_local.text[:200])
            return resp_local

        print(f"Zoekopdracht: {query} (minfin_id: {minfin_id})")
        resp = None
        items = []
        used_query = query
        error_msg = ""
        try:
            resp = do_request(query)
            data = resp.json()
            items = data.get("data", {}).get("items", []) or []
        except Exception as exc:  # noqa: BLE001
            error_msg = str(exc)

        # Fallback: if no items and we have both kvk number and ontvanger, try the ontvanger name
        if (not items) and kvk_minfin and ontvanger and query != ontvanger:
            try:
                used_query = ontvanger
                resp = do_request(used_query)
                data = resp.json()
                items = data.get("data", {}).get("items", []) or []
                error_msg = ""
            except Exception as exc:  # noqa: BLE001
                error_msg = f"{error_msg} | fallback:{exc}" if error_msg else f"fallback:{exc}"

        if not items:
            out_rows.append(
                {
                    "minfin_id": minfin_id,
                    "ontvanger": ontvanger,
                    "query": used_query,
                    "kvk_nummer_minfin": kvk_minfin,
                    "status_code": getattr(resp, "status_code", ""),
                    "company_name": "",
                    "kvk_nummer": "",
                    "actief": "",
                    "rechtsvorm": "",
                    "bezoek_straat": "",
                    "bezoek_huisnummer": "",
                    "bezoek_postcode": "",
                    "bezoek_plaats": "",
                    "post_straat": "",
                    "post_huisnummer": "",
                    "post_postcode": "",
                    "post_plaats": "",
                    "sbi_codes": "",
                    "error": error_msg,
                }
            )
        else:
            for item in items:
                bezoek = item.get("bezoeklocatie", {}) or {}
                post = item.get("postlocatie", {}) or {}
                has_bezoek = bool(bezoek)
                has_post = bool(post)
                if has_bezoek:
                    found_bezoek += 1
                if has_post:
                    found_post += 1
                if has_bezoek and has_post:
                    found_both += 1
                out_rows.append(
                    {
                        "minfin_id": minfin_id,
                        "ontvanger": ontvanger,
                        "query": used_query,
                        "kvk_nummer_minfin": kvk_minfin,
                        "status_code": getattr(resp, "status_code", ""),
                        "company_name": item.get("naam", ""),
                        "kvk_nummer": item.get("kvkNummer", ""),
                        "actief": item.get("actief", ""),
                        "rechtsvorm": item.get("rechtsvormCode", ""),
                        "bezoek_straat": bezoek.get("straat", ""),
                        "bezoek_huisnummer": bezoek.get("huisnummer", ""),
                        "bezoek_postcode": bezoek.get("postcode", ""),
                        "bezoek_plaats": bezoek.get("plaats", ""),
                        "post_straat": post.get("straat", ""),
                        "post_huisnummer": post.get("huisnummer", ""),
                        "post_postcode": post.get("postcode", ""),
                        "post_plaats": post.get("plaats", ""),
                        "sbi_codes": ",".join(
                            [a.get("code", "") for a in item.get("activiteiten", []) if a.get("code")]
                        ),
                        "error": error_msg,
                    }
                )

        time.sleep(1)

    # overwrite with combined previous + new (replacing errors/empty address rows)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in out_rows:
            writer.writerow(row)

    print(
        f"KVK minfin lookup done. Total rows: {total_rows}, unique queries: {unique_queries}.\n"
        f"Bezoekadres found for: {found_bezoek}; postadres found for: {found_post}; with both: {found_both}.\n"
        f"Results written to {OUTPUT_CSV}"
    )


if __name__ == "__main__":
    main()
