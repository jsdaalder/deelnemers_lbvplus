"""KVK lookup for permit dataset with resumable, incremental writes."""
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

INPUT_CSV = RAW_DIR / "06_deelnemers_lbv_lbvplus.csv"
OUTPUT_CSV = PROCESSED_DIR / "02_kvk_results.csv"

# Endpoint aligned with browser search; adjust if KVK changes their public search API.
url = "https://web-api.kvk.nl/zoeken/v3/search"

# Update cookie/profileId/User-Agent from a live browser request if you get blocked/empty responses.
headers = {
    "cookie": "TS01be00eb=014252a75b1c0c90a1249801f389b98a068b5c69667a6869475efeff5491ec9147a842fe9964fc6b8436c999efc56fe55e4a79bde5",
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


def load_processed(path: Path) -> set:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return {row.get("farm_id", "") for row in reader if row.get("farm_id")}


def write_rows(path: Path, rows: Iterable[dict], fieldnames: List[str]) -> None:
    exists = path.exists()
    mode = "a" if exists else "w"
    with path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_queries(df: pd.DataFrame) -> List[Tuple[str, str]]:
    queries: List[Tuple[str, str]] = []
    for _, r in df.iterrows():
        fid = clean(r.get("farm_id", ""))
        pc = clean(r.get("B_POSTCODE", "")).replace(" ", "")
        nr = clean(r.get("B_HUIS_NR", ""))
        toe = clean(r.get("B_HUIS_NR_TOEV", ""))
        parts = [pc]
        if nr:
            parts.append(nr + toe)
        query = " ".join(p for p in parts if p)
        queries.append((fid, query))
    return queries


def main() -> None:
    df = pd.read_csv(INPUT_CSV)
    queries = build_queries(df)
    processed = load_processed(OUTPUT_CSV)

    fieldnames = [
        "farm_id",
        "query",
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

    buffer: List[dict] = []
    flush_every = 10

    for idx, (farm_id, query) in enumerate(queries):
        if farm_id in processed:
            continue
        print(f"Zoekopdracht: {query} (farm_id: {farm_id})")
        params = {
            "q": query,
            "language": "nl",
            "site": "kvk2014",
            "size": "10",
            "start": "0",
            "inschrijvingsstatus": "ingeschreven",
        }
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            print("status", resp.status_code, "body snippet", resp.text[:200])
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            buffer.append(
                {
                    "farm_id": farm_id,
                    "query": query,
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
                    "error": str(exc),
                }
            )
            continue

        items = data.get("data", {}).get("items", []) or []
        if not items:
            buffer.append(
                {
                    "farm_id": farm_id,
                    "query": query,
                    "status_code": resp.status_code,
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
                    "error": "",
                }
            )
        else:
            for item in items:
                bezoek = item.get("bezoeklocatie", {}) or {}
                post = item.get("postlocatie", {}) or {}
                buffer.append(
                    {
                        "farm_id": farm_id,
                        "query": query,
                        "status_code": resp.status_code,
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
                        "error": "",
                    }
                )

        if len(buffer) >= flush_every:
            write_rows(OUTPUT_CSV, buffer, fieldnames)
            processed.update({row["farm_id"] for row in buffer})
            buffer.clear()
        time.sleep(1)

    if buffer:
        write_rows(OUTPUT_CSV, buffer, fieldnames)

    print(f"KVK lookup done. Results written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
