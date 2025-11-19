#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
from html import unescape
from typing import List, Dict
import csv
import re

try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except ImportError:
    raise SystemExit("This script requires BeautifulSoup4. Install with: pip install beautifulsoup4")

BASE_URL = "https://zoek.officielebekendmakingen.nl/"

def absolute_url(href: str, base: str = BASE_URL) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith(("http://", "https://")):
        return href
    return base.rstrip("/") + "/" + href.lstrip("/")

def gettext_trim(el) -> str:
    return unescape(el.get_text(strip=True)) if el else ""

def parse_li(li) -> Dict[str, str]:
    """
    Parse a single <li> result block. Returns dict for CSV row.
    Columns: Titel, Datum, URL, URL_PDF, Overheidslaag, Overheidsnaam, Zoekterm
    """
    # 1) Titel + URL (prefer the 'result--subtitle' anchor)
    a_sub = li.select_one('a.result--subtitle')
    titel = gettext_trim(a_sub) if a_sub else ""
    href = a_sub.get("href", "").strip() if a_sub else ""

    if not titel:
        # Fallback: sometimes only <h2><a> is present
        h2a = li.select_one("h2 a")
        if h2a:
            titel = gettext_trim(h2a)
            href = h2a.get("href", href)

    url = absolute_url(href)

    # 2) Datum publicatie
    datum = ""
    for dt in li.select("dl dt"):
        if gettext_trim(dt).lower() == "datum publicatie":
            dd = dt.find_next_sibling("dd")
            datum = gettext_trim(dd)
            break

    # 3) Overheidsnaam (Organisatie)
    overheidsnaam = ""
    for dt in li.select("dl dt"):
        if gettext_trim(dt).lower() == "organisatie":
            dd = dt.find_next_sibling("dd")
            overheidsnaam = gettext_trim(dd)
            break

    # 4) URL_PDF (first .pdf link in the block)
    url_pdf = ""
    a_pdf = li.select_one('a[href$=".pdf"], a[href*=".pdf"]')
    if a_pdf and a_pdf.has_attr("href"):
        url_pdf = absolute_url(a_pdf["href"])

    # Leave these empty per request
    overheidslaag = ""
    zoekterm = ""

    return {
        "Titel": titel,
        "Datum": datum,
        "URL": url,
        "URL_PDF": url_pdf,
        "Overheidslaag": overheidslaag,
        "Overheidsnaam": overheidsnaam,
        "Zoekterm": zoekterm,
    }

def parse_file(html_path: Path) -> List[Dict[str, str]]:
    text = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(text, "html.parser")

    # Results live under <div id="Publicaties"> <ul> <li>…</li>
    lis = soup.select("#Publicaties ul > li")
    rows: List[Dict[str, str]] = []
    for li in lis:
        row = parse_li(li)
        # Keep rows with minimum required fields
        if row["Titel"] and row["Datum"] and row["URL"]:
            rows.append(row)
    return rows

def main():
    ap = argparse.ArgumentParser(
        description="Extract Titel, Datum, URL, URL_PDF, Overheidslaag, Overheidsnaam, Zoekterm from overheid.nl result pages."
    )
    ap.add_argument(
        "--files",
        nargs="+",
        required=True,
        help="HTML files to parse (e.g., drenthe.html gelderland_1.html gelderland_2.html gelderland_3.html)",
    )
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    inputs = [Path(p).expanduser().resolve() for p in args.files]
    for p in inputs:
        if not p.exists():
            raise SystemExit(f"Input not found: {p}")

    all_rows: List[Dict[str, str]] = []
    for p in inputs:
        try:
            all_rows.extend(parse_file(p))
        except Exception as e:
            print(f"[WARN] Failed to parse {p.name}: {e}")

    # Deduplicate exact duplicates (Titel, Datum, URL) across files
    seen = set()
    deduped: List[Dict[str, str]] = []
    for r in all_rows:
        key = (r["Titel"], r["Datum"], r["URL"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    # Sort by date (DD-MM-YYYY) desc, then Titel asc
    def date_key(dmy: str):
        m = re.match(r"^\s*(\d{1,2})-(\d{1,2})-(\d{4})\s*$", dmy)
        if not m:
            return (0, 0, 0)
        d, mth, y = map(int, m.groups())
        return (y, mth, d)

    deduped.sort(key=lambda r: (date_key(r["Datum"]), r["Titel"]), reverse=True)

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["Titel", "Datum", "URL", "URL_PDF", "Overheidslaag", "Overheidsnaam", "Zoekterm"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in deduped:
            w.writerow(r)

    print(f"Wrote {len(deduped)} rows → {out_path}")

if __name__ == "__main__":
    main()
