#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree as ET

try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except ImportError:
    raise SystemExit("This script requires BeautifulSoup4. Install with: pip install beautifulsoup4")

import requests

BASE_URL = "https://zoek.officielebekendmakingen.nl/"
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
RUNS_DIR = DATA_DIR / "runs"
DEFAULT_OUTPUT = DATA_DIR / "01_overheid_results.csv"
DEFAULT_API_ENDPOINT = "https://repository.overheid.nl/sru"
DEFAULT_API_QUERY = 'c.product-area==officielepublicaties AND cql.textAndIndexes="lbv"'
REQUIRED_DOCUMENTSOORTS = {"provinciaal blad"}
ALLOWED_RUBRIEKEN = {"andere beschikking", "andere vergunning", "omgevingsvergunning"}
RUBRIEK_SUBSTRINGS = ("vergunning",)
ISO_INPUT_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f%z",
]
OUTPUT_FIELDNAMES = [
    "Titel",
    "Datum",
    "URL",
    "URL_PDF",
    "Overheidslaag",
    "Overheidsnaam",
    "Documentsoort",
    "Rubriek",
]
DATE_NORMALIZE_FORMATS = ["%d-%m-%Y", "%d/%m/%Y"]
NS = {
    "sru": "http://docs.oasis-open.org/ns/search-ws/sruResponse",
    "gzd": "http://standaarden.overheid.nl/sru",
    "dcterms": "http://purl.org/dc/terms/",
    "overheid": "http://standaarden.overheid.nl/owms/terms/",
    "overheidwetgeving": "http://standaarden.overheid.nl/wetgeving/",
}

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
    Columns: Titel, Datum, URL, URL_PDF, Overheidslaag, Overheidsnaam, Documentsoort, Rubriek
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

    return {
        "Titel": titel,
        "Datum": datum,
        "URL": url,
        "URL_PDF": url_pdf,
        "Overheidslaag": overheidslaag,
        "Overheidsnaam": overheidsnaam,
        "Documentsoort": "",
        "Rubriek": "",
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


def iso_to_dmy(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return dt.strftime("%d-%m-%Y")
    except ValueError:
        pass
    for fmt in ISO_INPUT_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%d-%m-%Y")
        except ValueError:
            continue
    return ""


def parse_any_date(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in ISO_INPUT_FORMATS + DATE_NORMALIZE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def dmy_to_iso(value: str) -> Optional[str]:
    dt = parse_any_date(value)
    if not dt:
        return None
    return dt.strftime("%Y-%m-%d")


def normalize_datum(value: str) -> str:
    iso_value = dmy_to_iso(value)
    if not iso_value:
        return (value or "").strip()
    return datetime.strptime(iso_value, "%Y-%m-%d").strftime("%d-%m-%Y")


def normalized_key(datum: str, url: str) -> Tuple[str, str]:
    return (normalize_datum(datum), (url or "").strip())


def ensure_output_columns(row: Dict[str, str]) -> Dict[str, str]:
    for field in OUTPUT_FIELDNAMES:
        row.setdefault(field, "")
    row["Datum"] = normalize_datum(row.get("Datum", ""))
    return row


def load_existing_rows(path: Path) -> Tuple[List[Dict[str, str]], Set[Tuple[str, str]], Optional[str]]:
    if not path.exists():
        return [], set(), None
    rows: List[Dict[str, str]] = []
    keys: Set[Tuple[str, str]] = set()
    latest_iso: Optional[str] = None
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ensured = ensure_output_columns({k: (v or "") for k, v in row.items()})
            rows.append(ensured)
            key = normalized_key(ensured.get("Datum", ""), ensured.get("URL", ""))
            keys.add(key)
            iso_value = dmy_to_iso(ensured.get("Datum", ""))
            if iso_value and (latest_iso is None or iso_value > latest_iso):
                latest_iso = iso_value
    return rows, keys, latest_iso


def augment_query_with_since(query: str, since_iso: str) -> str:
    clause = f'dt.available>="{since_iso}"'
    query = query.strip()
    if not query:
        return clause
    return f"({query}) AND {clause}"


def text_of(elem: Optional[ET.Element]) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def parse_api_record(record: ET.Element) -> Optional[Dict[str, str]]:
    data = record.find(".//sru:recordData", NS)
    if data is None:
        return None
    title = text_of(data.find(".//dcterms:title", NS))
    if not title:
        return None

    url_html = text_of(data.find(".//gzd:preferredUrl", NS))
    if not url_html:
        url_html = text_of(data.find(".//dcterms:hasVersion", NS))

    url_pdf = ""
    for item in data.findall(".//gzd:itemUrl", NS):
        if item.get("manifestation") == "pdf":
            url_pdf = text_of(item)
            break

    date_iso = text_of(data.find(".//dcterms:available", NS)) or text_of(
        data.find(".//dcterms:modified", NS)
    )
    datum = iso_to_dmy(date_iso)

    overheidslaag = text_of(data.find(".//overheidwetgeving:organisatietype", NS))
    overheidsnaam = text_of(data.find(".//dcterms:publisher", NS)) or text_of(
        data.find(".//dcterms:creator", NS)
    )
    documentsoort = text_of(data.find(".//overheidwetgeving:publicatienaam", NS))
    rubriek = ""
    for type_node in data.findall(".//dcterms:type", NS):
        if (type_node.get("scheme") or "").strip() == "OVERHEIDop.Rubriek":
            rubriek = text_of(type_node)
            break

    if not documentsoort or documentsoort.strip().lower() not in REQUIRED_DOCUMENTSOORTS:
        return None
    normalized_rubriek = rubriek.strip().lower()
    if not normalized_rubriek:
        return None
    if normalized_rubriek not in ALLOWED_RUBRIEKEN and not any(
        needle in normalized_rubriek for needle in RUBRIEK_SUBSTRINGS
    ):
        return None

    return {
        "Titel": title,
        "Datum": datum,
        "URL": url_html,
        "URL_PDF": url_pdf,
        "Overheidslaag": overheidslaag,
        "Overheidsnaam": overheidsnaam,
        "Documentsoort": documentsoort,
        "Rubriek": rubriek,
    }


def fetch_api_rows(
    endpoint: str, query: str, start: int, limit: int, chunk: int, timeout: int
) -> tuple[List[Dict[str, str]], int]:
    session = requests.Session()
    rows: List[Dict[str, str]] = []
    next_start = max(1, start)
    total_available = 0

    while len(rows) < limit:
        remaining = limit - len(rows)
        page_size = min(max(1, chunk), remaining)
        params = {
            "operation": "searchRetrieve",
            "version": "2.0",
            "startRecord": str(next_start),
            "maximumRecords": str(page_size),
            "recordSchema": "http://standaarden.overheid.nl/sru/",
            "query": query,
        }
        try:
            resp = session.get(endpoint, params=params, timeout=timeout)
        except requests.RequestException as exc:
            raise SystemExit(f"[error] API request failed: {exc}") from exc
        if resp.status_code != 200:
            raise SystemExit(f"[error] API request failed HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as exc:
            raise SystemExit(f"[error] Failed to parse API response: {exc}") from exc

        total_text = root.findtext(".//sru:numberOfRecords", namespaces=NS, default="0")
        try:
            total_available = max(total_available, int(total_text))
        except ValueError:
            pass

        record_nodes = root.findall(".//sru:record", NS)
        if not record_nodes:
            break

        for node in record_nodes:
            parsed = parse_api_record(node)
            if parsed:
                rows.append(parsed)
                if len(rows) >= limit:
                    break

        next_pos = root.findtext(".//sru:nextRecordPosition", namespaces=NS, default="")
        if not next_pos:
            break
        try:
            next_start = int(next_pos)
        except ValueError:
            break
        if next_start <= 0:
            break

    return rows, total_available

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Extract Titel, Datum, URL, URL_PDF, Overheidslaag en Overheidsnaam uit lokale HTML of via de SRU API."
    )
    ap.add_argument(
        "--mode",
        choices=["local", "api"],
        default=None,
        help="Selecteer 'local' of 'api' als bron (laat weg voor interactieve keuze).",
    )
    ap.add_argument(
        "--files",
        nargs="+",
        help="HTML-bestanden voor --mode local (bijv. drenthe.html gelderland_1.html).",
    )
    ap.add_argument(
        "--out",
        default=str(DEFAULT_OUTPUT),
        help=f"Uitvoer CSV (standaard: {DEFAULT_OUTPUT})",
    )
    ap.add_argument(
        "--api-endpoint",
        default=DEFAULT_API_ENDPOINT,
        help="SRU API endpoint wanneer --mode api wordt gebruikt.",
    )
    ap.add_argument(
        "--api-query",
        default=DEFAULT_API_QUERY,
        help="CQL zoekopdracht voor de SRU API (bijv. 'c.product-area==officielepublicaties AND cql.textAndIndexes=\"lbv\"').",
    )
    ap.add_argument("--api-start-record", type=int, default=1, help="Eerste record (1-based) om op te halen.")
    ap.add_argument(
        "--api-max-records",
        type=int,
        default=200,
        help="Maximaal aantal records dat vanuit de API wordt opgehaald.",
    )
    ap.add_argument(
        "--api-chunk-size",
        type=int,
        default=50,
        help="Aantal records per API-request.",
    )
    ap.add_argument(
        "--api-timeout",
        type=int,
        default=30,
        help="HTTP-timeout in seconden voor API requests.",
    )
    ap.add_argument(
        "--refresh-all",
        action="store_true",
        help="Overschrijf bestaande output en haal alle resultaten opnieuw op in plaats van alleen nieuwe toevoegingen.",
    )
    ap.add_argument(
        "--meta-out",
        default="",
        help="Optional JSON sidecar with fetch metadata (total available, fetched count, cap reached).",
    )
    return ap.parse_args()


def prompt_mode(default: str = "local") -> str:
    while True:
        try:
            choice = input(f"Kies modus (local/api) [{default}]: ").strip().lower()
        except EOFError:
            choice = ""
        if not choice:
            return default
        if choice in ("local", "api"):
            return choice
        print("Voer 'local' of 'api' in aub.")


def collect_rows(args: argparse.Namespace) -> List[Dict[str, str]]:
    if args.mode == "local":
        if not args.files:
            raise SystemExit("Gebruik --files wanneer --mode local is geselecteerd.")
        inputs = [Path(p).expanduser().resolve() for p in args.files]
        for path in inputs:
            if not path.exists():
                raise SystemExit(f"Input niet gevonden: {path}")
        rows: List[Dict[str, str]] = []
        for path in inputs:
            try:
                rows.extend(parse_file(path))
            except Exception as exc:
                print(f"[WARN] Failed to parse {path.name}: {exc}")
        return rows

    print(
        f"[info] API mode: endpoint={args.api_endpoint} query='{args.api_query}' "
        f"(max {args.api_max_records}, chunk {args.api_chunk_size})"
    )
    rows, total_available = fetch_api_rows(
        endpoint=args.api_endpoint,
        query=args.api_query,
        start=args.api_start_record,
        limit=args.api_max_records,
        chunk=args.api_chunk_size,
        timeout=args.api_timeout,
    )
    args._api_total_available = total_available
    return rows


def write_meta(args: argparse.Namespace, out_path: Path, fetched_rows: int, new_rows: int) -> None:
    if not args.meta_out:
        return
    meta_path = Path(args.meta_out).expanduser().resolve()
    total_available = int(getattr(args, "_api_total_available", 0) or 0)
    start = int(args.api_start_record) if args.mode == "api" else 1
    requested_limit = int(args.api_max_records) if args.mode == "api" else fetched_rows
    reached_cap = bool(args.mode == "api" and fetched_rows >= requested_limit)
    more_available = bool(
        args.mode == "api"
        and total_available > 0
        and (start - 1 + fetched_rows) < total_available
    )
    payload = {
        "mode": args.mode,
        "out_path": str(out_path),
        "fetched_rows": fetched_rows,
        "new_rows": new_rows,
        "api_total_available": total_available,
        "api_start_record": start,
        "api_max_records": requested_limit,
        "api_chunk_size": int(args.api_chunk_size) if args.mode == "api" else 0,
        "reached_cap": reached_cap,
        "more_available": more_available,
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def log_run(args: argparse.Namespace, out_path: Path, total_rows: int, new_rows: int) -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUNS_DIR / "01_parse_overheid_pages_runs.log"
    now = datetime.now().isoformat(timespec="seconds")
    mode = args.mode or ""
    parts = [
        f"time={now}",
        f"mode={mode}",
        f"out={out_path}",
        f"total_rows={total_rows}",
        f"new_rows={new_rows}",
    ]
    if mode == "api":
        parts.extend(
            [
                f"endpoint={args.api_endpoint}",
                f"query={args.api_query}",
                f"start={args.api_start_record}",
                f"max={args.api_max_records}",
                f"chunk={args.api_chunk_size}",
            ]
        )
    else:
        files = ",".join(args.files or [])
        parts.append(f"files={files}")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(" | ".join(parts) + "\n")


def main():
    args = parse_args()
    if not args.mode:
        args.mode = prompt_mode()

    out_path = Path(args.out).expanduser().resolve()
    existing_rows: List[Dict[str, str]] = []
    existing_keys: Set[Tuple[str, str]] = set()
    latest_iso: Optional[str] = None

    if not args.refresh_all:
        existing_rows, existing_keys, latest_iso = load_existing_rows(out_path)
        if existing_rows:
            print(f"[info] Loaded {len(existing_rows)} bestaande regels uit {out_path}")

    if args.mode == "api" and latest_iso and not args.refresh_all and int(args.api_start_record) <= 1:
        args.api_query = augment_query_with_since(args.api_query, latest_iso)
        print(
            f"[info] Alleen nieuwe resultaten ophalen vanaf {latest_iso}. Gebruik --refresh-all om alles opnieuw op te halen."
        )

    fetched_rows = collect_rows(args)
    ensured_rows = [ensure_output_columns(r) for r in fetched_rows]

    new_rows: List[Dict[str, str]] = []
    for row in ensured_rows:
        key = normalized_key(row.get("Datum", ""), row.get("URL", ""))
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(row)

    if existing_rows and not new_rows and not args.refresh_all:
        print("[info] Geen nieuwe bekendmakingen gevonden; bestaande output blijft ongewijzigd.")
        return

    if existing_rows:
        combined_rows = existing_rows + new_rows
    else:
        combined_rows = new_rows

    if not combined_rows:
        print("[warn] Geen resultaten gevonden.")
        return

    seen = set()
    deduped: List[Dict[str, str]] = []
    for r in combined_rows:
        key = (r["Titel"], r["Datum"], r["URL"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    def date_key(dmy: str):
        m = re.match(r"^\s*(\d{1,2})-(\d{1,2})-(\d{4})\s*$", dmy)
        if not m:
            return (0, 0, 0)
        d, mth, y = map(int, m.groups())
        return (y, mth, d)

    deduped.sort(key=lambda r: (date_key(r["Datum"]), r["Titel"]), reverse=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES)
        w.writeheader()
        for r in deduped:
            w.writerow(r)

    fetched_count = len(ensured_rows)
    total_available = int(getattr(args, "_api_total_available", 0) or 0)
    if args.mode == "api" and total_available:
        print(
            f"[info] API page fetched {fetched_count} raw search hits out of {total_available} total raw API hits "
            f"(cap {args.api_max_records}); accepted new scraper records this page: {len(new_rows)}."
        )
    print(f"Wrote {len(deduped)} rows → {out_path} (nieuwe records: {len(new_rows)})")
    write_meta(args, out_path, fetched_count, len(new_rows))
    log_run(args, out_path, len(deduped), len(new_rows))

if __name__ == "__main__":
    main()
