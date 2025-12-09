#!/usr/bin/env python3
"""
2_enrich_with_html_and_pdfs.py

Bridges the gap between the overheid.nl metadata export and the downstream LBV
pipeline by:
  * ensuring each row has a unique doc_id,
  * downloading PDFs locally and recording LOCAL_PDF_PATH,
  * fetching/parsing HTML pages and extracting TEXT_HTML,
  * creating the TEXT_PDF column (empty placeholders),
  * persisting the enriched CSV that later scripts operate on.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --------- Defaults / constants ------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PDF_DIR = REPO_ROOT / "pdfs"

INPUT_CANDIDATES = [
    DATA_DIR / "01_overheid_results.csv",
    DATA_DIR / "01_overheid_parsed.csv",
    REPO_ROOT / "overheid_result.csv",
    REPO_ROOT / "overheid_results.csv",
    REPO_ROOT / "overheid_parsed.csv",
]
DEFAULT_OUTPUT = DATA_DIR / "02_lbv_enriched.csv"
REQUEST_TIMEOUT = 30  # seconds
DOWNLOAD_DELAY = 0.25  # politeness throttle between remote fetches
MAX_HTML_CHARS = 100_000
USER_AGENT = "lbv-enricher/1.0 (+https://github.com/jandaalder/deelnemers_lbvplus)"

ENSURE_COLUMNS = {
    "URL_BEKENDMAKING": ["URL_BEKENDMAKING", "URL", "document_url", "documentUrl"],
    "URL_PDF": ["URL_PDF", "pdf_url", "PDF_URL"],
    "Titel": ["Titel", "title", "subtitle_clean", "subtitle_raw"],
    "Datum": ["Datum", "publication_date", "publication_date_iso"],
}
FINAL_COLUMN_ORDER = [
    "doc_id",
    "Titel",
    "Datum",
    "URL_BEKENDMAKING",
    "URL_PDF",
    "Overheidslaag",
    "Overheidsnaam",
    "Documentsoort",
    "Rubriek",
    "LOCAL_PDF_PATH",
    "TEXT_HTML",
    "TEXT_PDF",
    "URL",
    "doc_id_old_style",
]
DATE_FORMATS = ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"]
NEW_DOC_PATTERN = re.compile(r"^doc_(\d{5})$")

HTML_STRIP_TAGS = ("script", "style", "nav", "header", "footer", "noscript", "iframe", "aside")


# --------- Helpers -------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich overheid.nl metadata with local HTML/PDF content.")
    ap.add_argument("--in", dest="input_path", help="Input CSV (default: autodetect overheid_result.csv)", default=None)
    ap.add_argument("--out", dest="output_path", help=f"Output CSV (default: {DEFAULT_OUTPUT})", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--pdf-dir", dest="pdf_dir", default=str(PDF_DIR), help="Directory where PDFs are stored/downloaded.")
    ap.add_argument("--delay", dest="delay", type=float, default=DOWNLOAD_DELAY, help="Delay (seconds) between downloads.")
    ap.add_argument("--max-html-chars", dest="max_html_chars", type=int, default=MAX_HTML_CHARS, help="Maximum TEXT_HTML length.")
    return ap.parse_args()


def resolve_input_path(arg_value: str | None) -> Path:
    if arg_value:
        path = Path(arg_value).expanduser().resolve()
        if not path.exists():
            raise SystemExit(f"[error] Input CSV not found: {path}")
        return path
    for candidate in INPUT_CANDIDATES:
        path = Path(candidate).expanduser().resolve()
        if path.exists():
            print(f"[info] Using input CSV: {path}")
            return path
    raise SystemExit("[error] No input CSV found. Provide --in or place one of: "
                     + ", ".join(INPUT_CANDIDATES))


def load_metadata(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df


def ensure_standard_columns(df: pd.DataFrame) -> pd.DataFrame:
    for target, candidates in ENSURE_COLUMNS.items():
        if target in df.columns:
            df[target] = df[target].fillna("").astype(str)
            continue
        for cand in candidates:
            if cand in df.columns:
                df[target] = df[cand].fillna("").astype(str)
                break
        else:
            df[target] = ""
    # Guarantee placeholder columns required downstream
    for col in ("TEXT_HTML", "TEXT_PDF", "LOCAL_PDF_PATH"):
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str)
    return df


def ensure_doc_ids(df: pd.DataFrame, existing: Optional[Sequence[str]] = None) -> pd.DataFrame:
    if "doc_id" not in df.columns:
        df["doc_id"] = ""
    df["doc_id"] = df["doc_id"].fillna("").astype(str)
    if "doc_id_old_style" not in df.columns:
        df["doc_id_old_style"] = ""
    df["doc_id_old_style"] = df["doc_id_old_style"].fillna("").astype(str)

    def collect_existing(values: Sequence[str]) -> Tuple[set, int]:
        used_local = set()
        max_num = 0
        for value in values:
            val = (value or "").strip()
            match = NEW_DOC_PATTERN.match(val)
            if match:
                used_local.add(val)
                max_num = max(max_num, int(match.group(1)))
        return used_local, max_num

    used_existing, max_existing_num = collect_existing(existing or [])
    current_values = df["doc_id"].tolist()
    used_current, max_current_num = collect_existing(current_values)
    used = used_existing | used_current
    counter = max(max_existing_num, max_current_num) + 1 if used else 1

    def next_id() -> str:
        nonlocal counter
        while True:
            candidate = f"doc_{counter:05d}"
            counter += 1
            if candidate not in used:
                used.add(candidate)
                return candidate

    resolved: list[str] = []
    old_styles = df["doc_id_old_style"].tolist()
    for idx, raw in enumerate(current_values):
        candidate = (raw or "").strip()
        if candidate and candidate in used and NEW_DOC_PATTERN.match(candidate):
            resolved.append(candidate)
            continue
        if candidate and not old_styles[idx]:
            old_styles[idx] = candidate
        new_id = next_id()
        resolved.append(new_id)

    df["doc_id"] = resolved
    df["doc_id_old_style"] = old_styles
    return df


def sanitize_filename(name: str) -> str:
    name = unquote(name)
    name = name.replace("\\", "_").replace("/", "_")
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    name = name.strip("._")
    return name or "download"


def guess_pdf_filename(url_pdf: str, doc_id: str) -> str:
    parsed = urlparse(url_pdf)
    candidate = Path(parsed.path).name
    candidate = sanitize_filename(candidate)
    if not candidate.lower().endswith(".pdf"):
        candidate = f"{candidate or doc_id}.pdf"
    if not candidate:
        candidate = f"{doc_id}.pdf"
    return candidate


def download_pdf(url_pdf: str, doc_id: str, session: requests.Session, pdf_dir: Path, delay: float) -> Tuple[str, str, str]:
    if not url_pdf:
        return "", "missing_url", ""
    filename = guess_pdf_filename(url_pdf, doc_id)
    pdf_path = pdf_dir / filename
    if pdf_path.exists():
        return str(pdf_path), "exists", ""
    try:
        resp = session.get(url_pdf, timeout=REQUEST_TIMEOUT)
        time.sleep(delay)
    except requests.RequestException as exc:
        print(f"[warn] PDF download failed ({doc_id}): {exc}")
        return "", "error", ""
    if resp.status_code != 200:
        print(f"[warn] PDF HTTP {resp.status_code} ({doc_id}): {url_pdf}")
        return "", "http_error", resp.headers.get("Content-Type", "")
    pdf_dir.mkdir(parents=True, exist_ok=True)
    pdf_path.write_bytes(resp.content)
    ctype = resp.headers.get("Content-Type", "")
    return str(pdf_path), "downloaded", ctype


def extract_html_text(html: str, max_chars: int) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in HTML_STRIP_TAGS:
        for match in soup.find_all(tag):
            match.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = " ".join(text.split())
    if len(text) > max_chars:
        text = text[:max_chars] + " [truncated]"
    return text


def fetch_html(url: str, session: requests.Session, cache: Dict[str, Tuple[str, str]], delay: float, max_chars: int) -> Tuple[str, str, str]:
    if not url:
        return "", "missing_url", ""
    if url in cache:
        text, ctype = cache[url]
        return text, "cached", ctype
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        time.sleep(delay)
    except requests.RequestException as exc:
        print(f"[warn] HTML fetch failed: {exc} ({url})")
        return "", "error", ""
    if resp.status_code != 200:
        print(f"[warn] HTML HTTP {resp.status_code}: {url}")
        return "", "http_error", resp.headers.get("Content-Type", "")
    ctype = resp.headers.get("Content-Type", "")
    try:
        text = extract_html_text(resp.text, max_chars)
    except Exception as exc:
        print(f"[warn] HTML parse error: {exc} ({url})")
        return "", "parse_error", ctype
    cache[url] = (text, ctype)
    return text, "ok", ctype


def enrich(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    pdf_dir = Path(args.pdf_dir).expanduser().resolve()
    html_cache: Dict[str, Tuple[str, str]] = {}
    pdf_counts: Counter[str] = Counter()
    html_counts: Counter[str] = Counter()

    total = len(df)
    print(f"[info] Enriching {total} row(s)...")
    for idx, row in df.iterrows():
        doc_id = row["doc_id"]
        url_pdf = str(row.get("URL_PDF", "")).strip()
        local_path, pdf_status, pdf_type = download_pdf(url_pdf, doc_id, session, pdf_dir, args.delay)
        df.at[idx, "LOCAL_PDF_PATH"] = local_path
        pdf_counts[pdf_status] += 1

        url_html = str(row.get("URL_BEKENDMAKING", "") or row.get("URL", "")).strip()
        text_html, html_status, html_type = fetch_html(url_html, session, html_cache, args.delay, args.max_html_chars)
        df.at[idx, "TEXT_HTML"] = text_html
        html_counts[html_status] += 1

        # Ensure TEXT_PDF placeholder remains empty if missing
        if not isinstance(row.get("TEXT_PDF", ""), str):
            df.at[idx, "TEXT_PDF"] = ""

        if (idx + 1) % 25 == 0 or idx + 1 == total:
            print(f"[progress] {idx + 1}/{total} processed")

    print("[stats] PDF statuses:", dict(pdf_counts))
    print("[stats] HTML statuses:", dict(html_counts))
    return df


def normalize_date(value: str) -> str:
    value = (value or "").strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%d-%m-%Y")
        except ValueError:
            continue
    return value


def canonical_url(row: Dict[str, str]) -> str:
    url = row.get("URL_BEKENDMAKING") or row.get("URL") or ""
    return url.strip()


def row_key(row: Dict[str, str]) -> Tuple[str, str]:
    return (normalize_date(row.get("Datum", "")), canonical_url(row))


def merge_metadata(existing: Dict[str, str], fresh: Dict[str, str]) -> Dict[str, str]:
    merged = existing.copy()
    for field in (
        "Titel",
        "Datum",
        "URL_BEKENDMAKING",
        "URL_PDF",
        "URL",
        "Overheidslaag",
        "Overheidsnaam",
        "Documentsoort",
        "Rubriek",
    ):
        if field in fresh:
            merged[field] = fresh[field]
    return merged


def load_existing_output(path: Path) -> Tuple[List[Dict[str, str]], Dict[Tuple[str, str], Dict[str, str]], List[str]]:
    if not path.exists():
        return [], {}, []
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = ensure_standard_columns(df)
    df = ensure_doc_ids(df)
    records = df.to_dict("records")
    lookup = {row_key(rec): rec for rec in records}
    doc_ids = [rec.get("doc_id", "") for rec in records]
    return records, lookup, doc_ids


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input_path)
    df = load_metadata(input_path)
    df = ensure_standard_columns(df)
    output_path = Path(args.output_path).expanduser().resolve()
    existing_records, existing_lookup, existing_doc_ids = load_existing_output(output_path)

    metadata_records = df.to_dict("records")
    indexed_results: Dict[int, Dict[str, str]] = {}
    new_rows_with_index: List[Tuple[int, Dict[str, str]]] = []

    for idx, record in enumerate(metadata_records):
        key = row_key(record)
        if key in existing_lookup:
            indexed_results[idx] = merge_metadata(existing_lookup[key], record)
        else:
            new_rows_with_index.append((idx, record))

    if new_rows_with_index:
        new_df = pd.DataFrame([rec for _, rec in new_rows_with_index])
        new_df = ensure_standard_columns(new_df)
        new_df = ensure_doc_ids(new_df, existing_doc_ids)
        new_df = enrich(new_df, args)
        for (idx, _), row in zip(new_rows_with_index, new_df.to_dict("records")):
            indexed_results[idx] = row
            if row.get("doc_id"):
                existing_doc_ids.append(row["doc_id"])
    else:
        print("[info] Geen nieuwe rijen om te verrijken; bestaand resultaat wordt hergebruikt.")

    if not indexed_results:
        print("[warn] Geen gegevens om te schrijven.")
        return

    ordered_rows = [indexed_results[i] for i in range(len(metadata_records))]
    final_df = pd.DataFrame(ordered_rows)
    final_df = ensure_standard_columns(final_df)
    final_df = ensure_doc_ids(final_df)  # ensures column exists even if untouched
    drop_cols = [c for c in ("Zoekterm", "PDF_STATUS", "PDF_TYPE", "HTML_STATUS", "HTML_TYPE") if c in final_df.columns]
    if drop_cols:
        final_df = final_df.drop(columns=drop_cols)
    columns = [col for col in FINAL_COLUMN_ORDER if col in final_df.columns] + [
        col for col in final_df.columns if col not in FINAL_COLUMN_ORDER
    ]
    final_df = final_df[columns]
    final_df.to_csv(output_path, index=False)
    print(f"[done] Wrote {len(final_df)} rows → {output_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\n[abort] Interrupted by user.")
