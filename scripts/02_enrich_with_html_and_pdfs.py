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
from pathlib import Path
from typing import Dict, Tuple
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
    for col in ("TEXT_HTML", "TEXT_PDF", "LOCAL_PDF_PATH", "HTML_STATUS", "HTML_TYPE", "PDF_STATUS", "PDF_TYPE"):
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str)
    return df


def ensure_doc_ids(df: pd.DataFrame) -> pd.DataFrame:
    if "doc_id" not in df.columns:
        df["doc_id"] = ""
    df["doc_id"] = df["doc_id"].fillna("").astype(str)

    used = set()
    counter = 1
    resolved: list[str] = []
    for raw in df["doc_id"]:
        candidate = raw.strip()
        if not candidate:
            candidate = f"doc_{counter:05d}"
            counter += 1
        base = candidate
        suffix = 1
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        resolved.append(candidate)
    df["doc_id"] = resolved
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
        df.at[idx, "PDF_STATUS"] = pdf_status
        df.at[idx, "PDF_TYPE"] = pdf_type
        pdf_counts[pdf_status] += 1

        url_html = str(row.get("URL_BEKENDMAKING", "") or row.get("URL", "")).strip()
        text_html, html_status, html_type = fetch_html(url_html, session, html_cache, args.delay, args.max_html_chars)
        df.at[idx, "TEXT_HTML"] = text_html
        df.at[idx, "HTML_STATUS"] = html_status
        df.at[idx, "HTML_TYPE"] = html_type
        html_counts[html_status] += 1

        # Ensure TEXT_PDF placeholder remains empty if missing
        if not isinstance(row.get("TEXT_PDF", ""), str):
            df.at[idx, "TEXT_PDF"] = ""

        if (idx + 1) % 25 == 0 or idx + 1 == total:
            print(f"[progress] {idx + 1}/{total} processed")

    print("[stats] PDF statuses:", dict(pdf_counts))
    print("[stats] HTML statuses:", dict(html_counts))
    return df


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input_path)
    df = load_metadata(input_path)
    df = ensure_standard_columns(df)
    df = ensure_doc_ids(df)

    df = enrich(df, args)

    output_path = Path(args.output_path).expanduser().resolve()
    df.to_csv(output_path, index=False)
    print(f"[done] Wrote {len(df)} rows → {output_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\n[abort] Interrupted by user.")
