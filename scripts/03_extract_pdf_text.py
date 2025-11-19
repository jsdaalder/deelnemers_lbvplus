#!/usr/bin/env python3
"""
lbv_pipeline.py
- Reads IN_CSV
- Extracts PDF text into existing column TEXT_PDF (if empty)
- Ignores TEXT_HTML presence (HTML shouldn't block PDF extraction)
- Writes OUT_CSV
- Prints detailed diagnostics
"""

import os
import sys
import math
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

# ================== Config ===================================
IN_CSV        = DATA_DIR / "02_lbv_enriched.csv"
OUT_CSV       = DATA_DIR / "03_lbv_enriched_with_pdf.csv"

COL_TEXT_PDF  = "TEXT_PDF"         # existing column (M)
COL_PDF_PATH  = "LOCAL_PDF_PATH"   # existing column (N)
COL_TEXT_HTML = "TEXT_HTML"        # existing column (I) — used only for totals

MAX_WORKERS   = max(4, os.cpu_count() or 4)
MAX_CHARS     = 1_000_000          # truncate extremely large outputs
MAX_PAGES     = None               # None = all pages; optionally set an int (e.g., 50)
FORCE_OVERWRITE = False            # True = re-extract even if TEXT_PDF has content
# =============================================================

# pip install pdfminer.six pandas
def extract_pdf_text(path: Path, max_pages=None, max_chars=MAX_CHARS) -> str:
    """Return extracted text or '' on failure or image-only."""
    try:
        from pdfminer.high_level import extract_text
    except Exception as e:
        raise RuntimeError("pdfminer.six is required: pip install pdfminer.six") from e

    try:
        if not path or not str(path).strip():
            return ""
        if not path.exists() or not path.is_file():
            return ""

        if max_pages is None:
            txt = extract_text(str(path)) or ""
        else:
            txt = extract_text(str(path), page_numbers=list(range(max_pages))) or ""

        # Normalize whitespace and truncate
        txt = " ".join(txt.split())
        if len(txt) > max_chars:
            txt = txt[:max_chars] + " [truncated]"
        return txt
    except Exception:
        return ""


def _is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return (str(value).strip() == "")


def _resolve_pdf_path(csv_dir: Path, raw: str) -> Path | None:
    if _is_empty(raw):
        return None
    s = str(raw).strip()

    # Skip URLs (this script does not download)
    if s.lower().startswith(("http://", "https://")):
        return None

    # Expand ~ and env vars
    s = os.path.expanduser(os.path.expandvars(s))
    p = Path(s)
    if not p.is_absolute():
        p = (csv_dir / p).resolve()
    return p


def _task(row_idx: int, pdf_path: Path):
    """Worker wrapper."""
    try:
        text = extract_pdf_text(pdf_path, max_pages=MAX_PAGES, max_chars=MAX_CHARS)
        return (row_idx, text)
    except Exception:
        return (row_idx, "")


def main():
    in_path = Path(IN_CSV).resolve()
    if not in_path.exists():
        print(f"Input not found: {in_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(in_path)
    csv_dir = in_path.parent

    # Validate required columns
    missing = [c for c in (COL_TEXT_PDF, COL_PDF_PATH, COL_TEXT_HTML) if c not in df.columns]
    if missing:
        print(f"Missing required column(s): {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Build worklist: TEXT_PDF empty (or FORCE_OVERWRITE) AND local file path resolvable+exists
    eligible = []
    missing_path = []
    nonexistent = []

    for i, row in df.iterrows():
        txt_pdf = row[COL_TEXT_PDF]
        if not FORCE_OVERWRITE and not _is_empty(txt_pdf):
            continue  # already filled

        raw_path = row[COL_PDF_PATH]
        p = _resolve_pdf_path(csv_dir, raw_path)
        if p is None:
            missing_path.append(i)
            continue
        if not p.exists():
            nonexistent.append((i, str(p)))
            continue

        eligible.append((i, p))

    # Nothing to do?
    if not eligible:
        total_html_nonempty = int(df[COL_TEXT_HTML].apply(lambda v: not _is_empty(v)).sum())
        total_pdf_nonempty  = int(df[COL_TEXT_PDF].apply(lambda v: not _is_empty(v)).sum())
        print("[info] Nothing to extract (no eligible rows under current rules).")
        print(f"[diag] Rows with empty/missing LOCAL_PDF_PATH: {len(missing_path)}")
        print(f"[diag] Rows with non-existent PDF file path: {len(nonexistent)}")
        if nonexistent[:5]:
            print("[diag] First 5 non-existent examples:")
            for i, p in nonexistent[:5]:
                print(f"   row {i}: {p}")
        print(f"[summary] Written this run → TEXT_HTML: 0 | TEXT_PDF: 0")
        print(f"[summary] Totals after run → non-empty TEXT_HTML: {total_html_nonempty} | non-empty TEXT_PDF: {total_pdf_nonempty}")
        df.to_csv(OUT_CSV, index=False, encoding="utf-8")
        print(f"[ok] Wrote: {OUT_CSV}")
        return

    print(f"[info] Extracting text from {len(eligible)} PDF(s) with up to {MAX_WORKERS} worker(s)...")

    pdf_written_this_run = 0
    empty_after_parse = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(_task, i, p) for (i, p) in eligible]
        completed = 0
        for fut in as_completed(futures):
            row_idx, text = fut.result()
            if text and text.strip():
                pdf_written_this_run += 1
            else:
                empty_after_parse += 1
            df.at[row_idx, COL_TEXT_PDF] = text
            completed += 1
            if completed % 25 == 0:
                print(f"[info] {completed}/{len(eligible)} done...")

    # Persist
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # Totals after run
    total_html_nonempty = int(df[COL_TEXT_HTML].apply(lambda v: not _is_empty(v)).sum())
    total_pdf_nonempty  = int(df[COL_TEXT_PDF].apply(lambda v: not _is_empty(v)).sum())

    print(f"[diag] Skipped due to missing LOCAL_PDF_PATH: {len(missing_path)}")
    print(f"[diag] Skipped due to non-existent file path: {len(nonexistent)}")
    if nonexistent[:5]:
        print("[diag] First 5 non-existent examples:")
        for i, p in nonexistent[:5]:
            print(f"   row {i}: {p}")

    print(f"[diag] Parsed but empty text (likely scan/encrypted): {empty_after_parse}")
    print(f"[summary] Written this run → TEXT_HTML: 0 | TEXT_PDF: {pdf_written_this_run}")
    print(f"[summary] Totals after run → non-empty TEXT_HTML: {total_html_nonempty} | non-empty TEXT_PDF: {total_pdf_nonempty}")
    print(f"[ok] Wrote: {OUT_CSV}")
    print("[note] Empty TEXT_PDF usually means missing file, image-only PDF, or parser failure (consider OCR).")


if __name__ == "__main__":
    main()
