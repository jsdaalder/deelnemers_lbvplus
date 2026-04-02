#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import pandas as pd

PIPE_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PIPE_ROOT / "data"

DEFAULT_NOTICE_INPUT = DATA_DIR / "03_lbv_enriched_with_pdf.csv"
DEFAULT_FARM_INPUT = DATA_DIR / "06_all_unique_farms_review.csv"
DEFAULT_PARTICIPANTS_INPUT = DATA_DIR / "06_deelnemers_lbv_lbvplus.csv"
DEFAULT_NOTICE_OUTPUT = DATA_DIR / "08_notice_scheme_classification.csv"
DEFAULT_FARM_OUTPUT = DATA_DIR / "08_farm_scheme_classification.csv"

SCHEME_LBV = "lbv"
SCHEME_LBV_PLUS = "lbv_plus"
SCHEME_AMBIGUOUS = "ambiguous"
SCHEME_UNKNOWN = "unknown"
SCHEME_CONFLICTING = "conflicting"

DUAL_PATTERNS = [
    re.compile(r"\blbv\s*/\s*lbv\+(?![A-Za-z])", re.I),
    re.compile(r"\blbv\s*/\s*lbv-plus\b", re.I),
    re.compile(r"\blbv\s+of\s+lbv\+(?![A-Za-z])", re.I),
    re.compile(r"\blbv\s+of\s+lbv-plus\b", re.I),
]

LBV_PLUS_PATTERNS = [
    re.compile(r"\blbv\+(?![A-Za-z])", re.I),
    re.compile(r"\blbv-plus\b", re.I),
    re.compile(r"\blbv\s+plus\b", re.I),
    re.compile(r"landelijke\s+be[eë]indigingsregeling\s+veehouderijlocaties\s+met\s+piekbelasting", re.I),
    re.compile(r"\bmet\s+piekbelasting\b", re.I),
    re.compile(r"\bpiekbelasting\b", re.I),
]

LBV_PATTERNS = [
    re.compile(r"landelijke\s+be[eë]indigingsregeling\s+veehouderijlocaties\b(?!\s+met\s+piekbelasting)", re.I),
    re.compile(r"\blbv\b(?![\s-]*(?:\+|plus))", re.I),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify notices/farms as LBV or LBV+ using transparent rules.")
    parser.add_argument("--notice-input", default=str(DEFAULT_NOTICE_INPUT), help="Notice-level input CSV (default: data/03_lbv_enriched_with_pdf.csv).")
    parser.add_argument("--farm-input", default=str(DEFAULT_FARM_INPUT), help="One-row-per-farm input CSV (default: data/06_all_unique_farms_review.csv).")
    parser.add_argument("--participants-input", default=str(DEFAULT_PARTICIPANTS_INPUT), help="Full participants CSV with doc_ids_all (default: data/06_deelnemers_lbv_lbvplus.csv).")
    parser.add_argument("--notice-output", default=str(DEFAULT_NOTICE_OUTPUT), help="Notice-level scheme output CSV.")
    parser.add_argument("--farm-output", default=str(DEFAULT_FARM_OUTPUT), help="504-row farm output CSV with scheme columns.")
    return parser.parse_args()


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if text.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text).strip()


def extract_evidence_sentence(text: str, match: re.Match[str]) -> str:
    compact = clean_text(text)
    if not compact:
        return ""
    start = match.start()
    end = match.end()
    left = max(compact.rfind(".", 0, start), compact.rfind(";", 0, start), compact.rfind(":", 0, start))
    right_candidates = [compact.find(".", end), compact.find(";", end), compact.find(":", end)]
    right_candidates = [idx for idx in right_candidates if idx != -1]
    right = min(right_candidates) if right_candidates else len(compact)
    sentence = compact[left + 1:right + 1].strip(" -")
    return sentence if sentence else match.group(0)


def extract_match_context(text: str, match: re.Match[str], words_before: int = 5, words_after: int = 5) -> str:
    compact = clean_text(text)
    if not compact:
        return ""
    tokens = list(re.finditer(r"\S+", compact))
    if not tokens:
        return ""
    hit_indices = [
        i for i, token in enumerate(tokens)
        if token.start() < match.end() and token.end() > match.start()
    ]
    if not hit_indices:
        return match.group(0)
    start_i = max(0, hit_indices[0] - words_before)
    end_i = min(len(tokens) - 1, hit_indices[-1] + words_after)
    return " ".join(tokens[i].group(0) for i in range(start_i, end_i + 1))


def match_scheme(text: str) -> tuple[str, str, str]:
    for pattern in DUAL_PATTERNS:
        match = pattern.search(text)
        if match:
            return SCHEME_AMBIGUOUS, extract_evidence_sentence(text, match), extract_match_context(text, match)
    for pattern in LBV_PLUS_PATTERNS:
        match = pattern.search(text)
        if match:
            return SCHEME_LBV_PLUS, extract_evidence_sentence(text, match), extract_match_context(text, match)
    for pattern in LBV_PATTERNS:
        match = pattern.search(text)
        if match:
            return SCHEME_LBV, extract_evidence_sentence(text, match), extract_match_context(text, match)
    return SCHEME_UNKNOWN, "", ""


def classify_notice(row: pd.Series) -> dict[str, str]:
    title = clean_text(row.get("Titel", ""))
    body = " ".join(
        part for part in [clean_text(row.get("TEXT_HTML", "")), clean_text(row.get("TEXT_PDF", ""))] if part
    )

    title_scheme, title_evidence, title_context = match_scheme(title)
    if title_scheme != SCHEME_UNKNOWN:
        return {
            "scheme_class": title_scheme,
            "scheme_evidence": title_evidence,
            "scheme_match_context": title_context,
            "scheme_source": "rule_title",
        }

    body_scheme, body_evidence, body_context = match_scheme(body)
    if body_scheme != SCHEME_UNKNOWN:
        return {
            "scheme_class": body_scheme,
            "scheme_evidence": body_evidence,
            "scheme_match_context": body_context,
            "scheme_source": "rule_body",
        }

    return {
        "scheme_class": SCHEME_UNKNOWN,
        "scheme_evidence": "",
        "scheme_match_context": "",
        "scheme_source": "",
    }


def load_notice_rows(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"doc_id", "Titel", "Datum", "URL_BEKENDMAKING"}
    missing = required.difference(df.columns)
    if missing:
        raise SystemExit(f"Missing required notice columns in {path}: {', '.join(sorted(missing))}")
    return df


def load_farm_rows(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"farm_id_new", "Datum_latest", "URL_BEKENDMAKING"}
    missing = required.difference(df.columns)
    if missing:
        raise SystemExit(f"Missing required farm columns in {path}: {', '.join(sorted(missing))}")
    return df


def load_participants_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    required = {"farm_id_new", "doc_ids_all"}
    if not rows:
        return []
    missing = required.difference(rows[0].keys())
    if missing:
        raise SystemExit(f"Missing required participants columns in {path}: {', '.join(sorted(missing))}")
    return rows


def load_participants_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"farm_id_new", "doc_ids_all"}
    missing = required.difference(df.columns)
    if missing:
        raise SystemExit(f"Missing required participants columns in {path}: {', '.join(sorted(missing))}")
    return df


def resolve_farm_scheme(labels: list[str]) -> str:
    normalized = [label for label in labels if label and label != SCHEME_UNKNOWN]
    if not normalized:
        return SCHEME_AMBIGUOUS
    if SCHEME_LBV_PLUS in normalized:
        return SCHEME_LBV_PLUS
    if SCHEME_LBV in normalized:
        return SCHEME_LBV
    return SCHEME_AMBIGUOUS


def build_notice_history(doc_ids: list[str], notice_by_doc_id: dict[str, dict[str, str]]) -> str:
    history_parts: list[str] = []
    seen: set[str] = set()
    for doc_id in doc_ids:
        notice = notice_by_doc_id.get(doc_id)
        if not notice:
            continue
        key = (
            clean_text(notice.get("Datum", "")),
            clean_text(notice.get("doc_id", "")),
            clean_text(notice.get("scheme_class", "")),
            clean_text(notice.get("scheme_match_context", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        history_parts.append(
            f"{clean_text(notice.get('Datum', ''))}:{clean_text(notice.get('scheme_class', ''))}:{clean_text(notice.get('scheme_match_context', ''))}"
        )
    return " || ".join(history_parts)


def build_notice_class_set(doc_ids: list[str], notice_by_doc_id: dict[str, dict[str, str]]) -> str:
    ordered: list[str] = []
    for doc_id in doc_ids:
        notice = notice_by_doc_id.get(doc_id)
        if not notice:
            continue
        label = clean_text(notice.get("scheme_class", ""))
        if not label or label == SCHEME_UNKNOWN or label in ordered:
            continue
        ordered.append(label)
    return " | ".join(ordered)


def main() -> None:
    args = parse_args()
    notice_input = Path(args.notice_input).expanduser().resolve()
    farm_input = Path(args.farm_input).expanduser().resolve()
    participants_input = Path(args.participants_input).expanduser().resolve()
    notice_output = Path(args.notice_output).expanduser().resolve()
    farm_output = Path(args.farm_output).expanduser().resolve()

    notice_df = load_notice_rows(notice_input)
    farm_df = load_farm_rows(farm_input)
    participant_rows = load_participants_rows(participants_input)
    participants_df = load_participants_df(participants_input)

    notice_classifications = []
    scheme_by_doc_id: dict[str, dict[str, str]] = {}
    for _, row in notice_df.iterrows():
        classified = classify_notice(row)
        record = {
            "doc_id": clean_text(row.get("doc_id", "")),
            "Datum": clean_text(row.get("Datum", "")),
            "Titel": clean_text(row.get("Titel", "")),
            "URL_BEKENDMAKING": clean_text(row.get("URL_BEKENDMAKING", "")),
            "scheme_class": classified["scheme_class"],
            "scheme_evidence": classified["scheme_evidence"],
            "scheme_match_context": classified["scheme_match_context"],
            "scheme_source": classified["scheme_source"],
        }
        notice_classifications.append(record)
        if record["doc_id"]:
            scheme_by_doc_id[record["doc_id"]] = record

    notice_out_df = pd.DataFrame(notice_classifications)
    notice_output.parent.mkdir(parents=True, exist_ok=True)
    notice_out_df.to_csv(notice_output, index=False)

    notice_by_url = {record["URL_BEKENDMAKING"]: record for record in notice_classifications if record["URL_BEKENDMAKING"]}
    notice_by_doc_id = {record["doc_id"]: record for record in notice_classifications if record["doc_id"]}
    doc_ids_by_farm: dict[str, list[str]] = {}
    for row in participant_rows:
        farm_id_new = clean_text(row.get("farm_id_new", ""))
        doc_ids = [part.strip() for part in clean_text(row.get("doc_ids_all", "")).split(",") if part.strip()]
        if farm_id_new and farm_id_new not in doc_ids_by_farm:
            doc_ids_by_farm[farm_id_new] = doc_ids

    farm_out_df = farm_df.copy()
    farm_out_df["scheme_class_resolved_farm"] = farm_out_df["farm_id_new"].map(
        lambda farm_id: resolve_farm_scheme(
            [
                notice_by_doc_id.get(doc_id, {}).get("scheme_class", SCHEME_UNKNOWN)
                for doc_id in doc_ids_by_farm.get(clean_text(farm_id), [])
            ]
        )
    )
    # Backwards-compatible alias; use scheme_class_resolved_farm in downstream work.
    farm_out_df["scheme_class_farm"] = farm_out_df["scheme_class_resolved_farm"]
    farm_out_df["scheme_class_latest_notice"] = farm_out_df["URL_BEKENDMAKING"].map(
        lambda url: notice_by_url.get(clean_text(url), {}).get("scheme_class", "")
    )
    farm_out_df["scheme_match_context_latest_notice"] = farm_out_df["URL_BEKENDMAKING"].map(
        lambda url: notice_by_url.get(clean_text(url), {}).get("scheme_match_context", "")
    )
    farm_out_df["scheme_classes_all_notices"] = farm_out_df["farm_id_new"].map(
        lambda farm_id: build_notice_class_set(
            doc_ids_by_farm.get(clean_text(farm_id), []),
            notice_by_doc_id,
        )
    )
    farm_out_df["scheme_notice_history"] = farm_out_df["farm_id_new"].map(
        lambda farm_id: build_notice_history(
            doc_ids_by_farm.get(clean_text(farm_id), []),
            notice_by_doc_id,
        )
    )
    farm_columns = [
        "farm_id_new",
        "AddressKey",
        "COMPANY_NAME",
        "Datum_latest",
        "Instantie_latest",
        "stage_latest_llm",
        "stage_latest_manual",
        "URL_BEKENDMAKING",
        "scheme_class_latest_notice",
        "scheme_class_resolved_farm",
        "scheme_class_farm",
        "scheme_classes_all_notices",
        "scheme_match_context_latest_notice",
        "scheme_notice_history",
    ]
    farm_out_df = farm_out_df[farm_columns]
    farm_output.parent.mkdir(parents=True, exist_ok=True)
    farm_out_df.to_csv(farm_output, index=False)

    scheme_cols = [
        "farm_id_new",
        "scheme_class_latest_notice",
        "scheme_class_resolved_farm",
        "scheme_class_farm",
        "scheme_classes_all_notices",
        "scheme_match_context_latest_notice",
        "scheme_notice_history",
    ]
    participants_enriched = participants_df.merge(
        farm_out_df[scheme_cols],
        on="farm_id_new",
        how="left",
    )
    participants_enriched.to_csv(participants_input, index=False)

    print(f"[done] Wrote {len(notice_out_df)} notice classifications -> {notice_output}")
    print(f"[done] Wrote {len(farm_out_df)} farm classifications -> {farm_output}")
    print(f"[done] Enriched participants file in place -> {participants_input}")


if __name__ == "__main__":
    main()
