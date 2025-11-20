#!/usr/bin/env python3
import argparse
import os
import re
import time
import json
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

import pandas as pd
from openai import OpenAI
from fpdf import FPDF
from dotenv import load_dotenv

load_dotenv()

# =========================
# Config / Paths / Columns
# =========================

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

IN_PATH = DATA_DIR / "03_lbv_enriched_with_pdf.csv"
OUT_DIR = DATA_DIR
BASENAME_CSV = "04_lbv_enriched_with_ai_summary"

DEFAULT_MODEL = "gpt-4.1-mini"

# Column names
COL_HTML = "TEXT_HTML"
COL_PDF = "TEXT_PDF"

COL_LBV_TYPE = "LBV_TYPE"
COL_WITHDRAWAL = "WITHDRAWAL"
COL_STAGE = "STAGE"
COL_LBV_CONF = "LBV_CONFIDENCE"
COL_LBV_METHOD = "LBV_METHOD"

COL_ADDR_STREET = "B_STRAATNAAM"
COL_ADDR_NR = "B_HUIS_NR"
COL_ADDR_TOEV = "B_HUIS_NR_TOEV"
COL_ADDR_PC = "B_POSTCODE"
COL_ADDR_PLACE = "B_PLAATS"
COL_ADDR_CONF = "ADDR_CONFIDENCE"

COL_AI_SOURCE = "AI_SOURCE"

COL_COMPANY_ID = "company_id"
COL_COMPANY_NAME = "COMPANY_NAME"
ANNOTATION_COLUMNS = [
    COL_LBV_TYPE,
    COL_WITHDRAWAL,
    COL_STAGE,
    COL_LBV_CONF,
    COL_LBV_METHOD,
    COL_AI_SOURCE,
    COL_ADDR_STREET,
    COL_ADDR_NR,
    COL_ADDR_TOEV,
    COL_ADDR_PC,
    COL_ADDR_PLACE,
    COL_ADDR_CONF,
    COL_COMPANY_NAME,
    COL_COMPANY_ID,
]

# =========================
# Regex / Prescreen
# =========================

KW_LBV = re.compile(
    r"\b(lbv\+?|lbv-plus|lbv\s*plus|landelijke\s+be[eë]indigingsregeling\s+veehouderijlocaties(?:\s+met\s+piekbelasting)?)\b",
    re.I,
)
KW_N2000 = re.compile(r"\bnatura\s*2000\b", re.I)
KW_WITHDRAW = re.compile(r"\b(intrek\w+|ingetrokken)\b", re.I)

KW_STAGE_RECEIPT = re.compile(
    r"kennisgeving\s+ontvangst|aanvraag\s+ontvangen|aanvraagdatum|u\s+kunt\s+niet\s+reageren",
    re.I,
)
KW_STAGE_DRAFT = re.compile(
    r"ontwerpbesluit|terinzagelegging|zienswijze|inzage\s*(?:termijn|periode)?|gedurende\s+(?:zes|6)\s+weken",
    re.I,
)
KW_STAGE_DECIS = re.compile(
    r"\bbeschikking\b|\bbesluit\b|verzenddatum|in\s+werking|beroep|bezwaar",
    re.I,
)
KW_STAGE_INTENT = re.compile(r"\bvoornemen[s]?\b|\bbeoogt\b|\bvoornemens\b", re.I)

COMPANY_BRABANT_RE = re.compile(
    r"bedrijf\s*[:\-]\s*(.*?)\s*locatie\s*[:\-]", re.I | re.S
)

# =========================
# LLM Prompts
# =========================

SYSTEM_PROMPT_FULL = """Je bent een zorgvuldige Nederlandstalige analist van overheidspublicaties.
Je krijgt de volledige tekst van een publicatie (HTML-tekst + PDF-tekst samengevoegd).

Je moet twee dingen doen:
1) Lbv / intrekking / fase bepalen:
   - Herken of de publicatie gaat over de Landelijke beëindigingsregeling veehouderijlocaties (Lbv)
     of de Lbv-plus regeling (met piekbelasting). Gebruik hiervoor de neutrale categorie:
       "LBV/LBV+" (ongeacht of het exact Lbv of Lbv-plus is),
       "none" (geen aanwijzing voor Lbv of Lbv-plus),
       "unknown" (onduidelijk / tekst te vaag).
   - Herken of er sprake is van het intrekken van een natuurvergunning / Natura 2000-vergunning:
       is_withdrawal = true als er woorden voorkomen als
         "intrekken", "intrekking", "ingetrokken" in de betekenis van het intrekken van een vergunning.
       withdrawal_scope:
         - "full" als duidelijk is dat de vergunning geheel wordt ingetrokken (woorden als "geheel", "gehele", "volledig").
         - "partial" als duidelijk is dat de vergunning gedeeltelijk wordt ingetrokken
           (woorden als "gedeeltelijk", "gedeeltelijke").
         - "unknown" als wel intrekking, maar geen duidelijkheid over volledig of gedeeltelijk.
   - Bepaal de procedurefase "stage":
       - "receipt_of_application"  → kennisgeving ontvangst/aanvraag ontvangen, vaak met tekst zoals
         "aanvraag ontvangen", "aanvraagdatum", "In dit stadium is het niet mogelijk uw mening te geven".
       - "draft_decision"          → ontwerpbesluit / terinzagelegging / zienswijzen mogelijk (woorden zoals
         "ontwerpbesluit", "ontwerpbeschikking", "zienswijzen indienen", "het voornemen hebben").
       - "definitive_decision"     → definitief besluit/beschikking: vergunning wordt verleend of ingetrokken,
         er kan beroep/bezwaar worden ingesteld, er staan zinnen als "Met dit besluit..." of "Het besluit treedt in werking".
       - "intent_notice"           → een bekendmaking van een voornemen, zonder dat er al een ontwerp- of definitief besluit ligt.
       - "other"                   → alles wat niet in bovenstaande categorieën past.
   - Let op:
       * Het kan zijn dat iemand in de tekst "deeltneemt aan de Lbv (of Lbv-plus) regeling",
         maar dat de bekendmaking over iets anders gaat (bijvoorbeeld een grondruil).
         In dat geval:
           - lbv_type = "LBV/LBV+" (want relevant dat de partij Lbv-deelnemer is),
           - maar is_withdrawal = false als er in deze publicatie geen vergunning wordt ingetrokken.
       * Provinciale bekendmakingen beschrijven soms eerdere stappen (aanvragen of ontwerpbesluiten)
         voordat ze "Met dit besluit..." melden dat de vergunning daadwerkelijk is verleend of ingetrokken.
         Laat woorden als "vergunning verleend", "beroep instellen" en "het besluit treedt in werking"
         zwaarder wegen dan historische context.

2) Hoofdadres van de locatie bepalen:
   - Zoek het hoofd-adres van de locatie waar de vergunning of activiteit betrekking op heeft.
   - Meestal staat dit in de titel of in een regel "Locatie:" of vergelijkbaar.
   - Als er meerdere adressen worden genoemd, kies het belangrijkste/centrale adres
     (meestal het eerste adres in de titel of de hoofdlocatie).
   - Splits het adres in:
       - street: straatnaam (zonder huisnummer),
       - house_number: alleen het nummer + eventuele direct eraan vastzittende letters (bijv. 7a, 24A, 3-01, 7-9-11),
       - house_number_suffix: extra toevoeging die NIET al aan het nummer vastzit (bijv. "A", "bis"),
       - postcode: Nederlandse postcode (bijv. "1234 AB") als herkenbaar, anders lege string "",
       - place: plaatsnaam (dorp/stad).
   - Als je helemaal geen adres kunt herkennen, laat je alle adresvelden als lege string "" en confidence laag.

3) Confidences:
   - lbv_confidence: schatting 0.0–1.0 hoe zeker je bent van je lbv/withdrawal/stage-beoordeling.
   - address.confidence: schatting 0.0–1.0 hoe zeker je bent dat het gevonden adres klopt.

BELANGRIJK:
- Werk uitsluitend op basis van de aangeleverde tekst.
- Wees conservatief: als je twijfelt of het echt om Lbv/Lbv+ gaat, gebruik "unknown" of "none" en een lagere confidence.
- Als er geen enkele verwijzing is naar Lbv/Lbv+ of intrekken, zet lbv_type op "none", is_withdrawal op false.
- Geef ALLEEN het JSON-object terug, zonder extra uitleg.
"""

USER_TEMPLATE_FULL = """Publicatietekst (samengevoegd HTML + PDF):

<<<
{body}
>>>

Geef uitsluitend een JSON-object met exact deze structuur:

{{
  "lbv_type": "LBV/LBV+" | "none" | "unknown",
  "is_withdrawal": true | false,
  "withdrawal_scope": "full" | "partial" | "unknown",
  "stage": "receipt_of_application" | "draft_decision" | "definitive_decision" | "intent_notice" | "other",
  "lbv_confidence": 0.0,
  "address": {{
    "street": "string",
    "house_number": "string",
    "house_number_suffix": "string",
    "postcode": "string",
    "place": "string",
    "confidence": 0.0
  }}
}}
"""

SYSTEM_PROMPT_ADDR_ONLY = """Je bent een zorgvuldige Nederlandstalige analist van overheidspublicaties.
Je krijgt de volledige tekst van een publicatie (HTML-tekst + PDF-tekst samengevoegd).

Taak:
- Bepaal enkel het hoofd-adres van de locatie waar de vergunning of activiteit betrekking op heeft.
- Gebruik dezelfde definitie als in de andere taak:
   street, house_number, house_number_suffix, postcode, place, confidence (0.0–1.0).
- Als er meerdere adressen zijn, kies het belangrijkste/centrale adres (vaak in de titel of bij 'Locatie:').
- Als je geen bruikbaar adres kunt vinden, laat alle velden leeg ("") en zet confidence op 0.0.
- Geef ALLEEN het JSON-object terug, zonder extra tekst.
"""

USER_TEMPLATE_ADDR_ONLY = """Publicatietekst (samengevoegd HTML + PDF):

<<<
{body}
>>>

Geef uitsluitend een JSON-object met exact deze structuur:

{{
  "street": "string",
  "house_number": "string",
  "house_number_suffix": "string",
  "postcode": "string",
  "place": "string",
  "confidence": 0.0
}}
"""


# =========================
# CLI helpers
# =========================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classificeer LBV/LBV+ bekendmakingen met LLM of regels.")
    parser.add_argument("--in", dest="input_path", default=str(IN_PATH), help="Input CSV (default: 03_lbv_enriched_with_pdf.csv).")
    parser.add_argument(
        "--existing-output",
        dest="existing_output",
        help="Bestaand 04-output CSV om eerdere resultaten over te nemen.",
    )
    parser.add_argument(
        "--out-csv",
        dest="out_csv",
        help="Pad voor het CSV-resultaat. Standaard wordt een uniek bestand in data/ gemaakt.",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "addr", "rules"],
        help="Verwerkingsmodus: full (LLM + regels), addr (alleen adres-LLM) of rules (alleen regels).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximaal aantal rijen om te verwerken (standaard: allemaal).",
    )
    parser.add_argument(
        "--only-unclassified",
        action="store_true",
        help="Alleen rijen waarbij LBV_TYPE nog leeg is verwerken.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Alle rijen opnieuw verwerken, ongeacht bestaande AI-resultaten.",
    )
    parser.add_argument(
        "--model",
        dest="model",
        default=DEFAULT_MODEL,
        help=f"OpenAI-modelnaam (standaard: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--max-rows",
        dest="max_rows",
        type=int,
        default=None,
        help="Alias voor --limit voor achterwaartse compatibiliteit.",
    )
    return parser.parse_args()


def resolve_output_path(user_value: Optional[str], basename: str, ext: str) -> Path:
    if user_value:
        return Path(user_value).expanduser().resolve()
    return Path(make_unique_path(OUT_DIR, basename, ext)).expanduser().resolve()


def load_existing_annotations(path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    if not path or not path.exists():
        return {}
    df_prev = pd.read_csv(path)
    df_prev = ensure_columns(df_prev)
    df_prev = ensure_company_id(df_prev)
    mapping: Dict[str, Dict[str, Any]] = {}
    for _, row in df_prev.iterrows():
        doc_id = str(row.get("doc_id", "")).strip()
        alt_doc = str(row.get("doc_id_old_style", "")).strip()
        keys = []
        if doc_id:
            keys.append(doc_id)
        if alt_doc:
            keys.append(alt_doc)
        if not keys:
            continue
        row_dict = row.to_dict()
        for key in keys:
            mapping[key] = row_dict
    if mapping:
        print(f"[info] Overgenomen annotaties uit {path} ({len(mapping)} rijen).")
    return mapping


def apply_existing_annotations(df: pd.DataFrame, mapping: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    if not mapping or "doc_id" not in df.columns:
        return df
    for idx, row in df.iterrows():
        doc_id = str(row.get("doc_id", "")).strip()
        alt_doc = str(row.get("doc_id_old_style", "")).strip() if "doc_id_old_style" in df.columns else ""
        prev = None
        if doc_id and doc_id in mapping:
            prev = mapping[doc_id]
        elif alt_doc and alt_doc in mapping:
            prev = mapping[alt_doc]
        if not prev:
            continue
        for col in ANNOTATION_COLUMNS:
            if col in df.columns and col in prev:
                df.at[idx, col] = prev.get(col, df.at[idx, col])
    return df


# =========================
# Utility functions
# =========================

def ensure_out_dir() -> None:
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)


def make_unique_path(base_dir: os.PathLike[str] | str, basename: str, ext: str) -> str:
    """
    Maak een uniek pad in base_dir; voegt _1, _2, ... toe als bestand al bestaat.
    """
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    candidate = base / f"{basename}.{ext}"
    if not candidate.exists():
        return str(candidate)
    i = 1
    while True:
        candidate = base / f"{basename}_{i}.{ext}"
        if not candidate.exists():
            return str(candidate)
        i += 1


def create_client() -> Optional[OpenAI]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("[warn] OPENAI_API_KEY niet gezet; LLM-functionaliteit uitgeschakeld.")
        return None
    return OpenAI(api_key=api_key)


def normalize_text_field(val: Any) -> str:
    if isinstance(val, str):
        return val
    if pd.isna(val):
        return ""
    return str(val)


def combine_text_fields(html_text: str, pdf_text: str) -> str:
    html_text = normalize_text_field(html_text)
    pdf_text = normalize_text_field(pdf_text)
    parts = []
    if html_text.strip():
        parts.append(html_text.strip())
    if pdf_text.strip():
        parts.append(pdf_text.strip())
    return "\n\n---\n\n".join(parts)


def row_is_noord_brabant(row: pd.Series) -> bool:
    for col in ("Overheidsnaam", "Instantie", "Titel"):
        value = row.get(col, "")
        if isinstance(value, str) and "noord-brabant" in value.lower():
            return True
    return False


def extract_company_name(text: str) -> str:
    if not text:
        return ""
    match = COMPANY_BRABANT_RE.search(text)
    if not match:
        return ""
    candidate = match.group(1)
    candidate = re.sub(r"\s+", " ", candidate or "")
    return candidate.strip(" :;-.,")


def quick_prescreen(txt: str) -> Tuple[bool, Dict[str, bool]]:
    t = (txt or "").lower()
    if not t:
        return False, {"empty": True}
    signals = {
        "lbv": bool(KW_LBV.search(t)),
        "n2000": bool(KW_N2000.search(t)),
        "withdraw": bool(KW_WITHDRAW.search(t)),
        "stage_receipt": bool(KW_STAGE_RECEIPT.search(t)),
        "stage_draft": bool(KW_STAGE_DRAFT.search(t)),
        "stage_decis": bool(KW_STAGE_DECIS.search(t)),
        "stage_intent": bool(KW_STAGE_INTENT.search(t)),
    }
    possibly = signals["lbv"] or signals["n2000"] or signals["withdraw"]
    return possibly, signals


def parse_json_safe(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        return {}


def run_llm_full(client: Optional[OpenAI], text: str, model: str = DEFAULT_MODEL, max_retries: int = 3) -> Dict[str, Any]:
    if client is None:
        return {}
    if not text or not text.strip():
        return {}
    user_msg = USER_TEMPLATE_FULL.format(body=text.strip())
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=0.0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_FULL},
                    {"role": "user", "content": user_msg},
                ],
            )
            content = (resp.choices[0].message.content or "").strip()
            data = parse_json_safe(content)
            if data:
                return data
        except Exception as e:
            print(f"[warn] LLM full attempt {attempt} failed: {e}")
            if attempt == max_retries:
                break
            time.sleep(delay)
            delay = min(delay * 2, 20.0)
    return {}


def run_llm_addr_only(client: Optional[OpenAI], text: str, model: str = DEFAULT_MODEL, max_retries: int = 3) -> Dict[str, Any]:
    if client is None:
        return {}
    if not text or not text.strip():
        return {}
    user_msg = USER_TEMPLATE_ADDR_ONLY.format(body=text.strip())
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=0.0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_ADDR_ONLY},
                    {"role": "user", "content": user_msg},
                ],
            )
            content = (resp.choices[0].message.content or "").strip()
            data = parse_json_safe(content)
            if data:
                return data
        except Exception as e:
            print(f"[warn] LLM addr-only attempt {attempt} failed: {e}")
            if attempt == max_retries:
                break
            time.sleep(delay)
            delay = min(delay * 2, 20.0)
    return {}

# ================
# Rule-based fallback
# ================

def rule_based_lbv_extraction(text: str) -> Dict[str, Any]:
    t = (text or "").lower()
    if not t.strip():
        return {
            "lbv_type": "none",
            "is_withdrawal": False,
            "withdrawal_scope": "unknown",
            "stage": "other",
            "lbv_confidence": 0.0,
        }

    lbv_present = bool(KW_LBV.search(t))
    n2000_present = bool(KW_N2000.search(t))
    withdraw_present = bool(KW_WITHDRAW.search(t))

    if lbv_present or n2000_present or withdraw_present:
        lbv_type = "LBV/LBV+"
        is_withdrawal = withdraw_present
        scope = "unknown"
        if "gedeeltelijk" in t:
            scope = "partial"
        elif "geheel" in t or "volledig" in t:
            scope = "full"
        stage = "other"
        if KW_STAGE_RECEIPT.search(t):
            stage = "receipt_of_application"
        elif KW_STAGE_DRAFT.search(t):
            stage = "draft_decision"
        elif KW_STAGE_DECIS.search(t):
            stage = "definitive_decision"
        elif KW_STAGE_INTENT.search(t):
            stage = "intent_notice"
        conf = 0.5
    else:
        lbv_type = "none"
        is_withdrawal = False
        scope = "unknown"
        stage = "other"
        conf = 0.0

    return {
        "lbv_type": lbv_type,
        "is_withdrawal": is_withdrawal,
        "withdrawal_scope": scope,
        "stage": stage,
        "lbv_confidence": conf,
    }


# ================
# Company ID helper
# ================

def ensure_company_id(df: pd.DataFrame) -> pd.DataFrame:
    if COL_COMPANY_ID in df.columns:
        # Already present; assume user may have filled it
        return df

    # build key from address columns (if present)
    def addr_key(row) -> str:
        parts = []
        for col in (COL_ADDR_STREET, COL_ADDR_NR, COL_ADDR_TOEV, COL_ADDR_PC, COL_ADDR_PLACE):
            val = str(row[col]).strip().lower() if col in df.columns and pd.notna(row[col]) else ""
            parts.append(val)
        key = "|".join(parts)
        return key if key.strip("|") else ""

    keys = []
    for _, row in df.iterrows():
        keys.append(addr_key(row))

    df[COL_COMPANY_ID] = ""
    next_id = 1
    mapping: Dict[str, str] = {}
    for idx, k in enumerate(keys):
        if not k:
            continue
        if k not in mapping:
            mapping[k] = f"c{next_id:05d}"
            next_id += 1
        df.at[idx, COL_COMPANY_ID] = mapping[k]

    return df


# =========================
# DataFrame column helpers
# =========================

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # LBV-related
    for col, default in [
        (COL_LBV_TYPE, ""),
        (COL_WITHDRAWAL, ""),
        (COL_STAGE, ""),
        (COL_LBV_CONF, 0.0),
        (COL_LBV_METHOD, ""),
        (COL_AI_SOURCE, ""),
    ]:
        if col not in df.columns:
            df[col] = default

    # Address
    for col, default in [
        (COL_ADDR_STREET, ""),
        (COL_ADDR_NR, ""),
        (COL_ADDR_TOEV, ""),
        (COL_ADDR_PC, ""),
        (COL_ADDR_PLACE, ""),
        (COL_ADDR_CONF, 0.0),
    ]:
        if col not in df.columns:
            df[col] = default

    # Company name placeholder
    if COL_COMPANY_NAME not in df.columns:
        df[COL_COMPANY_NAME] = ""
    else:
        df[COL_COMPANY_NAME] = df[COL_COMPANY_NAME].fillna("").astype(str)
    return df


# ================
# House number post-process
# ================

def postprocess_house_number_suffix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliseer huisnummers zoals '7a' → '7' + 'a', maar alleen
    als B_HUIS_NR_TOEV nog leeg is en B_HUIS_NR in de vorm digits+letters is.
    Laat multi-nummers zoals '7-9-11' of '03-01' ongemoeid.
    """
    if COL_ADDR_NR not in df.columns or COL_ADDR_TOEV not in df.columns:
        return df

    pattern = re.compile(r"^\s*([0-9]+)\s*([A-Za-z]{1,4})\s*$")

    for idx, row in df.iterrows():
        nr_raw = row.get(COL_ADDR_NR, "")
        toe_raw = row.get(COL_ADDR_TOEV, "")

        nr = str(nr_raw) if nr_raw is not None else ""
        toe = str(toe_raw) if toe_raw is not None else ""

        nr = nr.strip()
        toe = toe.strip()

        if not nr or toe:
            continue

        m = pattern.match(nr)
        if m:
            base, suffix = m.group(1), m.group(2)
            df.at[idx, COL_ADDR_NR] = base
            df.at[idx, COL_ADDR_TOEV] = suffix

    return df


# ================
# Summary & reports
# ================

def build_summary_from_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    def vc(col: str) -> Dict[str, int]:
        if col not in df.columns:
            return {}
        return df[col].value_counts(dropna=False).to_dict()

    summary = {
        "total_rows": int(len(df)),
        "lbv_type_counts": vc(COL_LBV_TYPE),
        "withdrawal_counts": vc(COL_WITHDRAWAL),
        "stage_counts": vc(COL_STAGE),
        "ai_source_counts": vc(COL_AI_SOURCE),
        "lbv_method_counts": vc(COL_LBV_METHOD),
        "address_filled": int(
            ((df.get(COL_ADDR_STREET, "") != "") | (df.get(COL_ADDR_NR, "") != "")).sum()
        ),
    }
    return summary




# ================
# CLI helpers
# ================

def ask_llm_mode() -> int:
    """
    1 = volledige analyse (LBV + intrekking + fase + adres)
    2 = alleen nieuwe rijen (waar LBV_TYPE leeg is)
    3 = alleen adressen (B_* opnieuw/aanvullen)
    """
    print()
    print("Kies LLM-analyse:")
    print("  [1] Volledige analyse (LBV + intrekking + fase + adres)")
    print("  [2] Alleen nieuwe rijen (waar LBV_TYPE leeg is)")
    print("  [3] Alleen adressen (B_* kolommen aanvullen/herberekenen)")
    while True:
        choice = input("Uw keuze [1/2/3]: ").strip()
        if choice in ("1", "2", "3"):
            return int(choice)
        print("Ongeldige keuze, probeer opnieuw.")


def prompt_trial_or_all(total_rows: int) -> Optional[int]:
    print()
    print(f"Er zijn {total_rows} rijen in het bestand '{IN_PATH}'.")
    print()
    print("Kies analyse-modus:")
    print("  [T] Trial: alleen de eerste 10 rijen")
    print("  [A] Alles: alle rijen verwerken")
    while True:
        choice = input("Uw keuze [T/A]: ").strip().lower()
        if choice == "t":
            print("→ Trial-modus gekozen (10 rijen).")
            return 10
        if choice == "a":
            print("→ Alle rijen worden verwerkt.")
            return None
        print("Ongeldige keuze, kies 'T' of 'A'.")


# ================
# Main
# ================

def main():
    args = parse_args()
    ensure_out_dir()

    input_path = Path(args.input_path).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Invoerbestand niet gevonden: {input_path}")

    df = pd.read_csv(input_path)
    df = ensure_columns(df)
    df = ensure_company_id(df)

    existing_path: Optional[Path] = Path(args.existing_output).expanduser().resolve() if args.existing_output else None
    if existing_path is None and args.out_csv:
        candidate = Path(args.out_csv).expanduser().resolve()
        if candidate.exists():
            existing_path = candidate
    existing_map = load_existing_annotations(existing_path)
    df = apply_existing_annotations(df, existing_map)

    total_rows = len(df)
    limit = args.limit if args.limit is not None else args.max_rows
    if limit is None:
        limit = prompt_trial_or_all(total_rows)

    user_mode = args.mode
    only_unclassified = args.only_unclassified
    if user_mode is None:
        mode_choice = ask_llm_mode()
        if mode_choice == 2:
            only_unclassified = True
            user_mode = "full"
        elif mode_choice == 3:
            user_mode = "addr"
        else:
            user_mode = "full"

    indices = list(df.index)
    if limit is not None:
        indices = indices[: min(limit, len(indices))]

    if only_unclassified:
        indices = [i for i in indices if not str(df.at[i, COL_LBV_TYPE]).strip()]
    elif not args.force:
        indices = [i for i in indices if not str(df.at[i, COL_AI_SOURCE]).strip()]

    if not indices:
        print("[info] Geen rijen om te verwerken onder de huidige instellingen.")
        out_csv_path = resolve_output_path(args.out_csv, BASENAME_CSV, "csv")
        out_csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv_path, index=False, encoding="utf-8")
        print(f"[ok] Geschreven CSV : {out_csv_path}")
        return

    client = None if user_mode == "rules" else create_client()

    target = len(indices)
    verbose_every = 1 if limit is not None else 10
    print(f"[info] Start verwerking: {target} rijen (verbose elke {verbose_every} rij(en)).")

    start_all = time.time()
    llm_calls = 0

    for idx_pos, idx in enumerate(indices, start=1):
        row_start = time.time()
        html = df.at[idx, COL_HTML] if COL_HTML in df.columns else ""
        pdf_text = df.at[idx, COL_PDF] if COL_PDF in df.columns else ""
        combined_text = combine_text_fields(html, pdf_text)

        if not combined_text.strip():
            # Nothing to analyse
            row_time = time.time() - row_start
            if idx_pos % verbose_every == 0:
                print(f"[row {idx_pos}/{target}] (leeg) overgeslagen in {row_time:.2f}s")
            continue

        if user_mode == "addr":
            # Only address
            result_addr = run_llm_addr_only(client, combined_text, model=args.model)
            if result_addr:
                df.at[idx, COL_AI_SOURCE] = "llm_addr_only"
                if not str(df.at[idx, COL_LBV_METHOD]).strip():
                    df.at[idx, COL_LBV_METHOD] = "llm_addr_only"
                df.at[idx, COL_ADDR_STREET] = result_addr.get("street", "") or ""
                df.at[idx, COL_ADDR_NR] = result_addr.get("house_number", "") or ""
                df.at[idx, COL_ADDR_TOEV] = result_addr.get("house_number_suffix", "") or ""
                df.at[idx, COL_ADDR_PC] = result_addr.get("postcode", "") or ""
                df.at[idx, COL_ADDR_PLACE] = result_addr.get("place", "") or ""
                df.at[idx, COL_ADDR_CONF] = float(result_addr.get("confidence", 0.0) or 0.0)
                llm_calls += 1
            row_time = time.time() - row_start
            if idx_pos % verbose_every == 0:
                print(f"[row {idx_pos}/{target}] addr-only verwerkt in {row_time:.2f}s")
            continue

        # full LBV + address
        possibly, sig = quick_prescreen(combined_text)
        use_llm = False
        if client is not None and possibly and user_mode == "full":
            use_llm = True

        method_used = None

        if use_llm:
            data = run_llm_full(client, combined_text, model=args.model)
            llm_calls += 1
            if data:
                method_used = "llm_full"
                df.at[idx, COL_AI_SOURCE] = "llm_full"
                df.at[idx, COL_LBV_TYPE] = data.get("lbv_type", "") or "unknown"
                # withdrawal
                is_w = bool(data.get("is_withdrawal", False))
                scope = data.get("withdrawal_scope", "") or "unknown"
                if not is_w:
                    df.at[idx, COL_WITHDRAWAL] = "none"
                else:
                    if scope in ("full", "partial"):
                        df.at[idx, COL_WITHDRAWAL] = scope
                    else:
                        df.at[idx, COL_WITHDRAWAL] = "unknown"
                # stage
                st = data.get("stage", "") or "other"
                if st not in (
                    "receipt_of_application",
                    "draft_decision",
                    "definitive_decision",
                    "intent_notice",
                    "other",
                ):
                    st = "other"
                df.at[idx, COL_STAGE] = st
                # confidence
                try:
                    df.at[idx, COL_LBV_CONF] = float(data.get("lbv_confidence", 0.0) or 0.0)
                except Exception:
                    df.at[idx, COL_LBV_CONF] = 0.0

                # address from llm
                addr = data.get("address", {}) or {}
                df.at[idx, COL_ADDR_STREET] = addr.get("street", "") or ""
                df.at[idx, COL_ADDR_NR] = addr.get("house_number", "") or ""
                df.at[idx, COL_ADDR_TOEV] = addr.get("house_number_suffix", "") or ""
                df.at[idx, COL_ADDR_PC] = addr.get("postcode", "") or ""
                df.at[idx, COL_ADDR_PLACE] = addr.get("place", "") or ""
                try:
                    df.at[idx, COL_ADDR_CONF] = float(addr.get("confidence", 0.0) or 0.0)
                except Exception:
                    df.at[idx, COL_ADDR_CONF] = 0.0

        if not use_llm or not method_used:
            # fallback: rule-based only
            rb = rule_based_lbv_extraction(combined_text)
            if not str(df.at[idx, COL_AI_SOURCE]).strip():
                df.at[idx, COL_AI_SOURCE] = "rules_only"
            df.at[idx, COL_LBV_TYPE] = rb.get("lbv_type", "none")
            if rb.get("is_withdrawal", False):
                scope = rb.get("withdrawal_scope", "unknown")
                df.at[idx, COL_WITHDRAWAL] = scope if scope in ("full", "partial") else "unknown"
            else:
                df.at[idx, COL_WITHDRAWAL] = "none"
            st = rb.get("stage", "other")
            if st not in (
                "receipt_of_application",
                "draft_decision",
                "definitive_decision",
                "intent_notice",
                "other",
            ):
                st = "other"
            df.at[idx, COL_STAGE] = st
            df.at[idx, COL_LBV_CONF] = float(rb.get("lbv_confidence", 0.0) or 0.0)
            if not method_used:
                method_used = "rules_only"

        if COL_COMPANY_NAME in df.columns:
            existing_company = str(df.at[idx, COL_COMPANY_NAME]).strip()
            is_brabant = row_is_noord_brabant(row)
            if not is_brabant:
                if existing_company:
                    df.at[idx, COL_COMPANY_NAME] = ""
            else:
                if not existing_company:
                    company_guess = extract_company_name(html) or extract_company_name(combined_text)
                    if company_guess:
                        df.at[idx, COL_COMPANY_NAME] = company_guess

        df.at[idx, COL_LBV_METHOD] = method_used

        row_time = time.time() - row_start
        if idx_pos % verbose_every == 0:
            print(f"[row {idx_pos}/{target}] methode={method_used} verwerkt in {row_time:.2f}s")

    total_time = time.time() - start_all
    print(f"[summary] Verwerking klaar in {total_time:.1f}s; LLM-calls: {llm_calls}")

    # Postprocess housenumber suffixes (7a -> 7 + a, etc.)
    df = postprocess_house_number_suffix(df)

    # Output files
    out_csv = resolve_output_path(args.out_csv, BASENAME_CSV, "csv")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[ok] Geschreven CSV : {out_csv}")


if __name__ == "__main__":
    main()
