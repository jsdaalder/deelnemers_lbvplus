#!/usr/bin/env python3
"""
Experimental stage classifier (LLM) for LBV/LBV+ notices, plus on-the-spot eval.

- Lives under pipelines/participants/experiments/llm_improvement_testing/ to avoid touching the main pipeline.
- Reads a CSV with TEXT_HTML/TEXT_PDF (default: manual_stage_truth.csv).
- Runs an LLM prompt that only predicts STAGE, returns JSON {stage, evidence}.
- If Stage_manual exists, prints confusion/mismatch stats and can export them.
- When the prompt proves reliable, copy SYSTEM/USER templates into 04_ai_classify_lbv_and_addresses.py.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - optional dependency
    OpenAI = None


REPO_ROOT = Path(__file__).resolve().parents[4]
EXPERIMENT_ROOT = REPO_ROOT / "pipelines" / "participants" / "experiments" / "llm_improvement_testing"
DEFAULT_INPUT = EXPERIMENT_ROOT / "manual_stage_truth.csv"
DEFAULT_MODEL = os.environ.get("OPENAI_STAGE_MODEL", "gpt-4.1-mini")
DEFAULT_RUNLOG = EXPERIMENT_ROOT / "stage_run_results.json"
DOTENV_PATH = REPO_ROOT / ".env"


SYSTEM_PROMPT = """Je bent een formele, conservatieve juridisch schrijver.
Je bepaalt uitsluitend de fase (STAGE) van een bekendmaking over LBV/LBV+.
Lees de volledige tekst en kies de beste optie:
- receipt_of_application: tekst zegt dat de aanvraag is ontvangen/kennisgegeven.
- draft_decision: ontwerp, voornemen, ontwerpbeschikking/-besluit, ter inzage.
- definitive_decision: besluit/beschikking zonder ontwerp, vergunning verleend/ingetrokken, beroep/bezwaar fase.

Regels (prioriteiten):
- Definitief: expliciet "vergunning/beschikking/besluit verleend", "intrekken", "verzenddatum besluit", "beroep/bezwaar mogelijk", of formuleringen als "hebben verleend/hebben ingetrokken/hebben genomen" (voltooid tegenwoordige tijd, géén voornemen). Dat zijn definitieve besluiten.
- Draft: "voornemen te verlenen/intrekken" of "ontwerp/ontwerpbeschikking/ontwerpbesluit/ter inzage/zienswijzen indienen" als actieve context. Als zowel een zin met "hebben verleend/ingetrokken" als "voornemen/ontwerp/ter inzage/zienswijzen" in dezelfde bekendmaking staan, kies draft_decision (ontwerp-fase gaat voor).
- Als "voornemen" expliciet wordt genoemd, kies draft_decision, óók als er verzenddatum besluit of inwerkingtreding/beroep wordt genoemd (dat hoort bij ontwerp ter inzage).
- Receipt: duidelijke ontvangst-woorden zoals "aanvraag ontvangen", "kennisgeving ontvangst", "verzoek tot het behandelen van een aanvraag", en géén ontwerp-signalen (ontwerpbesluit/ter inzage/zienswijzen/voornemen) en géén besluitwoorden (verlenen/intrekken/beschikking/besluit/beroep) in de inhoud. Negeer losse "ontwerp" hits uit navigatie/metadata of in toekomstparagrafen ("er zal later een ontwerpbesluit komen") als er nu alleen een ontvangstmelding staat.
- Lege of onleesbare tekst: meld dat kort in de evidence en kies de best passende van de drie (geen "unknown" of "unreadable"). Zonder signalen val conservatief terug op receipt_of_application.
- Kies nooit receipt_of_application als er ontwerp, besluit/beschikking, verleend, intrekken of beroep/bezwaar wordt genoemd.
- Geef een korte quote als evidence.

Return JSON: {"stage": "...", "evidence": "..."}.
"""


USER_TEMPLATE = """Bepaal de STAGE op basis van de tekst.

TEXT:
"{TEXT}"
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test LLM stage-classifier en evalueer tegen manual truth.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input CSV met manual truth en TEXT_HTML/TEXT_PDF.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI modelnaam (env OPENAI_STAGE_MODEL).")
    parser.add_argument("--max-rows", type=int, default=None, help="Optioneel maximum voor snelle tests.")
    parser.add_argument("--export", default=None, help="Optioneel pad om alle resultaten te bewaren (CSV).")
    parser.add_argument("--export-mismatches", default=None, help="Optioneel pad om mismatches weg te schrijven (CSV).")
    parser.add_argument(
        "--runlog",
        default=str(DEFAULT_RUNLOG),
        help="Pad om prompt en scores in JSON te bewaren (default: llm_improvement_testing/stage_run_results.json).",
    )
    parser.add_argument("--compare", action="store_true", help="Vergelijk met Stage_manual als die kolom aanwezig is.")
    return parser.parse_args()


def maybe_load_dotenv() -> None:
    # Lightweight .env loader to avoid extra deps
    if DOTENV_PATH.exists():
        with DOTENV_PATH.open() as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value and key not in os.environ:
                    os.environ[key] = value


def build_prompt(text: str) -> str:
    snippet = text.strip().replace("\n", " ")
    return USER_TEMPLATE.replace("{TEXT}", snippet)


def call_llm(client: OpenAI, model: str, text: str) -> Tuple[str, str]:
    prompt = build_prompt(text)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content if resp.choices else ""
    try:
        data = json.loads(content)
        return data.get("stage", ""), data.get("evidence", "")
    except Exception:
        return "unknown", content


def main() -> None:
    args = parse_args()
    maybe_load_dotenv()
    infile = Path(args.input).expanduser().resolve()
    df = pd.read_csv(infile, dtype=str)
    if args.max_rows is not None:
        df = df.head(args.max_rows)

    if OpenAI is None:
        raise SystemExit("openai package not installed.")
    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("Set OPENAI_API_KEY (or place it in .env) to run the classifier.")

    client = OpenAI()
    stages: List[str] = []
    evidence: List[str] = []

    for _, row in df.iterrows():
        text = f"{row.get('TEXT_HTML', '')} {row.get('TEXT_PDF', '')}"
        stage, evid = call_llm(client, args.model, text)
        stages.append(stage)
        evidence.append(evid)

    df["STAGE_NEW_LLM"] = stages
    df["STAGE_EVIDENCE"] = evidence

    report: Dict[str, object] = {
        "model": args.model,
        "input_rows": len(df),
        "system_prompt": SYSTEM_PROMPT,
        "user_template": USER_TEMPLATE,
        "mismatches_count": None,
        "matched_rows": None,
        "confusion": None,
    }

    if args.compare and "Stage_manual" in df.columns:
        matched = df[df["Stage_manual"].notna()].copy()
        mismatches = matched[matched["Stage_manual"] != matched["STAGE_NEW_LLM"]]
        print(f"Compared {len(matched)} rows; mismatches: {len(mismatches)}")
        report["matched_rows"] = len(matched)
        report["mismatches_count"] = len(mismatches)
        if len(matched):
            print("\nConfusion (manual -> new LLM):")
            ctab = pd.crosstab(matched["Stage_manual"], matched["STAGE_NEW_LLM"]).sort_index()
            print(ctab)
            report["confusion"] = ctab.to_dict()
        if len(mismatches):
            print("\nSample mismatches:")
            print(mismatches[["farm_id", "AddressKey", "Stage_manual", "STAGE_NEW_LLM"]].head())
            if args.export_mismatches:
                mismatches.to_csv(Path(args.export_mismatches).expanduser().resolve(), index=False)
                print(f"Wrote mismatches to {Path(args.export_mismatches).resolve()}")

    # Always write runlog JSON
    runlog_path = Path(args.runlog).expanduser().resolve()
    runlog_path.parent.mkdir(parents=True, exist_ok=True)
    with runlog_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"Wrote runlog to {runlog_path}")

    if args.export:
        out_path = Path(args.export).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
