#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import pandas as pd

PIPE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PIPE_ROOT / "data" / "04_lbv_enriched_with_ai_summary.csv"
DEFAULT_OUTPUT = DEFAULT_INPUT
STEP4_PATH = PIPE_ROOT / "scripts" / "04_ai_classify_lbv_and_addresses.py"


def load_step4_module():
    spec = importlib.util.spec_from_file_location("participants_step4", STEP4_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair step-04 addresses from clear title patterns.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input CSV (default: step-04 output).")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV (default: overwrite step-04 output).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    mod = load_step4_module()
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    changed = 0

    for idx, row in df.iterrows():
        title_address = mod.extract_address_from_title(row.get("Titel", ""))
        if not title_address:
            continue
        current = {
            "street": row.get("B_STRAATNAAM", ""),
            "house_number": row.get("B_HUIS_NR", ""),
            "house_number_suffix": row.get("B_HUIS_NR_TOEV", ""),
            "postcode": row.get("B_POSTCODE", ""),
            "place": row.get("B_PLAATS", ""),
        }
        chosen = mod.choose_address(row, current, row.get("TEXT_HTML", "") + "\n" + row.get("TEXT_PDF", ""))
        if chosen == current:
            continue
        df.at[idx, "B_STRAATNAAM"] = chosen.get("street", "") or ""
        df.at[idx, "B_HUIS_NR"] = chosen.get("house_number", "") or ""
        df.at[idx, "B_HUIS_NR_TOEV"] = chosen.get("house_number_suffix", "") or ""
        df.at[idx, "B_POSTCODE"] = chosen.get("postcode", "") or ""
        df.at[idx, "B_PLAATS"] = chosen.get("place", "") or ""
        df.at[idx, "ADDR_CONFIDENCE"] = str(chosen.get("confidence", row.get("ADDR_CONFIDENCE", "")) or "")
        changed += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"[done] Repaired {changed} row(s) -> {output_path}")


if __name__ == "__main__":
    main()
