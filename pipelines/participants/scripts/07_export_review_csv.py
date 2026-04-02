#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

PIPE_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PIPE_ROOT / "data"
DEFAULT_INPUT = DATA_DIR / "06_deelnemers_lbv_lbvplus.csv"
DEFAULT_OUTPUT = DATA_DIR / "06_all_unique_farms_review.csv"

REVIEW_COLUMNS = [
    "farm_id_new",
    "AddressKey",
    "COMPANY_NAME",
    "Datum_latest",
    "Instantie_latest",
    "stage_latest_llm",
    "stage_latest_manual",
    "URL_BEKENDMAKING",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a compact one-row-per-farm review CSV.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input participants CSV (step 06 output).")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output review CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    missing = [col for col in REVIEW_COLUMNS if col not in df.columns]
    if missing:
        raise SystemExit(f"Missing required column(s) in {input_path}: {', '.join(missing)}")

    review = df[REVIEW_COLUMNS].copy()
    review = review.drop_duplicates(subset=["farm_id_new"])
    review = review.sort_values(["Datum_latest", "farm_id_new"], ascending=[False, True])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    review.to_csv(output_path, index=False)
    print(f"[done] Wrote {len(review)} rows -> {output_path}")


if __name__ == "__main__":
    main()
