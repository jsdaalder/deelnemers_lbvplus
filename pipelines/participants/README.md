# Participants pipeline

Scrapes LBV/LBV+ notices, extracts text from PDFs, asks an LLM to identify the stage and address, cleans the addresses, and groups publications into farms.

## Inputs
- HTML pages or PDF links from officielebekendmakingen.nl (use local files or the SRU API).
- `.env` with `OPENAI_API_KEY=...` for the LLM classification step.

## What it produces
- `data/01_overheid_results.csv` – raw notices and URLs.
- `data/02_lbv_enriched.csv` / `03_lbv_enriched_with_pdf.csv` – text extracted from HTML/PDF.
- `data/04_lbv_enriched_with_ai_summary.csv` – LLM-derived stage, address, and confidence.
- `data/05_lbv_enriched_addresses.csv` – cleaned addresses with grouping keys.
- `data/06_deelnemers_lbv_lbvplus.csv` – farms (one row per location) with latest stage; synced to `pipelines/matching_ftm/data/raw/`. No dated copy in repo root, and this CSV stays out of git.

## How to run
1) Create/activate a virtualenv and install dependencies (pandas, beautifulsoup4, pdfminer.six, openai, fpdf, python-dotenv, requests).
2) Set `OPENAI_API_KEY` in `.env` at repo root.
3) Run everything:  
   - Local files: `MODE=local FILES="prov1.html prov2.html" bash pipelines/participants/scripts/run_all.sh`  
   - SRU API: `MODE=api API_QUERY='c.product-area==officielepublicaties AND cql.textAndIndexes="lbv"' API_MAX=500 bash pipelines/participants/scripts/run_all.sh`
4) The script syncs the final participants CSV to `pipelines/matching_ftm/data/raw/` for downstream matching.

## Notes
- PDFs and data folders are git-ignored. Keep any personal data out of commits.
- Prompt experiments and LLM prompt/run comparisons live under `pipelines/participants/experiments/llm_improvement_testing/`.
- **LLM validation**: we manually labeled a sample of notices (`manual_stage_truth.csv`) and used the `llm_improvement_testing` mini-pipeline to iterate on the stage/address prompt. The helper script `stage_classifier_test.py` runs the prompt against the labeled set, prints confusion/mismatch stats, and writes a run log. Mismatches were reviewed and the prompt adjusted (e.g., draft vs definitive precedence, ignoring navigation text). The current prompt in `scripts/04_ai_classify_lbv_and_addresses.py` reflects those iterations; rerun the mini-pipeline if you tweak the prompt or model.
