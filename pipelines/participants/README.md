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
   - Incremental run with counts: `API_MAX=1000 bash pipelines/participants/scripts/run_incremental_with_stats.sh`
4) The script syncs the final participants CSV to `pipelines/matching_ftm/data/raw/` for downstream matching.
5) Export a compact farm review sheet when needed:
   - `python3 pipelines/participants/scripts/07_export_review_csv.py`
6) Classify notices/farms as `lbv` vs `lbv_plus` with a separate rule-based script:
   - `python3 pipelines/participants/scripts/08_classify_lbv_scheme.py`

## Notes
- PDFs and data folders are git-ignored. Keep any personal data out of commits.
- Prompt experiments and LLM prompt/run comparisons live under `pipelines/participants/experiments/llm_improvement_testing/`.
- The step-04 classifier now checks `experiments/llm_improvement_testing/manual_stage_truth.csv` first. Rows whose `URL_BEKENDMAKING` already has a manual stage label are prefilled and skipped by the LLM, so tokens are only spent on unlabeled notices.
- Step 04 also writes `data/diagnostics/04_address_mismatches.csv` when the LLM-selected address and title-derived address materially disagree, so those notices can be reviewed explicitly.
- `run_all.sh` remains the base pipeline runner. `run_incremental_with_stats.sh` is just a convenience wrapper around the same pipeline that prints before/after counts.
- March 2026 note: step 04 got a narrow stage-rule patch so notices that say a `beschikking` was `gewijzigd ten opzichte van de ontwerpbeschikking` are treated as `definitive_decision`, not `draft_decision`. This was based on 8 reviewed false-draft cases from Jan/Feb 2026 Brabant/Gelderland notices.
- March 2026 note: if you want to re-test the LLM on manually reviewed notices, step 04 will skip them by design whenever `Stage_manual` is present. A spot-check CSV therefore has to clear `Stage_manual` first, otherwise no OpenAI call is made.
- `scripts/08_classify_lbv_scheme.py` is intentionally separate from step 04, but `run_all.sh` now runs it after step 06b. It writes two canonical outputs: `08_notice_scheme_classification.csv` for all notices and `08_farm_scheme_classification.csv` for the 504 unique farms, and it also enriches `06_deelnemers_lbv_lbvplus.csv` in place with the farm-level scheme columns. Farm-level scheme logic uses all linked notices, with `lbv_plus` taking precedence over `lbv` and `ambiguous` if any linked notice explicitly says `lbv_plus`; otherwise `lbv` wins over `ambiguous`. `scheme_match_context_latest_notice` always comes from the latest notice.
- **LLM validation**: after the first prompt version, we manually labeled 343 notices (`manual_stage_truth.csv`) and used that corpus to iterate on the stage/address prompt via the `llm_improvement_testing` mini-pipeline. After a few iterations the model matched 342/343; review showed the remaining mismatch was a manual labeling error, so the prompt effectively scored 100% on that set. The current prompt in `scripts/04_ai_classify_lbv_and_addresses.py` reflects those iterations.
- After validation, the scraper was re-run and 63 additional notices were added; these were processed with the LLM only (no manual labels).
