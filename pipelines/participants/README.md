# Participants pipeline

This pipeline builds the participant dataset from official public notices.

It:
- scrapes LBV/LBV+ related notices from `officielebekendmakingen.nl`
- extracts HTML/PDF text
- classifies stage and extracts addresses
- normalizes addresses and groups notices into farms
- classifies each notice and farm as `lbv`, `lbv_plus`, or `ambiguous`

## Pipeline outputs

Canonical step outputs in `data/`:
- `01_overheid_results.csv`
  Raw accepted notice rows from the SRU/API scrape.
- `02_lbv_enriched.csv`
  Notice rows enriched with HTML content and PDF links.
- `03_lbv_enriched_with_pdf.csv`
  Step-02 rows plus extracted PDF text.
- `04_lbv_enriched_with_ai_summary.csv`
  Notice-level stage/address classification.
- `05_lbv_enriched_addresses.csv`
  Deterministically cleaned addresses and address keys.
- `06_deelnemers_lbv_lbvplus.csv`
  Canonical participant handoff file. Farm/location rows with latest notice fields, `doc_ids_all`, `AddressKeyAll`, and farm-level scheme columns.
- `06_all_unique_farms_review.csv`
  One row per `farm_id_new`, used for manual review of latest-notice fields.
- `06b_province_stage_overview.csv`
  Small province/stage summary export.
- `08_notice_scheme_classification.csv`
  Notice-level LBV/LBV+ classification.
- `08_farm_scheme_classification.csv`
  One row per unique farm with latest-notice scheme fields and full-notice scheme rollup.

Support folders:
- `data/diagnostics/`
  Debug outputs such as PDOK failures and address mismatches.
- `data/exports/`
  Ad hoc review or outreach exports.
- `data/archive/`
  Old snapshots and redundant files.
- `data/runs/`
  Per-run summaries and counters.

## How to run

From the repo root:

```bash
bash pipelines/participants/scripts/run_all.sh
```

Requirements:
- a virtualenv with the Python dependencies installed
- `.env` in repo root with `OPENAI_API_KEY`
- network access for Overheid, PDF downloads, PDOK, and OpenAI

Common variants:

Incremental API run:

```bash
API_MAX=1000 bash pipelines/participants/scripts/run_all.sh
```

Local HTML mode:

```bash
MODE=local FILES="prov1.html prov2.html" bash pipelines/participants/scripts/run_all.sh
```

Keep existing step 01 scrape and rerun downstream steps:

```bash
SKIP_STEP1=1 bash pipelines/participants/scripts/run_all.sh
```

## Current logic

### Step 04: stage + address classification
- first checks `experiments/llm_improvement_testing/manual_stage_truth.csv`
- if a notice URL already has a manual stage label:
  - `Stage_manual` is filled
  - the row skips the LLM
- manual and LLM stage columns remain separate
- writes `data/diagnostics/04_address_mismatches.csv` when title-derived and text-derived addresses disagree materially

### Step 05: address cleanup
- expands multi-number addresses
- normalizes address fields into `AddressKey`
- uses PDOK for missing/correctable postcode/address cases

### Step 06: farm grouping
- groups linked notices into farms
- writes `farm_id_new`, `doc_ids_all`, and `AddressKeyAll`

### Step 08: LBV/LBV+ scheme classification
- classifies notices as `lbv`, `lbv_plus`, or `ambiguous`
- farm-level scheme resolution uses all linked notices:
  - any `lbv_plus` notice => farm resolves to `lbv_plus`
  - else any `lbv` notice => farm resolves to `lbv`
  - else => `ambiguous`
- `08_classify_lbv_scheme.py` also enriches `06_deelnemers_lbv_lbvplus.csv` in place with:
  - `scheme_class_latest_notice`
  - `scheme_class_resolved_farm`
  - `scheme_classes_all_notices`
  - `scheme_match_context_latest_notice`
  - `scheme_notice_history`

## Useful scripts
- `scripts/run_all.sh`
  Canonical participants runner.
- `scripts/run_incremental_with_stats.sh`
  Thin wrapper around `run_all.sh`.
- `scripts/07_export_review_csv.py`
  Rebuilds `06_all_unique_farms_review.csv`.
- `scripts/08_classify_lbv_scheme.py`
  Rebuilds the two scheme outputs and enriches `06`.

## Notes
- `06_deelnemers_lbv_lbvplus.csv` is the canonical handoff file for downstream matching.
- `06_all_unique_farms_review.csv` is for manual review only.
- `08_farm_scheme_classification.csv` is the audit-friendly one-row-per-farm scheme view; it does not replace `06` as the matching input.
