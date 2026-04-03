# deelnemers_lbvplus

This repo reconstructs a public view of the Dutch `LBV` and `LBV+` livestock buyout schemes from official notices and other public datasets.

At a high level, it does three things:
- scrape and classify permit notices from `officielebekendmakingen.nl`
- group those notices into farm-level participants
- match those farms to other public datasets and export charts/results

## Repo structure
- `pipelines/participants/`
  Builds the participant dataset from public notices. This is where scraping, PDF extraction, stage classification, address cleanup, farm grouping, and LBV/LBV+ scheme classification happen.
- `pipelines/matching_ftm/`
  Matches those participants to FTM animal/address data, MinFin voorschotten, fosfaatbeschikkingen, and KVK-based linkages.
- `final_results/<date>/`
  Generated chart bundles and public-facing exports.

## Canonical outputs

Participants pipeline:
- `pipelines/participants/data/06_deelnemers_lbv_lbvplus.csv`
  Canonical participant handoff file. This is the main farm/location dataset used downstream.
- `pipelines/participants/data/06_all_unique_farms_review.csv`
  One row per `farm_id_new`, used for manual review of the latest notice.
- `pipelines/participants/data/08_notice_scheme_classification.csv`
  Notice-level LBV/LBV+ classification for the full notice corpus.
- `pipelines/participants/data/08_farm_scheme_classification.csv`
  Farm-level LBV/LBV+ classification for the `504` unique farms.

Matching pipeline:
- `pipelines/matching_ftm/data/raw/06_deelnemers_lbv_lbvplus.csv`
  Synced copy of the canonical participants file.
- `pipelines/matching_ftm/data/processed/master_*.csv`
  Matched farm-level datasets used by the chart/export scripts.

## How to run

### 1. Participants
From the repo root:

```bash
bash pipelines/participants/scripts/run_all.sh
```

Notes:
- requires `.env` with `OPENAI_API_KEY`
- defaults to SRU API mode
- runs incrementally if `data/01_overheid_results.csv` already exists
- writes before/after counts to the terminal
- syncs the refreshed `06_deelnemers_lbv_lbvplus.csv` into `matching_ftm/data/raw/`

### 2. Matching
Follow:

- [pipelines/matching_ftm/README.md](/Users/jandaalder/Desktop/coding_projects/deelnemers_lbvplus/pipelines/matching_ftm/README.md)

### 3. Charts / export
From the repo root:

```bash
python3 pipelines/matching_ftm/analysis/ftm/14_generate_charts.py
python3 pipelines/matching_ftm/analysis/ftm/13_export_final_results.py
```

## Current participants logic
- step 04 uses `manual_stage_truth.csv` first and skips OpenAI calls for URLs that already have a manual stage label
- manual and LLM stage labels stay separate
- address extraction uses the notice text plus title-based guardrails
- step 08 classifies notices as `lbv`, `lbv_plus`, or `ambiguous`
- farm-level scheme resolution uses all linked notices:
  - any `lbv_plus` notice => farm resolves to `lbv_plus`
  - else any `lbv` notice => farm resolves to `lbv`
  - else => `ambiguous`

## Data handling
- large intermediate CSVs, archives, diagnostics, and run logs are mostly git-ignored
- `data/archive/`, `data/diagnostics/`, `data/exports/`, and `data/runs/` are for support material, not canonical handoff files
- avoid committing generated charts/exports unless you explicitly want a public bundle in git

## Public sources
- Permit notices: `officielebekendmakingen.nl`
- MinFin voorschotten dataset
- RVO LBV/LBV+ overviews
- FTM / Woo animal-count datasets
- fosfaatbeschikkingen
- CBS and RAV helper datasets
