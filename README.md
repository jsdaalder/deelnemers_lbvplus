# deelnemers_lbvplus

This project pulls government notices about the LBV/LBV+ scheme, extracts text from attached PDFs, asks an LLM to identify the stage and address, cleans the data, and groups publications into farms. A second stage matches those farms to other sources (minfin, animal counts) and produces charts. Final shareable results (charts plus a trimmed CSV) live under `final_results/<date>/`.

Repo layout (two pipelines):
- `pipelines/participants/` – LBV/LBV+ scraping + LLM classification + address cleanup + farm aggregation (steps 01–06). Data lives in `pipelines/participants/data/`. PDFs under `pipelines/participants/pdfs/`.
- `pipelines/matching_and_analysis/` – downstream matching to minfin/FTM/fosfaat, animal counts, charts, and final export. Data lives in `pipelines/matching_and_analysis/data/`.
- `final_results/<date>/` – published charts (tracked) and trimmed CSV (git-ignored).
- `experiments/` – prompt/testing scratch space (unchanged).

## How to run (quick start)

1) **Setup**: create a venv, `pip install pandas beautifulsoup4 pdfminer.six openai fpdf python-dotenv requests`, add `.env` at repo root with `OPENAI_API_KEY=...`.

2) **Participants pipeline (LBV/LBV+)**  
   - Run everything: `bash pipelines/participants/scripts/run_all.sh`  
   - Inputs: `MODE=local FILES="prov1.html prov2.html"` **or** `MODE=api API_QUERY='c.product-area==officielepublicaties AND cql.textAndIndexes="lbv"' API_MAX=500`.  
   - Outputs land in `pipelines/participants/data/` and a dated copy `deelnemers_lbv_lbvplus_<date>.csv` at repo root. The run_all script also syncs the step-06 output to the matching/analysis pipeline.

3) **Matching & analysis pipeline**  
   - Requires `pipelines/matching_and_analysis/data/raw/06_deelnemers_lbv_lbvplus.csv` (auto-synced by step 2) plus other raw files (`minfin_dataset.csv`, `FTM_*`, fosfaat).  
   - Run its scripts in order (see `pipelines/matching_and_analysis/README.md`).  
   - Export final deliverables: `python3 pipelines/matching_and_analysis/scripts/13_export_final_results.py` → `final_results/<date>/` (charts tracked, CSV ignored).

## Participants pipeline (step details)
- **01_parse_overheid_pages.py** – scrape zoek.officielebekendmakingen.nl HTML or SRU API; normalize dates/URLs → `01_overheid_results.csv`.
- **02_enrich_with_html_and_pdfs.py** – download HTML/PDF, assign `doc_id`, store raw text (`TEXT_HTML`, `LOCAL_PDF_PATH`) → `02_lbv_enriched.csv`.
- **03_extract_pdf_text.py** – pull text from local PDFs into `TEXT_PDF` (keeps HTML text) → `03_lbv_enriched_with_pdf.csv`.
- **04_ai_classify_lbv_and_addresses.py** – concatenates HTML+PDF text and asks the LLM (default `gpt-4.1-mini`) for LBV/LBV+ flag, withdrawal scope, stage (receipt/draft/definitive), main address (street/number/suffix/postcode/place), confidences, and company name (for Noord-Brabant). Uses conservative Dutch prompts; draft wins over definitive if both appear. Outputs `04_lbv_enriched_with_ai_summary.csv`. Quality: refined prompt had 1 mismatch on 343 labeled rows; addresses matched all reviewed rows (see `experiments/llm_improvement_testing`).
- **05_enrich_addresses.py** – rule-based cleanup: split number ranges, fill postcodes via PDOK when available, build `AddressKey` for grouping → `05_lbv_enriched_addresses.csv`.
- **06_build_deelnemers.py** – group publications by `AddressKey` into farms (`farm_id`), keep latest stage, expose `stage_latest_manual` for overrides → `06_deelnemers_lbv_lbvplus.csv` (also copied to repo root with date tag).

## Matching & analysis pipeline (step details)
- **Matching (pipelines/matching_and_analysis/matching/)**  
  01 Combine FTM animals+addresses → `01_FTM_animals_with_addresses.csv`  
  02 KVK lookup minfin → `03_kvk_minfin_results.csv`  
  03 KVK lookup permits → `02_kvk_results.csv`  
  04 Overlap permits/minfin by KVK → `04_overlap_summary.csv`  
  05/06 Address match permits/minfin to FTM → `04_*_animals_join.csv`/summaries  
  07 Fosfaat prep (2015 crosswalk) → `05_fosfaat_rel_crosswalk.csv`  
  08 Fosfaat name fallback for permits → `07_permit_fosfaat_name_matches.csv`  
  09 Build master table → `master_permits.csv`
- **Analysis (pipelines/matching_and_analysis/analysis/)**  
  10 Generate charts → `data/processed/charts/`  
  13 Export final deliverables (slim `farms_permits_minfin_<date>.csv` + chart overviews) → `final_results/<date>/`

## Outputs & sharing
- Intermediate CSVs live under each pipeline’s `data/` (git-ignored).
- Final shareables: `final_results/<date>/chart_all_<date>.png`, `charts_overview_<date>.pdf`, `farms_permits_minfin_<date>.csv` (CSV is ignored; charts can be committed).

## Data sources
- LBV/LBV+ publications: scraped from officielebekendmakingen.nl (scraper by Follow the Money) → LLM pipeline.
- Cluster data: NRC Woo release of the 2021 agrarische basiskaart, enriched with deposition; FICTIEF_BEDRIJFSNUMMER split into `cluster_id`s. Source: https://www.rijksoverheid.nl/documenten/publicaties/2025/10/07/openbaargemaakt-document-bij-besluit-woo-verzoek-over-basiskaart-agrarische-bedrijfssituatie-2021
- Minfin dataset: https://data.overheid.nl/dataset/financile-instrumenten-2022#panel-resources
- Fosfaatbeschikkingen: Woo release: https://www.rijksoverheid.nl/documenten/publicaties/2023/09/19/tabel-gegevens-fosfaatbeschikkingen-bij-bob-woo-besluit-over-toekenning-fosfaatrechten-aan-agrarische-bedrijven
- Dieraantallen (animal counts): Woo release (FTM request): https://www.rijksoverheid.nl/documenten/woo-besluiten/2023/05/04/besluit-op-woo-verzoek-over-de-gecombineerde-opgaven-van-alle-agrarische-ondernemingen-in-nederland

## Data protection

- Sensitive inputs such as `.env`, PDFs, archives, run folders, and experiment CSVs are git-ignored.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.

## Data protection

- Sensitive inputs such as `.env`, `pdfs/`, archives (`data/archive/`), run folders (`data/runs/`), and experiment CSVs are intentionally excluded from git.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.
