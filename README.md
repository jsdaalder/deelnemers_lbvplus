# deelnemers_lbvplus

This project pulls government notices about the LBV/LBV+ scheme, extracts text from attached PDFs, asks an LLM to identify the stage and address, cleans the data, and groups publications into farms. A second stage matches those farms to other sources (minfin, animal counts) and produces charts. Final shareable results (charts plus a trimmed CSV) live under `final_results/<date>/`.

Repo layout (two pipelines):
- `pipelines/participants/` – LBV/LBV+ scraping + LLM classification + address cleanup + farm aggregation (steps 01–06). Data lives in `pipelines/participants/data/`. PDFs under `pipelines/participants/pdfs/`.
- `pipelines/uitgekochte/` – downstream matching to minfin/FTM/fosfaat, animal counts, charts, and final export. Data lives in `pipelines/uitgekochte/data/`.
- `final_results/<date>/` – published charts (tracked) and trimmed CSV (git-ignored).
- `experiments/` – prompt/testing scratch space (unchanged).

## Repository structure

- `pipelines/participants/scripts/01_parse_overheid_pages.py` – parse saved overheid.nl HTML or query the SRU API into `pipelines/participants/data/01_overheid_results.csv`.
- `pipelines/participants/scripts/02_enrich_with_html_and_pdfs.py` – download PDFs/HTML, add `doc_id`, and write `pipelines/participants/data/02_lbv_enriched.csv`.
- `pipelines/participants/scripts/03_extract_pdf_text.py` – fill `TEXT_PDF` for rows that have a local PDF (`pipelines/participants/data/03_lbv_enriched_with_pdf.csv`).
- `pipelines/participants/scripts/04_ai_classify_lbv_and_addresses.py` – call OpenAI to add LBV/LBV+, withdrawal, stage, address metadata, and extract company names when available (`pipelines/participants/data/04_lbv_enriched_with_ai_summary.csv`).
- `pipelines/participants/scripts/05_enrich_addresses.py` – deterministic address cleanup: split multi-number house strings, look up missing postcodes via PDOK, and emit `AddressKey` for grouping (`pipelines/participants/data/05_lbv_enriched_addresses.csv`).
- `pipelines/participants/scripts/06_build_deelnemers.py` – group permit rows into farm-level participants (`pipelines/participants/data/06_deelnemers_lbv_lbvplus.csv`), carrying `COMPANY_NAME`/`company_id` forward per farm/address.
- `pipelines/participants/data/` – numbered CSV checkpoints; see `pipelines/participants/data/DATA_README.md` for archive/run layout and provenance.
- `pipelines/uitgekochte/` – downstream matching pipeline (see its README) consuming `06_deelnemers_lbv_lbvplus.csv` and other Woo/minfin datasets.
- `experiments/llm_improvement_testing/` – scratch space for evaluating stage/address prompts; JSON results retained, CSVs git-ignored. See folder README.

## Setup

1. Install Python 3.11+ and create a virtual environment.
2. Install dependencies:  
   ```bash
   pip install pandas beautifulsoup4 pdfminer.six openai fpdf python-dotenv requests
   ```
3. Create a local `.env` file (in the repo root) with your API credentials, e.g.:
   ```
   OPENAI_API_KEY=sk-...
   ```

> ⚠️ **Never commit `.env`** – it contains secrets and is ignored via `.gitignore`. Every user must create their own `.env` locally before running the scripts.

## Typical workflow

1. **Parse overheid pages (local or API)**  
   Running without `--mode` will prompt you interactively to pick `local` or `api`.
   - Local HTML export:
     ```
     python pipelines/participants/scripts/01_parse_overheid_pages.py --mode local --files provincie1.html provincie2.html --out pipelines/participants/data/01_overheid_results.csv
     ```
   - Directly via the SRU API (example query for LBV terms):
     ```
     python pipelines/participants/scripts/01_parse_overheid_pages.py --mode api --api-query 'c.product-area==officielepublicaties AND cql.textAndIndexes="lbv"' --api-max-records 500
     ```
2. **Enrich with HTML/PDF context**  
   ```
   python pipelines/participants/scripts/02_enrich_with_html_and_pdfs.py --in pipelines/participants/data/01_overheid_results.csv --out pipelines/participants/data/02_lbv_enriched.csv
   ```
3. **Extract PDF text**  
   Run `python pipelines/participants/scripts/03_extract_pdf_text.py` to pull readable text from PDFs into `pipelines/participants/data/03_lbv_enriched_with_pdf.csv`.
4. **LLM classification**  
   Ensure `.env` exists (repo root), set `DEFAULT_MODEL` or basenames in `pipelines/participants/scripts/04_ai_classify_lbv_and_addresses.py`, then run it to generate `pipelines/participants/data/04_lbv_enriched_with_ai_summary.csv`.
5. **Address enrichment**  
   `python pipelines/participants/scripts/05_enrich_addresses.py --input pipelines/participants/data/04_lbv_enriched_with_ai_summary.csv --output pipelines/participants/data/05_lbv_enriched_addresses.csv`  
   Splits multi-number houses, fills missing postcodes via PDOK, and adds `AddressKey` used for grouping.
6. **Aggregate participants**  
   `python pipelines/participants/scripts/06_build_deelnemers.py --input pipelines/participants/data/05_lbv_enriched_addresses.csv --output pipelines/participants/data/06_deelnemers_lbv_lbvplus.csv`  
   Step 06 now reads the step-05 output directly. The legacy `06_vergunningen_lbv_lbvplus.csv` lives only in `pipelines/participants/data/archive/2025-11-25/` for reference.

### Uitgekochte boeren (second-stage pipeline)
- `pipelines/participants/scripts/run_all.sh` syncs the step-06 output to `pipelines/uitgekochte/data/raw/06_deelnemers_lbv_lbvplus.csv` so the downstream matching pipeline can run without manual copying.
- Run the second-stage scripts from `pipelines/uitgekochte/scripts/` (see its README) after supplying the other required raw files (`minfin_dataset.csv`, `FTM_*`, fosfaat).
- After generating `master_permits.csv` and charts, run `python3 pipelines/uitgekochte/scripts/13_export_final_results.py` to export a slimmed `farms_permits_minfin_<date>.csv` and chart overviews into `final_results/<YYYY_MM_DD>/` with dated filenames for each scrape run.

### Outputs & sharing
- Intermediate CSVs stay in `pipelines/participants/data/` and `pipelines/uitgekochte/data/` (git-ignored).
- Final shareable artifacts: `final_results/<date>/chart_all_<date>.png`, `charts_overview_<date>.pdf`, and `farms_permits_minfin_<date>.csv` (CSV is git-ignored; charts can be committed).

### Data sources
- LBV/LBV+ publications: scraped from officielebekendmakingen.nl (scraper built by Follow the Money) and processed via the LLM/enrichment pipeline in this repo.
- Cluster data: derived from NRC’s Woo release of the 2021 agrarische basiskaart, enriched with deposition info; FICTIEF_BEDRIJFSNUMMER values were split into `cluster_id`s to handle multi-location farms. Source: https://www.rijksoverheid.nl/documenten/publicaties/2025/10/07/openbaargemaakt-document-bij-besluit-woo-verzoek-over-basiskaart-agrarische-bedrijfssituatie-2021
- Minfin dataset: https://data.overheid.nl/dataset/financile-instrumenten-2022#panel-resources
- Fosfaatbeschikkingen: Woo release of fosfaatbeschikkingen table: https://www.rijksoverheid.nl/documenten/publicaties/2023/09/19/tabel-gegevens-fosfaatbeschikkingen-bij-bob-woo-besluit-over-toekenning-fosfaatrechten-aan-agrarische-bedrijven
- Dieraantallen (animal counts): Woo release of gecombineerde opgaven for agrarische ondernemingen (Follow the Money request): https://www.rijksoverheid.nl/documenten/woo-besluiten/2023/05/04/besluit-op-woo-verzoek-over-de-gecombineerde-opgaven-van-alle-agrarische-ondernemingen-in-nederland

### One-command run

Once you have a `.env` with `OPENAI_API_KEY`, you can run the full pipeline (including regeneration of `province_stage_irrevocable.csv`) in one go:

```bash
bash pipelines/participants/scripts/run_all.sh
```

Step 01 input options (set as env vars when invoking):
- Local HTML exports: `MODE=local FILES="prov1.html prov2.html" bash pipelines/participants/scripts/run_all.sh`
- Direct SRU API: `MODE=api API_QUERY='c.product-area==officielepublicaties AND cql.textAndIndexes=\"lbv\"' API_MAX=500 bash pipelines/participants/scripts/run_all.sh`

If `pipelines/participants/data/01_overheid_results.csv` already exists, step 01 is skipped unless you force it with `RUN_STEP1=1`. The script stops early if prerequisites are missing.
At the end, the script also copies the latest participants file to the repo root as `deelnemers_lbv_lbvplus_YYYY_MM_DD.csv` for convenient sharing while keeping the big CSVs under `pipelines/participants/data/` (git-ignored).

## Step-by-step detail (what each script does)

- **01_parse_overheid_pages.py**  
  Input: either local HTML exports (from zoek.officielebekendmakingen.nl) or a SRU API query.  
  Output: `pipelines/participants/data/01_overheid_results.csv` with raw publication info (title, date, URLs, government body). No AI yet.

- **02_enrich_with_html_and_pdfs.py**  
  Saves the HTML/PDF files locally, gives each row a `doc_id`, and combines the text into `TEXT_HTML` and `TEXT_PDF`. Produces `pipelines/participants/data/02_lbv_enriched.csv`; later steps build on this file.

- **03_extract_pdf_text.py**  
  Fills in `TEXT_PDF` by reading the PDFs when that column is still empty, yielding `pipelines/participants/data/03_lbv_enriched_with_pdf.csv`. The HTML text stays in place so the AI sees both sources.

- **04_ai_classify_lbv_and_addresses.py**  
  Combines `TEXT_HTML` + `TEXT_PDF` and asks the AI (default `gpt-4.1-mini`) to:  
  - spot LBV/LBV+ relevance and withdrawal scope,  
  - decide the stage (receipt, draft, definitive) using conservative Dutch rules,  
  - pull out the main farm address (street/number/suffix/postcode/place) with a confidence score.  
  Output: `pipelines/participants/data/04_lbv_enriched_with_ai_summary.csv` with stage, LBV fields, address fields, confidences, and source notes.

- **05_enrich_addresses.py**  
  Rule-based cleanup after the AI: split house numbers like “7-9”, look up missing postcodes via PDOK, tidy the address, and build `AddressKey` for grouping. Output: `pipelines/participants/data/05_lbv_enriched_addresses.csv`.

- **06_build_deelnemers.py**  
  Groups publications that share an `AddressKey` into farm-level records, assigns stable `farm_id`s, and keeps the latest publication per farm. Adds both `stage_latest_llm` and an empty `stage_latest_manual` column for anyone who wants to override the AI. Output: `pipelines/participants/data/06_deelnemers_lbv_lbvplus.csv`, which also feeds the province counts.

## LLM stage/address method (step 04) and quality checks

- Prompting: `scripts/04_ai_classify_lbv_and_addresses.py` combines `TEXT_HTML` + `TEXT_PDF` and asks the model for LBV/LBV+ classification, withdrawal scope, procedural stage (receipt, draft, definitive), and the main farm address. Prompts are in Dutch and conservative; draft signals win when both draft and definitive wording appear. Address confidence and LBV confidence are returned per row.
- Manual truth set: 406 publications exported to `experiments/llm_improvement_testing/manual_stage_truth.csv`; 343 rows were manually labeled (187 receipt, 70 draft, 86 definitive). Unlabeled rows stayed blank and were excluded from scoring.
- Baseline vs refined prompt: The earlier prompt (column `STAGE_LLM`) disagreed with manual labels on 94 of 343 labeled rows (mismatches driven by overly eager definitive choices). We hardened the rules (draft priority, explicit receipt heuristics, ignore nav/metadata noise), captured as `STAGE_NEW_LLM`.
- Latest results (model `gpt-4.1-mini`): `STAGE_NEW_LLM` vs manual labels shows 1 mismatch out of 343 labeled rows (see `experiments/llm_improvement_testing/mismatches_latest.csv`). The remaining mismatch appears to be a manual label error (publication was an ontwerpbesluit, so draft is correct). Confusion summary is stored in `experiments/llm_improvement_testing/stage_run_results.json`.
- Address extraction quality: On the same 343 manually reviewed rows, the LLM’s primary address extraction matched all manual checks (no observed address errors).
- Usage in pipeline: Step 04 writes `STAGE` (from the refined prompt) to `data/04_lbv_enriched_with_ai_summary.csv`, step 05 carries it forward, and step 06 exposes both `stage_latest_llm` and a blank `stage_latest_manual` column for optional human overrides. The province summary uses the manual value when present, otherwise the LLM value.

## Current province-stage overview (cutoff: definitive ≥6 weeks old as of 2025-11-25)

| province | total_farms | receipt_of_application | draft_decision | definitive_decision | irrevocable_on_2025_11_25 |
| --- | --- | --- | --- | --- | --- |
| Drenthe | 24 | 23 | 0 | 1 | 1 |
| Flevoland | 3 | 2 | 0 | 1 | 0 |
| Fryslân | 8 | 8 | 0 | 0 | 0 |
| Gelderland | 78 | 2 | 31 | 45 | 33 |
| Limburg | 123 | 108 | 15 | 0 | 0 |
| Noord-Brabant | 61 | 0 | 36 | 25 | 22 |
| Noord-Holland | 1 | 1 | 0 | 0 | 0 |
| Overijssel | 40 | 28 | 3 | 9 | 9 |
| Utrecht | 3 | 0 | 1 | 2 | 1 |
| Total | 341 | 172 | 86 | 83 | 66 |

Intermediate CSVs get overwritten, so save copies elsewhere if you want to keep history. Older drops live under `data/archive/<date>/` and full run folders under `data/runs/<timestamp>/` (both git-ignored). Keep the current “official” outputs directly under `data/` (CSV files are git-ignored to avoid publishing names/addresses by default; rerun the pipeline locally to regenerate them).

## Data protection

- Sensitive inputs such as `.env`, `pdfs/`, archives (`data/archive/`), run folders (`data/runs/`), and experiment CSVs are intentionally excluded from git.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.
