# deelnemers_lbvplus

Utilities for parsing overheid.nl publications, enriching them with local metadata, extracting PDF text, classifying LBV/LBV+ activity via OpenAI, post-processing addresses, and aggregating participants into a consolidated CSV for analysis. Current “blessed” outputs in `data/` come from run `data/runs/2025-11-19-151008` (git `0aa90980c03c86977a4c9e6b5cf9e789ad30688e`); earlier outputs are archived under `data/archive/2025-11-25/`.

## Repository structure

- `scripts/01_parse_overheid_pages.py` – parse saved overheid.nl HTML or query the official SRU API into `data/01_overheid_results.csv`.
- `scripts/02_enrich_with_html_and_pdfs.py` – download PDFs/HTML, add `doc_id`, and write `data/02_lbv_enriched.csv`.
- `scripts/03_extract_pdf_text.py` – fill `TEXT_PDF` for rows that have a local PDF (`data/03_lbv_enriched_with_pdf.csv`).
- `scripts/04_ai_classify_lbv_and_addresses.py` – call OpenAI to add LBV/LBV+, withdrawal, stage, address metadata, and extract company names when available (`data/04_lbv_enriched_with_ai_summary.csv`).
- `scripts/05_enrich_addresses.py` – deterministic address cleanup: split multi-number house strings, look up missing postcodes via PDOK, and emit `AddressKey` for grouping (`data/05_lbv_enriched_addresses.csv`).
- `scripts/06_build_deelnemers.py` – group permit rows into farm-level participants (`data/06_deelnemers_lbv_lbvplus.csv`).
- `data/` – numbered CSV checkpoints so it is obvious which step produced each file (e.g., `02_lbv_enriched.csv` came from script 02). See `data/DATA_README.md` for archive/run layout and provenance.
- `experiments/llm_improvement_testing/` – scratch space for evaluating stage/address prompts; JSON results retained, CSVs git-ignored. See folder README.
- `agents.md` – detailed prompt/agent documentation for the LLM steps.

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
     python scripts/01_parse_overheid_pages.py --mode local --files provincie1.html provincie2.html --out data/01_overheid_results.csv
     ```
   - Directly via the SRU API (example query for LBV terms):
     ```
     python scripts/01_parse_overheid_pages.py --mode api --api-query 'c.product-area==officielepublicaties AND cql.textAndIndexes="lbv"' --api-max-records 500
     ```
2. **Enrich with HTML/PDF context**  
   ```
   python scripts/02_enrich_with_html_and_pdfs.py --in data/01_overheid_results.csv --out data/02_lbv_enriched.csv
   ```
3. **Extract PDF text**  
   Run `python scripts/03_extract_pdf_text.py` to pull readable text from PDFs into `data/03_lbv_enriched_with_pdf.csv`.
4. **LLM classification**  
   Ensure `.env` exists, set `DEFAULT_MODEL` or basenames in `scripts/04_ai_classify_lbv_and_addresses.py`, then run it to generate `data/04_lbv_enriched_with_ai_summary.csv`.
5. **Address enrichment**  
   `python scripts/05_enrich_addresses.py --input data/04_lbv_enriched_with_ai_summary.csv --output data/05_lbv_enriched_addresses.csv`  
   Splits multi-number houses, fills missing postcodes via PDOK, and adds `AddressKey` used for grouping.
6. **Aggregate participants**  
   `python scripts/06_build_deelnemers.py --input data/05_lbv_enriched_addresses.csv --output data/06_deelnemers_lbv_lbvplus.csv`  
   Step 06 now reads the step-05 output directly. The legacy `06_vergunningen_lbv_lbvplus.csv` lives only in `data/archive/2025-11-25/` for reference.

### One-command run

Once you have a `.env` with `OPENAI_API_KEY`, you can run the full pipeline (including regeneration of `province_stage_irrevocable.csv`) in one go:

```bash
bash scripts/run_all.sh
```

Step 01 input options (set as env vars when invoking):
- Local HTML exports: `MODE=local FILES="prov1.html prov2.html" bash scripts/run_all.sh`
- Direct SRU API: `MODE=api API_QUERY='c.product-area==officielepublicaties AND cql.textAndIndexes=\"lbv\"' API_MAX=500 bash scripts/run_all.sh`

If `data/01_overheid_results.csv` already exists, step 01 is skipped unless you force it with `RUN_STEP1=1`. The script stops early if prerequisites are missing.
At the end, the script also copies the latest participants file to the repo root as `deelnemers_lbv_lbvplus_YYYY_MM_DD.csv` for convenient sharing while keeping the big CSVs under `data/` (git-ignored).

## Step-by-step detail (what each script does)

- **01_parse_overheid_pages.py**  
  Input: either local HTML exports (from zoek.officielebekendmakingen.nl) or a SRU API query.  
  Output: `data/01_overheid_results.csv` with raw publication info (title, date, URLs, government body). No AI yet.

- **02_enrich_with_html_and_pdfs.py**  
  Saves the HTML/PDF files locally, gives each row a `doc_id`, and combines the text into `TEXT_HTML` and `TEXT_PDF`. Produces `data/02_lbv_enriched.csv`; later steps build on this file.

- **03_extract_pdf_text.py**  
  Fills in `TEXT_PDF` by reading the PDFs when that column is still empty, yielding `data/03_lbv_enriched_with_pdf.csv`. The HTML text stays in place so the AI sees both sources.

- **04_ai_classify_lbv_and_addresses.py**  
  Combines `TEXT_HTML` + `TEXT_PDF` and asks the AI (default `gpt-4.1-mini`) to:  
  - spot LBV/LBV+ relevance and withdrawal scope,  
  - decide the stage (receipt, draft, definitive) using conservative Dutch rules,  
  - pull out the main farm address (street/number/suffix/postcode/place) with a confidence score.  
  Output: `data/04_lbv_enriched_with_ai_summary.csv` with stage, LBV fields, address fields, confidences, and source notes.

- **05_enrich_addresses.py**  
  Rule-based cleanup after the AI: split house numbers like “7-9”, look up missing postcodes via PDOK, tidy the address, and build `AddressKey` for grouping. Output: `data/05_lbv_enriched_addresses.csv`.

- **06_build_deelnemers.py**  
  Groups publications that share an `AddressKey` into farm-level records, assigns stable `farm_id`s, and keeps the latest publication per farm. Adds both `stage_latest_llm` and an empty `stage_latest_manual` column for anyone who wants to override the AI. Output: `data/06_deelnemers_lbv_lbvplus.csv`, which also feeds the province counts.

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

Intermediate CSVs get overwritten, so save copies elsewhere if you want to keep history. Older drops live under `data/archive/<date>/` and full run folders under `data/runs/<timestamp>/` (both git-ignored). Keep the current “official” outputs directly under `data/`.

## Data protection

- Sensitive inputs such as `.env`, `pdfs/`, archives (`data/archive/`), run folders (`data/runs/`), and experiment CSVs are intentionally excluded from git.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.
