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
   Adjust constants in `scripts/03_extract_pdf_text.py` if needed and run `python scripts/03_extract_pdf_text.py` to produce `data/03_lbv_enriched_with_pdf.csv`.
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
At the end, the script also copies the latest participants file to the repo root as `deelnemers_lbv_lbvplus_YYYY_MM_DD.csv` for convenient sharing while keeping bulk CSVs under `data/` (which is git-ignored).

Intermediate CSVs are overwritten in place, so archive raw exports if you need reproducibility. The repo now keeps older drops under `data/archive/<date>/` and full run folders under `data/runs/<timestamp>/` (both git-ignored). Promote only the “blessed” outputs into `data/`.

## Data protection

- Sensitive inputs such as `.env`, `pdfs/`, archives (`data/archive/`), run folders (`data/runs/`), and experiment CSVs are intentionally excluded from git.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.
