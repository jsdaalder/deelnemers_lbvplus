# deelnemers_lbvplus

Utilities for parsing overheid.nl publications, enriching them with local metadata, extracting PDF text, classifying LBV/LBV+ activity via OpenAI, and aggregating participants into a consolidated CSV for analysis.

## Repository structure

- `scripts/01_parse_overheid_pages.py` – parse saved overheid.nl HTML or query the official SRU API into `data/01_overheid_results.csv`.
- `scripts/02_enrich_with_html_and_pdfs.py` – download PDFs/HTML, add `doc_id`, and write `data/02_lbv_enriched.csv`.
- `scripts/03_extract_pdf_text.py` – fill `TEXT_PDF` for rows that have a local PDF (`data/03_lbv_enriched_with_pdf.csv`).
- `scripts/04_ai_classify_lbv_and_addresses.py` – call OpenAI to add LBV/LBV+, withdrawal, stage, address metadata, and extract company names when available (`data/04_lbv_enriched_with_ai_summary.csv`).
- `scripts/05_enrich_addresses.py` – reserved for post-LLM address cleanup (currently a stub).
- `scripts/06_build_deelnemers.py` – group permit rows into farm-level participants (`data/06_deelnemers_lbv_lbvplus.csv`).
- `data/` – numbered CSV checkpoints so it is obvious which step produced each file (e.g., `02_lbv_enriched.csv` came from script 02).
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
   Ensure `.env` exists, set `DEFAULT_MODEL` or basenames in `scripts/04_ai_classify_lbv_and_addresses.py`, then run it to generate `data/04_lbv_enriched_with_ai_summary.*`.
5. **(Optional) Address enrichment**  
   Flesh out `scripts/05_enrich_addresses.py` if additional deterministic cleanup is needed.
6. **Aggregate participants**  
   `python scripts/06_build_deelnemers.py` reads `data/06_vergunningen_lbv_lbvplus.csv` (source permits) and writes `data/06_deelnemers_lbv_lbvplus.csv`.

Intermediate CSVs are overwritten in place, so archive raw exports if you need reproducibility.

## Data protection

- Sensitive inputs such as `.env` and the `pdfs/` directory are intentionally excluded from git.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.
