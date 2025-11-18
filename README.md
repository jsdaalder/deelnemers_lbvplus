# deelnemers_lbvplus

Utilities for parsing overheid.nl publications, enriching them with local metadata, extracting PDF text, classifying LBV/LBV+ activity via OpenAI, and aggregating participants into a consolidated CSV for analysis.

## Repository structure

- `1_parse_overheid_pages.py` – parse saved overheid.nl HTML into `overheid_parsed.csv`.
- `3_extract_pdf_text.py` – fill `TEXT_PDF` by parsing local PDF downloads referenced in a CSV.
- `4_ai_classify_lbv_and_addresses.py` – call OpenAI to classify LBV/LBV+ involvement, permit stage, withdrawals, and the main address.
- `convert_permit_to_companies/6_build_deelnemers.py` – group permit docs into farm IDs and output `Deelnemers_LBV_LBVplus.csv`.
- `lbv_*.csv`, `overheid_*.csv` – intermediate datasets produced along the pipeline.
- `agents.md` – detailed prompt/agent documentation for the LLM steps.

## Setup

1. Install Python 3.11+ and create a virtual environment.
2. Install dependencies:  
   ```bash
   pip install pandas beautifulsoup4 pdfminer.six openai fpdf python-dotenv
   ```
3. Create a local `.env` file (in the repo root) with your API credentials, e.g.:
   ```
   OPENAI_API_KEY=sk-...
   ```

> ⚠️ **Never commit `.env`** – it contains secrets and is ignored via `.gitignore`. Every user must create their own `.env` locally before running the scripts.

## Typical workflow

1. **Parse overheid pages**  
   `python 1_parse_overheid_pages.py --files provincie1.html provincie2.html --out overheid_parsed.csv`
2. **Curate/enrich CSV** (outside this repo) to produce `lbv_enriched.csv`.
3. **Extract PDF text**  
   Adjust constants in `3_extract_pdf_text.py` if needed and run `python 3_extract_pdf_text.py`.
4. **LLM classification**  
   Ensure `.env` exists, set `DEFAULT_MODEL` or basenames in `4_ai_classify_lbv_and_addresses.py`, then run it to produce `lbv_enriched_with_ai_summary.*`.
5. **Aggregate participants**  
   From `convert_permit_to_companies`, run `python 6_build_deelnemers.py` to produce `Deelnemers_LBV_LBVplus.csv`.

Intermediate CSVs are overwritten in place, so archive raw exports if you need reproducibility.

## Data protection

- Sensitive inputs such as `.env` and the `pdfs/` directory are intentionally excluded from git.
- Before sharing datasets, remove columns that might contain personal information or rotate identifiers.

## Contributing

1. Branch from `main`.
2. Update or add documentation/tests when modifying the enrichment logic.
3. Run the relevant pipeline steps and keep outputs consistent.
4. Open a pull request summarizing which CSVs or scripts changed so reviewers can reproduce the flow.
