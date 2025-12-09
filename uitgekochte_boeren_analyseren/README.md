# Uitgekochte Boeren Analyzer

End-to-end flow to link permit-rescinded and minfin buy-out farms to the national farm registry (FTM) and animal counts.

## Folder Structure
- `data/raw/` — source CSVs (`06_deelnemers_lbv_lbvplus.csv`, `minfin_dataset.csv`, `FTM_addresses.csv`, `FTM_dieraantallen.csv`, fosfaatbeschikkingen).
- `data/processed/` — derived outputs.
- `scripts/` — numbered utilities (see below) plus `kvk_original.py` (reference only).
- `AGENTS.md` — contributor guidelines.

## Setup
- Python 3.11+ recommended.
- Optional venv: `python3 -m venv .venv && source .venv/bin/activate`
- Install extras if needed: `pip install pandas pytest`
- Deprecated/legacy kept for reference: `11_enrich_permits_with_company_deprecated.py`, `12_analysis_overview_deprecated.py`, `kvk_original.py`.
- The root `scripts/run_all.sh` now syncs `data/06_deelnemers_lbv_lbvplus.csv` into `data/raw/06_deelnemers_lbv_lbvplus.csv` here; run that first so you do not need to copy manually.

## Pipeline (aligned to the clarified matching flow)
1) **Combine FTM addresses + animal counts**  
   - `python3 scripts/01_combine_ftm_datasets.py`  
   - Output: `data/processed/01_FTM_animals_with_addresses.csv`

2) **KVK lookup for minfin companies**  
   - `python3 scripts/02_kvk_lookup_minfin.py`  
   - Output: `data/processed/03_kvk_minfin_results.csv` (bezoek/post address, kvk, actief/rechtsvorm)

3) **KVK lookup for permit notices**  
   - `python3 scripts/03_kvk_lookup_permits.py`  
   - Output: `data/processed/02_kvk_results.csv`

4) **Overlap check (minfin vs permits by KVK)**  
   - `python3 scripts/04_kvk_overlap.py`  
   - Outputs: `data/processed/04_overlap_summary.csv`, `data/processed/04_combined_kvk_addresses.csv`

5) **Direct address match to FTM (permit + minfin)**  
   - Permits: `python3 scripts/05_match_permits_ftm.py` → `04_permit_animals_join.csv` / `04_permit_animals_summary.csv`  
   - Minfin: `python3 scripts/06_match_minfin_ftm.py` → `04_minfin_animals_join.csv` / `04_minfin_animals_summary.csv`

6) **(Optional) Fosfaat fallback for unmatched companies**  
   - `python3 scripts/08_match_permits_fosfaat_names.py` (name-based)  
   - Fosfaat/2015 prep: `python3 scripts/07_prepare_fosfaat_2015_linkages.py`  
   - (Deprecated) `scripts/11_enrich_permits_with_company_deprecated.py` kept for reference.

7) **Master table + merge minfin**  
   - `python3 scripts/09_build_master_table.py`  
   - `python3 scripts/09_merge_minfin_into_master.py`  *(if present; ensure numbering if you keep a merge step)*  
   - Output: `data/processed/master_permits.csv` (includes all permits + minfin, animal counts when matched)

8) **Reporting**  
   - `python3 scripts/10_generate_report.py`  
   - Outputs charts in `data/processed/charts/`

9) **Export final deliverables**  
   - `python3 scripts/13_export_final_results.py`  
   - Copies chart overviews (PDF/PNG) and writes a slimmed `farms_permits_minfin_<date>.csv` (keeps key columns only) to `../final_results/<YYYY_MM_DD>/` with the date tag in filenames.

## Notes on matching logic
- Primary match is direct address-based (`normalized_address_key`) against `FTM_dieraantallen`.
- KVK is used to enrich permits and minfin with official names/addresses/kvk numbers and to measure overlap.
- Fosfaat name matches are a fallback for unmatched companies; if a name match is found, use that address to try an FTM address match (planned improvement).
- Method flags should distinguish: `address` (direct FTM), `kvk`, `fosfaat`, or `other`.

## Historical (2015) cross-links
- `05_prepare_2015_linkages.py` aligns fosfaat categories 100/101/102 to FTM dairy categories and builds `05_fosfaat_rel_crosswalk.csv` for additional KVK/Naam hints.
