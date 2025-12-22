# Matching FTM (Uitgekochte)

End-to-end flow to link permit-rescinded and minfin buy-out farms to the national farm registry (FTM) and animal counts.

## Folder Structure
- `data/raw/` — source CSVs (`06_deelnemers_lbv_lbvplus.csv`, `minfin_dataset.csv`, `FTM_addresses.csv`, `FTM_dieraantallen.csv`, fosfaatbeschikkingen).
- `data/processed/` — derived outputs.
- `scripts/` — numbered utilities (see below).
- `AGENTS.md` — contributor guidelines.

## Setup
- Python 3.11+ recommended.
- Optional venv: `python3 -m venv .venv && source .venv/bin/activate`
- Install extras if needed: `pip install pandas pytest`
- Legacy helpers have been removed; the active scripts are listed below.
- The participants pipeline (`pipelines/participants/scripts/run_all.sh`) syncs its step-06 output into `pipelines/matching_ftm/data/raw/06_deelnemers_lbv_lbvplus.csv`; run that first so you do not need to copy manually.

## Pipeline (aligned to the clarified matching flow)
1) **Combine FTM addresses + animal counts**  
   - `python3 scripts/01_combine_ftm_datasets.py`  
   - Output: `data/processed/01_FTM_animals_with_addresses.csv`

2) **KVK lookup for minfin companies**  
   - `python3 scripts/02_kvk_lookup_minfin.py`  
   - Output: `data/processed/03_kvk_minfin_results.csv` (bezoek/post address, kvk, actief/rechtsvorm)

3) **KVK lookup for permit notices**  
   - `python3 scripts/03_kvk_lookup_permits.py`  
   - Outputs (step 02 yields several `02_*` files):  
     - `data/processed/02_kvk_results.csv` (primary)  
     - `data/processed/02_linked_company_info.csv` (matched KVK/companies)  
     - `data/processed/02_permits_for_kvk_lookup.csv` (input list)  
     - `data/processed/02_unmatched_for_kvk_lookup.csv` (no KVK match)

4) **Overlap check (minfin vs permits by KVK)**  
   - `python3 scripts/04_kvk_overlap.py`  
   - Outputs: `data/processed/04_overlap_summary.csv`, `data/processed/04_combined_kvk_addresses.csv`

5) **Direct address match to FTM (permit + minfin)**  
   - Permits: `python3 scripts/05_match_permits_ftm.py` → `04_permit_animals_join.csv` / `04_permit_animals_summary.csv`  
   - Minfin: `python3 scripts/06_match_minfin_ftm.py` → `04_minfin_animals_join.csv` / `04_minfin_animals_summary.csv`

6) **(Optional) Fosfaat fallback for unmatched companies**  
   - `python3 scripts/08_match_permits_fosfaat_names.py` (name-based)  
   - Fosfaat/2015 prep (step 05 produces several `05_*` files):  
     - `python3 scripts/07_prepare_fosfaat_2015_linkages.py` → `05_FTM_2015_rundvee_dairy.csv`, `05_fosfaat_animals_2015.csv`, `05_fosfaat_rel_crosswalk.csv`

7) **Master table + merge minfin**  
   - `python3 scripts/09_build_master_table.py`  
   - `python3 scripts/09_merge_minfin_into_master.py`  *(if present; ensure numbering if you keep a merge step)*  
   - Output: `data/processed/master_permits.csv` (includes all permits + minfin, animal counts when matched)

8) **Reporting (root/analysis/ftm)**  
   - `python3 analysis/ftm/14_generate_charts.py` (charts to `analysis/ftm/charts/<YYYY_MM_DD>/`)  
   - (Legacy) `python3 analysis/ftm/10_generate_report.py` kept for reference

9) **Export final deliverables (root/analysis/ftm)**  
   - `python3 analysis/ftm/13_export_final_results.py`  
   - Copies chart overviews (PNG/PDF if present) and writes a slimmed `farms_permits_minfin_<date>.csv` (keeps key columns only) to `final_results/<YYYY_MM_DD>/` with the date tag in filenames.

## Notes on matching logic
- Primary match is direct address-based (`normalized_address_key`) against `FTM_dieraantallen`.
- KVK is used to enrich permits and minfin with official names/addresses/kvk numbers and to measure overlap.
- Fosfaat name matches are a fallback for unmatched companies; if a name match is found, use that address to try an FTM address match (planned improvement).
- Method flags should distinguish: `address` (direct FTM), `kvk`, `fosfaat`, or `other`.

## Historical (2015) cross-links
- `05_prepare_2015_linkages.py` aligns fosfaat categories 100/101/102 to FTM dairy categories and builds `05_fosfaat_rel_crosswalk.csv` for additional KVK/Naam hints.
