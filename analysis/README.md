# Analysis

Reporting scripts that consume processed outputs from the matching pipelines.

- `ftm/` – charts and exports based on `pipelines/matching_ftm/data/processed/` (master_participants, master_permits). Charts are written to dated folders under `analysis/ftm/charts/<YYYY_MM_DD>/`. Final exports (chart overview + slim CSV) land in `final_results/<YYYY_MM_DD>/`.
- `nrc/` – reserved for future cross-analysis of the NRC barn-level matches alongside FTM results.
