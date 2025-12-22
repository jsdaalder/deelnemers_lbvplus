# deelnemers_lbvplus

A public, end-to-end view of the LBV/LBV+ shutdown scheme. The repo scrapes permit notices, cleans addresses, matches them to other public datasets (MinFin payments, animal counts), and publishes simple charts so anyone can see progress.

## What's inside
- `pipelines/participants/` – pulls LBV/LBV+ publications, extracts text from PDFs, uses an LLM to identify stage and address, and groups notices into farms.
- `pipelines/matching_ftm/` – connects those farms and MinFin payments to the FTM farm registry and animal counts; builds a master table.
- `pipelines/matching_nrc/` – alternative matching against NRC’s barn-level WOO dataset.
- `pipelines/matching_ftm/analysis/ftm/` – lightweight scripts to turn the processed tables into charts and a trimmed CSV for sharing.
- `final_results/<date>/` – the public bundle (overview PNG/PDF plus a slim CSV). CSVs stay out of git; charts can be committed.
- `experiments/` – prompt tests and scratch space.

## How to run (high level)
1) **Participants**: run `bash pipelines/participants/scripts/run_all.sh` with either local HTML/PDF inputs (`MODE=local FILES="file1.html file2.html"`) or the SRU API (`MODE=api ...`). You need an OpenAI API key in `.env`.
2) **Matching**: follow `pipelines/matching_ftm/README.md` to link participants to FTM/MinFin, or `pipelines/matching_nrc/README.md` for the NRC flow.
3) **Charts & export**: from repo root run `python3 pipelines/matching_ftm/analysis/ftm/14_generate_charts.py` and then `python3 pipelines/matching_ftm/analysis/ftm/13_export_final_results.py` to write `final_results/<date>/chart_all_<date>.png` and a slim CSV.

## Outputs and sharing
- Working data (raw/processed CSVs) lives under each pipeline’s `data/` and stays git-ignored.
- The export step writes `final_results/<date>/chart_all_<date>.png` and CSVs (`farms_permits_minfin_<date>.csv` and `_agg_...`). Commit only the charts; keep the CSVs local so others can regenerate them.

## Data handling
- `.env`, PDFs, archives, caches, and all intermediate CSVs are ignored. Keep personal data out of commits; remove or anonymize before sharing.

## Contributing
- Open a PR with a short description of what you ran or changed. Keep scripts small and documented so others can reproduce results.
