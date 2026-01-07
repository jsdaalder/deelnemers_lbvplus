# deelnemers_lbvplus

A public, end-to-end view of the LBV/LBV+ shutdown scheme. The repo scrapes permit notices, cleans addresses, matches them to other public datasets (MinFin payments, animal counts), and publishes simple charts so anyone can see progress.

## What's inside
- `pipelines/participants/` – pulls LBV/LBV+ publications, extracts text from PDFs, uses an LLM to identify stage and address, and groups notices into farms.
- `pipelines/matching_ftm/` – connects those farms and MinFin payments to the FTM farm registry and animal counts; builds a master table.
- `pipelines/matching_ftm/analysis/ftm/` – lightweight scripts to turn the processed tables into charts and a trimmed CSV for sharing.
- `final_results/<date>/` – the public bundle (overview PNG/PDF plus a slim CSV). CSVs stay out of git; charts can be committed.
- `pipelines/participants/experiments/` – prompt tests and scratch space (LLM prompt/run comparisons).

## How to run (high level)
1) **Participants**: run `bash pipelines/participants/scripts/run_all.sh` with either local HTML/PDF inputs (`MODE=local FILES="file1.html file2.html"`) or the SRU API (`MODE=api ...`). You need an OpenAI API key in `.env`.
2) **Matching**: follow `pipelines/matching_ftm/README.md` to link participants to FTM/MinFin.
3) **Charts & export**: from repo root run `python3 pipelines/matching_ftm/analysis/ftm/14_generate_charts.py` and then `python3 pipelines/matching_ftm/analysis/ftm/13_export_final_results.py` to write `final_results/<date>/chart_all_<date>.png` and a slim CSV.

## Outputs and sharing
- Working data (raw/processed CSVs) lives under each pipeline’s `data/` and stays git-ignored.
- The export step writes `final_results/<date>/chart_all_<date>.png` and CSVs (`farms_permits_minfin_<date>.csv` and `_agg_...`). Commit only the charts; keep the CSVs local so others can regenerate them.

## LLM validation status
- All draft and definitive notices in the current corpus have been manually checked; other stages were not.
- Current corpus: 568 unique URLs; 533 manually labeled (93.8% coverage).
- Latest-stage draft/definitive notices: 356 URLs, all manually checked. Latest-stage receipt-only: 212 URLs, of which 177 are manually labeled.
- Manual vs LLM agreement on those labeled URLs: 489/533 (94.2%). This is a rough indication for the 35 unlabeled URLs, not a guarantee.
- Historical accuracy on the manual set: 284/287 (98.96%) unique notices.

## Data sources (public)
- Permit notices: scraped from https://www.officielebekendmakingen.nl/
- MinFin voorschot dataset: https://data.overheid.nl/dataset/financile-instrumenten-2022#panel-resources
- RVO actueel overzicht: https://www.rvo.nl/onderwerpen/lbv-plus-actueel
- Fosfaatbeschikkingen: https://www.rijksoverheid.nl/documenten/publicaties/2023/09/19/tabel-gegevens-fosfaatbeschikkingen-bij-bob-woo-besluit-over-toekenning-fosfaatrechten-aan-agrarische-bedrijven
- FTM dieraantallen (Woo gecombineerde opgaven): https://www.rijksoverheid.nl/documenten/woo-besluiten/2023/05/04/besluit-op-woo-verzoek-over-de-gecombineerde-opgaven-van-alle-agrarische-ondernemingen-in-nederland
- CBS totale veestapel 2021: https://opendata.cbs.nl/#/CBS/nl/dataset/80780ned/table?dl=CD311
- Woonplaatsen (CBS): https://www.cbs.nl/nl-nl/cijfers/detail/86097NED
- RAV conversietabel diercategorieën: https://iplo.nl/regelgeving/regels-voor-activiteiten/dierenverblijven/systeembeschrijvingen-stallen/conversietabel-bijlage-rav-code/

## Data handling
- `.env`, PDFs, archives, caches, and all intermediate CSVs are ignored. Keep personal data out of commits; remove or anonymize before sharing.

## Contributing
- Open a PR with a short description of what you ran or changed. Keep scripts small and documented so others can reproduce results.
