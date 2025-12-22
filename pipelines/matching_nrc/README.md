# Matching NRC (WOO barn-level)

This pipeline mirrors the FTM matching flow but uses the NRC WOO barn-level dataset (one row per barn with company ids) instead of the FTM farm-level registry. The NRC animal counts are linked to LBV/LBV+ permit/minfin data to size the buyout scheme.

## Folder layout
- `data/raw/` — source CSV/JSON from NRC WOO request and other reference tables.
- `data/processed/` — main workbook `peak_polluters_workbook.xlsx` (renamed from `10122025 output.xlsx`).
- `scripts/` — utilities to summarise and visualise (`metrics_summary.py`, `plot_venn.py`).
- `reports/` — generated Markdown summaries.
- `figures/` — generated charts (e.g., Venn overlaps).
- `.mplconfig`, `.cache/` — local Matplotlib/fontconfig caches for headless plotting.

## Core identifiers
- `rvo_id`: Unique RVO farm/company id.
- `cluster_id`: Location-level id of the form `<rvo_id>-<n>` (one company can own multiple locations).
- `iv_farm_id`: Scraper-generated id for notices on officielebekendmakingen.nl about permit withdrawals/applications.

## Sheet guide
- `depositie` (26,419 rows): All non-recreational farm locations with modeled deposition and LBV/LBV-plus metrics. Includes `lbv_plus_tot_dep`, `lbv_plus_num_hex`, `lbv_plus_avg`, and `lbv_plus_rank`.
- `piekbelasters` (3,025 rows): All locations depositing >2,500 moles N on overloaded hexagons. Core fields mirror `depositie` plus ranking.
- `merged` (3,026 rows, 3,025 unique `cluster_id`): `piekbelasters` enriched with compensation estimates, buildings, dierrechten, and scraped permit withdrawal notices. Columns include:
  - Compensation: `COMPENSATIE_DIERRECHTEN`, `COMPENSATIE_STAL`, `SLOOPKOSTEN`, `subsidie_lbv_plus`.
  - Permits: `iv_farm_id`, `iv_stage_latest_llm` (receipt/draft/definitive), `iv_Datum_latest`, `iv_Instantie_latest`, `iv_URL_BEKENDMAKING`.
  - Cross-links: `Kreeg geld`, `Ontvanger`, `Bedrag (minfin)`, `Ingetrokken vergunning`, `polygonen`, `x`, `y`.
- `dierrechten` (7,733 rows, 3,025 clusters): Per `cluster_id` breakdown of animal categories and rights (`AANTAL_DIERRECHTEN`, `DIERRECHTEN_REKENFACTOR`), with price factors and compensation.
- `productieverlies`: Derived compensation for stables (`COMPENSATIE_STAL`, `SLOOPKOSTEN`) by animal category and building surface.
- `panden`: Building footprints per cluster (`n_panden`, `oppervlakte_totaal`, `polygonen`).
- `woo`: Raw WOO request data (animal counts, RAV codes, derived dierrechten and compensation).
- `gestopt`: Peak polluters with flags for demolition/permit withdrawal and subsidy linkage.
- `ingetrokkken_vergunningen`: Broader set of clusters with permit withdrawal data and URL pointers to notices.
- `ontvangers` (179 rows, 147 unique clusters): Companies that already received LBV-plus payments (MinFin), with `KVKnummer`, amount (`Bedrag (x1000)`), and mapped `Cluster_id`.
- `productieverlies`, `Sheet11`: Supporting calculations similar to `woo`; retained for provenance.

## Computing metrics
From `pipelines/matching_nrc/`, run:
- `python3 scripts/metrics_summary.py` → updates `reports/metrics_summary.md`.
- `python3 scripts/plot_venn.py` → refreshes `figures/venn_permits_minfin.png`.

## Goals and status
- Quantify animal reduction if all known participants proceed: **Done** (≈1.80M of 38.32M peak-polluter animal rights, ~4.7%).
- Compare to total animals across peak polluters: **Done** (see above; broader 26,419 non-recreational locations not covered by `dierrechten` in this workbook).
- Estimate total/average cost for known participants: **Done** (€481M total; €3.08M/location).
- Cost per mole (and per gram) of nitrogen removed: **Done** (median ~€427 per mole ≈€30/g for known participants; overall median ~€302 per mole).
- Map where participants are and stage of process: **Done** (province counts and stage distribution listed).
- Identify overlap with payment recipients: **Partially done** (flags `Kreeg geld`/`Ontvanger` available; reconciliation of multi-site companies vs single `cluster_id` pending).
- Additional future goals (suggested):
  - Expand animal-rights coverage to all 26,419 clusters (not just peak polluters).
  - Tighten `cluster_id` vs `rvo_id`/`KVKnummer` mapping for companies with multiple locations.
  - Track timeline (application vs decision dates) and re-run aggregates as new notices appear.

## Notes and caveats
- Currency values are assumed euros; `Bedrag (x1000)` in `ontvangers` is already in thousands.
- `DIERRECHTEN_PER_STAL` in `merged` is a comma-separated list; for animal totals use `AANTAL_DIERRECHTEN` from `dierrechten`.
- Province name encoding includes the Frisian spelling `Fryslân` (`FryslÃ¢n` in raw data).
- `merged` has one duplicate `cluster_id` (3,026 rows vs 3,025 unique); downstream code should de-duplicate on `cluster_id`.
