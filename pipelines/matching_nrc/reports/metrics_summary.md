# Metrics summary (2025-12-10)

## Coverage
- Depositie table: 26 419 rows (non-recreational locations)
- Piekbelasters: 3 025 rows (>2 500 moles N)
- Merged peak polluters: 3 026 rows / 3 025 unique cluster_id
- Known permit-notice participants (iv_farm_id present): 156 rows / 155 unique cluster_id

## Subsidy estimates (EUR)
- All peak polluters: total €7 113 711 382.59; mean €2 350 862.98 per location
- Known participants: total €480 653 802.10; mean €3 081 114.12 per location

## Nitrogen deposition (moles, lbv_plus_tot_dep)
- All peak polluters: 26 594 947.92 moles
- Known participants: 1 301 371.22 moles
- Share of peak-polluter deposition addressed: 4.89 %

## Animals (AANTAL_DIERRECHTEN as proxy)
- All peak polluters: 38 321 738.90 rights
- Known participants: 1 797 194.88 rights
- Share of peak-polluter animal rights addressed: 4.69 %
- Mean rights per location: all 12 668.34; participants 11 520.48

## Cost efficiency
- Median cost per mole N (participants): €426.94 (≈ €30.50 per gram N)
- Median cost per mole N (all peak polluters): €302.41 (≈ €21.60 per gram N)

## Process stage (known participants)
- receipt_of_application: 67 (42.95 %)
- definitive_decision: 46 (29.49 %)
- draft_decision: 43 (27.56 %)

## Province distribution (known participants)
- Gelderland: 57 (36.54 %)
- Limburg: 51 (32.69 %)
- Noord-Brabant: 22 (14.10 %)
- Drenthe: 11 (7.05 %)
- Overijssel: 9 (5.77 %)
- FryslÃƒÂ¢n: 3 (1.92 %)
- Flevoland: 2 (1.28 %)
- Utrecht: 1 (0.64 %)

## MinFin payment recipients (ontvangers)
- Total amount: €153 732.00k
- Unique clusters mapped: 147
- Rows: 179

## Notes
- Currency assumed EUR; `Bedrag (x1000)` already in thousands.
- Province name may appear as the Frisian spelling `Fryslân`.
- `DIERRECHTEN_PER_STAL` in `merged` is comma-separated; totals use `AANTAL_DIERRECHTEN` from `dierrechten`.
- `merged` contains one duplicate `cluster_id`; de-duplicate if needed.