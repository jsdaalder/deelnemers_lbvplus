Data layout and retention

- Current canonical outputs live directly in `data/`. As of 2025-11-25 they come from run `runs/2025-11-19-151008` (git `0aa90980c03c86977a4c9e6b5cf9e789ad30688e`).
- Older outputs move to `data/archive/<date>/`. The previously blessed set is under `data/archive/2025-11-25/`.
- Per-run artifacts (full drops, deltas, logs) live in `data/runs/<timestamp>/`. Keep runs intact; copy only the files you want to bless into `data/`.
- Legacy `06_vergunningen_lbv_lbvplus.csv` exists only in the archive; step 06 now reads `05_lbv_enriched_addresses.csv` directly.
- `data/archive/` and `data/runs/` are git-ignored to avoid pushing bulky historical outputs; keep any notes or hashes in small README files instead.
