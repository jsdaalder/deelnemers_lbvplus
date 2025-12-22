Data layout and retention

- Current canonical outputs live directly in `data/`. Copy the latest blessed run outputs here after verifying.
- Older outputs can move to `data/archive/<date>/` for provenance if you need to keep them; delete stale archives once the latest run is blessed.
- Per-run artifacts (full drops, deltas, logs) live in `data/runs/<timestamp>/`. Keep runs only as long as needed for provenance; after blessing outputs, delete stale run folders to save space.
- Legacy `06_vergunningen_lbv_lbvplus.csv` exists only in the archive; step 06 now reads `05_lbv_enriched_addresses.csv` directly.
- `data/archive/` and `data/runs/` are git-ignored to avoid pushing bulky historical outputs; keep any notes or hashes in small README files instead.
