Chart data summary
==================

This folder now contains `chart_data.json`, a single file with every number used to generate the charts. You can hand this file to colleagues so they can build their own visualisations without running the pipeline.

Contents (top-level keys)
- `meta`: timestamp, data year, and source master CSV path.
- `chart1`..`chart9`: inputs for each national chart (totals, link-method counts, animal totals, stages, averages, etc.).
- `rvo_comparison`: RVO participant totals per province, plus definitive/known counts from our dataset.
- `regions`: per-region data (e.g. Gelderland) including buyout totals and animals-by-stage.

Schema highlights
- Counts are stored as integers; percentages as floats.
- Series-like data are stored as lists of dicts with a `category` or `stage` key.
- Buyout tables include attributes: `buyout` (category-level totals), `buyout_farms` (number of linked farms contributing).

Usage
- Charts are (re)built by `14_generate_charts.py` by first writing this JSON, then reading it back to render PNGs. Downstream consumers can read the same JSON directly to drive external charts.
