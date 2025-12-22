LLM improvement testing

- Purpose: quick experiments around stage classification accuracy and address handling for LBV/LBV+ publications.
- Key artifacts: `stage_run_results.json` (kept for transparency), supporting CSVs (`manual_stage_truth.csv`, `mismatches_latest.csv`, `results_latest.csv`) that stay git-ignored to avoid pushing bulky/PII-like data.
- Script: `stage_classifier_test.py` contains the test harness used in this folder.
- Status: exploratory; nothing automatically feeds the main pipeline. Update this README with findings if you re-run tests or change prompt/logic. 
